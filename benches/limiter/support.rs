use std::hash::Hash;
use std::hint::black_box;
use std::num::NonZeroU32;
use std::sync::{Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use rapidhash::fast::SeedableState;

/// Keep the rapidhash seed fixed across libraries, workloads, and comparison runs.
pub(super) fn hasher() -> SeedableState<'static> {
    SeedableState::fixed()
}

/// Construct a keyed limiter with the shared benchmark hasher.
pub(super) fn keyed_limiter<K: Eq + Hash>(
    quota: bottlecap::Quota,
) -> bottlecap::KeyedLimiter<K, SeedableState<'static>> {
    bottlecap::KeyedLimiter::with_hasher(quota, hasher())
}

/// Construct Governor's equivalent initially full quota.
pub(super) fn quota(interval: Duration, burst: u32) -> governor::Quota {
    governor::Quota::with_period(interval)
        .unwrap()
        .allow_burst(NonZeroU32::new(burst).unwrap())
}

fn report_admissions(attempted: u64, admitted: u64) {
    if attempted > 0 {
        eprintln!(
            "  admissions (including warmup): {admitted}/{attempted} ({:.4}%)",
            admitted as f64 / attempted as f64 * 100.0
        );
    }
}

/// Reuse workers across samples, synchronizing starts and reporting admissions.
pub(super) fn run_workers<K: Copy + Sync>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    id: BenchmarkId,
    traces: &[Vec<K>],
    check: impl Fn(K) -> bool + Sync,
) {
    let workers = traces.len();
    let checks_per_iteration = traces.iter().map(|trace| trace.len() as u64).sum::<u64>();
    let start = Barrier::new(workers);
    let mut attempted = 0;
    let mut admitted = 0;
    thread::scope(|scope| {
        let (done_tx, done_rx) = mpsc::channel();
        let jobs: Vec<_> = traces
            .iter()
            .map(|trace| {
                let (tx, rx) = mpsc::channel::<u64>();
                let done = done_tx.clone();
                let check = &check;
                let start = &start;
                scope.spawn(move || {
                    while let Ok(iterations) = rx.recv() {
                        start.wait();
                        let mut allowed = 0u64;
                        for _ in 0..iterations {
                            for &key in trace {
                                allowed += u64::from(check(black_box(key)));
                            }
                        }
                        done.send(allowed).unwrap();
                    }
                });
                tx
            })
            .collect();

        group.bench_function(id, |b| {
            b.iter_custom(|iterations| {
                let start = Instant::now();
                for job in &jobs {
                    job.send(iterations).unwrap();
                }
                let allowed = (0..workers).map(|_| done_rx.recv().unwrap()).sum::<u64>();
                black_box(allowed);
                let elapsed = start.elapsed();
                attempted += iterations * checks_per_iteration;
                admitted += allowed;
                elapsed
            })
        });
    });
    report_admissions(attempted, admitted);
}
