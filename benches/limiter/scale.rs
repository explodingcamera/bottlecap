//! Unpaced replay of existing-client checks, not end-to-end HTTP throughput.
//! Ordinary clients are shuffled and share a hot client every fifth request.
//! Insertion happens before timing. The hot client's burst
//! starts drained, but refills during the run, so report actual admissions.

use std::net::{IpAddr, Ipv4Addr};
use std::num::NonZeroU32;
use std::time::Duration;

use bottlecap::Quota;
use criterion::{BenchmarkId, Criterion, Throughput};

use super::support::{hasher, keyed_limiter, quota, run_workers};

fn ip_key(key: u64) -> IpAddr {
    if key.is_multiple_of(2) {
        IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + key as u32))
    } else {
        IpAddr::V6(((0x2001_0db8u128 << 96) | (u128::from(key) << 64)).into())
    }
}

/// Replay mixed IP traffic across increasing working sets, with setup excluded.
pub(super) fn benchmarks(c: &mut Criterion) {
    let interval = Duration::from_micros(4);
    let burst = 250_000;
    for workers in [1, 8] {
        let mut group = c.benchmark_group(if workers == 1 {
            "ip_count_single_thread"
        } else {
            "ip_count"
        });
        for exponent in 10..=21 {
            let keys = 1u64 << exponent;
            let addresses: Vec<_> = (0..keys).map(ip_key).collect();
            // Visit every client, inserting a shared hot client every fifth request.
            // Key zero is only the hot client, not part of the ordinary traffic.
            let mut requests: Vec<_> = (1..keys).collect();
            let mut random = 1u64;
            for index in (1..requests.len()).rev() {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                requests.swap(index, (random >> 32) as usize % (index + 1));
            }
            let request_ids: Vec<_> = requests
                .chunks(requests.len().div_ceil(workers))
                .map(|trace| {
                    let mut requests = Vec::with_capacity(trace.len() * 5 / 4);
                    for (index, &key) in trace.iter().enumerate() {
                        requests.push(key);
                        if index % 4 == 3 {
                            requests.push(0);
                        }
                    }
                    requests
                })
                .collect();
            let traces: Vec<Vec<_>> = request_ids
                .iter()
                .map(|trace| trace.iter().map(|&key| addresses[key as usize]).collect())
                .collect();
            let checks = request_ids.iter().map(|trace| trace.len() as u64).sum();
            group.throughput(Throughput::Elements(checks));
            {
                let limiter = keyed_limiter(Quota::per(interval, 1).with_burst(burst));
                let now = bottlecap::Instant::now();
                for key in addresses.iter().skip(1) {
                    assert!(limiter.check_at(key, now).is_ok());
                }
                assert!(limiter.check_n(&addresses[0], burst).is_ok());
                run_workers(
                    &mut group,
                    BenchmarkId::new("bottlecap", keys),
                    &traces,
                    |key| limiter.check(&key).is_ok(),
                );
            }
            {
                let limiter =
                    governor::RateLimiter::dashmap_with_hasher(quota(interval, burst), hasher());
                for key in addresses.iter().skip(1) {
                    assert!(limiter.check_key(key).is_ok());
                }
                assert!(
                    limiter
                        .check_key_n(&addresses[0], NonZeroU32::new(burst).unwrap())
                        .unwrap()
                        .is_ok()
                );
                run_workers(
                    &mut group,
                    BenchmarkId::new("governor", keys),
                    &traces,
                    |key| limiter.check_key(&key).is_ok(),
                );
            }
        }
        group.finish();
    }
}
