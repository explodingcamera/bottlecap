use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use bottlecap::{Instant, KeyedLimiter, Quota, Rejection, Rejection::InsufficientCapacity};

#[test]
fn checks_and_peeks_respect_key_capacity() {
    let limiter = KeyedLimiter::new(Quota::per_hour(1).with_burst(2)).with_max_keys(1);
    let now = Instant::now();
    assert_eq!(limiter.check_n_at(&0, 2, now), Ok(()));
    assert_eq!(limiter.peek_at(&1, now), Err(Duration::from_millis(1)));
    assert_eq!(
        limiter.check_n_at(&1, 1, now),
        Err(Rejection::RetryAfter(Duration::from_millis(1)))
    );
    assert_eq!(limiter.len(), 1);
    let later = now + Duration::from_secs(7200);
    assert_eq!(limiter.peek_n_at(&1, 2, later), Ok(()));
    assert!(limiter.is_empty());
    assert_eq!(limiter.check_n_at(&1, 2, later), Ok(()));
    assert_eq!(limiter.len(), 1);
}

#[test]
fn waits_recheck_after_sleep() {
    let limiter = KeyedLimiter::new(Quota::per_hour(1).with_burst(2));
    assert_eq!(limiter.check_n(&0, 2), Ok(()));
    let mut sleeps = 0;
    let sleep = |delay: Duration| {
        assert!(!delay.is_zero());
        sleeps += 1;
        assert!(limiter.reset(&0));
        std::future::ready(())
    };
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        std::pin::pin!(limiter.wait_n(&0, 2, sleep))
            .as_mut()
            .poll(&mut context),
        Poll::Ready(Ok(()))
    );
    assert_eq!(sleeps, 1);
    assert!(limiter.check(&0).is_err());
}

#[test]
fn reservation_preserves_keys_and_allowance() {
    for expected in [0, 16_384, 200_000] {
        let mut limiter = KeyedLimiter::new(Quota::per_hour(1));
        let now = Instant::now();
        for key in 0..100 {
            assert_eq!(limiter.check_at(&key, now), Ok(()));
        }
        for capacity in [expected, expected * 2] {
            limiter = limiter.with_capacity(capacity);
            assert_eq!(limiter.len(), 100);
            for key in 0..100 {
                assert!(limiter.check_at(&key, now).is_err());
            }
        }
        assert_eq!(limiter.check_at(&100, now), Ok(()));
    }
}

#[test]
fn independent_allowances() {
    let shared =
        KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(2)).with_max_keys(2);
    let now = Instant::now();
    for (key, expected) in [
        ("a", Ok(())),
        ("a", Ok(())),
        ("a", Err(Duration::from_nanos(10))),
        ("b", Ok(())),
        ("b", Ok(())),
        ("b", Err(Duration::from_nanos(10))),
        // Capacity retries wait until the next cleanup pass, without reserving a slot.
        ("c", Err(Duration::from_nanos(10))),
    ] {
        assert_eq!(shared.check_at(&key, now), expected);
    }
}

#[test]
fn reset_restores_allowance_and_frees_capacity() {
    let limiter: KeyedLimiter<&str> =
        KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(2)).with_max_keys(1);
    let now = Instant::now();
    for _ in 0..2 {
        assert!(limiter.is_empty());
        assert_eq!(limiter.len(), 0);
        assert!(!limiter.reset(&"missing"));
        assert_eq!(limiter.check_n_at(&"a", 2, now), Ok(()));
        assert_eq!(limiter.check_at(&"a", now), Err(Duration::from_nanos(10)));
        assert!(!limiter.reset(&"missing"));
        assert_eq!(limiter.len(), 1);
        assert!(limiter.reset(&"a"));
    }
    assert!(limiter.check_at(&"b", now).is_ok());
    assert_eq!(limiter.prune_at(now + Duration::from_nanos(10)), 1);
    assert!(limiter.is_empty());
}

#[test]
fn setting_capacity_preserves_existing_allowance() {
    let shared = KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(2));
    let start = Instant::now();
    for key in [0, 0, 1] {
        assert!(shared.check_at(&key, start).is_ok());
    }
    let shared = shared.with_max_keys(1);
    for (elapsed, key, expected) in [
        (0, 0, Err(Duration::from_nanos(10))),
        (0, 1, Ok(())),
        (10, 2, Err(Duration::from_nanos(10))),
        (20, 2, Ok(())),
    ] {
        let now = start + Duration::from_nanos(elapsed);
        assert_eq!(shared.check_at(&key, now), expected);
    }
}

#[test]
fn reclamation_preserves_allowance() {
    let shared =
        KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(2)).with_max_keys(1);
    let start = Instant::now();
    for (elapsed, key, expected) in [
        (0, "a", Ok(())),
        (0, "a", Ok(())),
        (10, "b", Err(Duration::from_nanos(10))),
        (10, "a", Ok(())),
        (20, "b", Err(Duration::from_nanos(10))),
        (30, "b", Ok(())),
        (30, "a", Err(Duration::from_nanos(10))),
        (30, "b", Ok(())),
        (30, "b", Err(Duration::from_nanos(10))),
    ] {
        let now = start + Duration::from_nanos(elapsed);
        assert_eq!(shared.check_at(&key, now), expected);
    }
}

#[test]
fn old_timestamps_cannot_recreate_reclaimed_state() {
    let shared = KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1)).with_max_keys(2);
    let start = Instant::now();
    for (elapsed, key, expected) in [
        (0, 0, Ok(())),
        (0, 1, Ok(())),
        (10, 2, Ok(())),
        (9, 0, Err(Duration::from_nanos(1))),
        (10, 0, Ok(())),
        (10, 0, Err(Duration::from_nanos(10))),
    ] {
        let now = start + Duration::from_nanos(elapsed);
        assert_eq!(shared.check_at(&key, now), expected);
    }
}

#[test]
fn concurrent_insertion() {
    for (same_key, capacity, burst, expected) in [(true, 1, 100, 100), (false, 4, 1, 4)] {
        let limiter =
            KeyedLimiter::new(Quota::per_second(1).with_burst(burst)).with_max_keys(capacity);
        let now = Instant::now();
        let start = Barrier::new(16);
        let admitted = thread::scope(|scope| {
            let workers: Vec<_> = (0..16)
                .map(|worker| {
                    let limiter = &limiter;
                    let start = &start;
                    scope.spawn(move || {
                        let key = if same_key { 0 } else { worker };
                        start.wait();
                        (0..100)
                            .filter(|_| limiter.check_at(&key, now).is_ok())
                            .count()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(admitted, expected);
    }
}

#[test]
fn churn_keeps_key_storage_bounded() {
    // Arc counts track retained keys without exposing the map in the public API.
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(u64, Arc<()>);

    let capacity = 32;
    let shared = KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(2))
        .with_max_keys(capacity);
    let live = Arc::new(());
    let start = Instant::now();
    for wave in 0..128 {
        let now = start + Duration::from_nanos(wave * 20);
        for index in 0..64 {
            let key = Key(wave * 64 + index, Arc::clone(&live));
            assert_eq!(shared.check_at(&key, now).is_ok(), index < capacity as u64);
            drop(key);
            assert!(Arc::strong_count(&live) <= 1 + capacity);
        }
    }
    drop(shared);
    assert_eq!(Arc::strong_count(&live), 1);
}

#[test]
fn concurrent_reclamation_respects_capacity() {
    let limiter = KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1)).with_max_keys(8);
    let epoch = Instant::now();
    let phase = Barrier::new(17);
    let admitted = AtomicUsize::new(0);
    let mut counts = Vec::new();
    thread::scope(|scope| {
        for worker in 0..16 {
            let limiter = &limiter;
            let phase = &phase;
            let admitted = &admitted;
            scope.spawn(move || {
                for wave in 0..128 {
                    let now = epoch + Duration::from_nanos(wave * 10);
                    phase.wait();
                    admitted.fetch_add(
                        usize::from(limiter.check_at(&(wave * 16 + worker), now).is_ok()),
                        Relaxed,
                    );
                    phase.wait();
                }
            });
        }
        for _ in 0..128 {
            admitted.store(0, Relaxed);
            phase.wait();
            phase.wait();
            counts.push(admitted.load(Relaxed));
        }
    });
    for (wave, count) in counts.into_iter().enumerate() {
        assert_eq!(count, 8, "wave {wave}");
    }
}

#[test]
fn pruning_during_checks_preserves_allowance() {
    let limiter =
        KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(100)).with_max_keys(1);
    let start = Instant::now();
    assert_eq!(limiter.check_n_at(&0, 100, start), Ok(()));
    for wave in 1..=16 {
        let now = start + Duration::from_nanos(wave * 1000);
        let barrier = Barrier::new(9);
        let admitted = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    barrier.wait();
                    let count = (0..100)
                        .filter(|_| limiter.check_n_at(&0, 3, now).is_ok())
                        .count();
                    admitted.fetch_add(count, Relaxed);
                });
            }
            barrier.wait();
            for _ in 0..100 {
                limiter.prune_at(now);
            }
        });
        assert_eq!(admitted.load(Relaxed), 33);
        assert_eq!(limiter.remaining_at(&0, now), 1);
        assert_eq!(limiter.len(), 1);
    }
}

#[test]
fn unused_allowance_does_not_store_keys() {
    for quota in [Quota::per_second(1), Quota::per_second(0)] {
        let limiter = KeyedLimiter::new(quota).with_max_keys(1);
        assert_eq!(limiter.check_n(&0, 0), Ok(()));
        assert_eq!(limiter.check_n(&0, 2), Err(InsufficientCapacity));
        assert_eq!(limiter.peek_n(&0, 0), Ok(()));
        assert_eq!(limiter.peek_n(&0, 2), Err(InsufficientCapacity));
        let _ = limiter.peek(&0);
        assert!(limiter.is_empty());
    }
}

#[test]
fn custom_clock_drives_pruning() {
    let now = Cell::new(Instant::now());
    let limiter = KeyedLimiter::new(Quota::per_second(1).with_burst(2)).with_clock(|| now.get());
    assert_eq!(limiter.check_n(&0, 2), Ok(()));
    assert_eq!(limiter.prune(), 0);
    now.set(now.get() + Duration::from_secs(2));
    assert_eq!(limiter.peek_n(&0, 2), Ok(()));
    assert_eq!(limiter.prune(), 1);
    assert_eq!(limiter.check_n(&0, 2), Ok(()));
}
