use std::cell::Cell;
use std::future::{Future, pending, ready};
use std::sync::Barrier;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use bottlecap::Rejection::{InsufficientCapacity, RetryAfter};
use bottlecap::{Instant, KeyedLimiter, Limiter, Quota, Rejection};

fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn timestamp_overflow_panics() {
    let interval = Duration::from_nanos(u64::MAX / 2 + 1);
    let quota = Quota::per(interval, 1).with_burst(2);
    let limiter = Limiter::new(quota);
    let keyed = KeyedLimiter::new(quota);
    let now = Instant::now();
    assert!(std::panic::catch_unwind(|| limiter.check_at(now + interval)).is_err());
    assert!(std::panic::catch_unwind(|| keyed.check_at(&0, now + interval)).is_err());
    let keyed = KeyedLimiter::new(quota);
    assert!(std::panic::catch_unwind(|| limiter.check_n_at(2, now)).is_err());
    assert!(std::panic::catch_unwind(|| keyed.check_n_at(&0, 2, now)).is_err());
    assert!(keyed.is_empty());
}

#[test]
fn concurrent_burst_and_refill() {
    for (requests, burst, refill) in [(1, 100, 2), (3, 33, 1)] {
        let quota = Quota::per(Duration::from_nanos(10), 1).with_burst(100);
        let limiter = Limiter::new(quota);
        let keyed = KeyedLimiter::new(quota);
        let epoch = Instant::now();
        for (elapsed, expected) in [(0, burst), (20, refill)] {
            let now = epoch + Duration::from_nanos(elapsed);
            let start = Barrier::new(16);
            let admitted = thread::scope(|scope| {
                let workers: Vec<_> = (0..16)
                    .map(|_| {
                        scope.spawn(|| {
                            start.wait();
                            let mut counts = [0, 0];
                            for _ in 0..100 {
                                let results = if requests == 1 {
                                    [
                                        limiter.check_at(now).is_ok(),
                                        keyed.check_at(&0, now).is_ok(),
                                    ]
                                } else {
                                    [
                                        limiter.check_n_at(requests, now).is_ok(),
                                        keyed.check_n_at(&0, requests, now).is_ok(),
                                    ]
                                };
                                for (count, admitted) in counts.iter_mut().zip(results) {
                                    *count += usize::from(admitted);
                                }
                            }
                            counts
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .fold([0, 0], |total, count| {
                        [total[0] + count[0], total[1] + count[1]]
                    })
            });
            assert_eq!(admitted, [expected, expected]);
            for remaining in [limiter.remaining_at(now), keyed.remaining_at(&0, now)] {
                assert_eq!(remaining, if elapsed == 0 { 100 % requests } else { 0 });
            }
        }
    }
}

#[test]
fn retry_delay_tracks_refill() {
    let quota = Quota::per(Duration::from_nanos(10), 1).with_burst(2);
    let limiter = Limiter::new(quota);
    let keyed = KeyedLimiter::new(quota);
    let start = Instant::now();
    for (elapsed, expected) in [
        (0, Ok(())),
        (0, Ok(())),
        (0, Err(10)),
        (9, Err(1)),
        (10, Ok(())),
        (10, Err(10)),
        (40, Ok(())),
        (40, Ok(())),
        (40, Err(10)),
    ] {
        let now = start + Duration::from_nanos(elapsed);
        for result in [limiter.check_at(now), keyed.check_at(&0, now)] {
            assert_eq!(result, expected.map_err(Duration::from_nanos));
        }
    }
}

#[test]
fn checks_match_available_credit() {
    for interval in [1, 7, 1_000_000] {
        for burst in [1, 2, 20] {
            let quota = Quota::per(Duration::from_nanos(interval), 1).with_burst(burst);
            let limiter = Limiter::new(quota);
            let keyed = KeyedLimiter::new(quota);
            let start = Instant::now();
            let capacity = interval * u64::from(burst);
            let mut credit = capacity;
            let mut elapsed = 0;
            let mut random = 1u64;
            for _ in 0..4096 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let delta = (random >> 32) % (capacity + 1);
                let requests = (random % u64::from(burst + 2)) as u32;
                elapsed += delta;
                credit = capacity.min(credit + delta);
                let cost = interval * u64::from(requests);
                let expected = if requests > burst {
                    Err(InsufficientCapacity)
                } else if cost > credit {
                    Err(RetryAfter(Duration::from_nanos(cost - credit)))
                } else {
                    credit -= cost;
                    Ok(())
                };
                let now = start + Duration::from_nanos(elapsed);
                let results = if requests == 1 {
                    [
                        limiter.check_at(now).map_err(Rejection::from),
                        keyed.check_at(&0, now).map_err(Rejection::from),
                    ]
                } else {
                    [
                        limiter.check_n_at(requests, now),
                        keyed.check_n_at(&0, requests, now),
                    ]
                };
                for result in results {
                    assert_eq!(result, expected);
                }
                for remaining in [limiter.remaining_at(now), keyed.remaining_at(&0, now)] {
                    assert_eq!(remaining, (credit / interval) as u32);
                }
            }
        }
    }
}

#[test]
fn rejected_batches_do_not_consume_allowance() {
    let quota = Quota::per_hour(1).with_burst(3);
    let limiter = Limiter::new(quota);
    let keyed = KeyedLimiter::new(quota);
    let now = Instant::now();
    for (requests, expected) in [
        (4, Err(InsufficientCapacity)),
        (0, Ok(())),
        (2, Ok(())),
        (2, Err(RetryAfter(Duration::from_secs(3600)))),
        (1, Ok(())),
    ] {
        for result in [
            limiter.check_n_at(requests, now),
            keyed.check_n_at(&0, requests, now),
        ] {
            assert_eq!(result, expected);
        }
    }
}

#[test]
fn peeks_leave_allowance_unchanged() {
    let quota = Quota::per(Duration::from_nanos(10), 1).with_burst(3);
    let limiter = Limiter::new(quota);
    let keyed = KeyedLimiter::new(quota);
    let now = Instant::now();
    for remaining in (0..=3).rev() {
        for requests in 0..=4 {
            let expected = if requests > 3 {
                Err(InsufficientCapacity)
            } else if requests <= remaining {
                Ok(())
            } else {
                Err(RetryAfter(Duration::from_nanos(
                    u64::from(requests - remaining) * 10,
                )))
            };
            for _ in 0..10 {
                for result in [
                    limiter.peek_n_at(requests, now),
                    keyed.peek_n_at(&0, requests, now),
                ] {
                    assert_eq!(result, expected);
                }
                if requests == 1 {
                    for result in [limiter.peek_at(now), keyed.peek_at(&0, now)] {
                        assert_eq!(result.map_err(Rejection::from), expected);
                    }
                }
            }
        }
        if remaining == 3 {
            assert!(keyed.is_empty());
        }
        for actual in [limiter.remaining_at(now), keyed.remaining_at(&0, now)] {
            assert_eq!(actual, remaining);
        }
        if remaining > 0 {
            assert_eq!(limiter.check_at(now), Ok(()));
            assert_eq!(keyed.check_at(&0, now), Ok(()));
        }
    }
}

#[test]
fn custom_clocks_drive_waiting() {
    let now = Cell::new(Instant::now());
    let quota = Quota::per_second(1).with_burst(2);
    let limiter = Limiter::new(quota);
    let keyed = KeyedLimiter::new(quota);
    assert_eq!(limiter.check_n_at(2, now.get()), Ok(()));
    assert_eq!(keyed.check_n_at(&0, 2, now.get()), Ok(()));
    let limiter = limiter.with_clock(|| now.get());
    let keyed = keyed.with_clock(|| now.get());
    let sleeps = Cell::new(0);
    let sleep = |delay| {
        assert_eq!(delay, Duration::from_secs(2));
        sleeps.set(sleeps.get() + 1);
        now.set(now.get() + delay);
        ready(())
    };
    assert_eq!(poll_once(limiter.wait_n(2, sleep)), Poll::Ready(Ok(())));
    assert_eq!(limiter.remaining(), 0);
    assert_eq!(keyed.check_n(&0, 2), Ok(()));
    assert_eq!(poll_once(keyed.wait_n(&0, 2, sleep)), Poll::Ready(Ok(())));
    assert_eq!(keyed.remaining(&0), 0);
    assert_eq!(sleeps.get(), 2);
}

#[test]
fn impossible_waits_never_sleep() {
    for quota in [
        Quota::per_hour(1),
        Quota::per_second(0),
        Quota::per_second(10).with_burst(0),
    ] {
        let limiter = Limiter::new(quota);
        let keyed = KeyedLimiter::new(quota);
        let sleep = |_: Duration| -> std::future::Ready<()> { panic!("unexpected sleep") };
        for (requests, expected) in [(0, Ok(())), (2, Err(InsufficientCapacity))] {
            for result in [limiter.check_n(requests), keyed.check_n(&0, requests)] {
                assert_eq!(result, expected);
            }
            for result in [
                poll_once(limiter.wait_n(requests, sleep)),
                poll_once(keyed.wait_n(&0, requests, sleep)),
            ] {
                assert_eq!(result, Poll::Ready(expected));
            }
        }
        if quota != Quota::per_hour(1) {
            for result in [
                limiter.check(),
                keyed.check(&0),
                limiter.peek(),
                keyed.peek(&0),
            ] {
                assert_eq!(result, Err(Duration::MAX));
            }
            for result in [
                poll_once(limiter.wait(sleep)),
                poll_once(keyed.wait(&0, sleep)),
            ] {
                assert_eq!(result, Poll::Ready(Err(InsufficientCapacity)));
            }
        }
        assert!(keyed.is_empty());
    }
}

#[test]
fn cancelled_waits_do_not_reserve_allowance() {
    let now = Cell::new(Instant::now());
    let limiter = Limiter::new(Quota::per_hour(1)).with_clock(|| now.get());
    let keyed = KeyedLimiter::new(Quota::per_hour(1)).with_clock(|| now.get());
    assert_eq!(limiter.check(), Ok(()));
    assert_eq!(keyed.check(&0), Ok(()));
    let sleep = |delay| {
        assert_eq!(delay, Duration::from_secs(3600));
        pending::<()>()
    };
    for result in [
        poll_once(limiter.wait(sleep)),
        poll_once(keyed.wait(&0, sleep)),
    ] {
        assert_eq!(result, Poll::Pending);
    }
    now.set(now.get() + Duration::from_secs(3600));
    assert_eq!(limiter.check(), Ok(()));
    assert_eq!(keyed.check(&0), Ok(()));
}
