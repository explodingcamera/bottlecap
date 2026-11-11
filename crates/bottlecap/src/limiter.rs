use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use crate::gcra::Gcra;
use crate::{Clock, DefaultClock, Instant, Quota, Rejection};

/// A rate limiter with one limit shared by all callers.
///
/// Allowance updates are lock-free.
/// Share it through `Arc` when the same allowance is used by multiple workers.
pub struct Limiter<C = DefaultClock> {
    quota: Gcra,
    state: AtomicU64,
    clock: C,
}

impl Limiter {
    /// Create an initially full limiter for one shared quota.
    ///
    /// ```
    /// use bottlecap::{Limiter, Quota};
    /// let limiter = Limiter::new(Quota::per_second(10));
    /// assert!(limiter.check().is_ok());
    /// ```
    pub fn new(quota: Quota) -> Self {
        Self {
            quota: quota.0,
            state: AtomicU64::new(0),
            clock: DefaultClock,
        }
    }
}

impl<C: Clock> Limiter<C> {
    /// Return this limiter's rate and burst configuration.
    pub fn quota(&self) -> Quota {
        Quota(self.quota)
    }

    /// Return the number of requests currently available, without reserving them.
    pub fn remaining(&self) -> u32 {
        self.remaining_at(self.clock.now())
    }

    /// Return available requests using a caller-supplied current time.
    pub fn remaining_at(&self, now: Instant) -> u32 {
        self.quota.remaining(now.0, self.state.load(Relaxed))
    }

    /// Use a custom clock without changing consumed allowance.
    pub fn with_clock<D: Clock>(self, clock: D) -> Limiter<D> {
        Limiter {
            quota: self.quota,
            state: self.state,
            clock,
        }
    }

    /// Consume one request's allowance, or return the minimum retry delay.
    /// The delay does not reserve allowance, check again after waiting.
    #[inline]
    pub fn check(&self) -> Result<(), Duration> {
        self.check_at(self.clock.now())
    }

    /// Check using a caller-supplied current time, which can be reused across limiters.
    #[inline]
    pub fn check_at(&self, now: Instant) -> Result<(), Duration> {
        self.quota.check_atomic(now.0, &self.state)
    }

    /// Consume `requests` atomically, without consuming partial allowance on rejection.
    /// Zero requests succeed without changing state. Counts above the burst return
    /// [`Rejection::InsufficientCapacity`], regardless of available allowance.
    #[inline]
    pub fn check_n(&self, requests: u32) -> Result<(), Rejection> {
        self.check_n_at(requests, self.clock.now())
    }

    /// Consume `requests` using a caller-supplied current time.
    #[inline]
    pub fn check_n_at(&self, requests: u32, now: Instant) -> Result<(), Rejection> {
        if requests == 0 {
            return Ok(());
        }
        self.quota
            .batch(requests)?
            .check_atomic(now.0, &self.state)
            .map_err(Rejection::from)
    }

    /// Check allowance without consuming or reserving it.
    #[inline]
    pub fn peek(&self) -> Result<(), Duration> {
        self.peek_at(self.clock.now())
    }

    /// Check allowance without consuming it, using a caller-supplied current time.
    #[inline]
    pub fn peek_at(&self, now: Instant) -> Result<(), Duration> {
        self.quota
            .check(now.0, self.state.load(Relaxed))
            .map(|_| ())
    }

    /// Check whether `requests` fit without consuming or reserving allowance.
    #[inline]
    pub fn peek_n(&self, requests: u32) -> Result<(), Rejection> {
        self.peek_n_at(requests, self.clock.now())
    }

    /// Check a batch without consuming it, using a caller-supplied current time.
    #[inline]
    pub fn peek_n_at(&self, requests: u32, now: Instant) -> Result<(), Rejection> {
        if requests == 0 {
            return Ok(());
        }
        self.quota
            .batch(requests)?
            .check(now.0, self.state.load(Relaxed))
            .map(|_| ())
            .map_err(Rejection::from)
    }

    /// Wait for and consume one request using the supplied async sleep function.
    /// Waiting does not reserve allowance or guarantee fairness. Dropping the
    /// future while sleeping consumes nothing.
    pub async fn wait<F, Fut>(&self, sleep: F) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.wait_n(1, sleep).await
    }

    /// Wait for and atomically consume `requests`. Impossible batches return
    /// immediately without sleeping. The sleep function must yield to the runtime
    /// and should wait at least the supplied duration to avoid busy retrying.
    pub async fn wait_n<F, Fut>(&self, requests: u32, mut sleep: F) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            match self.check_n(requests) {
                Err(Rejection::RetryAfter(delay)) => sleep(delay).await,
                result => return result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::{pending, ready};
    use std::sync::Barrier;
    use std::task::{Context, Poll, Waker};
    use std::thread;

    use super::*;
    use crate::Rejection::{InsufficientCapacity, RetryAfter};

    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        std::pin::pin!(future)
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn concurrent_burst_and_refill() {
        for (requests, burst, refill) in [(1, 100, 2), (3, 33, 1)] {
            let limiter = Limiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(100));
            let epoch = Instant::now();
            for (elapsed, expected) in [(0, burst), (20, refill)] {
                let now = epoch + Duration::from_nanos(elapsed);
                let start = Barrier::new(16);
                let admitted = thread::scope(|scope| {
                    let workers: Vec<_> = (0..16)
                        .map(|_| {
                            scope.spawn(|| {
                                start.wait();
                                (0..100)
                                    .filter(|_| {
                                        if requests == 1 {
                                            limiter.check_at(now).is_ok()
                                        } else {
                                            limiter.check_n_at(requests, now).is_ok()
                                        }
                                    })
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
                assert_eq!(
                    limiter.remaining_at(now),
                    if elapsed == 0 { 100 % requests } else { 0 }
                );
            }
        }
    }

    #[test]
    fn batches_and_peeks_preserve_allowance() {
        let limiter = Limiter::new(Quota::per_second(1).with_burst(3));
        let now = Instant::now();
        for (requests, expected) in [
            (4, Err(InsufficientCapacity)),
            (0, Ok(())),
            (2, Ok(())),
            (2, Err(RetryAfter(Duration::from_secs(1)))),
            (1, Ok(())),
        ] {
            assert_eq!(limiter.peek_n_at(requests, now), expected);
            assert_eq!(limiter.check_n_at(requests, now), expected);
        }
        assert_eq!(limiter.remaining_at(now), 0);
        assert_eq!(limiter.check_at(now), Err(Duration::from_secs(1)));
        assert_eq!(
            limiter.check_at(now + Duration::from_millis(999)),
            Err(Duration::from_millis(1))
        );
        let later = now + Duration::from_secs(3);
        for _ in 0..10 {
            assert_eq!(limiter.peek_at(later), Ok(()));
            assert_eq!(limiter.peek_n_at(3, later), Ok(()));
        }
        assert_eq!(limiter.check_n_at(3, later), Ok(()));
    }

    #[test]
    fn custom_clock_drives_waiting() {
        let now = Cell::new(Instant::now());
        let limiter = Limiter::new(Quota::per_second(1).with_burst(2));
        assert_eq!(limiter.check_n_at(2, now.get()), Ok(()));
        let limiter = limiter.with_clock(|| now.get());
        let sleeps = Cell::new(0);
        assert_eq!(
            poll_once(limiter.wait_n(2, |delay| {
                assert_eq!(delay, Duration::from_secs(2));
                sleeps.set(sleeps.get() + 1);
                now.set(now.get() + delay);
                ready(())
            })),
            Poll::Ready(Ok(()))
        );
        assert_eq!(sleeps.get(), 1);
        assert_eq!(limiter.remaining(), 0);
    }

    #[test]
    fn impossible_waits_never_sleep() {
        for quota in [
            Quota::per_hour(1),
            Quota::per_second(0),
            Quota::per_second(10).with_burst(0),
        ] {
            let limiter = Limiter::new(quota);
            let sleep = |_: Duration| -> std::future::Ready<()> { panic!("unexpected sleep") };
            for (requests, expected) in [(0, Ok(())), (2, Err(InsufficientCapacity))] {
                assert_eq!(limiter.check_n(requests), expected);
                assert_eq!(
                    poll_once(limiter.wait_n(requests, sleep)),
                    Poll::Ready(expected)
                );
            }
            if quota != Quota::per_hour(1) {
                assert_eq!(limiter.check(), Err(Duration::MAX));
                assert_eq!(limiter.peek(), Err(Duration::MAX));
                assert_eq!(
                    poll_once(limiter.wait(sleep)),
                    Poll::Ready(Err(InsufficientCapacity))
                );
            }
        }
    }

    #[test]
    fn cancelled_waits_do_not_reserve_allowance() {
        let now = Cell::new(Instant::now());
        let limiter = Limiter::new(Quota::per_hour(1)).with_clock(|| now.get());
        assert_eq!(limiter.check(), Ok(()));
        assert_eq!(
            poll_once(limiter.wait(|delay| {
                assert_eq!(delay, Duration::from_secs(3600));
                pending::<()>()
            })),
            Poll::Pending
        );
        now.set(now.get() + Duration::from_secs(3600));
        assert_eq!(limiter.check(), Ok(()));
    }
}
