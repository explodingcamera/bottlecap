use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use crate::gcra::Gcra;
use crate::{Clock, DefaultClock, Instant, Quota, Rejection};

/// A rate limiter with one limit shared by all callers.
///
/// Rate-limit updates are lock-free.
/// Use `Arc<Limiter>` to share a limiter across threads.
pub struct Limiter<C = DefaultClock> {
    quota: Gcra,
    state: AtomicU64,
    clock: C,
}

impl Limiter {
    /// Create a limiter with the maximum number of requests available.
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
    /// This limiter's rate and burst configuration.
    pub fn quota(&self) -> Quota {
        Quota(self.quota)
    }

    /// The number of requests currently available.
    pub fn remaining(&self) -> u32 {
        self.remaining_at(self.clock.now())
    }

    /// Available requests at a caller-supplied time.
    pub fn remaining_at(&self, now: Instant) -> u32 {
        self.quota.remaining(now.0, self.state.load(Relaxed))
    }

    /// Switch to a custom clock without resetting the limiter.
    pub fn with_clock<D: Clock>(self, clock: D) -> Limiter<D> {
        Limiter {
            quota: self.quota,
            state: self.state,
            clock,
        }
    }

    /// Consume one request, or get the minimum retry delay.
    ///
    /// Other calls can use available requests while you wait, so check again afterward.
    #[inline]
    pub fn check(&self) -> Result<(), Duration> {
        self.check_at(self.clock.now())
    }

    /// Consume one request at the supplied time.
    #[inline]
    pub fn check_at(&self, now: Instant) -> Result<(), Duration> {
        self.quota.check_atomic(now.0, &self.state)
    }

    /// Consume a batch of `requests` atomically.
    ///
    /// Rejected batches do not count toward the limit. Zero requests always succeed.
    /// Counts above the burst return
    /// [`Rejection::InsufficientCapacity`].
    #[inline]
    pub fn check_n(&self, requests: u32) -> Result<(), Rejection> {
        self.check_n_at(requests, self.clock.now())
    }

    /// Consume a batch of `requests` at the supplied time.
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

    /// Check one request without counting it toward the limit.
    #[inline]
    pub fn peek(&self) -> Result<(), Duration> {
        self.peek_at(self.clock.now())
    }

    /// Check one request at the supplied time without counting it toward the limit.
    #[inline]
    pub fn peek_at(&self, now: Instant) -> Result<(), Duration> {
        self.quota
            .check(now.0, self.state.load(Relaxed))
            .map(|_| ())
    }

    /// Check a batch without counting it toward the limit.
    #[inline]
    pub fn peek_n(&self, requests: u32) -> Result<(), Rejection> {
        self.peek_n_at(requests, self.clock.now())
    }

    /// Check a batch at the supplied time without counting it toward the limit.
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
    ///
    /// Other calls can use available requests while this future sleeps, so waiting
    /// calls are not guaranteed to complete in order. Dropping the future while
    /// sleeping does not use any requests.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), bottlecap::Rejection> {
    /// use bottlecap::{Limiter, Quota};
    ///
    /// let limiter = Limiter::new(Quota::per_second(10));
    /// limiter.wait(tokio::time::sleep).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait<F, Fut>(&self, sleep: F) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.wait_n(1, sleep).await
    }

    /// Wait for and consume a batch of `requests` atomically.
    ///
    /// Batches larger than the burst fail without sleeping. The sleep function
    /// must yield to the runtime and should wait at least the supplied duration
    /// to avoid a busy loop.
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
