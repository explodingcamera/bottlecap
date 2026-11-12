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
