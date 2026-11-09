use crate::Rejection;
use std::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Duration,
};

/// GCRA parameters, see [ITU-T I.371, Annex A](https://www.itu.int/rec/T-REC-I.371-200403-I/en).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Gcra {
    pub(crate) interval: u64,
    pub(crate) tolerance: u64,
}

impl Gcra {
    /// Return available requests at the given timestamp.
    pub(crate) fn remaining(&self, now: u64, arrival: u64) -> u32 {
        if self.interval == 0 {
            return 0;
        }
        let burst = self.tolerance / self.interval + 1;
        let used = arrival.saturating_sub(now).div_ceil(self.interval);
        burst.saturating_sub(used) as u32
    }

    /// Treat a batch as one admission with a larger cost and less burst tolerance.
    /// This preserves the original timestamp units and the existing atomic path.
    /// The caller must handle zero requests before constructing a batch.
    #[inline]
    pub(crate) fn batch(self, requests: u32) -> Result<Self, Rejection> {
        debug_assert!(requests > 0);
        if self.interval == 0 || u64::from(requests) > self.tolerance / self.interval + 1 {
            return Err(Rejection::InsufficientCapacity);
        }
        let interval = self
            .interval
            .checked_mul(u64::from(requests))
            .expect("limiter timestamp overflow");
        Ok(Self {
            interval,
            tolerance: self.tolerance - (interval - self.interval),
        })
    }

    /// Build parameters from a refill interval and burst size.
    pub(crate) fn new(interval: Duration, burst: u32) -> Self {
        if burst == 0 {
            return Self {
                interval: 0,
                tolerance: 0,
            };
        }
        let interval = u64::try_from(interval.as_nanos()).expect("interval too large");
        assert!(interval > 0);
        let tolerance = interval
            .checked_mul(u64::from(burst - 1))
            .expect("burst too large");
        Self {
            interval,
            tolerance,
        }
    }

    /// Compute the next arrival timestamp or the required retry delay.
    #[inline]
    pub(crate) fn check(&self, now: u64, arrival: u64) -> Result<u64, Duration> {
        if self.interval == 0 {
            return Err(Duration::MAX);
        }
        let earliest = arrival.saturating_sub(self.tolerance);
        if now < earliest {
            Err(Duration::from_nanos(earliest - now))
        } else {
            Ok(now
                .max(arrival)
                .checked_add(self.interval)
                .expect("limiter timestamp overflow"))
        }
    }

    /// Atomically consume allowance without updating state on rejection.
    #[inline]
    pub(crate) fn check_atomic(&self, now: u64, state: &AtomicU64) -> Result<(), Duration> {
        let mut arrival = state.load(Relaxed);
        loop {
            let next = self.check(now, arrival)?;
            match state.compare_exchange_weak(arrival, next, Relaxed, Relaxed) {
                Ok(_) => return Ok(()),
                Err(current) => arrival = current,
            }
        }
    }
}
