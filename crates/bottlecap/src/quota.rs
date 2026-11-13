use std::time::Duration;

use crate::Gcra;

/// A request rate and burst size.
///
/// The rate determines how quickly requests become available, while the burst
/// is the maximum number you can use at once.
///
/// ```
/// use bottlecap::Quota;
/// use std::time::Duration;
///
/// let per_second = Quota::per_second(10);
/// let per_minute = Quota::per_minute(100);
/// let per_hour = Quota::per_hour(1_000);
/// let custom = Quota::per(Duration::from_secs(30), 20);
/// ```
///
/// ## Bursts
///
/// With a rate of ten requests per second and a burst of five, you can make five
/// requests immediately before waiting 100 ms for the next one. Unused requests
/// accumulate up to five, so waiting longer does not let you save up more requests.
///
/// ```
/// use bottlecap::{Instant, Limiter, Quota};
/// use std::time::Duration;
///
/// let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
/// let now = Instant::now();
///
/// // All five requests can be made at the same time.
/// assert!(limiter.check_n_at(5, now).is_ok());
/// assert_eq!(limiter.check_at(now), Err(Duration::from_millis(100)));
///
/// // After 100 ms, one more request is available.
/// assert!(limiter.check_at(now + Duration::from_millis(100)).is_ok());
///
/// // Even after a whole second, only five requests can be saved up.
/// assert_eq!(limiter.remaining_at(now + Duration::from_secs(1)), 5);
/// ```
///
/// Without [`with_burst`](Self::with_burst), the burst equals the request count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quota(pub(crate) Gcra);

impl Quota {
    /// Allow `requests` per `period`, replenishing continuously.
    /// Refill intervals round up to the nearest nanosecond.
    /// Panics if `period` is zero or the quota exceeds the timestamp range.
    pub fn per(period: Duration, requests: u32) -> Self {
        assert!(!period.is_zero(), "quota period must be positive");
        if requests == 0 {
            return Self(Gcra {
                interval: 0,
                tolerance: 0,
            });
        }
        let interval = period.as_nanos().div_ceil(u128::from(requests));
        let interval = u64::try_from(interval).expect("quota interval too large");
        Self(Gcra::new(Duration::from_nanos(interval), requests))
    }

    /// Allow `requests` per second.
    pub fn per_second(requests: u32) -> Self {
        Self::per(Duration::from_secs(1), requests)
    }

    /// Allow `requests` per minute.
    pub fn per_minute(requests: u32) -> Self {
        Self::per(Duration::from_secs(60), requests)
    }

    /// Allow `requests` per hour.
    pub fn per_hour(requests: u32) -> Self {
        Self::per(Duration::from_secs(3600), requests)
    }

    /// Set how many requests can be allowed at once, without changing the refill rate.
    /// Panics if the burst exceeds the timestamp range.
    pub fn with_burst(self, burst: u32) -> Self {
        if self.0.interval == 0 {
            return self;
        }
        Self(Gcra::new(self.interval(), burst))
    }

    /// The interval between replenished requests, or zero for a disabled quota.
    pub fn interval(self) -> Duration {
        Duration::from_nanos(self.0.interval)
    }

    /// The maximum burst size.
    pub fn burst(self) -> u32 {
        if self.0.interval == 0 {
            return 0;
        }
        (self.0.tolerance / self.0.interval + 1) as u32
    }
}
