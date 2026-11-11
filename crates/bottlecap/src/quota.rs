use std::time::Duration;

use crate::Gcra;

/// A request rate and burst size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quota(pub(crate) Gcra);

impl Quota {
    /// Allow `requests` per `period`, replenishing continuously.
    /// Refill intervals round up to the nearest nanosecond.
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
    pub fn with_burst(self, burst: u32) -> Self {
        if self.0.interval == 0 {
            return self;
        }
        Self(Gcra::new(self.interval(), burst))
    }

    /// Return the interval between replenished requests.
    /// Returns zero for a disabled quota.
    pub fn interval(self) -> Duration {
        Duration::from_nanos(self.0.interval)
    }

    /// Return the maximum burst size.
    pub fn burst(self) -> u32 {
        if self.0.interval == 0 {
            return 0;
        }
        (self.0.tolerance / self.0.interval + 1) as u32
    }
}
