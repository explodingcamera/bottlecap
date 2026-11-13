use std::sync::OnceLock;
use std::time::Duration;

/// A point in time used for rate-limit checks.
///
/// Timestamps are monotonic and can be reused across limiters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(pub(crate) u64);

/// A time source for a limiter.
///
/// Closures returning [`Instant`] also implement this trait.
pub trait Clock {
    /// The current time.
    fn now(&self) -> Instant;
}

impl<F: Fn() -> Instant> Clock for F {
    fn now(&self) -> Instant {
        self()
    }
}

/// The system clock used by default.
///
/// Uses a coarse monotonic clock when `coarsetime` is enabled, or
/// [`std::time::Instant`] otherwise.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultClock;

impl Clock for DefaultClock {
    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct ClockOrigin {
    standard: std::time::Instant,
    #[cfg(feature = "coarsetime")]
    coarse: coarsetime::Instant,
}

#[inline]
fn clock_origin() -> &'static ClockOrigin {
    static ORIGIN: OnceLock<ClockOrigin> = OnceLock::new();
    ORIGIN.get_or_init(|| ClockOrigin {
        standard: std::time::Instant::now(),
        #[cfg(feature = "coarsetime")]
        coarse: coarsetime::Instant::now_without_cache_update(),
    })
}

impl Instant {
    /// The current time from the selected clock.
    #[inline]
    pub fn now() -> Self {
        #[cfg(feature = "coarsetime")]
        {
            coarsetime::Instant::now_without_cache_update().into()
        }
        #[cfg(not(feature = "coarsetime"))]
        {
            std::time::Instant::now().into()
        }
    }

    /// Time elapsed since this timestamp.
    #[inline]
    pub fn elapsed(&self) -> Duration {
        Self::now().duration_since(*self)
    }

    /// The time since `earlier`, or zero if `earlier` is later than this timestamp.
    #[inline]
    pub fn duration_since(&self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// Add a duration, returning `None` if the timestamp would overflow.
    #[inline]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0
            .checked_add(u64::try_from(duration.as_nanos()).ok()?)
            .map(Self)
    }

    /// Subtract a duration, returning `None` if it would precede the clock origin.
    #[inline]
    pub fn checked_sub(self, duration: Duration) -> Option<Self> {
        self.0
            .checked_sub(u64::try_from(duration.as_nanos()).ok()?)
            .map(Self)
    }
}

impl From<std::time::Instant> for Instant {
    /// Convert a std timestamp to a limiter timestamp, saturating at the supported bounds.
    #[inline]
    fn from(instant: std::time::Instant) -> Self {
        let elapsed = instant.saturating_duration_since(clock_origin().standard);
        Self(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
    }
}

#[cfg(feature = "coarsetime")]
impl From<coarsetime::Instant> for Instant {
    /// Convert a coarse timestamp to a limiter timestamp, saturating times before the origin at zero.
    #[inline]
    fn from(instant: coarsetime::Instant) -> Self {
        Self(instant.duration_since(clock_origin().coarse).as_nanos())
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Self;

    #[inline]
    fn add(self, duration: Duration) -> Self {
        self.checked_add(duration).expect("instant overflow")
    }
}

impl std::ops::Sub<Duration> for Instant {
    type Output = Self;

    #[inline]
    fn sub(self, duration: Duration) -> Self {
        self.checked_sub(duration).expect("instant underflow")
    }
}

impl std::ops::Sub for Instant {
    type Output = Duration;

    #[inline]
    fn sub(self, earlier: Self) -> Duration {
        self.duration_since(earlier)
    }
}
