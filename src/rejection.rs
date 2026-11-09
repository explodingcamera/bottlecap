use std::fmt;
use std::time::Duration;

/// Why a limiter rejected a batch of requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// Allowance or key capacity is temporarily unavailable. Check again after
    /// this delay, which does not reserve allowance or guarantee admission.
    RetryAfter(Duration),
    /// The request count exceeds the quota's burst and can never fit.
    InsufficientCapacity,
}

impl From<Duration> for Rejection {
    fn from(delay: Duration) -> Self {
        Self::RetryAfter(delay)
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetryAfter(delay) => write!(formatter, "retry after {delay:?}"),
            Self::InsufficientCapacity => formatter.write_str("request count exceeds quota burst"),
        }
    }
}

impl std::error::Error for Rejection {}
