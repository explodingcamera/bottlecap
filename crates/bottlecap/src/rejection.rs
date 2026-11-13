use std::fmt;
use std::time::Duration;

/// Why a limiter rejected a batch of requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// The rate limit has been reached or there is no room to store a new key.
    ///
    /// Check again after the delay, since other calls can use available requests
    /// in the meantime.
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
