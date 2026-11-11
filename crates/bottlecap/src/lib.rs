//! A fast, memory-efficient rate limiter for Rust.

mod gcra;
mod instant;
mod keyed;
mod limiter;
mod quota;
mod rejection;
mod store;

use gcra::Gcra;
pub use instant::{Clock, DefaultClock, Instant};
pub use keyed::KeyedLimiter;
pub use limiter::Limiter;
pub use quota::Quota;
/// The default hash builder for keyed limiters.
#[doc(hidden)]
#[cfg(feature = "default-hasher")]
pub use rapidhash::fast::RandomState as DefaultHashBuilder;
pub use rejection::Rejection;
/// The fallback hash builder when the default-hasher feature is disabled.
#[doc(hidden)]
#[cfg(not(feature = "default-hasher"))]
pub use std::collections::hash_map::RandomState as DefaultHashBuilder;
