//! A fast, memory-efficient rate limiter for Rust.

mod gcra;
mod instant;
mod keyed;
mod limiter;
mod quota;
mod rejection;
mod store;

use gcra::Gcra;
/// The default hash builder for keyed limiters.
#[doc(hidden)]
pub use hashbrown::DefaultHashBuilder;
pub use instant::{Clock, DefaultClock, Instant};
pub use keyed::KeyedLimiter;
pub use limiter::Limiter;
pub use quota::Quota;
pub use rejection::Rejection;
