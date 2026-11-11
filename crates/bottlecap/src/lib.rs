//! A fast, memory-efficient rate limiter for Rust.
//! Uses the [Generic Cell Rate Algorithm (GCRA)][gcra].
//!
//! [gcra]: https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm
//!
//! ## Quotas
//!
//! A [`Quota`] sets how quickly allowance refills and how much can accumulate.
//!
//! ```
//! # use bottlecap::Quota;
//! let quota = Quota::per_second(10) // Refill one request every 100 ms.
//!     .with_burst(5); // Allow up to five requests at once.
//! ```
//!
//! A new limiter starts with the full burst available. Without `with_burst`,
//! the burst defaults to the request count, ten in this example.
//!
//! ## Shared limit
//!
//! [`Limiter`] applies one quota to all checks:
//!
//! ```no_run
//! # async fn example() -> Result<(), bottlecap::Rejection> {
//! # use bottlecap::{Limiter, Quota};
//! let quota = Quota::per_second(10).with_burst(5);
//! let limiter = Limiter::new(quota);
//!
//! // Consume one request, or return a retry delay.
//! assert!(limiter.check().is_ok());
//! assert!(limiter.check_n(2).is_ok());
//!
//! // Check without consuming allowance.
//! assert!(limiter.peek().is_ok());
//!
//! // Wait for the next free request
//! limiter.wait(tokio::time::sleep).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Per-key limits
//!
//! [`KeyedLimiter`] gives each key its own allowance. Keys can be IP addresses,
//! user IDs, or any other hashable value.
//!
//! ```no_run
//! # async fn example() -> Result<(), bottlecap::Rejection> {
//! # use bottlecap::{KeyedLimiter, Quota};
//! let quota = Quota::per_minute(100);
//! let clients = KeyedLimiter::new(quota);
//!
//! assert!(clients.check(&"alice").is_ok());
//! assert!(clients.check_n(&"alice", 3).is_ok());
//! assert!(clients.peek(&"bob").is_ok());
//!
//! clients.wait(&"alice", tokio::time::sleep).await?;
//! # Ok(())
//! # }
//! ```
//!
//! To use a different hashing algorithm, pass a hash builder to
//! [`KeyedLimiter::with_hasher`].

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
