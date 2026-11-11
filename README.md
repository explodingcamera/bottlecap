## Bottlecap

[![Crates.io](https://img.shields.io/crates/v/bottlecap?style=flat-square)](https://crates.io/crates/bottlecap)
&nbsp;
[![docs.rs](https://img.shields.io/docsrs/bottlecap?style=flat-square)](https://docs.rs/bottlecap)

A fast, memory-efficient rate limiter for Rust.

```rust
use bottlecap::{KeyedLimiter, Limiter, Quota};
let quota = Quota::per_second(10).with_burst(5);
let limiter = Limiter::new(quota);

// Consume one request, or return a retry delay.
assert!(limiter.check().is_ok());
assert!(limiter.check_n(2).is_ok());

// Check without consuming allowance.
assert!(limiter.peek().is_ok());

// Wait for the next free request
limiter.wait(tokio::time::sleep).await?;
```

## License

Licensed under either of [Apache License, Version 2.0](./LICENSE-APACHE) or [MIT license](./LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `bottlecap` by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
