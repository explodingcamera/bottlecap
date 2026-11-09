## Bottlecap

A fast, memory-efficient rate limiter for Rust.

```rust
use bottlecap::{Limiter, Quota};

let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
assert!(limiter.check().is_ok());
```
