## Bottlecap

A fast, memory-efficient rate limiter for Rust.

```rust
use bottlecap::{Limiter, Quota};

let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
assert!(limiter.check().is_ok());
```

Use `KeyedLimiter` for separate allowances per key:

```rust
use bottlecap::{KeyedLimiter, Quota};

let clients = KeyedLimiter::new(Quota::per_second(1));
assert!(clients.check(&"alice").is_ok());
assert!(clients.check(&"bob").is_ok());
```
