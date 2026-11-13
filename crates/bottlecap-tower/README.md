## Bottlecap for Tower

[![Crates.io](https://img.shields.io/crates/v/bottlecap-tower?style=flat-square)](https://crates.io/crates/bottlecap-tower)
&nbsp;
[![docs.rs](https://img.shields.io/docsrs/bottlecap-tower?style=flat-square)](https://docs.rs/bottlecap-tower)

Rate-limiting middleware for [Tower](https://docs.rs/tower), [Axum](https://docs.rs/axum),
and [Tonic](https://docs.rs/tonic), powered by [Bottlecap](https://docs.rs/bottlecap).

```rust
use axum::{Router, routing::get};
use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Limiter, LimiterLayer, Quota};

// One budget for all requests.
let shared = LimiterLayer::new(Limiter::new(Quota::per_second(100)));
// A separate budget per IP, with bursts of up to five requests.
let clients = KeyedLimiterLayer::by_ip(
    KeyedLimiter::new(Quota::per_second(10).with_burst(5)),
)
.with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.

let app: Router = Router::new()
    .route("/", get(|| async { "hello" }))
    .layer(shared)
    .layer(clients);
```

Rejected requests receive HTTP 429 and `Retry-After`. For Axum, enable `axum`
and supply connection information as shown in the example below. For Tonic,
enable `tonic` and call `.with_tonic()` on the layer.

See the [Axum](https://github.com/explodingcamera/bottlecap/blob/main/examples/axum.rs)
and [Tonic](https://github.com/explodingcamera/bottlecap/blob/main/examples/tonic.rs)
examples for server setup, or the [API documentation](https://docs.rs/bottlecap-tower).
