## Bottlecap for Actix Web

[![Crates.io](https://img.shields.io/crates/v/bottlecap-actix-web?style=flat-square)](https://crates.io/crates/bottlecap-actix-web)
&nbsp;
[![docs.rs](https://img.shields.io/docsrs/bottlecap-actix-web?style=flat-square)](https://docs.rs/bottlecap-actix-web)

Rate-limiting middleware for [Actix Web](https://docs.rs/actix-web),
powered by [Bottlecap](https://docs.rs/bottlecap).

```rust
use actix_web::{App, HttpResponse, web};
use bottlecap_actix_web::{KeyedLimiter, KeyedLimiterLayer, Limiter, LimiterLayer, Quota};

// One budget for all requests.
let shared = LimiterLayer::new(Limiter::new(Quota::per_second(100)));
// A separate budget per IP, with bursts of up to five requests.
let clients = KeyedLimiterLayer::by_ip(
    KeyedLimiter::new(Quota::per_second(10).with_burst(5)),
)
.with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.

let app = App::new()
    .wrap(shared.clone())
    .wrap(clients.clone())
    .route("/", web::get().to(|| async { HttpResponse::Ok().body("hello") }));
```

Rejected requests receive HTTP 429 and `Retry-After`.
Create the layers outside the server factory and clone them into each app.

See the [runnable example](https://github.com/explodingcamera/bottlecap/blob/main/examples/actix-web.rs)
or the [API documentation](https://docs.rs/bottlecap-actix-web).
