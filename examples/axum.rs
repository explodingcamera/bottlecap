use std::net::SocketAddr;

use axum::{Router, routing::get};
use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Limiter, LimiterLayer, Quota};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // One budget for all requests.
    let shared = LimiterLayer::new(Limiter::new(Quota::per_second(100)));
    // A separate budget per IP, with bursts of up to five requests.
    let clients = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_second(10).with_burst(5)))
        .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.

    let app = Router::new()
        .route("/", get(|| async { "hello\n" }))
        .layer(shared)
        .layer(clients);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    // Supply connection addresses to the per-IP layer.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
}
