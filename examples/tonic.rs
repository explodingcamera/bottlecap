use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Limiter, LimiterLayer, Quota};
use tonic::transport::Server;
use tonic_health::server::health_reporter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One budget for all requests.
    let shared = LimiterLayer::new(Limiter::new(Quota::per_second(100)));
    // A separate budget per IP, with bursts of up to five requests.
    let clients = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_second(10).with_burst(5)))
        .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.

    let (_reporter, health) = health_reporter();
    Server::builder()
        // Return gRPC errors rather than HTTP 429 responses.
        .layer(shared.with_tonic())
        .layer(clients.with_tonic())
        .add_service(health)
        .serve("127.0.0.1:50051".parse()?)
        .await?;

    Ok(())
}
