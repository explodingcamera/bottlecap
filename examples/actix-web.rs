use actix_web::{App, HttpResponse, HttpServer, web};
use bottlecap_actix_web::{KeyedLimiter, KeyedLimiterLayer, Limiter, LimiterLayer, Quota};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // One budget for all requests.
    let shared = LimiterLayer::new(Limiter::new(Quota::per_second(100)));
    // A separate budget per IP, with bursts of up to five requests.
    let clients = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_second(10).with_burst(5)))
        .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.

    // Each app uses the same limiters through cloned layers.
    HttpServer::new(move || {
        App::new().wrap(shared.clone()).wrap(clients.clone()).route(
            "/",
            web::get().to(|| async { HttpResponse::Ok().body("hello\n") }),
        )
    })
    .bind(("127.0.0.1", 3000))?
    .run()
    .await
}
