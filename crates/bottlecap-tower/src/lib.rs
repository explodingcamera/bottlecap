//! Rate-limiting middleware for [Tower](https://docs.rs/tower), [Axum](https://docs.rs/axum),
//! and [Tonic](https://docs.rs/tonic), powered by [Bottlecap](https://docs.rs/bottlecap).
//!
//! ## Shared limit
//!
//! [`LimiterLayer`] applies one quota to all requests:
//!
//! ```
//! use bottlecap_tower::{Limiter, LimiterLayer, Quota};
//!
//! let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
//! let layer = LimiterLayer::new(limiter)
//!     .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.
//!
//! let layers = tower::ServiceBuilder::new().layer(layer);
//! ```
//!
//! ## Per-key limits
//!
//! [`KeyedLimiterLayer`] gives each key its own rate limit. For per-IP limits:
//!
//! ```
//! use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Quota};
//!
//! let clients = KeyedLimiter::new(Quota::per_second(10).with_burst(5));
//! let layer = KeyedLimiterLayer::by_ip(clients)
//!     .with_rate_limit_headers(true);
//!
//! let layers = tower::ServiceBuilder::new().layer(layer);
//! ```
//!
//! Requests over the limit receive an HTTP 429 response with a `Retry-After` header.
//!
//! ## Axum and Tonic
//!
//! Tower services work without extra features. For Axum, enable `axum` and serve
//! with `app.into_make_service_with_connect_info::<std::net::SocketAddr>()` when
//! using per-IP limits.
//!
//! For Tonic, enable `tonic` and call `.with_tonic()` on each rate-limit layer
//! before adding it to the server. This returns gRPC `RESOURCE_EXHAUSTED`
//! instead of HTTP 429:
//!
//! ```
//! # #[cfg(feature = "tonic")]
//! # {
//! use bottlecap_tower::{Limiter, LimiterLayer, Quota};
//!
//! let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
//! let server = tonic::transport::Server::builder()
//!     .layer(LimiterLayer::new(limiter).with_tonic());
//! # }
//! ```
//!
//! ## Features
//!
//! - `coarsetime` (default): use a coarse monotonic clock instead of `std::time::Instant`.
//! - `default-hasher` (default): use [rapidhash](https://docs.rs/rapidhash) for keyed lookups.
//! - `axum`: read peer addresses from Axum connection information.
//! - `tonic`: read Tonic peer addresses and enable `.with_tonic()` for gRPC responses.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, Request, Response, StatusCode, header::RETRY_AFTER};
use pin_project_lite::pin_project;
use tower::Service;

pub use bottlecap::{KeyedLimiter, Limiter, Quota};

mod keyed;
mod shared;

pub use keyed::{
    ExtensionKey, HeaderKey, HeaderLimiterLayer, IpKey, IpLimiterLayer, KeyedLimiterLayer,
};
pub use shared::LimiterLayer;

trait Policy {
    fn check<B>(&self, request: &Request<B>) -> Result<Option<(u32, u32)>, Denied>;
    fn is_grpc(&self) -> bool {
        false
    }
}

/// The service produced by [`LimiterLayer`] or [`KeyedLimiterLayer`].
#[doc(hidden)]
pub struct LimiterService<S, L = LimiterLayer> {
    inner: S,
    layer: L,
}

impl<S: Clone, L: Clone> Clone for LimiterService<S, L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum Denied {
    Limited(Duration, Option<(u32, u32)>),
    MissingKey,
}

impl Denied {
    fn into_response<B: Default>(self, grpc: bool) -> Response<B> {
        #[cfg(feature = "tonic")]
        if grpc {
            let status = match self {
                Self::Limited(_, _) => tonic::Status::resource_exhausted("rate limit exceeded"),
                Self::MissingKey => tonic::Status::internal("missing rate-limit key"),
            };
            let mut response = status.into_http();
            if let Self::Limited(delay, Some(limits)) = self {
                add_rate_limit_headers(response.headers_mut(), limits, Some(delay));
            }
            return response;
        }
        // Without the tonic feature, policies always use HTTP responses.
        #[cfg(not(feature = "tonic"))]
        debug_assert!(!grpc);

        let mut response = Response::new(B::default());
        match self {
            Self::Limited(delay, limits) => {
                *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                response
                    .headers_mut()
                    .insert(RETRY_AFTER, retry_seconds(delay).into());
                if let Some(limits) = limits {
                    add_rate_limit_headers(response.headers_mut(), limits, Some(delay));
                }
            }
            Self::MissingKey => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
        }
        response
    }
}

impl<S, B, R, L> Service<Request<B>> for LimiterService<S, L>
where
    S: Service<Request<B>, Response = Response<R>>,
    L: Policy,
    R: Default,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future, R>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        match self.layer.check(&request) {
            Ok(limits) => ResponseFuture::Inner {
                future: self.inner.call(request),
                limits,
            },
            Err(denied) => ResponseFuture::Rejected {
                response: Some(denied.into_response(self.layer.is_grpc())),
            },
        }
    }
}

fn add_rate_limit_headers(
    headers: &mut HeaderMap,
    (burst, remaining): (u32, u32),
    retry: Option<Duration>,
) {
    headers.append(
        "ratelimit-policy",
        format!("\"bottlecap\";q={burst}").try_into().unwrap(),
    );
    let value = match retry {
        Some(delay) if delay != Duration::MAX => {
            let seconds = retry_seconds(delay);
            format!("\"bottlecap\";r={remaining};t={seconds}")
        }
        _ => format!("\"bottlecap\";r={remaining}"),
    };
    headers.append("ratelimit", value.try_into().unwrap());
}

fn retry_seconds(delay: Duration) -> u64 {
    delay
        .as_secs()
        .saturating_add(u64::from(delay.subsec_nanos() > 0))
}

pin_project! {
    /// A response future without per-request boxing.
    #[doc(hidden)]
    #[project = ResponseFutureProj]
    pub enum ResponseFuture<F, B> {
        Inner { #[pin] future: F, limits: Option<(u32, u32)> },
        Rejected { response: Option<Response<B>> },
    }
}

impl<F, B, E> Future for ResponseFuture<F, B>
where
    F: Future<Output = Result<Response<B>, E>>,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            ResponseFutureProj::Inner { future, limits } => future.poll(cx).map(|result| {
                result.map(|mut response| {
                    if let Some(limits) = limits.take() {
                        add_rate_limit_headers(response.headers_mut(), limits, None);
                    }
                    response
                })
            }),
            ResponseFutureProj::Rejected { response } => Poll::Ready(Ok(response
                .take()
                .expect("response future polled after completion"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_rounds_up() {
        for (delay, seconds) in [
            (Duration::ZERO, 0),
            (Duration::from_nanos(1), 1),
            (Duration::from_millis(1001), 2),
            (Duration::MAX, u64::MAX),
        ] {
            assert_eq!(retry_seconds(delay), seconds);
        }
    }
}
