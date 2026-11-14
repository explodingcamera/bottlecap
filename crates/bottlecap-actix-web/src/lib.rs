//! Rate-limiting middleware for [Actix Web](https://docs.rs/actix-web),
//! powered by [Bottlecap](https://docs.rs/bottlecap).
//!
//! ## Shared limit
//!
//! [`LimiterLayer`] applies one quota to all requests:
//!
//! ```
//! use actix_web::App;
//! use bottlecap_actix_web::{Limiter, LimiterLayer, Quota};
//!
//! let limiter = Limiter::new(Quota::per_second(10).with_burst(5));
//! let layer = LimiterLayer::new(limiter)
//!     .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.
//!
//! let app = App::new().wrap(layer.clone());
//! ```
//!
//! ## Per-key limits
//!
//! [`KeyedLimiterLayer`] gives each key its own rate limit. For per-IP limits:
//!
//! ```
//! use actix_web::App;
//! use bottlecap_actix_web::{KeyedLimiter, KeyedLimiterLayer, Quota};
//!
//! let clients = KeyedLimiter::new(Quota::per_second(10).with_burst(5));
//! let layer = KeyedLimiterLayer::by_ip(clients)
//!     .with_rate_limit_headers(true);
//!
//! let app = App::new().wrap(layer.clone());
//! ```
//!
//! Requests over the limit receive an HTTP 429 response with a `Retry-After` header.
//!
//! ## Features
//!
//! - `coarsetime` (default): use a coarse monotonic clock instead of `std::time::Instant`.
//! - `default-hasher` (default): use [rapidhash](https://docs.rs/rapidhash) for keyed lookups.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse};
use actix_web::{
    Error, HttpResponse,
    http::header::{HeaderMap, HeaderName, RETRY_AFTER},
};
use pin_project_lite::pin_project;

pub use bottlecap::{KeyedLimiter, Limiter, Quota};

mod keyed;
mod shared;

pub use keyed::{ExtensionKey, IpKey, IpLimiterLayer, KeyedLimiterLayer};
pub use shared::LimiterLayer;

trait Policy {
    fn check(&self, request: &ServiceRequest) -> Result<Option<(u32, u32)>, Denied>;
}

/// The service produced by [`LimiterLayer`] or [`KeyedLimiterLayer`].
#[doc(hidden)]
pub struct LimiterService<S, L = LimiterLayer> {
    inner: S,
    layer: L,
}

enum Denied {
    Limited(Duration, Option<(u32, u32)>),
    MissingKey,
}

impl Denied {
    fn into_response(self) -> HttpResponse {
        match self {
            Self::Limited(delay, limits) => {
                let mut response = HttpResponse::TooManyRequests()
                    .insert_header((RETRY_AFTER, retry_seconds(delay).to_string()))
                    .finish();
                if let Some(limits) = limits {
                    add_rate_limit_headers(response.headers_mut(), limits, Some(delay));
                }
                response
            }
            Self::MissingKey => HttpResponse::InternalServerError().finish(),
        }
    }
}

impl<S, B, L> Service<ServiceRequest> for LimiterService<S, L>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    L: Policy,
    B: MessageBody,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type Future = ResponseFuture<S::Future, B>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&self, request: ServiceRequest) -> Self::Future {
        match self.layer.check(&request) {
            Ok(limits) => ResponseFuture::Inner {
                future: self.inner.call(request),
                limits,
            },
            Err(denied) => ResponseFuture::Rejected {
                response: Some(request.into_response(denied.into_response().map_into_right_body())),
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
        HeaderName::from_static("ratelimit-policy"),
        format!("\"bottlecap\";q={burst}").try_into().unwrap(),
    );
    let value = match retry {
        Some(delay) if delay != Duration::MAX => {
            let seconds = retry_seconds(delay);
            format!("\"bottlecap\";r={remaining};t={seconds}")
        }
        _ => format!("\"bottlecap\";r={remaining}"),
    };
    headers.append(
        HeaderName::from_static("ratelimit"),
        value.try_into().unwrap(),
    );
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
        Rejected { response: Option<ServiceResponse<EitherBody<B>>> },
    }
}

impl<F, B> Future for ResponseFuture<F, B>
where
    F: Future<Output = Result<ServiceResponse<B>, Error>>,
{
    type Output = Result<ServiceResponse<EitherBody<B>>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            ResponseFutureProj::Inner { future, limits } => future.poll(cx).map(|result| {
                result.map(|mut response| {
                    if let Some(limits) = limits.take() {
                        add_rate_limit_headers(response.headers_mut(), limits, None);
                    }
                    response.map_into_left_body()
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
