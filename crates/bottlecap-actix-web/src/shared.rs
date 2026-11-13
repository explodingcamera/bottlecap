use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use bottlecap::{Clock, DefaultClock};

use crate::{Denied, Limiter, LimiterService, Policy};

/// A shared rate-limit layer for Actix Web apps.
///
/// Cloning the layer keeps the same limiter, so its apps share the rate limit.
///
/// ```
/// use actix_web::App;
/// use bottlecap_actix_web::{Limiter, LimiterLayer, Quota};
///
/// let limiter = Limiter::new(Quota::per_second(100));
/// let layer = LimiterLayer::new(limiter)
///     .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.
///
/// let app = App::new().wrap(layer.clone());
/// ```
///
/// Create the layer outside `HttpServer::new` and clone it inside the factory
/// to share the limiter across apps.
///
/// Use [`crate::KeyedLimiterLayer`] for a separate budget per request key.
pub struct LimiterLayer<C = DefaultClock> {
    limiter: Arc<Limiter<C>>,
    rate_limit_headers: bool,
}

impl<C> Clone for LimiterLayer<C> {
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            rate_limit_headers: self.rate_limit_headers,
        }
    }
}

impl<C: Clock> LimiterLayer<C> {
    /// Create a layer using the given limiter.
    pub fn new(limiter: impl Into<Arc<Limiter<C>>>) -> Self {
        Self {
            limiter: limiter.into(),
            rate_limit_headers: false,
        }
    }

    /// Add `RateLimit` and `RateLimit-Policy` response headers (disabled by default).
    pub fn with_rate_limit_headers(mut self, enabled: bool) -> Self {
        self.rate_limit_headers = enabled;
        self
    }

    /// The shared limiter.
    pub fn limiter(&self) -> &Arc<Limiter<C>> {
        &self.limiter
    }
}

impl<S, B, C: Clock> Transform<S, ServiceRequest> for LimiterLayer<C>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
{
    type Response = ServiceResponse<EitherBody<B>>;
    type Error = Error;
    type InitError = ();
    type Transform = LimiterService<S, Self>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, inner: S) -> Self::Future {
        ready(Ok(LimiterService {
            inner,
            layer: self.clone(),
        }))
    }
}

impl<C: Clock> Policy for LimiterLayer<C> {
    fn check(&self, _: &ServiceRequest) -> Result<Option<(u32, u32)>, Denied> {
        let result = self.limiter.check();
        let limits = self.rate_limit_headers.then(|| {
            let remaining = if result.is_ok() {
                self.limiter.remaining()
            } else {
                0
            };
            (self.limiter.quota().burst(), remaining)
        });
        result
            .map(|()| limits)
            .map_err(|delay| Denied::Limited(delay, limits))
    }
}
