use std::sync::Arc;

use bottlecap::{Clock, DefaultClock};
use http::Request;
use tower::Layer;

use crate::{Denied, Limiter, LimiterService, Policy};

/// A shared rate-limit layer for Tower services.
///
/// Cloning the layer keeps the same limiter, so its services share the rate limit.
///
/// ```
/// use bottlecap_tower::{Limiter, LimiterLayer, Quota};
///
/// let limiter = Limiter::new(Quota::per_second(100));
/// let layer = LimiterLayer::new(limiter)
///     .with_rate_limit_headers(true); // Add RateLimit and RateLimit-Policy headers.
///
/// // With the tonic feature, return gRPC RESOURCE_EXHAUSTED instead of HTTP 429.
/// # #[cfg(feature = "tonic")]
/// let layer = layer.with_tonic();
///
/// let layers = tower::ServiceBuilder::new().layer(layer);
/// ```
///
/// Add the layer to a Tower service, an Axum router, or a Tonic server.
/// Use [`crate::KeyedLimiterLayer`] for a separate budget per request key.
pub struct LimiterLayer<C = DefaultClock> {
    limiter: Arc<Limiter<C>>,
    rate_limit_headers: bool,
    #[cfg(feature = "tonic")]
    grpc: bool,
}

impl<C> Clone for LimiterLayer<C> {
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            rate_limit_headers: self.rate_limit_headers,
            #[cfg(feature = "tonic")]
            grpc: self.grpc,
        }
    }
}

impl<C: Clock> LimiterLayer<C> {
    /// Create a layer using the given limiter.
    pub fn new(limiter: impl Into<Arc<Limiter<C>>>) -> Self {
        Self {
            limiter: limiter.into(),
            rate_limit_headers: false,
            #[cfg(feature = "tonic")]
            grpc: false,
        }
    }

    /// Add `RateLimit` and `RateLimit-Policy` response headers (disabled by default).
    pub fn with_rate_limit_headers(mut self, enabled: bool) -> Self {
        self.rate_limit_headers = enabled;
        self
    }

    /// Use gRPC rejection responses for Tonic services.
    #[cfg(feature = "tonic")]
    pub fn with_tonic(mut self) -> Self {
        self.grpc = true;
        self
    }

    /// The shared limiter.
    pub fn limiter(&self) -> &Arc<Limiter<C>> {
        &self.limiter
    }
}

impl<S, C: Clock> Layer<S> for LimiterLayer<C> {
    type Service = LimiterService<S, Self>;

    fn layer(&self, inner: S) -> Self::Service {
        LimiterService {
            inner,
            layer: self.clone(),
        }
    }
}

impl<C: Clock> Policy for LimiterLayer<C> {
    fn check<B>(&self, _: &Request<B>) -> Result<Option<(u32, u32)>, Denied> {
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

    #[cfg(feature = "tonic")]
    fn is_grpc(&self) -> bool {
        self.grpc
    }
}
