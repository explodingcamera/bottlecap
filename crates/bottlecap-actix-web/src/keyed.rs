use std::future::{Ready, ready};
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::net::IpAddr;
use std::sync::Arc;

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpMessage, http::header::HeaderMap};
use bottlecap::{Clock, DefaultClock, DefaultHashBuilder};

use crate::{Denied, KeyedLimiter, LimiterService, Policy};

type IpExtractor = Arc<dyn Fn(&HeaderMap, Option<IpAddr>) -> Option<IpAddr> + Send + Sync>;

/// A per-key rate-limit layer for Actix Web apps.
///
/// Keys come from request extensions unless the layer is created with [`Self::by_ip`].
///
/// ```
/// use actix_web::App;
/// use bottlecap_actix_web::{KeyedLimiter, KeyedLimiterLayer, Quota};
///
/// // A separate budget per IP, with bursts of up to five requests.
/// let limiter = KeyedLimiter::new(Quota::per_second(10).with_burst(5));
/// let layer = KeyedLimiterLayer::by_ip(limiter)
///     .with_rate_limit_headers(true) // Add RateLimit and RateLimit-Policy headers.
///     .with_ipv6_prefix(64); // Group IPv6 clients by /64, the default.
///
/// let app = App::new().wrap(layer.clone());
/// ```
///
/// Create the layer outside `HttpServer::new` and clone it inside the factory
/// to share the limiter across apps.
///
/// Requests without a key receive HTTP 500 without counting toward the limit.
pub struct KeyedLimiterLayer<K, H = DefaultHashBuilder, C = DefaultClock, E = ExtensionKey<K>> {
    limiter: Arc<KeyedLimiter<K, H, C>>,
    extractor: E,
    rate_limit_headers: bool,
}

/// A per-IP rate-limit layer.
/// Create it with [`KeyedLimiterLayer::by_ip`].
pub type IpLimiterLayer<H = DefaultHashBuilder, C = DefaultClock> =
    KeyedLimiterLayer<IpAddr, H, C, IpKey>;

/// Reads a typed key from request extensions.
#[derive(Clone)]
#[doc(hidden)]
pub struct ExtensionKey<K>(PhantomData<fn() -> K>);

/// Extracts peer addresses and groups IPv6 clients by prefix.
#[derive(Clone)]
#[doc(hidden)]
pub struct IpKey {
    prefix: u8,
    extractor: Option<IpExtractor>,
}

impl<K, H, C, E: Clone> Clone for KeyedLimiterLayer<K, H, C, E> {
    fn clone(&self) -> Self {
        Self {
            limiter: Arc::clone(&self.limiter),
            extractor: self.extractor.clone(),
            rate_limit_headers: self.rate_limit_headers,
        }
    }
}

impl<K, H, C> KeyedLimiterLayer<K, H, C>
where
    K: Eq + Hash + Clone + 'static,
    H: BuildHasher + Clone,
    C: Clock,
{
    /// Create a layer that reads keys from request extensions.
    ///
    /// Requests without a `K` extension receive HTTP 500. Since Actix runs
    /// middleware in reverse registration order, register authentication after
    /// this layer so it can insert the key through [`HttpMessage::extensions_mut`].
    ///
    /// ```
    /// use bottlecap_actix_web::{KeyedLimiter, KeyedLimiterLayer, Quota};
    ///
    /// #[derive(Clone, PartialEq, Eq, Hash)]
    /// struct UserId(u64);
    ///
    /// let users = KeyedLimiter::<UserId>::new(Quota::per_minute(100));
    /// let layer = KeyedLimiterLayer::new(users);
    /// ```
    pub fn new(limiter: impl Into<Arc<KeyedLimiter<K, H, C>>>) -> Self {
        Self {
            limiter: limiter.into(),
            extractor: ExtensionKey(PhantomData),
            rate_limit_headers: false,
        }
    }
}

impl<H: BuildHasher + Clone, C: Clock> KeyedLimiterLayer<IpAddr, H, C, IpKey> {
    /// Create a layer that limits requests by client IP.
    ///
    /// Uses the connection's peer address, grouping IPv6 addresses by /64 prefix.
    pub fn by_ip(limiter: impl Into<Arc<KeyedLimiter<IpAddr, H, C>>>) -> Self {
        Self {
            limiter: limiter.into(),
            extractor: IpKey {
                prefix: 64,
                extractor: None,
            },
            rate_limit_headers: false,
        }
    }

    /// Set the IPv6 prefix length, from 0 to 128 bits (default 64).
    pub fn with_ipv6_prefix(mut self, prefix: u8) -> Self {
        assert!(prefix <= 128, "IPv6 prefix must be at most 128 bits");
        self.extractor.prefix = prefix;
        self
    }

    /// Extract the client IP using request headers and the peer address.
    ///
    /// Only trust forwarded headers when the peer is a known proxy.
    pub fn with_ip_extractor<F>(mut self, extractor: F) -> Self
    where
        F: Fn(&HeaderMap, Option<IpAddr>) -> Option<IpAddr> + Send + Sync + 'static,
    {
        self.extractor.extractor = Some(Arc::new(extractor));
        self
    }
}

impl<K, H, C, E> KeyedLimiterLayer<K, H, C, E> {
    /// Extract request keys with a custom function.
    ///
    /// The function can read headers, extensions, and app data. Requests for which
    /// it returns `None` receive HTTP 500 without counting toward the limit.
    pub fn with_key_extractor<F>(self, extractor: F) -> KeyedLimiterLayer<K, H, C, F>
    where
        F: Fn(&ServiceRequest) -> Option<K> + Clone,
    {
        KeyedLimiterLayer {
            limiter: self.limiter,
            extractor,
            rate_limit_headers: self.rate_limit_headers,
        }
    }

    /// Add `RateLimit` and `RateLimit-Policy` response headers (disabled by default).
    pub fn with_rate_limit_headers(mut self, enabled: bool) -> Self {
        self.rate_limit_headers = enabled;
        self
    }

    /// The keyed limiter.
    pub fn limiter(&self) -> &Arc<KeyedLimiter<K, H, C>> {
        &self.limiter
    }
}

trait ExtractKey<K> {
    fn extract(&self, request: &ServiceRequest) -> Option<K>;
}

impl<K: Clone + 'static> ExtractKey<K> for ExtensionKey<K> {
    fn extract(&self, request: &ServiceRequest) -> Option<K> {
        request.extensions().get::<K>().cloned()
    }
}

impl<K, F: Fn(&ServiceRequest) -> Option<K>> ExtractKey<K> for F {
    fn extract(&self, request: &ServiceRequest) -> Option<K> {
        self(request)
    }
}

impl ExtractKey<IpAddr> for IpKey {
    fn extract(&self, request: &ServiceRequest) -> Option<IpAddr> {
        let peer = request.peer_addr().map(|peer| peer.ip());
        let ip = match &self.extractor {
            Some(extract) => extract(request.headers(), peer),
            None => peer,
        }?;
        Some(match ip.to_canonical() {
            IpAddr::V4(ip) => IpAddr::V4(ip),
            IpAddr::V6(ip) => {
                let mask = u128::MAX
                    .checked_shl(u32::from(128 - self.prefix))
                    .unwrap_or(0);
                IpAddr::V6((u128::from(ip) & mask).into())
            }
        })
    }
}

impl<S, B, K, H, C, E: Clone> Transform<S, ServiceRequest> for KeyedLimiterLayer<K, H, C, E>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    B: MessageBody,
    Self: Policy,
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

impl<K, H, C, E> Policy for KeyedLimiterLayer<K, H, C, E>
where
    K: Eq + Hash + Clone,
    H: BuildHasher + Clone,
    C: Clock,
    E: ExtractKey<K>,
{
    fn check(&self, request: &ServiceRequest) -> Result<Option<(u32, u32)>, Denied> {
        let key = self.extractor.extract(request).ok_or(Denied::MissingKey)?;
        let result = self.limiter.check(&key);
        let limits = self.rate_limit_headers.then(|| {
            let remaining = if result.is_ok() {
                self.limiter.remaining(&key)
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
