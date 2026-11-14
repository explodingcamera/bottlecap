use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use bottlecap::{Clock, DefaultClock, DefaultHashBuilder};
use http::{Extensions, HeaderMap, HeaderName, Request};
use tower::Layer;

use crate::{Denied, KeyedLimiter, LimiterService, Policy};

type IpExtractor = Arc<dyn Fn(&HeaderMap, Option<IpAddr>) -> Option<IpAddr> + Send + Sync>;

/// A per-key rate-limit layer for Tower services.
///
/// Keys come from request extensions unless created with [`Self::by_ip`] or [`Self::by_header`].
///
/// ```
/// use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Quota};
///
/// // A separate budget per IP, with bursts of up to five requests.
/// let limiter = KeyedLimiter::new(Quota::per_second(10).with_burst(5));
/// let layer = KeyedLimiterLayer::by_ip(limiter)
///     .with_rate_limit_headers(true) // Add RateLimit and RateLimit-Policy headers.
///     .with_ipv6_prefix(64); // Group IPv6 clients by /64, the default.
///
/// // With the tonic feature, return gRPC errors instead of HTTP rejections.
/// # #[cfg(feature = "tonic")]
/// let layer = layer.with_tonic();
///
/// let layers = tower::ServiceBuilder::new().layer(layer);
/// ```
///
/// Cloning the layer keeps the same limiter, so its services share the per-key limits.
///
/// For Axum, enable `axum` and serve through
/// `app.into_make_service_with_connect_info::<std::net::SocketAddr>()` so peer
/// addresses are available. For Tonic, enable `tonic` and call `.with_tonic()`.
///
/// Missing keys return HTTP 500, or gRPC `INTERNAL` with `.with_tonic()`.
pub struct KeyedLimiterLayer<K, H = DefaultHashBuilder, C = DefaultClock, E = ExtensionKey<K>> {
    limiter: Arc<KeyedLimiter<K, H, C>>,
    extractor: E,
    rate_limit_headers: bool,
    #[cfg(feature = "tonic")]
    grpc: bool,
}

/// A per-IP rate-limit layer.
/// Create it with [`KeyedLimiterLayer::by_ip`].
pub type IpLimiterLayer<H = DefaultHashBuilder, C = DefaultClock> =
    KeyedLimiterLayer<IpAddr, H, C, IpKey>;

/// A per-header-value rate-limit layer.
/// Create it with [`KeyedLimiterLayer::by_header`].
pub type HeaderLimiterLayer<H = DefaultHashBuilder, C = DefaultClock> =
    KeyedLimiterLayer<String, H, C, HeaderKey>;

/// Reads a typed key from request extensions.
#[derive(Clone)]
#[doc(hidden)]
pub struct ExtensionKey<K>(PhantomData<fn() -> K>);

/// Reads a borrowed string key from a request header.
#[derive(Clone)]
#[doc(hidden)]
pub struct HeaderKey(HeaderName);

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
            #[cfg(feature = "tonic")]
            grpc: self.grpc,
        }
    }
}

impl<K, H, C> KeyedLimiterLayer<K, H, C>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    H: BuildHasher + Clone,
    C: Clock,
{
    /// Create a layer that reads keys from request extensions.
    ///
    /// Requests without a `K` extension receive HTTP 500, so middleware that
    /// inserts the key must run before this layer.
    ///
    /// ```
    /// use bottlecap_tower::{KeyedLimiter, KeyedLimiterLayer, Quota};
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
            #[cfg(feature = "tonic")]
            grpc: false,
        }
    }
}

impl<H: BuildHasher + Clone, C: Clock> KeyedLimiterLayer<String, H, C, HeaderKey> {
    /// Create a layer that limits requests by a header's string value.
    ///
    /// Missing, repeated, empty, or invalid values receive HTTP 500 without consuming allowance,
    /// or gRPC `INTERNAL` with `with_tonic()`.
    /// Use a trusted or authenticated header, otherwise clients can choose their own keys.
    pub fn by_header(
        limiter: impl Into<Arc<KeyedLimiter<String, H, C>>>,
        header: HeaderName,
    ) -> Self {
        Self {
            limiter: limiter.into(),
            extractor: HeaderKey(header),
            rate_limit_headers: false,
            #[cfg(feature = "tonic")]
            grpc: false,
        }
    }
}

impl<H: BuildHasher + Clone, C: Clock> KeyedLimiterLayer<IpAddr, H, C, IpKey> {
    /// Create a layer that limits requests by client IP.
    ///
    /// IPv6 addresses are grouped by /64 prefix. Peer addresses are read from
    /// `SocketAddr` request extensions, or Axum/Tonic connection information
    /// when their features are enabled.
    ///
    /// With Axum, serve through
    /// `app.into_make_service_with_connect_info::<SocketAddr>()` to supply peer addresses.
    /// Tonic supports both plain and TLS connections.
    pub fn by_ip(limiter: impl Into<Arc<KeyedLimiter<IpAddr, H, C>>>) -> Self {
        Self {
            limiter: limiter.into(),
            extractor: IpKey {
                prefix: 64,
                extractor: None,
            },
            rate_limit_headers: false,
            #[cfg(feature = "tonic")]
            grpc: false,
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
    /// Requests for which the function returns `None` receive HTTP 500 without
    /// counting toward the limit.
    pub fn with_key_extractor<F>(self, extractor: F) -> KeyedLimiterLayer<K, H, C, F>
    where
        F: Fn(&HeaderMap, &Extensions) -> Option<K> + Clone,
    {
        KeyedLimiterLayer {
            limiter: self.limiter,
            extractor,
            rate_limit_headers: self.rate_limit_headers,
            #[cfg(feature = "tonic")]
            grpc: self.grpc,
        }
    }

    /// Add `RateLimit` and `RateLimit-Policy` response headers (disabled by default).
    pub fn with_rate_limit_headers(mut self, enabled: bool) -> Self {
        self.rate_limit_headers = enabled;
        self
    }

    /// Use gRPC errors instead of HTTP rejections.
    ///
    /// Rate-limited requests receive `RESOURCE_EXHAUSTED`, while requests without
    /// a key receive `INTERNAL`.
    #[cfg(feature = "tonic")]
    pub fn with_tonic(mut self) -> Self {
        self.grpc = true;
        self
    }

    /// The keyed limiter.
    pub fn limiter(&self) -> &Arc<KeyedLimiter<K, H, C>> {
        &self.limiter
    }
}

trait ExtractKey<K> {
    type Query: Eq + Hash + ToOwned<Owned = K> + ?Sized;

    fn with_key<B, R>(
        &self,
        request: &Request<B>,
        check: impl FnOnce(&Self::Query) -> R,
    ) -> Option<R>;
}

impl<K: Eq + Hash + Clone + Send + Sync + 'static> ExtractKey<K> for ExtensionKey<K> {
    type Query = K;

    fn with_key<B, R>(&self, request: &Request<B>, check: impl FnOnce(&K) -> R) -> Option<R> {
        request.extensions().get::<K>().map(check)
    }
}

impl<K: Eq + Hash + Clone, F: Fn(&HeaderMap, &Extensions) -> Option<K>> ExtractKey<K> for F {
    type Query = K;

    fn with_key<B, R>(&self, request: &Request<B>, check: impl FnOnce(&K) -> R) -> Option<R> {
        self(request.headers(), request.extensions()).map(|key| check(&key))
    }
}

impl ExtractKey<String> for HeaderKey {
    type Query = str;

    fn with_key<B, R>(&self, request: &Request<B>, check: impl FnOnce(&str) -> R) -> Option<R> {
        let mut values = request.headers().get_all(&self.0).iter();
        let key = values.next()?.to_str().ok()?;
        if key.is_empty() || values.next().is_some() {
            return None;
        }
        Some(check(key))
    }
}

impl ExtractKey<IpAddr> for IpKey {
    type Query = IpAddr;

    fn with_key<B, R>(&self, request: &Request<B>, check: impl FnOnce(&IpAddr) -> R) -> Option<R> {
        let extensions = request.extensions();
        let peer = extensions.get::<SocketAddr>().map(|peer| peer.ip());
        #[cfg(feature = "axum")]
        let peer = peer.or_else(|| {
            extensions
                .get::<axum::extract::ConnectInfo<SocketAddr>>()
                .map(|info| info.0.ip())
        });
        #[cfg(feature = "tonic")]
        let peer = peer.or_else(|| {
            use tonic::transport::server::TcpConnectInfo;
            extensions
                .get::<TcpConnectInfo>()
                .and_then(TcpConnectInfo::remote_addr)
                .or_else(|| {
                    // Resolve TLS metadata without selecting a TLS backend here.
                    tonic::Request::from_parts(Default::default(), extensions.clone(), ())
                        .remote_addr()
                })
                .map(|peer| peer.ip())
        });
        let ip = match &self.extractor {
            Some(extract) => extract(request.headers(), peer),
            None => peer,
        }?;
        let key = match ip.to_canonical() {
            IpAddr::V4(ip) => IpAddr::V4(ip),
            IpAddr::V6(ip) => {
                let mask = u128::MAX
                    .checked_shl(u32::from(128 - self.prefix))
                    .unwrap_or(0);
                IpAddr::V6((u128::from(ip) & mask).into())
            }
        };
        Some(check(&key))
    }
}

impl<S, K, H, C, E: Clone> Layer<S> for KeyedLimiterLayer<K, H, C, E>
where
    Self: Policy,
{
    type Service = LimiterService<S, Self>;

    fn layer(&self, inner: S) -> Self::Service {
        LimiterService {
            inner,
            layer: self.clone(),
        }
    }
}

impl<K, H, C, E> Policy for KeyedLimiterLayer<K, H, C, E>
where
    K: Eq + Hash + Clone + Borrow<E::Query>,
    H: BuildHasher + Clone,
    C: Clock,
    E: ExtractKey<K>,
{
    fn check<B>(&self, request: &Request<B>) -> Result<Option<(u32, u32)>, Denied> {
        self.extractor
            .with_key(request, |key| {
                let result = self.limiter.check(key);
                let limits = self.rate_limit_headers.then(|| {
                    let remaining = if result.is_ok() {
                        self.limiter.remaining(key)
                    } else {
                        0
                    };
                    (self.limiter.quota().burst(), remaining)
                });
                result
                    .map(|()| limits)
                    .map_err(|delay| Denied::Limited(delay, limits))
            })
            .ok_or(Denied::MissingKey)?
    }

    #[cfg(feature = "tonic")]
    fn is_grpc(&self) -> bool {
        self.grpc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Quota;

    #[test]
    fn header_values_are_validated() {
        let layer: HeaderLimiterLayer = KeyedLimiterLayer::by_header(
            KeyedLimiter::new(Quota::per_hour(1)),
            HeaderName::from_static("x-user-id"),
        );
        let mut request = Request::new(());
        assert!(matches!(layer.check(&request), Err(Denied::MissingKey)));
        for (value, valid) in [(b"".as_slice(), false), (b"\xff", false), (b"alice", true)] {
            request
                .headers_mut()
                .insert("x-user-id", http::HeaderValue::from_bytes(value).unwrap());
            assert_eq!(layer.check(&request).is_ok(), valid);
            assert_eq!(layer.limiter().len(), usize::from(valid));
        }
        request
            .headers_mut()
            .append("x-user-id", http::HeaderValue::from_static("bob"));
        assert!(matches!(layer.check(&request), Err(Denied::MissingKey)));
    }
}
