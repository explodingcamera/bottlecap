use bottlecap_tower::{KeyedLimiterLayer, LimiterLayer};
use std::future::{Future, Ready, ready};
use std::hash::{BuildHasherDefault, DefaultHasher};
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::task::{Context, Poll, Waker};

use bottlecap::{Clock, Instant, KeyedLimiter, Limiter, Quota};
use http::{Request, Response, StatusCode, header::RETRY_AFTER};
use tower::{Layer, Service};

#[derive(Clone)]
struct Echo {
    calls: Arc<AtomicUsize>,
    readiness: Poll<Result<(), &'static str>>,
}

impl Service<Request<()>> for Echo {
    type Response = Response<()>;
    type Error = &'static str;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.readiness
    }

    fn call(&mut self, request: Request<()>) -> Self::Future {
        self.calls.fetch_add(1, Relaxed);
        ready(if request.extensions().get::<bool>() == Some(&true) {
            Err("inner error")
        } else {
            Ok(Response::new(()))
        })
    }
}

fn respond<S>(service: &mut S, request: Request<()>) -> Response<()>
where
    S: Service<Request<()>, Response = Response<()>, Error = &'static str>,
{
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(service.poll_ready(&mut cx), Poll::Ready(Ok(())));
    match pin!(service.call(request)).poll(&mut cx) {
        Poll::Ready(response) => response.unwrap(),
        Poll::Pending => panic!("expected an immediate response"),
    }
}

#[test]
fn shared_allowance_does_not_need_a_peer() {
    let now = Instant::now();
    let limiter = Limiter::new(Quota::per_hour(1).with_burst(2)).with_clock(move || now);
    let layer = LimiterLayer::new(limiter).with_rate_limit_headers(true);
    let calls = Arc::new(AtomicUsize::new(0));
    let service = layer.layer(Echo {
        calls: calls.clone(),
        readiness: Poll::Ready(Ok(())),
    });
    for (status, remaining) in [
        (StatusCode::OK, "\"bottlecap\";r=1"),
        (StatusCode::OK, "\"bottlecap\";r=0"),
        (StatusCode::TOO_MANY_REQUESTS, "\"bottlecap\";r=0;t=3600"),
    ] {
        let response = respond(&mut service.clone(), Request::new(()));
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["ratelimit"], remaining);
        if status == StatusCode::TOO_MANY_REQUESTS {
            assert_eq!(response.headers()[RETRY_AFTER], "3600");
        }
    }
    assert_eq!(calls.load(Relaxed), 2);
    assert_eq!(layer.limiter().remaining(), 0);
}

#[test]
fn typed_extensions_have_independent_allowances() {
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct UserId(u64);

    let layer = KeyedLimiterLayer::new(KeyedLimiter::<UserId>::new(Quota::per_hour(1)));
    let service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    assert_eq!(
        respond(&mut service.clone(), Request::new(())).status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(layer.limiter().is_empty());
    for (id, status) in [
        (1, StatusCode::OK),
        (2, StatusCode::OK),
        (1, StatusCode::TOO_MANY_REQUESTS),
    ] {
        let mut request = Request::new(());
        request.extensions_mut().insert(UserId(id));
        assert_eq!(respond(&mut service.clone(), request).status(), status);
    }
    assert_eq!(layer.limiter().len(), 2);
}

#[test]
fn custom_keys_use_headers_and_extensions() {
    let layer = KeyedLimiterLayer::new(KeyedLimiter::<u64>::new(Quota::per_hour(1)))
        .with_rate_limit_headers(true)
        .with_key_extractor(|headers, extensions| {
            extensions
                .get::<u64>()
                .copied()
                .or_else(|| headers.get("x-user-id")?.to_str().ok()?.parse().ok())
        });
    let mut service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    let mut request = Request::new(());
    request
        .headers_mut()
        .insert("x-user-id", "42".parse().unwrap());
    let response = respond(&mut service, request);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["ratelimit"], "\"bottlecap\";r=0");
    let mut request = Request::new(());
    request.extensions_mut().insert(42u64);
    let response = respond(&mut service, request);
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["ratelimit"], "\"bottlecap\";r=0;t=3600");
    assert_eq!(layer.limiter().len(), 1);
}

#[cfg(feature = "tonic")]
#[test]
fn shared_grpc_budget_does_not_need_a_peer() {
    let layer = LimiterLayer::new(Limiter::new(Quota::per_hour(1))).with_tonic();
    let mut service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    assert!(
        !respond(&mut service, Request::new(()))
            .headers()
            .contains_key("grpc-status")
    );
    let rejected = respond(&mut service, Request::new(()));
    assert_eq!(rejected.headers()["grpc-status"], "8");
    assert!(!rejected.headers().contains_key(RETRY_AFTER));
}

#[test]
fn peer_limits_normalize_and_share_keys() {
    let layer =
        KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1))).with_ipv6_prefix(65);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut service = layer.layer(Echo {
        calls: calls.clone(),
        readiness: Poll::Ready(Ok(())),
    });
    for (peer, expected) in [
        ("192.0.2.1:1", StatusCode::OK),
        ("[::ffff:192.0.2.1]:2", StatusCode::TOO_MANY_REQUESTS),
        ("192.0.2.2:1", StatusCode::OK),
        ("[2001:db8::1]:1", StatusCode::OK),
        ("[2001:db8::2]:2", StatusCode::TOO_MANY_REQUESTS),
        ("[2001:db8:0:0:8000::1]:1", StatusCode::OK),
    ] {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(peer.parse::<SocketAddr>().unwrap());
        request
            .headers_mut()
            .insert("x-forwarded-for", "203.0.113.99".parse().unwrap());
        assert_eq!(respond(&mut service.clone(), request).status(), expected);
    }
    assert_eq!(layer.limiter().len(), 4);
    assert_eq!(calls.load(Relaxed), 4);
    assert!(
        layer
            .limiter()
            .reset(&"192.0.2.1".parse::<IpAddr>().unwrap())
    );
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert("192.0.2.1:3".parse::<SocketAddr>().unwrap());
    assert_eq!(respond(&mut service, request).status(), StatusCode::OK);
    let response = respond(&mut service, Request::new(()));
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.headers().contains_key(RETRY_AFTER));
}

#[test]
fn custom_ip_policy_and_response_headers() {
    struct FixedClock(Instant);
    impl Clock for FixedClock {
        fn now(&self) -> Instant {
            self.0
        }
    }
    let limiter = KeyedLimiter::with_hasher(
        Quota::per_hour(1).with_burst(2),
        BuildHasherDefault::<DefaultHasher>::default(),
    )
    .with_clock(FixedClock(Instant::now()));
    let layer = KeyedLimiterLayer::by_ip(limiter)
        .with_ip_extractor(|headers, peer| {
            if peer.is_some_and(|ip| ip.is_loopback()) {
                headers
                    .get("x-client-ip")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
                    .or(peer)
            } else {
                peer
            }
        })
        .with_rate_limit_headers(true);
    let mut service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    for (client, status, remaining) in [
        ("192.0.2.1", StatusCode::OK, "\"bottlecap\";r=1"),
        ("::ffff:192.0.2.1", StatusCode::OK, "\"bottlecap\";r=0"),
        (
            "192.0.2.1",
            StatusCode::TOO_MANY_REQUESTS,
            "\"bottlecap\";r=0;t=3600",
        ),
    ] {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert("127.0.0.1:80".parse::<SocketAddr>().unwrap());
        request
            .headers_mut()
            .insert("x-client-ip", client.parse().unwrap());
        let response = respond(&mut service.clone(), request);
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["ratelimit-policy"], "\"bottlecap\";q=2");
        assert_eq!(response.headers()["ratelimit"], remaining);
        if status == StatusCode::TOO_MANY_REQUESTS {
            assert_eq!(response.headers()[RETRY_AFTER], "3600");
        }
    }
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert("192.0.2.1:80".parse::<SocketAddr>().unwrap());
    request
        .headers_mut()
        .insert("x-client-ip", "198.51.100.1".parse().unwrap());
    assert_eq!(
        respond(&mut service, request).status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(layer.limiter().len(), 1);
}

#[test]
fn ipv6_prefix_bounds() {
    for (prefix, expected) in [(0, StatusCode::TOO_MANY_REQUESTS), (128, StatusCode::OK)] {
        let layer = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1)))
            .with_ipv6_prefix(prefix);
        let mut service = layer.layer(Echo {
            calls: Arc::new(AtomicUsize::new(0)),
            readiness: Poll::Ready(Ok(())),
        });
        for (peer, status) in [
            ("[2001:db8::1]:1", StatusCode::OK),
            ("[2001:db8::2]:1", expected),
        ] {
            let mut request = Request::new(());
            request
                .extensions_mut()
                .insert(peer.parse::<SocketAddr>().unwrap());
            assert_eq!(respond(&mut service, request).status(), status);
        }
    }
}

#[cfg(feature = "axum")]
#[test]
fn axum_connect_info_supplies_peer() {
    let layer = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1)));
    let mut service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    for expected in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
        let mut request = Request::new(());
        request.extensions_mut().insert(axum::extract::ConnectInfo(
            "192.0.2.1:80".parse::<SocketAddr>().unwrap(),
        ));
        assert_eq!(respond(&mut service, request).status(), expected);
    }
}

#[test]
fn readiness_and_errors_are_preserved() {
    let layer = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut service = layer.layer(Echo {
        calls: Arc::clone(&calls),
        readiness: Poll::Pending,
    });
    let mut cx = Context::from_waker(Waker::noop());
    assert!(service.poll_ready(&mut cx).is_pending());
    let mut service = layer.layer(Echo {
        calls,
        readiness: Poll::Ready(Ok(())),
    });
    assert_eq!(service.poll_ready(&mut cx), Poll::Ready(Ok(())));
    let mut request = Request::new(());
    request.extensions_mut().insert(true);
    request
        .extensions_mut()
        .insert("192.0.2.1:80".parse::<SocketAddr>().unwrap());
    assert!(matches!(
        pin!(service.call(request)).poll(&mut cx),
        Poll::Ready(Err("inner error"))
    ));
}

#[cfg(feature = "tonic")]
#[test]
fn tonic_peers_receive_grpc_rejections() {
    let layer = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1))).with_tonic();
    let mut service = layer.layer(Echo {
        calls: Arc::new(AtomicUsize::new(0)),
        readiness: Poll::Ready(Ok(())),
    });
    for expected in [None, Some("8")] {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(tonic::transport::server::TcpConnectInfo {
                local_addr: None,
                remote_addr: Some("192.0.2.1:80".parse().unwrap()),
            });
        let response = respond(&mut service, request);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .map(|value| value.to_str().unwrap()),
            expected
        );
        if expected.is_some() {
            assert_eq!(response.headers()["content-type"], "application/grpc");
            assert!(!response.headers().contains_key(RETRY_AFTER));
        }
    }
    #[cfg(feature = "axum")]
    {
        let http_layer = KeyedLimiterLayer::by_ip(layer.limiter().clone());
        let mut http = http_layer.layer(Echo {
            calls: Arc::new(AtomicUsize::new(0)),
            readiness: Poll::Ready(Ok(())),
        });
        let mut request = Request::new(());
        request.extensions_mut().insert(axum::extract::ConnectInfo(
            "192.0.2.1:80".parse::<SocketAddr>().unwrap(),
        ));
        let rejected = respond(&mut http, request);
        assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(!rejected.headers().contains_key("grpc-status"));
    }
    let response = respond(&mut service, Request::new(()));
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["grpc-status"], "13");
}

#[cfg(feature = "tonic")]
#[tokio::test]
async fn server_limits_transport_peers() {
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Code, transport::Server};
    use tonic_health::{
        pb::{HealthCheckRequest, health_client::HealthClient},
        server::health_reporter,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let middleware = KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1)))
        .with_rate_limit_headers(true)
        .with_tonic();
    let limiter = middleware.limiter().clone();
    let (_reporter, health) = health_reporter();
    let server = tokio::spawn(async move {
        Server::builder()
            .layer(middleware)
            .add_service(health)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = HealthClient::new(channel);
    let response = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        response.metadata().get("ratelimit").unwrap(),
        "\"bottlecap\";r=0"
    );
    let mut request = tonic::Request::new(HealthCheckRequest {
        service: String::new(),
    });
    request
        .metadata_mut()
        .insert("x-forwarded-for", "203.0.113.1".parse().unwrap());
    let rejected = client.check(request).await.unwrap_err();
    assert_eq!(rejected.code(), Code::ResourceExhausted);
    assert_eq!(
        rejected.metadata().get("ratelimit").unwrap(),
        "\"bottlecap\";r=0;t=3600"
    );
    assert_eq!(limiter.len(), 1);
    assert!(limiter.reset(&address.ip()));
    assert!(
        client
            .check(HealthCheckRequest {
                service: String::new()
            })
            .await
            .is_ok()
    );
    server.abort();
}
