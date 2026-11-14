use std::hash::{BuildHasherDefault, DefaultHasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use actix_web::{
    App, HttpMessage, HttpResponse,
    http::{StatusCode, header::RETRY_AFTER},
    test, web,
};
use bottlecap::{Clock, Instant, KeyedLimiter, Limiter, Quota};

use bottlecap_actix_web::{IpLimiterLayer, KeyedLimiterLayer, LimiterLayer};

#[test]
fn shared_allowance_spans_workers_without_peers() {
    actix_web::rt::System::new().block_on(async {
        let now = Instant::now();
        let limiter = Limiter::new(Quota::per_hour(1).with_burst(2)).with_clock(move || now);
        let middleware = LimiterLayer::new(limiter).with_rate_limit_headers(true);
        let first = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        let second = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        let response = test::call_service(&first, test::TestRequest::default().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("ratelimit").unwrap(),
            "\"bottlecap\";r=1"
        );
        let response = test::call_service(&second, test::TestRequest::default().to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = test::call_service(&first, test::TestRequest::default().to_request()).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "3600");
        assert_eq!(middleware.limiter().remaining(), 0);
    });
}

#[test]
fn typed_extensions_have_independent_allowances() {
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct UserId(u64);

    actix_web::rt::System::new().block_on(async {
        let middleware = KeyedLimiterLayer::new(KeyedLimiter::<UserId>::new(Quota::per_hour(1)));
        let app = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::default().to_request()).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(middleware.limiter().is_empty());
        for (id, status) in [
            (1, StatusCode::OK),
            (2, StatusCode::OK),
            (1, StatusCode::TOO_MANY_REQUESTS),
        ] {
            let request = test::TestRequest::default().to_request();
            request.extensions_mut().insert(UserId(id));
            assert_eq!(test::call_service(&app, request).await.status(), status);
        }
        assert_eq!(middleware.limiter().len(), 2);
    });
}

#[test]
fn custom_keys_use_request_context() {
    actix_web::rt::System::new().block_on(async {
        let middleware = KeyedLimiterLayer::new(KeyedLimiter::<u64>::new(Quota::per_hour(1)))
            .with_rate_limit_headers(true)
            .with_key_extractor(|request| request.app_data::<u64>().copied());
        let app = test::init_service(
            App::new()
                .app_data(42u64)
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        for (status, remaining) in [
            (StatusCode::OK, "\"bottlecap\";r=0"),
            (StatusCode::TOO_MANY_REQUESTS, "\"bottlecap\";r=0;t=3600"),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::default().to_request()).await;
            assert_eq!(response.status(), status);
            assert_eq!(response.headers().get("ratelimit").unwrap(), remaining);
        }
        assert_eq!(middleware.limiter().len(), 1);
    });
}

#[test]
fn rejects_without_calling_handler() {
    actix_web::rt::System::new().block_on(async {
        let limiter = KeyedLimiter::new(Quota::per_hour(1)).with_max_keys(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = Arc::clone(&calls);
        let app = test::init_service(
            App::new()
                .wrap(KeyedLimiterLayer::by_ip(limiter))
                .default_service(web::to(move || {
                    handler_calls.fetch_add(1, Relaxed);
                    async { HttpResponse::Ok().finish() }
                })),
        )
        .await;
        let first = test::call_service(
            &app,
            test::TestRequest::default()
                .peer_addr("192.0.2.1:80".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let rejected = test::call_service(
            &app,
            test::TestRequest::default()
                .peer_addr("192.0.2.1:80".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(rejected.headers().get(RETRY_AFTER).unwrap(), "3600");
        assert!(test::read_body(rejected).await.is_empty());
        let second = test::call_service(
            &app,
            test::TestRequest::default()
                .peer_addr("192.0.2.2:80".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(calls.load(Relaxed), 2);
    });
}

#[test]
fn peer_limits_share_allowance_across_workers() {
    actix_web::rt::System::new().block_on(async {
        let middleware: IpLimiterLayer =
            KeyedLimiterLayer::by_ip(KeyedLimiter::new(Quota::per_hour(1))).with_ipv6_prefix(65);
        let first = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        let second = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        for (peer, expected) in [
            ("192.0.2.1:1", StatusCode::OK),
            ("[::ffff:192.0.2.1]:2", StatusCode::TOO_MANY_REQUESTS),
            ("192.0.2.2:1", StatusCode::OK),
            ("[2001:db8::1]:1", StatusCode::OK),
            ("[2001:db8::2]:2", StatusCode::TOO_MANY_REQUESTS),
            ("[2001:db8:0:0:8000::1]:1", StatusCode::OK),
        ] {
            let response = test::call_service(
                &first,
                test::TestRequest::default()
                    .peer_addr(peer.parse().unwrap())
                    .insert_header(("x-forwarded-for", "203.0.113.99"))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), expected);
        }
        let response = test::call_service(
            &second,
            test::TestRequest::default()
                .peer_addr("192.0.2.1:80".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(middleware.limiter().len(), 4);
        let response = test::call_service(&second, test::TestRequest::default().to_request()).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!response.headers().contains_key(RETRY_AFTER));
    });
}

#[test]
fn custom_ip_policy_and_response_headers() {
    struct FixedClock(Instant);
    impl Clock for FixedClock {
        fn now(&self) -> Instant {
            self.0
        }
    }
    actix_web::rt::System::new().block_on(async {
        let limiter = KeyedLimiter::with_hasher(
            Quota::per_hour(1).with_burst(2),
            BuildHasherDefault::<DefaultHasher>::default(),
        )
        .with_clock(FixedClock(Instant::now()));
        let middleware = KeyedLimiterLayer::by_ip(limiter)
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
        let app = test::init_service(
            App::new()
                .wrap(middleware.clone())
                .default_service(web::to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;
        for (client, status, remaining) in [
            ("192.0.2.1", StatusCode::OK, "\"bottlecap\";r=1"),
            ("::ffff:192.0.2.1", StatusCode::OK, "\"bottlecap\";r=0"),
            (
                "192.0.2.1",
                StatusCode::TOO_MANY_REQUESTS,
                "\"bottlecap\";r=0;t=3600",
            ),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::default()
                    .peer_addr("127.0.0.1:80".parse().unwrap())
                    .insert_header(("x-client-ip", client))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), status);
            assert_eq!(
                response.headers().get("ratelimit-policy").unwrap(),
                "\"bottlecap\";q=2"
            );
            assert_eq!(response.headers().get("ratelimit").unwrap(), remaining);
            if status == StatusCode::TOO_MANY_REQUESTS {
                assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "3600");
            }
        }
        let response = test::call_service(
            &app,
            test::TestRequest::default()
                .peer_addr("192.0.2.1:80".parse().unwrap())
                .insert_header(("x-client-ip", "198.51.100.1"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(middleware.limiter().len(), 1);
    });
}
