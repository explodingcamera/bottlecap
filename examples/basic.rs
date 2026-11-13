use bottlecap::{KeyedLimiter, Limiter, Quota};

fn main() {
    let limiter = Limiter::new(Quota::per_second(1).with_burst(2));
    for _ in 0..3 {
        match limiter.check() {
            Ok(()) => println!("allowed"),
            Err(delay) => println!("retry after {delay:?}"),
        }
    }

    let clients = KeyedLimiter::new(Quota::per_second(1));
    assert!(clients.check(&"alice").is_ok());
    assert!(clients.check(&"bob").is_ok());
    assert!(clients.check(&"alice").is_err());
}
