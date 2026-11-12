use std::time::Duration;

use bottlecap::Quota;

#[test]
fn refill_rounds_up() {
    let quota = Quota::per(Duration::from_nanos(10), 3);
    assert_eq!(quota.interval(), Duration::from_nanos(4));
    assert_eq!(
        Quota::per(Duration::from_nanos(1), 2).interval(),
        Duration::from_nanos(1)
    );
}

#[test]
fn zero_disables_quota() {
    for quota in [Quota::per_second(0), Quota::per_second(10).with_burst(0)] {
        assert_eq!(quota.burst(), 0);
        assert_eq!(quota.interval(), Duration::ZERO);
        assert_eq!(quota.with_burst(10), quota);
    }
}

#[test]
fn invalid_quotas_panic() {
    for (interval, burst) in [
        (Duration::ZERO, 1),
        (Duration::MAX, 1),
        (Duration::from_nanos(u64::MAX), 3),
    ] {
        assert!(std::panic::catch_unwind(|| Quota::per(interval, 1).with_burst(burst)).is_err());
    }
}
