//! Measure retained Linux RSS in a fresh process for each backend and key count.
//! Run with `cargo bench --bench memory -- 100000 1000000`.

use std::collections::HashMap;
use std::error::Error;
use std::hint::black_box;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::process::Command;
use std::time::Duration;

use bottlecap::{Instant, KeyedLimiter, Limiter, Quota};
use governor::clock::{Clock, DefaultClock};
use rapidhash::fast::SeedableState;

const INTERVAL: Duration = Duration::from_millis(1);
const BURST: u32 = 20;

fn quota() -> governor::Quota {
    governor::Quota::with_period(INTERVAL)
        .unwrap()
        .allow_burst(NonZeroU32::new(BURST).unwrap())
}

fn ratelimiter() -> ratelimit::Ratelimiter {
    ratelimit::Ratelimiter::builder(1)
        .period(INTERVAL)
        .max_tokens(u64::from(BURST))
        .initial_available(u64::from(BURST))
        .build()
        .unwrap()
}

fn ip_key(key: usize) -> IpAddr {
    IpAddr::V6(((key as u128) << 64).into())
}

fn rss() -> Result<usize, Box<dyn Error>> {
    let status = std::fs::read_to_string("/proc/self/smaps_rollup")?;
    let line = status.lines().find(|line| line.starts_with("Rss:"));
    let kilobytes = line
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing Linux RSS measurement")?
        .parse::<usize>()?;
    Ok(kilobytes * 1024)
}

fn measure<T>(name: &str, count: usize, build: impl FnOnce() -> T) -> Result<(), Box<dyn Error>> {
    let before = rss()?;
    let value = build();
    black_box(&value);
    let retained = rss()?.saturating_sub(before);
    // Churn counts processed keys, not stored keys, so a per-key ratio would mislead.
    if count == 0 || name == "bottlecap_churn" {
        println!("{name},{count},{retained},");
    } else {
        println!(
            "{name},{count},{retained},{:.2}",
            retained as f64 / count as f64
        );
    }
    black_box(value);
    Ok(())
}

fn child(name: &str, count: usize) -> Result<(), Box<dyn Error>> {
    let config = Quota::per(INTERVAL, 1).with_burst(BURST);
    // Initialize clock globals before the baseline, including Quanta calibration.
    let _ = black_box(Limiter::new(config).check());
    black_box(DefaultClock::default().now());
    match name {
        "bottlecap_empty" => measure(name, 0, || {
            KeyedLimiter::<u64, _>::with_hasher(config, SeedableState::fixed())
        }),
        "bottlecap_direct" => measure(name, count, || {
            (0..count).map(|_| Limiter::new(config)).collect::<Vec<_>>()
        }),
        "governor_direct" => measure(name, count, || {
            (0..count)
                .map(|_| governor::RateLimiter::direct(quota()))
                .collect::<Vec<_>>()
        }),
        "ratelimit_direct" => measure(name, count, || {
            (0..count).map(|_| ratelimiter()).collect::<Vec<_>>()
        }),
        "bottlecap_keyed" | "bottlecap_reserved" => measure(name, count, || {
            let limiter =
                KeyedLimiter::with_hasher(config, SeedableState::fixed()).with_max_keys(count);
            let limiter = if name == "bottlecap_reserved" {
                limiter.with_capacity(count)
            } else {
                limiter
            };
            for key in 0..count {
                let _ = limiter.check(&(key as u64));
            }
            limiter
        }),
        "bottlecap_churn" => measure(name, count, || {
            let capacity = count.min(4096);
            let limiter =
                KeyedLimiter::with_hasher(config, SeedableState::fixed()).with_max_keys(capacity);
            let epoch = Instant::now();
            for key in 0..count {
                let now = epoch + Duration::from_millis((key / capacity) as u64);
                assert!(limiter.check_at(&(key as u64), now).is_ok());
            }
            limiter
        }),
        "bottlecap_ip" => measure(name, count, || {
            let limiter =
                KeyedLimiter::with_hasher(config, SeedableState::fixed()).with_max_keys(count);
            for key in 0..count {
                assert!(limiter.check(&ip_key(key)).is_ok());
            }
            limiter
        }),
        "governor_ip" => measure(name, count, || {
            let limiter =
                governor::RateLimiter::dashmap_with_hasher(quota(), SeedableState::fixed());
            for key in 0..count {
                assert!(limiter.check_key(&ip_key(key)).is_ok());
            }
            limiter
        }),
        "ratelimit_ip" => measure(name, count, || {
            let mut map = HashMap::with_hasher(SeedableState::fixed());
            for key in 0..count {
                map.insert(ip_key(key), ratelimiter());
            }
            map
        }),
        "governor_keyed" => measure(name, count, || {
            let limiter =
                governor::RateLimiter::dashmap_with_hasher(quota(), SeedableState::fixed());
            for key in 0..count {
                let _ = limiter.check_key(&(key as u64));
            }
            limiter
        }),
        "ratelimit_keyed" => measure(name, count, || {
            let mut map = HashMap::with_hasher(SeedableState::fixed());
            for key in 0..count {
                map.insert(key as u64, ratelimiter());
            }
            map
        }),
        _ => Err("unknown backend".into()),
    }
}

fn run_case(name: &str, count: usize) -> Result<(), Box<dyn Error>> {
    let status = Command::new(std::env::current_exe()?)
        .args(["--child", name, &count.to_string()])
        .status()?;
    if !status.success() {
        return Err(format!("memory measurement failed for {name}").into());
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    if args.first().is_some_and(|arg| arg == "--child") {
        return child(
            args.get(1).ok_or("missing backend")?,
            args.get(2).ok_or("missing count")?.parse()?,
        );
    }
    let counts = if args.is_empty() {
        vec![100_000, 1_000_000]
    } else {
        args.iter()
            .map(|arg| arg.parse())
            .collect::<Result<Vec<usize>, _>>()?
    };
    if counts.contains(&0) {
        return Err("key counts must be positive".into());
    }
    eprintln!(
        "inline bytes: bottlecap={}, governor={}, ratelimit={}",
        size_of::<Limiter>(),
        size_of_val(&governor::RateLimiter::direct(quota())),
        size_of::<ratelimit::Ratelimiter>(),
    );
    println!("backend,keys,retained_rss_delta_bytes,bytes_per_key");
    run_case("bottlecap_empty", 0)?;
    for count in counts {
        for name in [
            "bottlecap_direct",
            "governor_direct",
            "ratelimit_direct",
            "bottlecap_keyed",
            "bottlecap_reserved",
            "bottlecap_churn",
            "governor_keyed",
            "ratelimit_keyed",
            "bottlecap_ip",
            "governor_ip",
            "ratelimit_ip",
        ] {
            run_case(name, count)?;
        }
    }
    Ok(())
}
