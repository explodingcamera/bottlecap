use std::future::Future;
use std::hash::{BuildHasher, Hash};
use std::time::Duration;

use crate::store::ShardedStore;
use crate::{Clock, DefaultClock, DefaultHashBuilder, Gcra, Instant, Quota, Rejection};

/// A rate limiter that tracks a separate limit for each key.
pub struct KeyedLimiter<K, S = DefaultHashBuilder, C = DefaultClock> {
    quota: Gcra,
    state: ShardedStore<K, S>,
    clock: C,
}

impl<K: Eq + Hash> KeyedLimiter<K> {
    /// Create per-key allowances without a fixed key limit.
    /// Use `with_max_keys` to cap stored keys, or call `prune` periodically.
    pub fn new(quota: Quota) -> Self {
        Self::with_hasher(quota, DefaultHashBuilder::default())
    }
}

impl<K: Eq + Hash, S: BuildHasher + Clone> KeyedLimiter<K, S> {
    /// Create per-key allowances with a custom hash builder.
    /// Use collision-resistant hashing for untrusted keys.
    pub fn with_hasher(quota: Quota, hasher: S) -> Self {
        Self {
            quota: quota.0,
            state: ShardedStore::new(hasher),
            clock: DefaultClock,
        }
    }
}

impl<K: Eq + Hash + Clone, S: BuildHasher + Clone, C: Clock> KeyedLimiter<K, S, C> {
    /// Return the rate and burst configuration shared by all keys.
    pub fn quota(&self) -> Quota {
        Quota(self.quota)
    }

    /// Return a key's available requests, without consuming allowance or storing the key.
    pub fn remaining(&self, key: &K) -> u32 {
        self.remaining_at(key, self.clock.now())
    }

    /// Return a key's available requests using a caller-supplied current time.
    pub fn remaining_at(&self, key: &K, now: Instant) -> u32 {
        self.state.remaining_at(&self.quota, key, now)
    }

    /// Use a custom clock without changing stored keys or consumed allowance.
    pub fn with_clock<D: Clock>(self, clock: D) -> KeyedLimiter<K, S, D> {
        KeyedLimiter {
            quota: self.quota,
            state: self.state,
            clock,
        }
    }

    /// Return the number of stored keys, including idle keys not yet pruned.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Return whether no keys are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Forget a key's allowance, returning whether it was stored.
    /// Its next check starts full, subject to the key capacity.
    pub fn reset(&self, key: &K) -> bool {
        self.state.reset(key)
    }

    /// Cap stored keys, rejecting unknown keys while storage is full.
    /// Checks reclaim idle keys in small batches. Call [`prune`](Self::prune) for a full pass.
    /// Existing keys above a newly lowered limit remain until they can be pruned.
    ///
    /// # Panics
    ///
    /// Panics if `max_keys` is zero.
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.state = self.state.with_max_keys(max_keys);
        self
    }

    /// Preallocate for approximately `expected_keys`, without limiting stored keys.
    pub fn with_capacity(mut self, expected_keys: usize) -> Self {
        self.state = self.state.with_capacity(expected_keys);
        self
    }

    /// Remove fully replenished entries, returning the number removed.
    /// Oversized buckets shrink after removals, while shard chunks remain reusable.
    pub fn prune(&self) -> usize {
        self.prune_at(self.clock.now())
    }

    /// Remove fully replenished entries using a caller-supplied current time.
    pub fn prune_at(&self, now: Instant) -> usize {
        self.state.prune_at(now)
    }

    /// Consume allowance for `key`, or return a quota or storage-capacity retry delay.
    /// The key is cloned only when a new entry is inserted.
    #[inline]
    pub fn check(&self, key: &K) -> Result<(), Duration> {
        self.check_at(key, self.clock.now())
    }

    /// Check a key using a caller-supplied current time.
    #[inline]
    pub fn check_at(&self, key: &K, now: Instant) -> Result<(), Duration> {
        self.state.check_at::<true>(&self.quota, key, now)
    }

    /// Consume `requests` for a key atomically, or leave its allowance unchanged.
    /// Zero requests succeed without storing a key. Counts above the burst return
    /// [`Rejection::InsufficientCapacity`] before accessing storage.
    #[inline]
    pub fn check_n(&self, key: &K, requests: u32) -> Result<(), Rejection> {
        self.check_n_at(key, requests, self.clock.now())
    }

    /// Consume `requests` for a key using a caller-supplied current time.
    #[inline]
    pub fn check_n_at(&self, key: &K, requests: u32, now: Instant) -> Result<(), Rejection> {
        if requests == 0 {
            return Ok(());
        }
        self.state
            .check_at::<true>(&self.quota.batch(requests)?, key, now)
            .map_err(Rejection::from)
    }

    /// Check allowance without consuming it or inserting the key.
    /// May prune idle keys at the key limit. Does not reserve allowance or a key slot.
    #[inline]
    pub fn peek(&self, key: &K) -> Result<(), Duration> {
        self.peek_at(key, self.clock.now())
    }

    /// Check a key without consuming allowance, using a caller-supplied current time.
    #[inline]
    pub fn peek_at(&self, key: &K, now: Instant) -> Result<(), Duration> {
        self.state.check_at::<false>(&self.quota, key, now)
    }

    /// Check whether `requests` fit without consuming allowance or inserting the key.
    /// May prune idle keys at the key limit. Does not reserve allowance or a key slot.
    #[inline]
    pub fn peek_n(&self, key: &K, requests: u32) -> Result<(), Rejection> {
        self.peek_n_at(key, requests, self.clock.now())
    }

    /// Check a batch for a key without consuming it, using a caller-supplied current time.
    #[inline]
    pub fn peek_n_at(&self, key: &K, requests: u32, now: Instant) -> Result<(), Rejection> {
        if requests == 0 {
            return Ok(());
        }
        self.state
            .check_at::<false>(&self.quota.batch(requests)?, key, now)
            .map_err(Rejection::from)
    }

    /// Wait for and consume one request for a key using an async sleep function.
    /// Waiting reserves neither allowance nor a key slot and guarantees no fairness.
    /// Dropping the future while sleeping consumes nothing.
    pub async fn wait<F, Fut>(&self, key: &K, sleep: F) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.wait_n(key, 1, sleep).await
    }

    /// Wait for and atomically consume `requests` for a key. Impossible batches
    /// return without sleeping. The sleep function must yield to the runtime and
    /// should wait at least the supplied duration to avoid busy retrying.
    pub async fn wait_n<F, Fut>(
        &self,
        key: &K,
        requests: u32,
        mut sleep: F,
    ) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        loop {
            match self.check_n(key, requests) {
                Err(Rejection::RetryAfter(delay)) => sleep(delay).await,
                result => return result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Barrier;
    use std::task::{Context, Poll, Waker};
    use std::thread;

    use super::*;

    #[test]
    fn independent_allowances_and_reset() {
        let limiter = KeyedLimiter::new(Quota::per_second(1).with_burst(2)).with_max_keys(2);
        let now = Instant::now();
        for key in ["alice", "bob"] {
            assert_eq!(limiter.check_n_at(&key, 2, now), Ok(()));
            assert_eq!(limiter.check_at(&key, now), Err(Duration::from_secs(1)));
        }
        assert_eq!(limiter.len(), 2);
        assert!(limiter.check_at(&"charlie", now).is_err());
        assert!(!limiter.reset(&"missing"));
        assert!(limiter.reset(&"alice"));
        assert_eq!(limiter.check_n_at(&"charlie", 2, now), Ok(()));
        assert_eq!(limiter.prune_at(now + Duration::from_secs(2)), 2);
        assert!(limiter.is_empty());
    }

    #[test]
    fn checks_and_peeks_respect_capacity() {
        let limiter = KeyedLimiter::new(Quota::per_hour(1).with_burst(2)).with_max_keys(1);
        let now = Instant::now();
        assert_eq!(limiter.check_n_at(&0, 2, now), Ok(()));
        assert_eq!(limiter.peek_at(&1, now), Err(Duration::from_millis(1)));
        assert_eq!(
            limiter.check_n_at(&1, 1, now),
            Err(Rejection::RetryAfter(Duration::from_millis(1)))
        );
        let later = now + Duration::from_secs(7200);
        assert_eq!(limiter.peek_n_at(&1, 2, later), Ok(()));
        assert!(limiter.is_empty());
        assert_eq!(limiter.check_n_at(&1, 2, later), Ok(()));
        assert_eq!(limiter.len(), 1);
    }

    #[test]
    fn unused_allowance_does_not_store_keys() {
        for quota in [Quota::per_second(1), Quota::per_second(0)] {
            let limiter = KeyedLimiter::new(quota);
            assert_eq!(limiter.check_n(&0, 0), Ok(()));
            assert_eq!(limiter.check_n(&0, 2), Err(Rejection::InsufficientCapacity));
            assert_eq!(limiter.peek_n(&0, 0), Ok(()));
            assert_eq!(limiter.peek_n(&0, 2), Err(Rejection::InsufficientCapacity));
            let _ = limiter.peek(&0);
            assert!(limiter.is_empty());
        }
    }

    #[test]
    fn reservation_preserves_allowance() {
        let mut limiter = KeyedLimiter::new(Quota::per_hour(1));
        let now = Instant::now();
        for key in 0..100 {
            assert_eq!(limiter.check_at(&key, now), Ok(()));
        }
        for capacity in [0, 16_384, 200_000] {
            limiter = limiter.with_capacity(capacity);
            assert_eq!(limiter.len(), 100);
            for key in 0..100 {
                assert!(limiter.check_at(&key, now).is_err());
            }
        }
    }

    #[test]
    fn custom_clock_drives_waiting_and_pruning() {
        let now = Cell::new(Instant::now());
        let limiter =
            KeyedLimiter::new(Quota::per_second(1).with_burst(2)).with_clock(|| now.get());
        assert_eq!(limiter.check_n(&0, 2), Ok(()));
        assert_eq!(limiter.prune(), 0);
        let mut sleeps = 0;
        let sleep = |delay| {
            assert_eq!(delay, Duration::from_secs(2));
            sleeps += 1;
            now.set(now.get() + delay);
            std::future::ready(())
        };
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(
            std::pin::pin!(limiter.wait_n(&0, 2, sleep))
                .as_mut()
                .poll(&mut context),
            Poll::Ready(Ok(()))
        );
        assert_eq!(sleeps, 1);
        assert_eq!(limiter.remaining(&0), 0);
        now.set(now.get() + Duration::from_secs(2));
        assert_eq!(limiter.prune(), 1);
    }

    #[test]
    fn concurrent_batches_and_refill() {
        let limiter = KeyedLimiter::new(Quota::per(Duration::from_nanos(10), 1).with_burst(100));
        let epoch = Instant::now();
        for (elapsed, expected) in [(0, 33), (20, 1)] {
            let now = epoch + Duration::from_nanos(elapsed);
            let start = Barrier::new(16);
            let admitted = thread::scope(|scope| {
                let workers: Vec<_> = (0..16)
                    .map(|_| {
                        scope.spawn(|| {
                            start.wait();
                            (0..100)
                                .filter(|_| limiter.check_n_at(&0, 3, now).is_ok())
                                .count()
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .map(|worker| worker.join().unwrap())
                    .sum::<usize>()
            });
            assert_eq!(admitted, expected);
        }
    }
}
