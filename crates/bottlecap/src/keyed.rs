use std::borrow::Borrow;
use std::future::Future;
use std::hash::{BuildHasher, Hash};
use std::time::Duration;

use crate::store::ShardedStore;
use crate::{Clock, DefaultClock, DefaultHashBuilder, Gcra, Instant, Quota, Rejection};

/// A rate limiter that tracks a separate limit for each key.
///
/// By default, checks for missing keys reclaim fully replenished keys in rotating
/// batches. Existing-key checks do not trigger cleanup, and no background task runs.
/// Use [`Self::with_auto_prune`] to disable proactive cleanup or [`Self::prune`]
/// for a full cleanup.
pub struct KeyedLimiter<K, S = DefaultHashBuilder, C = DefaultClock> {
    quota: Gcra,
    state: ShardedStore<K, S>,
    clock: C,
}

impl<K: Eq + Hash> KeyedLimiter<K> {
    /// Create a limiter with a separate rate limit for each key.
    ///
    /// ```
    /// use bottlecap::{KeyedLimiter, Quota};
    /// let clients = KeyedLimiter::new(Quota::per_second(10));
    /// assert!(clients.check(&"alice").is_ok());
    /// ```
    pub fn new(quota: Quota) -> Self {
        Self::with_hasher(quota, DefaultHashBuilder::default())
    }
}

impl<K: Eq + Hash, S: BuildHasher + Clone> KeyedLimiter<K, S> {
    /// Create a keyed limiter with a custom hash builder.
    ///
    /// Use collision-resistant hashing for untrusted keys.
    ///
    /// ```
    /// use bottlecap::{KeyedLimiter, Quota};
    /// use std::collections::hash_map::RandomState;
    /// let clients = KeyedLimiter::with_hasher(Quota::per_second(10), RandomState::new());
    /// assert!(clients.check(&"alice").is_ok());
    /// ```
    pub fn with_hasher(quota: Quota, hasher: S) -> Self {
        Self {
            quota: quota.0,
            state: ShardedStore::new(hasher),
            clock: DefaultClock,
        }
    }
}

impl<K: Eq + Hash + Clone, S: BuildHasher + Clone, C: Clock> KeyedLimiter<K, S, C> {
    /// The rate and burst configuration shared by all keys.
    pub fn quota(&self) -> Quota {
        Quota(self.quota)
    }

    /// The number of requests available for a key, without storing it.
    pub fn remaining<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(&self, key: &Q) -> u32
    where
        K: Borrow<Q>,
    {
        self.remaining_at(key, self.clock.now())
    }

    /// A key's available requests at a caller-supplied time.
    pub fn remaining_at<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        now: Instant,
    ) -> u32
    where
        K: Borrow<Q>,
    {
        self.state.remaining_at::<Q>(&self.quota, key, now)
    }

    /// Switch to a custom clock without resetting limits or removing keys.
    pub fn with_clock<D: Clock>(self, clock: D) -> KeyedLimiter<K, S, D> {
        KeyedLimiter {
            quota: self.quota,
            state: self.state,
            clock,
        }
    }

    /// The number of stored keys, including idle keys not yet pruned.
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// True if no keys are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove a key and reset its rate limit.
    ///
    /// Returns `true` if the key was stored. Its next check starts with the maximum
    /// number of requests available, provided there is room for the key.
    ///
    /// ```
    /// use bottlecap::{KeyedLimiter, Quota};
    /// let clients = KeyedLimiter::new(Quota::per_minute(1));
    /// assert!(clients.check(&"alice").is_ok());
    /// assert!(clients.reset(&"alice"));
    /// assert!(clients.check(&"alice").is_ok());
    /// ```
    pub fn reset<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.state.reset::<Q>(key)
    }

    /// Set the maximum number of stored keys.
    ///
    /// When full, checks for new keys try to remove idle keys before rejecting the request.
    /// Lowering the limit does not remove existing keys.
    /// Panics if `max_keys` is zero.
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.state = self.state.with_max_keys(max_keys);
        self
    }

    /// Enable or disable proactive cleanup during missing-key checks. Enabled by default.
    ///
    /// Disabling this leaves manual pruning and cleanup at the
    /// [`with_max_keys`](Self::with_max_keys) storage limit enabled.
    /// Without a key limit, disabled cleanup retains keys until explicitly removed.
    pub fn with_auto_prune(mut self, enabled: bool) -> Self {
        self.state = self.state.with_auto_prune(enabled);
        self
    }

    /// Reserve storage for approximately `expected_keys` without setting a key limit.
    pub fn with_capacity(mut self, expected_keys: usize) -> Self {
        self.state = self.state.with_capacity(expected_keys);
        self
    }

    /// Remove fully replenished keys and return the number removed.
    pub fn prune(&self) -> usize {
        self.prune_at(self.clock.now())
    }

    /// Remove fully replenished keys at the supplied time.
    pub fn prune_at(&self, now: Instant) -> usize {
        self.state.prune_at(now)
    }

    /// Remove fully replenished keys in one rotating batch and return the number removed.
    ///
    /// Uses the same batch sizing and cursor as automatic cleanup. Small stores
    /// may fit in one batch. Call [`prune`](Self::prune) for a full cleanup.
    pub fn prune_partial(&self) -> usize {
        self.prune_partial_at(self.clock.now())
    }

    /// Remove fully replenished keys in one rotating batch at the supplied time.
    pub fn prune_partial_at(&self, now: Instant) -> usize {
        self.state.prune_partial_at(now)
    }

    /// Consume one request for `key`, or get a retry delay.
    ///
    /// Requests are rejected when their per-key limit is reached or there is no
    /// room to store a new key.
    /// An owned key is created only when a new entry is inserted.
    #[inline]
    pub fn check<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(&self, key: &Q) -> Result<(), Duration>
    where
        K: Borrow<Q>,
    {
        self.check_at(key, self.clock.now())
    }

    /// Consume one request for a key at the supplied time.
    #[inline]
    pub fn check_at<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        now: Instant,
    ) -> Result<(), Duration>
    where
        K: Borrow<Q>,
    {
        self.state.check_at::<true, Q>(&self.quota, key, now)
    }

    /// Consume a batch of `requests` for a key atomically.
    ///
    /// Rejected batches do not count toward the limit.
    /// Zero requests succeed without storing a key. Counts above the burst return
    /// [`Rejection::InsufficientCapacity`] before accessing storage.
    #[inline]
    pub fn check_n<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        requests: u32,
    ) -> Result<(), Rejection>
    where
        K: Borrow<Q>,
    {
        self.check_n_at(key, requests, self.clock.now())
    }

    /// Consume a batch of `requests` for a key at the supplied time.
    #[inline]
    pub fn check_n_at<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        requests: u32,
        now: Instant,
    ) -> Result<(), Rejection>
    where
        K: Borrow<Q>,
    {
        if requests == 0 {
            return Ok(());
        }
        self.state
            .check_at::<true, Q>(&self.quota.batch(requests)?, key, now)
            .map_err(Rejection::from)
    }

    /// Check one request without counting it toward the limit or storing the key.
    ///
    /// This may remove idle keys in a batch, but does not reserve space for the key.
    #[inline]
    pub fn peek<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(&self, key: &Q) -> Result<(), Duration>
    where
        K: Borrow<Q>,
    {
        self.peek_at(key, self.clock.now())
    }

    /// Check one request at the supplied time without counting it toward the limit.
    #[inline]
    pub fn peek_at<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        now: Instant,
    ) -> Result<(), Duration>
    where
        K: Borrow<Q>,
    {
        self.state.check_at::<false, Q>(&self.quota, key, now)
    }

    /// Check a batch without counting it toward the limit or storing the key.
    ///
    /// This may remove idle keys in a batch, but does not reserve space for the key.
    #[inline]
    pub fn peek_n<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        requests: u32,
    ) -> Result<(), Rejection>
    where
        K: Borrow<Q>,
    {
        self.peek_n_at(key, requests, self.clock.now())
    }

    /// Check a batch at the supplied time without counting it toward the limit.
    #[inline]
    pub fn peek_n_at<Q: Eq + Hash + ToOwned<Owned = K> + ?Sized>(
        &self,
        key: &Q,
        requests: u32,
        now: Instant,
    ) -> Result<(), Rejection>
    where
        K: Borrow<Q>,
    {
        if requests == 0 {
            return Ok(());
        }
        self.state
            .check_at::<false, Q>(&self.quota.batch(requests)?, key, now)
            .map_err(Rejection::from)
    }

    /// Wait for and consume one request for a key using an async sleep function.
    ///
    /// Other calls can use available requests while this future sleeps, so waiting
    /// calls are not guaranteed to complete in order. Dropping the future while
    /// sleeping does not use any requests.
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), bottlecap::Rejection> {
    /// use bottlecap::{KeyedLimiter, Quota};
    ///
    /// let clients = KeyedLimiter::new(Quota::per_second(10));
    /// clients.wait(&"alice", tokio::time::sleep).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait<F, Fut>(&self, key: &K, sleep: F) -> Result<(), Rejection>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.wait_n(key, 1, sleep).await
    }

    /// Wait for and consume a batch of `requests` for a key atomically.
    ///
    /// Batches larger than the burst fail without sleeping. The sleep function
    /// must yield to the runtime and should wait at least the supplied duration
    /// to avoid a busy loop.
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
