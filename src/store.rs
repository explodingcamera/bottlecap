use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock, RwLock};
use std::time::Duration;

use hashbrown::HashTable;

use crate::{DefaultHashBuilder, Gcra, Instant};

const SHARDS: usize = 4096;
const SHARDS_PER_CHUNK: usize = 64;
const MAX_VECTOR_KEYS: usize = 4;
const CLEANUP_KEYS: usize = 4096;
const MAX_CLEANUP_INTERVAL: u64 = 1_000_000;

type ShardChunk<K> = OnceLock<Box<[Shard<K>]>>;

/// A sharded concurrent map of keys to GCRA timestamps for [`crate::KeyedLimiter`].
/// Optimized for existing-key checks and sparse storage, not general-purpose use.
///
/// Structural changes take the insertion mutex before any shard lock.
/// Timestamp references must not outlive their shard lock.
pub(crate) struct ShardedStore<K, S = DefaultHashBuilder> {
    shards: Vec<ShardChunk<K>>,
    hasher: S,
    insertion: Mutex<Capacity>,
}

#[repr(align(64))]
struct Shard<K>(RwLock<Bucket<K>>);

struct Entry<K> {
    hash: u64,
    key: K,
    arrival: AtomicU64,
}

// Empty small buckets allocate nothing. The first key lives inline, and larger
// buckets reuse the hash used to select the shard.
enum Bucket<K> {
    One(Entry<K>),
    Small(SmallBucket<K>),
    Dense(HashTable<(K, AtomicU64)>),
}

struct SmallBucket<K> {
    entries: Vec<(K, AtomicU64)>,
    // Occupied fingerprints have the high bit set, unused slots are zero.
    fingerprints: [u8; MAX_VECTOR_KEYS],
}

impl<K: Eq> Default for Bucket<K> {
    fn default() -> Self {
        Self::Small(SmallBucket::new([]))
    }
}

impl<K: Eq> SmallBucket<K> {
    fn new(entries: impl IntoIterator<Item = Entry<K>>) -> Self {
        let mut fingerprints = [0; MAX_VECTOR_KEYS];
        let entries = entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                fingerprints[index] = (entry.hash >> 57) as u8 | 0x80;
                (entry.key, entry.arrival)
            })
            .collect();
        Self {
            entries,
            fingerprints,
        }
    }

    fn push(&mut self, entry: Entry<K>) {
        self.fingerprints[self.entries.len()] = (entry.hash >> 57) as u8 | 0x80;
        self.entries.push((entry.key, entry.arrival));
    }

    fn remove(&mut self, index: usize) {
        let last = self.entries.len() - 1;
        self.entries.swap_remove(index);
        self.fingerprints.swap(index, last);
        self.fingerprints[last] = 0;
    }

    fn retain(&mut self, mut keep: impl FnMut(&AtomicU64) -> bool) {
        let mut index = 0;
        while index < self.entries.len() {
            if keep(&self.entries[index].1) {
                index += 1;
            } else {
                self.remove(index);
            }
        }
    }

    #[inline]
    fn get(&self, hash: u64, key: &K) -> Option<&AtomicU64> {
        let fingerprint = (hash >> 57) as u32 | 0x80;
        let difference = u32::from_le_bytes(self.fingerprints) ^ (fingerprint * 0x0101_0101);
        // One high bit per matching byte. Subtraction may also mark a later byte
        // after a borrow, so verify each candidate's key.
        let mut matches = difference.wrapping_sub(0x0101_0101) & !difference & 0x8080_8080;
        while matches != 0 {
            let index = matches.trailing_zeros() as usize / 8;
            let entry = &self.entries[index];
            if entry.0 == *key {
                return Some(&entry.1);
            }
            std::hint::cold_path();
            matches &= matches - 1;
        }
        None
    }
}

impl<K: Eq + Hash> Bucket<K> {
    #[inline]
    fn get(&self, hash: u64, key: &K) -> Option<&AtomicU64> {
        match self {
            Self::One(entry) => (entry.hash == hash && entry.key == *key).then_some(&entry.arrival),
            Self::Small(bucket) => bucket.get(hash, key),
            Self::Dense(entries) => entries
                .find(hash, |(stored, _)| stored == key)
                .map(|(_, arrival)| arrival),
        }
    }

    /// Insert an absent key. The caller must exclude concurrent insertions and
    /// check for an existing entry to avoid discarding its consumed allowance.
    fn insert<S: BuildHasher>(&mut self, entry: Entry<K>, hasher: &S) {
        if matches!(self, Self::Small(bucket) if bucket.entries.len() == MAX_VECTOR_KEYS) {
            self.reserve(MAX_VECTOR_KEYS + 1, hasher);
        }
        match self {
            // Preserve explicitly reserved vectors, otherwise keep the first key inline.
            Self::Small(bucket) if bucket.entries.capacity() == 0 => *self = Self::One(entry),
            Self::One(_) => {
                let Self::One(first) = std::mem::take(self) else {
                    unreachable!()
                };
                *self = Self::Small(SmallBucket::new([first, entry]));
            }
            Self::Small(bucket) => bucket.push(entry),
            Self::Dense(entries) => {
                entries.insert_unique(entry.hash, (entry.key, entry.arrival), |(key, _)| {
                    hasher.hash_one(key)
                });
            }
        }
    }

    fn reserve<S: BuildHasher>(&mut self, capacity: usize, hasher: &S) {
        match self {
            Self::Dense(entries) => {
                entries.reserve(capacity.saturating_sub(entries.len()), |(key, _)| {
                    hasher.hash_one(key)
                });
            }
            Self::Small(bucket)
                if capacity <= MAX_VECTOR_KEYS && bucket.entries.capacity() != 0 =>
            {
                bucket
                    .entries
                    .reserve(capacity.saturating_sub(bucket.entries.len()));
            }
            _ if capacity <= MAX_VECTOR_KEYS => {
                let mut bucket = SmallBucket {
                    entries: Vec::with_capacity(capacity),
                    fingerprints: [0; MAX_VECTOR_KEYS],
                };
                if let Self::One(entry) = std::mem::take(self) {
                    bucket.push(entry);
                }
                *self = Self::Small(bucket);
            }
            _ => {
                let mut entries = HashTable::with_capacity(capacity);
                match std::mem::take(self) {
                    Self::One(entry) => {
                        entries.insert_unique(
                            entry.hash,
                            (entry.key, entry.arrival),
                            |(key, _)| hasher.hash_one(key),
                        );
                    }
                    Self::Small(bucket) => {
                        for entry in bucket.entries {
                            entries.insert_unique(hasher.hash_one(&entry.0), entry, |(key, _)| {
                                hasher.hash_one(key)
                            });
                        }
                    }
                    Self::Dense(_) => unreachable!(),
                }
                *self = Self::Dense(entries);
            }
        }
    }

    fn retain<S: BuildHasher>(
        &mut self,
        mut keep: impl FnMut(&AtomicU64) -> bool,
        hasher: &S,
    ) -> usize {
        let removed = match self {
            Self::One(entry) => {
                if keep(&entry.arrival) {
                    0
                } else {
                    *self = Self::default();
                    1
                }
            }
            Self::Small(bucket) => {
                let before = bucket.entries.len();
                bucket.retain(keep);
                before - bucket.entries.len()
            }
            Self::Dense(entries) => {
                let before = entries.len();
                entries.retain(|(_, arrival)| keep(arrival));
                before - entries.len()
            }
        };
        if removed > 0 {
            self.compact(hasher);
        }
        removed
    }

    fn remove<S: BuildHasher>(&mut self, hash: u64, key: &K, hasher: &S) -> bool {
        let removed = match self {
            Self::One(entry) => {
                if entry.hash != hash || entry.key != *key {
                    return false;
                }
                *self = Self::default();
                true
            }
            Self::Small(bucket) => {
                let Some(index) = bucket.entries.iter().position(|(stored, _)| stored == key)
                else {
                    return false;
                };
                bucket.remove(index);
                true
            }
            Self::Dense(entries) => match entries.find_entry(hash, |(stored, _)| stored == key) {
                Ok(entry) => {
                    entry.remove();
                    true
                }
                Err(_) => false,
            },
        };
        if removed {
            self.compact(hasher);
        }
        removed
    }

    /// Release oversized buckets after removals. Separate growth and shrink
    /// thresholds avoid repeatedly reallocating near a size boundary.
    fn compact<S: BuildHasher>(&mut self, hasher: &S) {
        match self {
            Self::Small(bucket) if bucket.entries.len() <= 1 => {
                *self = bucket
                    .entries
                    .pop()
                    .map_or_else(Self::default, |(key, arrival)| {
                        Self::One(Entry {
                            hash: hasher.hash_one(&key),
                            key,
                            arrival,
                        })
                    });
            }
            Self::Dense(entries) if entries.len() <= MAX_VECTOR_KEYS / 2 => {
                let mut compact = Self::default();
                for (key, arrival) in entries.drain() {
                    compact.insert(
                        Entry {
                            hash: hasher.hash_one(&key),
                            key,
                            arrival,
                        },
                        hasher,
                    );
                }
                *self = compact;
            }
            Self::Dense(entries) if entries.len() <= entries.num_buckets() / 4 => {
                // A few active keys must not retain a table sized for a traffic spike.
                // capacity() excludes tombstones, so compare allocated slots instead.
                entries.shrink_to_fit(|(key, _)| hasher.hash_one(key));
            }
            _ => {}
        }
    }
}

struct Capacity {
    max_keys: Option<usize>,
    used: usize,
    cleanup_at: u64,
    prune_cursor: usize,
    reclaimed_at: u64,
}

impl<K: Eq + Hash, S: BuildHasher> ShardedStore<K, S> {
    /// Create an uncapped store without allocating shard chunks.
    /// Chunks allocate on admission, keeping unused limiters small without a
    /// separate allocation for each shard. No background cleanup runs.
    pub(crate) fn new(hasher: S) -> Self {
        let shards = (0..SHARDS / SHARDS_PER_CHUNK)
            .map(|_| OnceLock::new())
            .collect();
        Self {
            shards,
            hasher,
            insertion: Mutex::new(Capacity {
                max_keys: None,
                used: 0,
                cleanup_at: u64::MAX,
                prune_cursor: 0,
                reclaimed_at: 0,
            }),
        }
    }
}

impl<K: Eq + Hash + Clone, S: BuildHasher + Clone> ShardedStore<K, S> {
    /// Serialize admission of unknown keys to enforce an exact global key limit.
    /// Recheck after locking because another check may have inserted the key.
    /// Reject timestamps older than the last reclamation to prevent recreating
    /// a full bucket for a time when the removed entry still had consumed allowance.
    #[cold]
    #[inline(never)]
    fn check_missing<const CONSUME: bool>(
        &self,
        quota: &Gcra,
        key: &K,
        now: u64,
        hash: u64,
        index: usize,
    ) -> Result<(), Duration> {
        let mut capacity = self.insertion.lock().expect("limiter state poisoned");
        if let Some(chunk) = self.shards[index / SHARDS_PER_CHUNK].get() {
            let entries = chunk[index % SHARDS_PER_CHUNK]
                .0
                .read()
                .expect("limiter state poisoned");
            if let Some(arrival) = entries.get(hash, key) {
                return if CONSUME {
                    quota.check_atomic(now, arrival)
                } else {
                    quota.check(now, arrival.load(Relaxed)).map(|_| ())
                };
            }
        }
        if now < capacity.reclaimed_at {
            return Err(Duration::from_nanos(capacity.reclaimed_at - now));
        }
        if let Some(max) = capacity.max_keys
            && capacity.used >= max
            && now >= capacity.cleanup_at
        {
            self.prune_locked(&mut capacity, now, Some(max));
            if capacity.used >= max {
                capacity.cleanup_at = now.saturating_add(quota.interval.min(MAX_CLEANUP_INTERVAL));
            }
        }
        if capacity.max_keys.is_some_and(|max| capacity.used >= max) {
            return Err(Duration::from_nanos(
                capacity.cleanup_at.saturating_sub(now),
            ));
        }
        let next = quota.check(now, 0)?;
        if !CONSUME {
            return Ok(());
        }
        // Allocate only after admission succeeds, including the capacity check.
        let chunk = self.shards[index / SHARDS_PER_CHUNK].get_or_init(|| {
            (0..SHARDS_PER_CHUNK)
                .map(|_| Shard(RwLock::new(Bucket::default())))
                .collect()
        });
        let shard = &chunk[index % SHARDS_PER_CHUNK];
        shard.0.write().expect("limiter state poisoned").insert(
            Entry {
                hash,
                key: key.clone(),
                arrival: AtomicU64::new(next),
            },
            &self.hasher,
        );
        capacity.used += 1;
        capacity.cleanup_at = capacity
            .cleanup_at
            .min(now.saturating_add(quota.interval.min(MAX_CLEANUP_INTERVAL)));
        Ok(())
    }

    /// Reclaim fully replenished entries, whose absence preserves allowance.
    /// The caller must hold the insertion mutex, and shard locks exclude
    /// checks while deciding which timestamps are safe to discard.
    ///
    /// Automatic cleanup scans a small batch of shards in large maps and stops
    /// once there is room. Explicit pruning scans every shard. The cursor rotates
    /// so later passes reach the rest of the map. Allocated chunks remain reusable.
    fn prune_locked(&self, capacity: &mut Capacity, now: u64, stop_below: Option<usize>) -> usize {
        if now < capacity.reclaimed_at {
            return 0;
        }
        let before = capacity.used;
        let shard_count = self.shards.len() * SHARDS_PER_CHUNK;
        // Estimate how many shards hold one batch of keys, keeping small maps cheap.
        let mut remaining = if stop_below.is_some() {
            (CLEANUP_KEYS * shard_count / capacity.used.max(1)).clamp(1, shard_count)
        } else {
            shard_count
        };
        'scan: while remaining > 0 {
            let index = capacity.prune_cursor;
            let offset = index % SHARDS_PER_CHUNK;
            let count = remaining.min(SHARDS_PER_CHUNK - offset);
            let Some(chunk) = self.shards[index / SHARDS_PER_CHUNK].get() else {
                capacity.prune_cursor = (index + count) & (shard_count - 1);
                remaining -= count;
                continue;
            };
            for shard in &chunk[offset..offset + count] {
                capacity.prune_cursor = (capacity.prune_cursor + 1) & (shard_count - 1);
                remaining -= 1;
                let mut entries = shard.0.write().expect("limiter state poisoned");
                capacity.used -=
                    entries.retain(|arrival| arrival.load(Relaxed) > now, &self.hasher);
                if stop_below.is_some_and(|max| capacity.used < max) {
                    break 'scan;
                }
            }
        }
        capacity.cleanup_at = 0;
        let removed = before - capacity.used;
        if removed > 0 {
            capacity.reclaimed_at = now;
        }
        removed
    }

    /// Return the number of stored keys, including idle keys not yet pruned.
    pub(crate) fn len(&self) -> usize {
        self.insertion.lock().expect("limiter state poisoned").used
    }

    /// Read a key's remaining allowance without inserting it or reclaiming other keys.
    pub(crate) fn remaining_at(&self, quota: &Gcra, key: &K, now: Instant) -> u32 {
        let hash = self.hasher.hash_one(key);
        let index = (hash >> 32) as usize & (self.shards.len() * SHARDS_PER_CHUNK - 1);
        if let Some(chunk) = self.shards[index / SHARDS_PER_CHUNK].get() {
            let entries = chunk[index % SHARDS_PER_CHUNK]
                .0
                .read()
                .expect("limiter state poisoned");
            if let Some(arrival) = entries.get(hash, key) {
                return quota.remaining(now.0, arrival.load(Relaxed));
            }
        }
        quota.remaining(now.0, 0)
    }

    /// Forget a key's allowance, returning whether it was stored.
    /// Unlike pruning, this deliberately discards consumed allowance. Updating
    /// the key count under the insertion mutex makes the freed slot reusable.
    pub(crate) fn reset(&self, key: &K) -> bool {
        let hash = self.hasher.hash_one(key);
        let index = (hash >> 32) as usize & (self.shards.len() * SHARDS_PER_CHUNK - 1);
        let mut capacity = self.insertion.lock().expect("limiter state poisoned");
        let Some(chunk) = self.shards[index / SHARDS_PER_CHUNK].get() else {
            return false;
        };
        let mut entries = chunk[index % SHARDS_PER_CHUNK]
            .0
            .write()
            .expect("limiter state poisoned");
        let removed = entries.remove(hash, key, &self.hasher);
        capacity.used -= usize::from(removed);
        removed
    }

    /// Cap stored keys, rejecting unknown keys while storage is full.
    /// Existing keys above a newly lowered limit remain until they can be pruned.
    /// Only an empty store can resize its directory, since resizing would change
    /// shard placement for existing keys.
    ///
    /// # Panics
    ///
    /// Panics if `max_keys` is zero.
    pub(crate) fn with_max_keys(mut self, max_keys: usize) -> Self {
        assert!(max_keys > 0, "key capacity must be positive");
        let capacity = self.insertion.get_mut().expect("limiter state poisoned");
        capacity.max_keys = Some(max_keys);
        if capacity.used == 0 {
            capacity.prune_cursor = 0;
            capacity.cleanup_at = u64::MAX;
            self.shards.resize_with(
                max_keys
                    .min(SHARDS)
                    .next_power_of_two()
                    .div_ceil(SHARDS_PER_CHUNK),
                OnceLock::new,
            );
            self.shards.shrink_to_fit();
        }
        self
    }

    /// Spread the expected key count across shards to reduce allocation on insertion.
    /// Small estimates keep the inline storage rather than allocating empty buckets.
    pub(crate) fn with_capacity(mut self, expected_keys: usize) -> Self {
        let max = self
            .insertion
            .get_mut()
            .expect("limiter state poisoned")
            .max_keys;
        let expected_keys = max.map_or(expected_keys, |max| expected_keys.min(max));
        let per_shard = expected_keys.div_ceil(self.shards.len() * SHARDS_PER_CHUNK);
        if per_shard <= 1 {
            return self;
        }
        for chunk in &mut self.shards {
            chunk.get_or_init(|| {
                (0..SHARDS_PER_CHUNK)
                    .map(|_| Shard(RwLock::new(Bucket::default())))
                    .collect()
            });
            let chunk = chunk.get_mut().unwrap();
            for shard in chunk.iter_mut() {
                shard
                    .0
                    .get_mut()
                    .expect("limiter state poisoned")
                    .reserve(per_shard, &self.hasher);
            }
        }
        self
    }

    /// Remove fully replenished entries using a caller-supplied current time.
    /// Scan all shards, regardless of the key limit or cleanup schedule.
    pub(crate) fn prune_at(&self, now: Instant) -> usize {
        let mut capacity = self.insertion.lock().expect("limiter state poisoned");
        self.prune_locked(&mut capacity, now.0, None)
    }

    /// Check allowance without taking the insertion mutex for existing keys.
    /// A shard read lock protects timestamps during atomic request-budget updates.
    /// Release that lock before entering the missing-key path to preserve lock order.
    #[inline]
    pub(crate) fn check_at<const CONSUME: bool>(
        &self,
        quota: &Gcra,
        key: &K,
        now: Instant,
    ) -> Result<(), Duration> {
        let hash = self.hasher.hash_one(key);
        // Dense maps use low bits for indexing and high bits for fingerprints.
        // Select shards from the middle so sharing a builder does not force
        // every key in a dense shard into the same table probe sequence.
        let index = (hash >> 32) as usize & (self.shards.len() * SHARDS_PER_CHUNK - 1);
        {
            if let Some(chunk) = self.shards[index / SHARDS_PER_CHUNK].get() {
                let shard = &chunk[index % SHARDS_PER_CHUNK];
                let entries = shard.0.read().expect("limiter state poisoned");
                if let Some(arrival) = entries.get(hash, key) {
                    return if CONSUME {
                        quota.check_atomic(now.0, arrival)
                    } else {
                        quota.check(now.0, arrival.load(Relaxed)).map(|_| ())
                    };
                }
            }
        }
        self.check_missing::<CONSUME>(quota, key, now.0, hash, index)
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasherDefault, Hasher};

    use super::*;
    use crate::{KeyedLimiter, Quota};

    // Map integer keys to known shards, so cleanup tests do not depend on random hashing.
    #[derive(Default)]
    struct ShardHasher(u64);

    impl Hasher for ShardHasher {
        fn write(&mut self, bytes: &[u8]) {
            self.0 = bytes
                .iter()
                .fold(0u64, |hash, byte| hash.wrapping_mul(31) + u64::from(*byte));
        }

        fn write_u64(&mut self, value: u64) {
            self.0 = value << 32;
        }

        fn finish(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn small_bucket_fingerprints_match_full_keys() {
        for len in 0..=4 {
            for fingerprint in 0..128u64 {
                let entries = (0..len).map(|key| Entry {
                    hash: ((fingerprint + key) % 128) << 57,
                    key,
                    arrival: AtomicU64::new(key + 1),
                });
                let bucket = SmallBucket::new(entries);
                for (key, _) in &bucket.entries {
                    let hash = ((fingerprint + key) % 128) << 57;
                    assert_eq!(bucket.get(hash, key).unwrap().load(Relaxed), key + 1);
                    assert!(bucket.get(hash, &99).is_none());
                }
                for query in 0..128u64 {
                    assert!(bucket.get((query << 57) | 1, &99).is_none());
                }
            }
        }
        // Distinct full hashes can share a fingerprint, and full hashes can collide too.
        let bucket = SmallBucket::new((0..4).map(|key| Entry {
            hash: key / 2,
            key,
            arrival: AtomicU64::new(key + 1),
        }));
        for key in 0..4 {
            assert_eq!(bucket.get(key / 2, &key).unwrap().load(Relaxed), key + 1);
        }
    }

    #[test]
    fn small_bucket_updates_preserve_fingerprints() {
        let make_bucket = || {
            SmallBucket::new((0u64..4).map(|key| Entry {
                hash: key << 57,
                key,
                arrival: AtomicU64::new(key),
            }))
        };
        for removed in 0..4 {
            let mut bucket = make_bucket();
            bucket.remove(removed);
            for key in 0u64..4 {
                assert_eq!(bucket.get(key << 57, &key).is_some(), key != removed as u64);
            }
            bucket.push(Entry {
                hash: 4 << 57,
                key: 4,
                arrival: AtomicU64::new(4),
            });
            assert_eq!(bucket.get(4 << 57, &4).unwrap().load(Relaxed), 4);
        }
        for mask in 0..16 {
            let mut bucket = make_bucket();
            bucket.retain(|arrival| mask & (1 << arrival.load(Relaxed)) != 0);
            for key in 0u64..4 {
                assert_eq!(
                    bucket.get(key << 57, &key).is_some(),
                    mask & (1 << key) != 0
                );
            }
            assert!(
                bucket.fingerprints[bucket.entries.len()..]
                    .iter()
                    .all(|&byte| byte == 0)
            );
            if bucket.entries.len() < 4 {
                bucket.push(Entry {
                    hash: 4 << 57,
                    key: 4,
                    arrival: AtomicU64::new(4),
                });
                assert!(bucket.get(4 << 57, &4).is_some());
            }
        }
    }

    #[test]
    fn empty_store_resizes_in_both_directions() {
        let store = ShardedStore::<u64>::new(DefaultHashBuilder::default()).with_max_keys(1);
        assert_eq!(store.shards.len(), 1);
        let store = store.with_max_keys(SHARDS);
        assert_eq!(store.shards.len(), SHARDS / SHARDS_PER_CHUNK);
        assert!(store.shards.iter().all(|chunk| chunk.get().is_none()));
    }

    #[test]
    fn small_reservations_avoid_geometric_growth() {
        let hasher = BuildHasherDefault::<ShardHasher>::default();
        for capacity in 2..=MAX_VECTOR_KEYS {
            let mut bucket = Bucket::default();
            bucket.reserve(capacity, &hasher);
            for key in 0..capacity as u64 {
                bucket.insert(
                    Entry {
                        hash: hasher.hash_one(key),
                        key,
                        arrival: AtomicU64::new(1),
                    },
                    &hasher,
                );
                let Bucket::Small(small) = &bucket else {
                    panic!("reserved storage should stay reusable");
                };
                assert_eq!(small.entries.capacity(), capacity);
            }
        }
    }

    #[test]
    fn capacity_cleanup_stops_when_room_is_available() {
        let limiter = KeyedLimiter::with_hasher(
            Quota::per(Duration::from_nanos(10), 1),
            BuildHasherDefault::<ShardHasher>::default(),
        )
        .with_max_keys(8);
        let start = Instant::now();
        for key in 0u64..8 {
            assert!(limiter.check_at(&key, start).is_ok());
        }
        let later = start + Duration::from_nanos(10);
        assert!(limiter.check_at(&0, later).is_ok());
        assert!(limiter.check_at(&8, later).is_ok());
        assert_eq!(limiter.check_at(&0, later), Err(Duration::from_nanos(10)));
        assert_eq!(limiter.check_at(&8, later), Err(Duration::from_nanos(10)));
        assert!(limiter.check_at(&9, later).is_ok());
        assert_eq!(limiter.prune_at(later), 5);

        // Emptying a map after a partial sweep must allow shrinking the directory.
        let limiter = KeyedLimiter::with_hasher(
            Quota::per(Duration::from_nanos(10), 1),
            BuildHasherDefault::<ShardHasher>::default(),
        )
        .with_max_keys(1024);
        for key in [500u64, 501] {
            assert!(limiter.check_at(&key, start).is_ok());
        }
        let limiter = limiter.with_max_keys(2);
        assert!(limiter.check_at(&502, later).is_ok());
        let next = later + Duration::from_nanos(10);
        assert_eq!(limiter.prune_at(next), 2);
        let limiter = limiter.with_max_keys(1);
        assert!(limiter.check_at(&0, next).is_ok());
        assert!(
            limiter
                .check_at(&1, next + Duration::from_nanos(10))
                .is_ok()
        );
    }

    #[test]
    fn capacity_cleanup_resumes_without_resetting_active_keys() {
        let limiter = KeyedLimiter::with_hasher(
            Quota::per_second(1).with_burst(2),
            BuildHasherDefault::<ShardHasher>::default(),
        )
        .with_max_keys(8192);
        let start = Instant::now();
        for key in 0u64..8192 {
            assert_eq!(limiter.check_at(&key, start), Ok(()));
            if key != 4095 {
                assert_eq!(limiter.check_at(&key, start), Ok(()));
            }
        }
        // Only the last shard contains an idle key. One check must not scan that far.
        let now = start + Duration::from_secs(1);
        for _ in 0..512 {
            assert_eq!(limiter.check_at(&8192, now), Err(Duration::from_millis(1)));
        }
        let admitted = (1..512).find(|step| {
            let now = now + Duration::from_millis(*step);
            let admitted = limiter.check_at(&8192, now).is_ok();
            assert_eq!(limiter.remaining_at(&0, now), 1);
            assert_eq!(limiter.len(), 8192);
            admitted
        });
        assert!(admitted.is_some());
        assert!(!limiter.reset(&4095));
        assert_eq!(limiter.prune_at(start + Duration::from_secs(3)), 8192);
    }

    #[test]
    fn bucket_transitions_preserve_colliding_keys() {
        // Each pair shares a hash, and every key maps to the same shard.
        // Distinct hashes also exercise the rehash callback during table growth.
        #[derive(Clone, PartialEq, Eq)]
        struct Key(u64);
        impl Hash for Key {
            fn hash<H: Hasher>(&self, state: &mut H) {
                state.write_u64((self.0 / 2) * SHARDS as u64);
            }
        }

        let limiter = KeyedLimiter::with_hasher(
            Quota::per(Duration::from_nanos(10), 1),
            BuildHasherDefault::<ShardHasher>::default(),
        )
        .with_max_keys(64);
        let now = Instant::now();
        for key in 0..33 {
            assert!(limiter.check_at(&Key(key), now).is_ok());
            assert_eq!(
                limiter.check_at(&Key(key), now),
                Err(Duration::from_nanos(10))
            );
        }
        let later = now + Duration::from_nanos(10);
        for key in 0..2 {
            assert!(limiter.check_at(&Key(key), later).is_ok());
        }
        assert_eq!(limiter.prune_at(later), 31);
        for key in 0..2 {
            assert_eq!(
                limiter.check_at(&Key(key), later),
                Err(Duration::from_nanos(10))
            );
        }
        for key in 33..57 {
            assert!(limiter.check_at(&Key(key), later).is_ok());
        }
        // Reset also compacts dense buckets, without losing other keys' allowance.
        for key in 33..57 {
            assert!(limiter.reset(&Key(key)));
        }
        assert_eq!(limiter.len(), 2);
        for key in 0..2 {
            assert_eq!(
                limiter.check_at(&Key(key), later),
                Err(Duration::from_nanos(10))
            );
        }
        assert_eq!(limiter.prune_at(later + Duration::from_nanos(10)), 2);
        assert!(limiter.is_empty());
        assert!(
            limiter
                .check_at(&Key(0), later + Duration::from_nanos(10))
                .is_ok()
        );
    }

    #[test]
    fn cleanup_releases_peak_storage() {
        use std::sync::Arc;

        #[derive(Clone, PartialEq, Eq)]
        struct Key(u64, Arc<()>);

        impl Hash for Key {
            fn hash<H: Hasher>(&self, state: &mut H) {
                state.write_u64(self.0 * SHARDS as u64);
            }
        }

        for cleanup in ["prune", "reset", "admission"] {
            let live = Arc::new(());
            let key = |id| Key(id, Arc::clone(&live));
            let store =
                ShardedStore::new(BuildHasherDefault::<ShardHasher>::default()).with_max_keys(1024);
            let quota = Quota::per(Duration::from_nanos(10), 1).0;
            for wave in 0..8 {
                let now = Instant(wave * 20);
                for id in 0..1024 {
                    assert_eq!(store.check_at::<true>(&quota, &key(id), now), Ok(()));
                }
                assert_eq!(store.len(), 1024);
                assert_eq!(Arc::strong_count(&live), 1025);

                let later = now + Duration::from_nanos(10);
                for id in 0..3 {
                    assert_eq!(store.check_at::<true>(&quota, &key(id), later), Ok(()));
                }
                match cleanup {
                    "prune" => assert_eq!(store.prune_at(later), 1021),
                    "reset" => {
                        for id in 3..1024 {
                            assert!(store.reset(&key(id)));
                        }
                    }
                    _ => {
                        assert_eq!(store.check_at::<true>(&quota, &key(1024), later), Ok(()));
                        assert_eq!(store.len(), 4);
                        assert!(store.reset(&key(1024)));
                    }
                }
                assert_eq!(store.len(), 3);
                assert_eq!(Arc::strong_count(&live), 4);
                for id in 0..3 {
                    assert_eq!(
                        store.check_at::<true>(&quota, &key(id), later),
                        Err(Duration::from_nanos(10))
                    );
                }
                let bucket = store.shards[0].get().unwrap()[0].0.read().unwrap();
                let Bucket::Dense(entries) = &*bucket else {
                    panic!("three keys should retain dense storage");
                };
                assert!(entries.num_buckets() < 32, "retained peak table allocation");
            }
            assert_eq!(store.prune_at(Instant(160)), 3);
            assert_eq!(Arc::strong_count(&live), 1);
            assert!(matches!(
                &*store.shards[0].get().unwrap()[0].0.read().unwrap(),
                Bucket::Small(bucket) if bucket.entries.capacity() == 0
            ));
            drop(store);
            assert_eq!(Arc::strong_count(&live), 1);
        }
    }

    #[test]
    fn compaction_counts_deleted_slots() {
        let hasher = BuildHasherDefault::<ShardHasher>::default();
        let mut entries = HashTable::with_capacity(1024);
        for key in 0..entries.capacity() as u64 {
            entries.insert_unique(
                hasher.hash_one(key),
                (key, AtomicU64::new(key)),
                |(key, _)| hasher.hash_one(key),
            );
        }
        // These keys share a probe sequence. Removing most of a full table leaves
        // tombstones, which reduce reported capacity without releasing any memory.
        entries.retain(|(key, _)| *key < 32);
        let mut bucket = Bucket::Dense(entries);
        bucket.compact(&hasher);
        let Bucket::Dense(entries) = &bucket else {
            panic!("32 keys should retain dense storage");
        };
        assert!(entries.num_buckets() <= 64);
        for key in 0..32 {
            assert_eq!(
                bucket
                    .get(hasher.hash_one(key), &key)
                    .unwrap()
                    .load(Relaxed),
                key
            );
        }
    }
}
