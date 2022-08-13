use crate::{shard::VersionedCacheShard, DefaultHashBuilder};
use parking_lot::RwLock;
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

/// A concurrent version aware cache.
///
/// # Key and Version
/// The key version pair exists for cases where you want a cache keyed by (T, U).
/// Other rust maps/caches are accessed via the Borrow trait,
/// so they require the caller to build &(T, U) which might involve cloning T and/or U.
///
/// # Value
/// Cache values are cloned when fetched. Users should wrap their values with `Arc<_>`
/// if necessary to avoid expensive clone operations. If interior mutability is required
/// `Arc<Mutex<_>>` or `Arc<RwLock<_>>` can also be used.
///
/// # Thread Safety and Concurrency
/// The cache instance can wrapped with an `Arc` (or equivalent) and shared between threads.
/// All methods are accessible via non-mut references so no further synchronization (e.g. Mutex) is needed.
pub struct VersionedCache<Key, Ver, Val, B = DefaultHashBuilder> {
    hash_builder: B,
    shards: Box<[RwLock<VersionedCacheShard<Key, Ver, Val, B>>]>,
    shards_mask: usize,
}

impl<Key: Eq + Hash, Ver: Eq + Hash, Val: Clone, B: Default + BuildHasher + Clone>
    VersionedCache<Key, Ver, Val, B>
{
    /// Creates a new cache with holds up to `max_capacity` items (approximately)
    /// and have `initial_capacity` pre-allocated.
    pub fn new(initial_capacity: usize, max_capacity: usize) -> Self {
        assert!(initial_capacity <= max_capacity);
        let hasher = B::default();
        let mut num_shards = std::thread::available_parallelism()
            .map_or(2, |n| n.get() * 2)
            .min(max_capacity)
            .next_power_of_two();
        let mut shard_max_capacity = max_capacity.saturating_add(num_shards - 1) / num_shards;
        // try to make each shard hold at least 32 items
        while shard_max_capacity < 32 && num_shards > 1 {
            num_shards /= 2;
            shard_max_capacity = max_capacity.saturating_add(num_shards - 1) / num_shards;
        }
        let shard_initial_capacity = initial_capacity.saturating_add(num_shards - 1) / num_shards;
        let shards = (0..num_shards)
            .map(|_| {
                RwLock::new(VersionedCacheShard::new(
                    shard_initial_capacity,
                    shard_max_capacity,
                    hasher.clone(),
                ))
            })
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            hash_builder: hasher,
            shards_mask: num_shards - 1,
        }
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns the maximum number of cached items
    pub fn capacity(&self) -> usize {
        self.shards.iter().map(|s| s.read().capacity()).sum()
    }

    /// Returns the number of misses
    pub fn misses(&self) -> u64 {
        self.shards.iter().map(|s| s.read().misses()).sum()
    }

    /// Returns the number of hits
    pub fn hits(&self) -> u64 {
        self.shards.iter().map(|s| s.read().hits()).sum()
    }

    fn shard_for<Q: ?Sized, W: ?Sized>(
        &self,
        key: &Q,
        version: &W,
    ) -> Option<(&RwLock<VersionedCacheShard<Key, Ver, Val, B>>, u64)>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        version.hash(&mut hasher);
        let hash = hasher.finish();
        self.shards
            .get(hash as usize & self.shards_mask)
            .map(|s| (s, hash))
    }

    /// Fetches an item from the cache whose key is `key` and version is <= `highest_version`.
    pub fn get<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key, version)?;
        shard.read().get(hash, key, version).cloned()
    }

    /// Peeks an item from the cache whose key is `key` and version is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key, version)?;
        shard.read().peek(hash, key, version).cloned()
    }

    /// Peeks an item from the cache whose key is `key` and version is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some((shard, hash)) = self.shard_for(&key, &version) {
            // Any evictions will be dropped outside of the lock
            let evicted = shard.write().remove(hash, key, version);
            matches!(evicted, Some(Ok(_)))
        } else {
            false
        }
    }

    /// Inserts an item in the cache with key `key` and version `version`.
    pub fn insert(&self, key: Key, version: Ver, value: Val) {
        if let Some((shard, hash)) = self.shard_for(&key, &version) {
            // Any evictions will be dropped outside of the lock
            let _evicted = shard.write().insert(hash, key, version, value);
        }
    }
}

impl<Key: Eq + Hash, Ver: Eq + Hash, Val: Clone> std::fmt::Debug for VersionedCache<Key, Ver, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedCache").finish_non_exhaustive()
    }
}

/// A concurrent cache.
///
/// # Value
/// Cache values are cloned when fetched. Users should wrap their values with `Arc<_>`
/// if necessary to avoid expensive clone operations. If interior mutability is required
/// `Arc<Mutex<_>>` or `Arc<RwLock<_>>` can also be used.
///
/// # Thread Safety and Concurrency
/// The cache instance can wrapped with an `Arc` (or equivalent) and shared between threads.
/// All methods are accessible via non-mut references so no further synchronization (e.g. Mutex) is needed.
pub struct Cache<Key, Val, B = DefaultHashBuilder>(VersionedCache<Key, (), Val, B>);

impl<Key: Eq + Hash, Val: Clone, B: Default + Clone + BuildHasher> Cache<Key, Val, B> {
    /// Creates a new cache with holds up to `capacity` items (approximately).
    pub fn new(initial_capacity: usize, max_capacity: usize) -> Self {
        Self(VersionedCache::new(initial_capacity, max_capacity))
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the maximum number of cached items
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns the number of misses
    pub fn misses(&self) -> u64 {
        self.0.misses()
    }

    /// Returns the number of hits
    pub fn hits(&self) -> u64 {
        self.0.hits()
    }

    /// Fetches an item from the cache.
    /// Callers should prefer `get_mut` whenever possible as it's more efficient.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.get(key, &())
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized>(&self, key: &Q) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.peek(key, &())
    }

    /// Peeks an item from the cache whose key is `key` and version is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove(key, &())
    }

    /// Inserts an item in the cache with key `key` and version `version`.
    pub fn insert(&self, key: Key, value: Val) {
        self.0.insert(key, (), value);
    }
}

impl<Key: Eq + Hash, Val: Clone> std::fmt::Debug for Cache<Key, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    #[test]
    fn test_multiple_threads() {
        const N_THREAD_PAIRS: usize = 8;
        const N_ROUNDS: usize = 5_000;
        const ITEMS_PER_THREAD: usize = 1_000;
        let mut threads = Vec::new();
        let barrier = Arc::new(Barrier::new(N_THREAD_PAIRS * 2));
        let cache = Arc::new(Cache::new(0, N_THREAD_PAIRS * ITEMS_PER_THREAD / 10));
        for t in 0..N_THREAD_PAIRS {
            let barrier = barrier.clone();
            let cache = cache.clone();
            let handle = thread::spawn(move || {
                let start = ITEMS_PER_THREAD * t;
                barrier.wait();
                for _round in 0..N_ROUNDS {
                    for i in start..start + ITEMS_PER_THREAD {
                        cache.insert(i, i);
                    }
                }
            });
            threads.push(handle);
        }
        for t in 0..N_THREAD_PAIRS {
            let barrier = barrier.clone();
            let cache = cache.clone();
            let handle = thread::spawn(move || {
                let start = ITEMS_PER_THREAD * t;
                barrier.wait();
                for _round in 0..N_ROUNDS {
                    for i in start..start + ITEMS_PER_THREAD {
                        if let Some(cached) = cache.get(&i) {
                            assert_eq!(cached, i);
                        }
                    }
                }
            });
            threads.push(handle);
        }
        for t in threads {
            t.join().unwrap();
        }
    }
}
