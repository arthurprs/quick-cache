use crate::{
    options::{Options, OptionsBuilder},
    rw_lock::RwLock,
    shard::KQCacheShard,
    DefaultHashBuilder, UnitWeighter, Weighter,
};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

/// A concurrent two keys cache.
///
/// # Key and Qey
/// The key qey pair exists for cases where you want a cache keyed by (K, Q).
/// Other rust maps/caches are accessed via the Borrow trait,
/// so they require the caller to build &(K, Q) which might involve cloning K and/or Q.
///
/// # Value
/// Cache values are cloned when fetched. Users should wrap their values with `Arc<_>`
/// if necessary to avoid expensive clone operations. If interior mutability is required
/// `Arc<Mutex<_>>` or `Arc<RwLock<_>>` can also be used.
///
/// # Thread Safety and Concurrency
/// The cache instance can wrapped with an `Arc` (or equivalent) and shared between threads.
/// All methods are accessible via non-mut references so no further synchronization (e.g. Mutex) is needed.
pub struct KQCache<Key, Qey, Val, We = UnitWeighter, B = DefaultHashBuilder> {
    hash_builder: B,
    #[allow(clippy::type_complexity)]
    shards: Box<[RwLock<KQCacheShard<Key, Qey, Val, We, B>>]>,
    shards_mask: u64,
}

impl<Key: Eq + Hash, Qey: Eq + Hash, Val: Clone>
    KQCache<Key, Qey, Val, UnitWeighter, DefaultHashBuilder>
{
    /// Creates a new cache with holds up to `items_capacity` items (approximately).
    pub fn new(items_capacity: usize) -> Self {
        Self::with(
            items_capacity,
            items_capacity as u64,
            UnitWeighter,
            DefaultHashBuilder::default(),
        )
    }
}

impl<Key: Eq + Hash, Qey: Eq + Hash, Val: Clone, We: Weighter<Key, Qey, Val> + Clone>
    KQCache<Key, Qey, Val, We, DefaultHashBuilder>
{
    pub fn with_weighter(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
    ) -> Self {
        Self::with(
            estimated_items_capacity,
            weight_capacity,
            weighter,
            DefaultHashBuilder::default(),
        )
    }
}

impl<
        Key: Eq + Hash,
        Qey: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Qey, Val> + Clone,
        B: BuildHasher + Clone,
    > KQCache<Key, Qey, Val, We, B>
{
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
    ) -> Self {
        Self::with_options(
            OptionsBuilder::new()
                .estimated_items_capacity(estimated_items_capacity)
                .weight_capacity(weight_capacity)
                .build()
                .unwrap(),
            weighter,
            hash_builder,
        )
    }

    pub fn with_options(options: Options, weighter: We, hash_builder: B) -> Self {
        let mut num_shards = options.shards.next_power_of_two() as u64;
        let estimated_items_capacity = options.estimated_items_capacity as u64;
        let weight_capacity = options.weight_capacity;
        let mut shard_items_cap =
            estimated_items_capacity.saturating_add(num_shards - 1) / num_shards;
        let mut shard_weight_cap =
            options.weight_capacity.saturating_add(num_shards - 1) / num_shards;
        // try to make each shard hold at least 32 items
        while shard_items_cap < 32 && num_shards > 1 {
            num_shards /= 2;
            shard_items_cap = estimated_items_capacity.saturating_add(num_shards - 1) / num_shards;
            shard_weight_cap = weight_capacity.saturating_add(num_shards - 1) / num_shards;
        }
        let shards = (0..num_shards)
            .map(|_| {
                RwLock::new(KQCacheShard::new(
                    options.hot_allocation,
                    options.ghost_allocation,
                    shard_items_cap as usize,
                    shard_weight_cap,
                    weighter.clone(),
                    hash_builder.clone(),
                ))
            })
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            hash_builder,
            shards_mask: num_shards - 1,
        }
    }

    #[cfg(fuzzing)]
    pub fn validate(&self) {
        for s in &*self.shards {
            s.read().validate()
        }
    }

    /// Returns whether the cache is empty
    pub fn is_empty(&self) -> bool {
        self.shards.iter().any(|s| s.read().len() == 0)
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns the total weight of cached items
    pub fn weight(&self) -> u64 {
        self.shards.iter().map(|s| s.read().weight()).sum()
    }

    /// Returns the maximum weight of cached items
    pub fn capacity(&self) -> u64 {
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

    #[allow(clippy::type_complexity)]
    #[inline]
    fn shard_for<Q: ?Sized, W: ?Sized>(
        &self,
        key: &Q,
        qey: &W,
    ) -> Option<(&RwLock<KQCacheShard<Key, Qey, Val, We, B>>, u64)>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        qey.hash(&mut hasher);
        let hash = hasher.finish();
        // When choosing the shard, rotate the hash bits usize::BITS / 2 so that we
        // give preference to the bits in the middle of the hash.
        // Internally hashbrown uses the lower bits for start of probing + the 7 highest,
        // so by picking something else we improve the real entropy available to each hashbrown shard.
        let shard_idx = (hash.rotate_right(usize::BITS / 2) & self.shards_mask) as usize;
        self.shards.get(shard_idx).map(|s| (s, hash))
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        let additional_per_shard =
            additional.saturating_add(self.shards.len() - 1) / self.shards.len();
        for s in &*self.shards {
            s.write().reserve(additional_per_shard);
        }
    }

    /// Fetches an item from the cache whose keys are `key` + `qey`.
    pub fn get<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key, qey)?;
        shard.read().get(hash, key, qey).cloned()
    }

    /// Peeks an item from the cache whose keys are `key` + `qey`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key, qey)?;
        shard.read().peek(hash, key, qey).cloned()
    }

    /// Peeks an item from the cache whose key is `key` and qey is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some((shard, hash)) = self.shard_for(key, qey) {
            // Any evictions will be dropped outside of the lock
            let evicted = shard.write().remove(hash, key, qey);
            matches!(evicted, Some(Ok(_)))
        } else {
            false
        }
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    pub fn insert(&self, key: Key, qey: Qey, value: Val) {
        if let Some((shard, hash)) = self.shard_for(&key, &qey) {
            // Any evictions will be dropped outside of the lock
            let _evicted = shard.write().insert(hash, key, qey, value);
        }
    }
}

impl<Key, Qey, Val, We, B> std::fmt::Debug for KQCache<Key, Qey, Val, We, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KQCache").finish_non_exhaustive()
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
pub struct Cache<Key, Val, We = UnitWeighter, B = DefaultHashBuilder>(KQCache<Key, (), Val, We, B>);

impl<Key: Eq + Hash, Val: Clone> Cache<Key, Val, UnitWeighter, DefaultHashBuilder> {
    /// Creates a new cache with holds up to `items_capacity` items (approximately).
    pub fn new(items_capacity: usize) -> Self {
        Self(KQCache::new(items_capacity))
    }
}

impl<Key: Eq + Hash, Val: Clone, We: Weighter<Key, (), Val> + Clone>
    Cache<Key, Val, We, DefaultHashBuilder>
{
    pub fn with_weighter(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
    ) -> Self {
        Self::with(
            estimated_items_capacity,
            weight_capacity,
            weighter,
            DefaultHashBuilder::default(),
        )
    }
}

impl<Key: Eq + Hash, Val: Clone, We: Weighter<Key, (), Val> + Clone, B: BuildHasher + Clone>
    Cache<Key, Val, We, B>
{
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
    ) -> Self {
        Self(KQCache::with(
            estimated_items_capacity,
            weight_capacity,
            weighter,
            hash_builder,
        ))
    }

    pub fn with_options(options: Options, weighter: We, hash_builder: B) -> Self {
        Self(KQCache::with_options(options, weighter, hash_builder))
    }

    /// Returns whether the cache is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the total weight of cached items
    pub fn weight(&self) -> u64 {
        self.0.weight()
    }

    /// Returns the maximum weight of cached items
    pub fn capacity(&self) -> u64 {
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

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional)
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

    /// Peeks an item from the cache whose key is `key` and qey is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove(key, &())
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    pub fn insert(&self, key: Key, value: Val) {
        self.0.insert(key, (), value);
    }
}

impl<Key, Val, We, B> std::fmt::Debug for Cache<Key, Val, We, B> {
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
        const N_ROUNDS: usize = 1_000;
        const ITEMS_PER_THREAD: usize = 1_000;
        let mut threads = Vec::new();
        let barrier = Arc::new(Barrier::new(N_THREAD_PAIRS * 2));
        let cache = Arc::new(Cache::new(N_THREAD_PAIRS * ITEMS_PER_THREAD / 10));
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
