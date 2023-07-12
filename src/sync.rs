use crate::{
    options::{Options, OptionsBuilder},
    placeholder::{GuardResult, JoinFuture, PlaceholderGuard},
    rw_lock::RwLock,
    shard::{Entry, CacheShard},
    DefaultHashBuilder, UnitWeighter, Weighter,
};
use std::{
    borrow::Borrow,
    future::Future,
    hash::{BuildHasher, Hash, Hasher},
    time::Duration,
};

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
pub struct Cache<Key, Val, We = UnitWeighter, B = DefaultHashBuilder> {
    hash_builder: B,
    #[allow(clippy::type_complexity)]
    shards: Box<[RwLock<CacheShard<Key, Val, We, B>>]>,
    shards_mask: u64,
}

impl<Key: Eq + Hash, Val: Clone>
    Cache<Key, Val, UnitWeighter, DefaultHashBuilder>
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

impl<Key: Eq + Hash, Val: Clone, We: Weighter<Key, Val> + Clone>
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

impl<
        Key: Eq + Hash,

        Val: Clone,
        We: Weighter<Key, Val> + Clone,
        B: BuildHasher + Clone,
    > Cache<Key, Val, We, B>
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

    /// Constructs a cache based on [OptionsBuilder].
    ///
    /// # Example
    ///
    /// ```rust
    /// use quick_cache::{sync::Cache, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
    ///
    /// Cache::<(String, u64), String>::with_options(
    ///   OptionsBuilder::new()
    ///     .estimated_items_capacity(10000)
    ///     .weight_capacity(10000)
    ///     .build()
    ///     .unwrap(),
    ///     UnitWeighter,
    ///     DefaultHashBuilder::default(),
    /// );
    /// ```
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
                RwLock::new(CacheShard::new(
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
    fn shard_for<Q: ?Sized>(
        &self,
        key: &Q,
    ) -> Option<(&RwLock<CacheShard<Key, Val, We, B>>, u64)>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
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

    /// Fetches an item from the cache whose key is `key`.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key)?;
        shard.read().get(hash, key).cloned()
    }

    /// Peeks an item from the cache whose key is `key`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized>(&self, key: &Q) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
    {
        let (shard, hash) = self.shard_for(key)?;
        shard.read().peek(hash, key).cloned()
    }

    /// Remove an item from the cache whose key is `key`.
    /// Returns whether an entry was removed.
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some((shard, hash)) = self.shard_for(key) {
            // Any evictions will be dropped outside of the lock
            let evicted = shard.write().remove(hash, key);
            matches!(evicted, Some(Entry::Resident(_)))
        } else {
            false
        }
    }

    /// Inserts an item in the cache with key `key` .
    pub fn insert(&self, key: Key, value: Val) {
        if let Some((shard, hash)) = self.shard_for(&key) {
            // Any evictions will be dropped outside of the lock
            let _evicted = shard.write().insert(hash, key, value);
        }
    }

    /// Gets an item from the cache with key `key` .
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    pub fn get_value_or_guard<'a>(
        &'a self,
        key: &Key,

        timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Val, We, B>
    where
        Key: Clone,

    {
        let (shard, hash) = self.shard_for(key).unwrap();
        if let Some(v) = shard.read().get(hash, key) {
            return GuardResult::Value(v.clone());
        }
        PlaceholderGuard::join(shard, hash, key.clone(), timeout)
    }

    /// Gets or inserts an item in the cache with key `key` .
    pub fn get_or_insert_with<E>(
        &self,
        key: &Key,

        with: impl FnOnce() -> Result<Val, E>,
    ) -> Result<Val, E>
    where
        Key: Clone,

    {
        match self.get_value_or_guard(key, None) {
            GuardResult::Value(v) => Ok(v),
            GuardResult::Guard(g) => {
                let v = with()?;
                g.insert(v.clone());
                Ok(v)
            }
            GuardResult::Timeout => unreachable!(),
        }
    }

    /// Gets an item from the cache with key `key` .
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    pub async fn get_value_or_guard_async<'a, 'b>(
        &'a self,
        key: &'b Key,
    ) -> Result<Val, PlaceholderGuard<'a, Key, Val, We, B>>
    where
        Key: Clone,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        if let Some(v) = shard.read().get(hash, key) {
            return Ok(v.clone());
        }
        JoinFuture::new(shard, hash, key).await
    }

    /// Gets or inserts an item in the cache with key `key` .
    pub async fn get_or_insert_async<'a, E>(
        &self,
        key: &Key,

        with: impl Future<Output = Result<Val, E>>,
    ) -> Result<Val, E>
    where
        Key: Clone,

    {
        match self.get_value_or_guard_async(key).await {
            Ok(v) => Ok(v),
            Err(g) => {
                let v = with.await?;
                g.insert(v.clone());
                Ok(v)
            }
        }
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
