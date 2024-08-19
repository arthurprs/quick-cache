use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    time::Duration,
};

use crate::{
    options::{Options, OptionsBuilder},
    shard::{CacheShard, InsertStrategy},
    shim::rw_lock::RwLock,
    sync_placeholder::SharedPlaceholder,
    DefaultHashBuilder, Equivalent, Lifecycle, UnitWeighter, Weighter,
};

pub use crate::sync_placeholder::{GuardResult, JoinFuture, PlaceholderGuard};

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
pub struct Cache<
    Key,
    Val,
    We = UnitWeighter,
    B = DefaultHashBuilder,
    L = DefaultLifecycle<Key, Val>,
> {
    hash_builder: B,
    shards: Box<[RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>]>,
    shards_mask: u64,
    lifecycle: L,
}

impl<Key: Eq + Hash, Val: Clone> Cache<Key, Val> {
    /// Creates a new cache with holds up to `items_capacity` items (approximately).
    pub fn new(items_capacity: usize) -> Self {
        Self::with(
            items_capacity,
            items_capacity as u64,
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }
}

impl<Key: Eq + Hash, Val: Clone, We: Weighter<Key, Val> + Clone> Cache<Key, Val, We> {
    pub fn with_weighter(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
    ) -> Self {
        Self::with(
            estimated_items_capacity,
            weight_capacity,
            weighter,
            Default::default(),
            Default::default(),
        )
    }
}

impl<
        Key: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Val> + Clone,
        B: BuildHasher + Clone,
        L: Lifecycle<Key, Val> + Clone,
    > Cache<Key, Val, We, B, L>
{
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
        lifecycle: L,
    ) -> Self {
        Self::with_options(
            OptionsBuilder::new()
                .estimated_items_capacity(estimated_items_capacity)
                .weight_capacity(weight_capacity)
                .build()
                .unwrap(),
            weighter,
            hash_builder,
            lifecycle,
        )
    }

    /// Constructs a cache based on [OptionsBuilder].
    ///
    /// # Example
    ///
    /// ```rust
    /// use quick_cache::{sync::{Cache, DefaultLifecycle}, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
    ///
    /// Cache::<(String, u64), String>::with_options(
    ///   OptionsBuilder::new()
    ///     .estimated_items_capacity(10000)
    ///     .weight_capacity(10000)
    ///     .build()
    ///     .unwrap(),
    ///     UnitWeighter,
    ///     DefaultHashBuilder::default(),
    ///     DefaultLifecycle::default(),
    /// );
    /// ```
    pub fn with_options(options: Options, weighter: We, hash_builder: B, lifecycle: L) -> Self {
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
                    lifecycle.clone(),
                ))
            })
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            hash_builder,
            shards_mask: num_shards - 1,
            lifecycle,
        }
    }

    #[cfg(fuzzing)]
    pub fn validate(&self) {
        for s in &*self.shards {
            s.read().validate(false)
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
    #[cfg(feature = "stats")]
    pub fn misses(&self) -> u64 {
        self.shards.iter().map(|s| s.read().misses()).sum()
    }

    /// Returns the number of hits
    #[cfg(feature = "stats")]
    pub fn hits(&self) -> u64 {
        self.shards.iter().map(|s| s.read().hits()).sum()
    }

    #[inline]
    fn shard_for<Q>(
        &self,
        key: &Q,
    ) -> Option<(
        &RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        u64,
    )>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let hash = self.hash_builder.hash_one(key);
        // When choosing the shard, rotate the hash bits usize::BITS / 2 so that we
        // give preference to the bits in the middle of the hash.
        // Internally hashbrown uses the lower bits for start of probing + the 7 highest,
        // so by picking something else we improve the real entropy available to each hashbrown shard.
        let shard_idx = (hash.rotate_right(usize::BITS / 2) & self.shards_mask) as usize;
        self.shards.get(shard_idx).map(|s| (s, hash))
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&self, additional: usize) {
        let additional_per_shard =
            additional.saturating_add(self.shards.len() - 1) / self.shards.len();
        for s in &*self.shards {
            s.write().reserve(additional_per_shard);
        }
    }

    /// Fetches an item from the cache whose key is `key`.
    pub fn get<Q>(&self, key: &Q) -> Option<Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key)?;
        shard.read().get(hash, key).cloned()
    }

    /// Peeks an item from the cache whose key is `key`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q>(&self, key: &Q) -> Option<Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key)?;
        shard.read().peek(hash, key).cloned()
    }

    /// Remove an item from the cache whose key is `key`.
    /// Returns the removed entry, if any.
    pub fn remove<Q>(&self, key: &Q) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        let removed = shard.write().remove(hash, key);
        removed
    }

    /// Inserts an item in the cache, but _only_ if an entry with key `key` already exists.
    /// If `soft` is set, the replace operation won't affect the "hotness" of the entry,
    /// even if the value is replaced.
    ///
    /// Returns `Ok` if the entry was admitted and `Err(_)` if it wasn't.
    pub fn replace(&self, key: Key, value: Val, soft: bool) -> Result<(), (Key, Val)> {
        let lcs = self.replace_with_lifecycle(key, value, soft)?;
        self.lifecycle.end_request(lcs);
        Ok(())
    }

    /// Inserts an item in the cache, but _only_ if an entry with key `key` already exists.
    /// If `soft` is set, the replace operation won't affect the "hotness" of the entry,
    /// even if the value is replaced.
    ///
    /// Returns `Ok` if the entry was admitted and `Err(_)` if it wasn't.
    pub fn replace_with_lifecycle(
        &self,
        key: Key,
        value: Val,
        soft: bool,
    ) -> Result<L::RequestState, (Key, Val)> {
        let mut lcs = self.lifecycle.begin_request();
        let (shard, hash) = self.shard_for(&key).unwrap();
        shard
            .write()
            .insert(&mut lcs, hash, key, value, InsertStrategy::Replace { soft })?;
        Ok(lcs)
    }

    /// Inserts an item in the cache with key `key`.
    pub fn insert(&self, key: Key, value: Val) {
        let lcs = self.insert_with_lifecycle(key, value);
        self.lifecycle.end_request(lcs);
    }

    /// Inserts an item in the cache with key `key`.
    pub fn insert_with_lifecycle(&self, key: Key, value: Val) -> L::RequestState {
        let mut lcs = self.lifecycle.begin_request();
        let (shard, hash) = self.shard_for(&key).unwrap();
        let result = shard
            .write()
            .insert(&mut lcs, hash, key, value, InsertStrategy::Insert);
        // result cannot err with the Insert strategy
        debug_assert!(result.is_ok());
        lcs
    }

    /// Clear all items from the cache
    pub fn clear(&self) {
        for s in self.shards.iter() {
            s.write().clear();
        }
    }

    /// Gets an item from the cache with key `key` .
    ///
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    ///
    /// A `None` `timeout` means waiting forever.
    pub fn get_value_or_guard<'a, Q>(
        &'a self,
        key: &Q,
        timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Val, We, B, L>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        if let Some(v) = shard.read().get(hash, key) {
            return GuardResult::Value(v.clone());
        }
        PlaceholderGuard::join(&self.lifecycle, shard, hash, key, timeout)
    }

    /// Gets or inserts an item in the cache with key `key`.
    ///
    /// See also `get_value_or_guard` and `get_value_or_guard_async`.
    pub fn get_or_insert_with<Q, E>(
        &self,
        key: &Q,
        with: impl FnOnce() -> Result<Val, E>,
    ) -> Result<Val, E>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        match self.get_value_or_guard(key, None) {
            GuardResult::Value(v) => Ok(v),
            GuardResult::Guard(g) => {
                let v = with()?;
                let _ = g.insert(v.clone());
                Ok(v)
            }
            GuardResult::Timeout => unreachable!(),
        }
    }

    /// Gets an item from the cache with key `key`.
    ///
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    pub async fn get_value_or_guard_async<'a, Q>(
        &'a self,
        key: &Q,
    ) -> Result<Val, PlaceholderGuard<'a, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        if let Some(v) = shard.read().get(hash, key) {
            return Ok(v.clone());
        }
        JoinFuture::new(&self.lifecycle, shard, hash, key).await
    }

    /// Gets or inserts an item in the cache with key `key`.
    pub async fn get_or_insert_async<'a, Q, E>(
        &self,
        key: &Q,
        with: impl Future<Output = Result<Val, E>>,
    ) -> Result<Val, E>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        match self.get_value_or_guard_async(key).await {
            Ok(v) => Ok(v),
            Err(g) => {
                let v = with.await?;
                let _ = g.insert(v.clone());
                Ok(v)
            }
        }
    }
}

impl<Key, Val, We, B, L> std::fmt::Debug for Cache<Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}

/// Default `Lifecycle` for a sync cache.
///
/// Temporally stashes one evicted item for dropping outside the cache locks.
pub struct DefaultLifecycle<Key, Val>(std::marker::PhantomData<(Key, Val)>);

impl<Key, Val> std::fmt::Debug for DefaultLifecycle<Key, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DefaultLifecycle").finish()
    }
}

impl<Key, Val> Default for DefaultLifecycle<Key, Val> {
    #[inline]
    fn default() -> Self {
        Self(Default::default())
    }
}
impl<Key, Val> Clone for DefaultLifecycle<Key, Val> {
    #[inline]
    fn clone(&self) -> Self {
        Self(Default::default())
    }
}

impl<Key, Val> Lifecycle<Key, Val> for DefaultLifecycle<Key, Val> {
    type RequestState = Option<(Key, Val)>;

    #[inline]
    fn begin_request(&self) -> Self::RequestState {
        None
    }

    #[inline]
    fn on_evict(&self, state: &mut Self::RequestState, key: Key, val: Val) {
        *state = Some((key, val));
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
    #[cfg_attr(miri, ignore)]
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
