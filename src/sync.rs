use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    time::Duration,
};

use crate::{
    linked_slab::Token,
    options::{Options, OptionsBuilder},
    shard::{CacheShard, InsertStrategy},
    shim::rw_lock::RwLock,
    sync_placeholder::SharedPlaceholder,
    DefaultHashBuilder, Equivalent, Lifecycle, MemoryUsed, UnitWeighter, Weighter,
};

use crate::shard::EntryOrPlaceholder;
pub use crate::sync_placeholder::{EntryAction, EntryResult, GuardResult, PlaceholderGuard};
use crate::sync_placeholder::{JoinFuture, JoinResult};

/// A concurrent cache
///
/// The concurrent cache is internally composed of equally sized shards, each of which is independently
/// synchronized. This allows for low contention when multiple threads are accessing the cache but limits the
/// maximum weight capacity of each shard.
///
/// # Value
/// Cache values are cloned when fetched. Users should wrap their values with `Arc<_>`
/// if necessary to avoid expensive clone operations. If interior mutability is required
/// `Arc<Mutex<_>>` or `Arc<RwLock<_>>` can also be used.
///
/// # Thread Safety and Concurrency
/// The cache instance can be wrapped with an `Arc` (or equivalent) and shared between threads.
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
        self.shards.iter().all(|s| s.read().len() == 0)
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns the total weight of cached items
    pub fn weight(&self) -> u64 {
        self.shards.iter().map(|s| s.read().weight()).sum()
    }

    /// Returns the _total_ maximum weight capacity of cached items.
    /// Note that the cache may be composed of multiple shards and each shard has its own maximum weight capacity,
    /// see [`Self::shard_capacity`].
    pub fn capacity(&self) -> u64 {
        self.shards.iter().map(|s| s.read().capacity()).sum()
    }

    /// Returns the maximum weight capacity of each shard.
    pub fn shard_capacity(&self) -> u64 {
        self.shards[0].read().capacity()
    }

    /// Returns the number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
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
    fn compute_shard_index(&self, hash: u64) -> u64 {
        // Give preference to the bits in the middle of the hash. When choosing the
        // shard, rotate the hash by usize::BITS / 2 so we avoid the lower bits and
        // the highest 7 bits that hashbrown uses internally for probing, improving
        // the real entropy available to each hashbrown shard.
        hash.rotate_right(usize::BITS / 2) & self.shards_mask
    }

    /// Returns the shard index for the given key.
    ///
    /// The returned index is guaranteed to be in `[0, num_shards())`.
    ///
    /// # Use cases
    ///
    /// - **Batching**: group keys by shard index before acquiring shard locks, so
    ///   each lock is taken only once per batch instead of once per key.
    ///
    /// # Notes
    ///
    /// The mapping from key to shard index depends on the [`BuildHasher`] supplied
    /// at construction time. If two `Cache` instances are built with different
    /// hashers, the same key may map to different shard indices.
    ///
    /// [`BuildHasher`]: std::hash::BuildHasher
    #[inline]
    pub fn shard_index<Q: Hash + Equivalent<Key> + ?Sized>(&self, key: &Q) -> usize {
        let hash = self.hash_builder.hash_one(key);
        self.compute_shard_index(hash) as usize
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
        let shard_idx = self.compute_shard_index(hash) as usize;
        self.shards.get(shard_idx).map(|s| (s, hash))
    }

    /// Reserve additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&self, additional: usize) {
        let additional_per_shard =
            additional.saturating_add(self.shards.len() - 1) / self.shards.len();
        for s in &*self.shards {
            s.write().reserve(additional_per_shard);
        }
    }

    /// Checks if a key exists in the cache.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard_for(key)
            .is_some_and(|(shard, hash)| shard.read().contains(hash, key))
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
        shard.write().remove(hash, key)
    }

    /// Remove an item from the cache whose key is `key` if `f(&value)` returns `true` for that entry.
    /// Compared to peek and remove, this method guarantees that no new value was inserted in-between.
    ///
    /// Returns the removed entry, if any.
    pub fn remove_if<Q, F>(&self, key: &Q, f: F) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
        F: FnOnce(&Val) -> bool,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        shard.write().remove_if(hash, key, f)
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

    /// Retains only the items specified by the predicate.
    /// In other words, remove all items for which `f(&key, &value)` returns `false`. The
    /// elements are visited in arbitrary order.
    pub fn retain<F>(&self, f: F)
    where
        F: Fn(&Key, &Val) -> bool,
    {
        for s in self.shards.iter() {
            s.write().retain(&f);
        }
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

    /// Iterates over the items in the cache returning cloned key value pairs.
    ///
    /// The iterator is guaranteed to yield all items in the cache at the time of creation
    /// provided that they are not removed or evicted from the cache while iterating.
    /// The iterator may also yield items added to the cache after the iterator is created.
    pub fn iter(&self) -> Iter<'_, Key, Val, We, B, L>
    where
        Key: Clone,
    {
        Iter {
            shards: &self.shards,
            current_shard: 0,
            last: None,
        }
    }

    /// Drains items from the cache.
    ///
    /// The iterator is guaranteed to drain all items in the cache at the time of creation
    /// provided that they are not removed or evicted from the cache while draining.
    /// The iterator may also drain items added to the cache after the iterator is created.
    /// Due to the above, the cache may not be empty after the iterator is fully consumed
    /// if items are added to the cache while draining.
    ///
    /// Note that dropping the iterator will _not_ finish the draining process, unlike other
    /// drain methods.
    pub fn drain(&self) -> Drain<'_, Key, Val, We, B, L> {
        Drain {
            shards: &self.shards,
            current_shard: 0,
            last: None,
        }
    }

    /// Sets the cache to a new weight capacity.
    ///
    /// This will adjust the weight capacity of each shard proportionally.
    /// If the new capacity is smaller than the current weight, items will be evicted
    /// to bring the cache within the new limit.
    pub fn set_capacity(&self, new_weight_capacity: u64) {
        let shard_weight_cap = new_weight_capacity.saturating_add(self.shards.len() as u64 - 1)
            / self.shards.len() as u64;
        for shard in &*self.shards {
            shard.write().set_capacity(shard_weight_cap);
        }
    }

    /// Gets an item from the cache with key `key` .
    ///
    /// If the corresponding value isn't present in the cache, this function returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_or_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    ///
    /// A `None` `timeout` means waiting forever.
    /// A `Some(<zero>)` timeout will return a Timeout error immediately if the value is not present
    /// and a guard is alive elsewhere.
    pub fn get_value_or_guard<Q>(
        &self,
        key: &Q,
        timeout: Option<Duration>,
    ) -> GuardResult<'_, Key, Val, We, B, L>
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
            GuardResult::Timeout => unsafe { unreachable_unchecked() },
        }
    }

    /// Gets an item from the cache with key `key`.
    ///
    /// If the corresponding value isn't present in the cache, this function returns a guard
    /// that can be used to insert the value once it's computed.
    /// While the returned guard is alive, other calls with the same key using the
    /// `get_value_or_guard` or `get_or_insert` family of functions will wait until the guard
    /// is dropped or the value is inserted.
    pub async fn get_value_or_guard_async<'a, Q>(
        &'a self,
        key: &Q,
    ) -> Result<Val, PlaceholderGuard<'a, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        loop {
            if let Some(v) = shard.read().get(hash, key) {
                return Ok(v.clone());
            }
            match JoinFuture::new(&self.lifecycle, shard, hash, key).await {
                JoinResult::Filled(Some(shared)) => {
                    // SAFETY: Filled means the value was set by the loader.
                    return Ok(unsafe { shared.value().unwrap_unchecked().clone() });
                }
                JoinResult::Filled(None) => continue,
                JoinResult::Guard(g) => return Err(g),
                JoinResult::Timeout => unsafe { unreachable_unchecked() },
            }
        }
    }

    /// Gets or inserts an item in the cache with key `key`.
    pub async fn get_or_insert_async<Q, E>(
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

    /// Atomically accesses an existing entry, or gets a guard for insertion.
    ///
    /// If a value exists for `key`, `on_occupied` is called with a mutable reference
    /// to the key and value. The callback returns an [`EntryAction`] to decide what to do:
    /// - [`EntryAction::Retain`]`(T)` — keep the entry, return `T`.
    ///   Weight is recalculated after the callback returns.
    /// - [`EntryAction::Remove`] — remove the entry from the cache.
    /// - [`EntryAction::ReplaceWithGuard`] — remove the entry and get a guard for re-insertion.
    ///
    /// If no value exists, a [`PlaceholderGuard`] is returned for inserting a new value.
    /// If another thread is already loading this key, waits up to `timeout` for the value
    /// to arrive, then calls `on_occupied` on the result.
    ///
    /// A `None` `timeout` means waiting forever.
    /// A `Some(<zero>)` timeout will return a Timeout immediately if a guard is alive elsewhere.
    ///
    /// The callback is `FnOnce` and runs **at most once**.
    ///
    /// # Performance
    ///
    /// Always acquires a **write lock** on the shard. For read-only lookups where
    /// contention matters, prefer [`get`](Self::get), [`get_value_or_guard`](Self::get_value_or_guard)
    /// or similar.
    ///
    /// The callback runs under the shard write lock — keep it short to avoid blocking
    /// other operations on the same shard. **Do not** call back into the cache from the
    /// callback, as this will deadlock when the same shard is accessed.
    ///
    /// # Panics
    ///
    /// If the callback panics, weight accounting is automatically corrected.
    /// However, any partial mutation to the value will remain.
    ///
    /// # Examples
    ///
    /// ```
    /// use quick_cache::sync::{Cache, EntryAction, EntryResult};
    ///
    /// let cache: Cache<String, u64> = Cache::new(5);
    /// cache.insert("counter".to_string(), 0);
    ///
    /// // Mutate in place: increment a counter
    /// let result = cache.entry("counter", None, |_k, v| {
    ///     *v += 1;
    ///     EntryAction::Retain(*v)
    /// });
    /// assert!(matches!(result, EntryResult::Retained(1)));
    /// assert_eq!(cache.get("counter"), Some(1));
    /// ```
    pub fn entry<Q, T>(
        &self,
        key: &Q,
        timeout: Option<Duration>,
        on_occupied: impl FnOnce(&Key, &mut Val) -> EntryAction<T>,
    ) -> EntryResult<'_, Key, Val, We, B, L, T>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        // Wrap FnOnce in Option so we can pass &mut FnMut to entry_or_placeholder
        // in a loop. The loop retries only on ExistingPlaceholder (another thread is
        // loading), which does not invoke the callback — so the Option is still Some
        // on retry and the callback runs at most once.
        let mut on_occupied = Some(on_occupied);
        let mut callback = |k: &Key, v: &mut Val| on_occupied.take().unwrap()(k, v);
        let mut deadline = timeout.map(Ok);

        loop {
            let mut shard_guard = shard.write();
            match shard_guard.entry_or_placeholder(hash, key, &mut callback) {
                EntryOrPlaceholder::Kept(t) => return EntryResult::Retained(t),
                EntryOrPlaceholder::Removed(k, v) => return EntryResult::Removed(k, v),
                EntryOrPlaceholder::Replaced(shared, old_val) => {
                    drop(shard_guard);
                    return EntryResult::Replaced(
                        PlaceholderGuard::start_loading(&self.lifecycle, shard, shared),
                        old_val,
                    );
                }
                EntryOrPlaceholder::NewPlaceholder(shared) => {
                    drop(shard_guard);
                    return EntryResult::Vacant(PlaceholderGuard::start_loading(
                        &self.lifecycle,
                        shard,
                        shared,
                    ));
                }
                EntryOrPlaceholder::ExistingPlaceholder(shared) => {
                    match PlaceholderGuard::wait_for_placeholder(
                        &self.lifecycle,
                        shard,
                        shard_guard,
                        shared,
                        deadline.as_mut(),
                    ) {
                        JoinResult::Filled(_) => continue,
                        JoinResult::Guard(g) => return EntryResult::Vacant(g),
                        JoinResult::Timeout => return EntryResult::Timeout,
                    }
                }
            }
        }
    }

    /// Async version of [`Self::entry`].
    ///
    /// Atomically accesses an existing entry, or gets a guard for insertion.
    /// If another task is already loading this key, waits asynchronously for the value.
    ///
    /// See [`entry`](Self::entry) for full documentation.
    pub async fn entry_async<'a, Q, T>(
        &'a self,
        key: &Q,
        on_occupied: impl FnOnce(&Key, &mut Val) -> EntryAction<T>,
    ) -> EntryResult<'a, Key, Val, We, B, L, T>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let (shard, hash) = self.shard_for(key).unwrap();
        // See entry() for explanation of the Option::take pattern.
        let mut on_occupied = Some(on_occupied);
        let mut callback = |k: &Key, v: &mut Val| on_occupied.take().unwrap()(k, v);

        loop {
            // Scope the write guard so it doesn't appear in the async state machine,
            // which would make the future !Send.
            let result = {
                let mut shard_guard = shard.write();
                match shard_guard.entry_or_placeholder(hash, key, &mut callback) {
                    EntryOrPlaceholder::Kept(t) => Ok(EntryResult::Retained(t)),
                    EntryOrPlaceholder::Removed(k, v) => Ok(EntryResult::Removed(k, v)),
                    EntryOrPlaceholder::Replaced(shared, old_val) => {
                        drop(shard_guard);
                        Ok(EntryResult::Replaced(
                            PlaceholderGuard::start_loading(&self.lifecycle, shard, shared),
                            old_val,
                        ))
                    }
                    EntryOrPlaceholder::NewPlaceholder(shared) => {
                        drop(shard_guard);
                        Ok(EntryResult::Vacant(PlaceholderGuard::start_loading(
                            &self.lifecycle,
                            shard,
                            shared,
                        )))
                    }
                    EntryOrPlaceholder::ExistingPlaceholder(_) => Err(()),
                }
            };
            match result {
                Ok(entry_result) => return entry_result,
                Err(()) => match JoinFuture::new(&self.lifecycle, shard, hash, key).await {
                    JoinResult::Filled(_) => continue,
                    JoinResult::Guard(g) => return EntryResult::Vacant(g),
                    JoinResult::Timeout => unsafe { unreachable_unchecked() },
                },
            }
        }
    }

    /// Get total memory used by cache data structures
    ///
    /// It should be noted that if cache key or value is some type like `Vec<T>`,
    /// the memory allocated in the heap will not be counted.
    pub fn memory_used(&self) -> MemoryUsed {
        let mut total = MemoryUsed { entries: 0, map: 0 };
        self.shards.iter().for_each(|shard| {
            let shard_memory = shard.read().memory_used();
            total.entries += shard_memory.entries;
            total.map += shard_memory.map;
        });
        total
    }
}

impl<Key, Val, We, B, L> std::fmt::Debug for Cache<Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}

/// Iterator over the items in the cache.
///
/// See [`Cache::iter`] for more details.
pub struct Iter<'a, Key, Val, We, B, L> {
    shards: &'a [RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>],
    current_shard: usize,
    last: Option<Token>,
}

impl<Key, Val, We, B, L> Iterator for Iter<'_, Key, Val, We, B, L>
where
    Key: Clone,
    Val: Clone,
{
    type Item = (Key, Val);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_shard < self.shards.len() {
            let shard = &self.shards[self.current_shard];
            let lock = shard.read();
            if let Some((new_last, key, val)) = lock.iter_from(self.last).next() {
                self.last = Some(new_last);
                return Some((key.clone(), val.clone()));
            }
            self.last = None;
            self.current_shard += 1;
        }
        None
    }
}

impl<Key, Val, We, B, L> std::fmt::Debug for Iter<'_, Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iter").finish_non_exhaustive()
    }
}

/// Draining iterator for the items in the cache.
///
/// See [`Cache::drain`] for more details.
pub struct Drain<'a, Key, Val, We, B, L> {
    shards: &'a [RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>],
    current_shard: usize,
    last: Option<Token>,
}

impl<Key, Val, We, B, L> Iterator for Drain<'_, Key, Val, We, B, L>
where
    Key: Hash + Eq,
    We: Weighter<Key, Val>,
    B: BuildHasher,
    L: Lifecycle<Key, Val>,
{
    type Item = (Key, Val);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_shard < self.shards.len() {
            let shard = &self.shards[self.current_shard];
            let mut lock = shard.write();
            if let Some((new_last, key, value)) = lock.remove_next(self.last) {
                self.last = Some(new_last);
                return Some((key, value));
            }
            self.last = None;
            self.current_shard += 1;
        }
        None
    }
}

impl<Key, Val, We, B, L> std::fmt::Debug for Drain<'_, Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Drain").finish_non_exhaustive()
    }
}

/// Default `Lifecycle` for a sync cache.
///
/// Stashes up to two evicted items for dropping them outside the cache locks.
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
    // Why two items?
    // Because assuming the cache has roughly similarly weighted items,
    // we can expect that at one or two items will be evicted per request
    // in most cases. And we want to avoid introducing any extra
    // overhead (e.g. a vector) for this default lifecycle.
    type RequestState = [Option<(Key, Val)>; 2];

    #[inline]
    fn begin_request(&self) -> Self::RequestState {
        [None, None]
    }

    #[inline]
    fn on_evict(&self, state: &mut Self::RequestState, key: Key, val: Val) {
        if std::mem::needs_drop::<(Key, Val)>() {
            if state[0].is_none() {
                state[0] = Some((key, val));
            } else if state[1].is_none() {
                state[1] = Some((key, val));
            }
        }
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

    #[test]
    fn test_iter() {
        let capacity = if cfg!(miri) { 100 } else { 100000 };
        let options = OptionsBuilder::new()
            .estimated_items_capacity(capacity)
            .weight_capacity(capacity as u64)
            .shards(2)
            .build()
            .unwrap();
        let cache = Cache::with_options(
            options,
            UnitWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );
        let items = capacity / 2;
        for i in 0..items {
            cache.insert(i, i);
        }
        assert_eq!(cache.len(), items);
        let mut iter_collected = cache.iter().collect::<Vec<_>>();
        assert_eq!(iter_collected.len(), items);
        iter_collected.sort();
        for (i, v) in iter_collected.into_iter().enumerate() {
            assert_eq!((i, i), v);
        }
    }

    #[test]
    fn test_drain() {
        let capacity = if cfg!(miri) { 100 } else { 100000 };
        let options = OptionsBuilder::new()
            .estimated_items_capacity(capacity)
            .weight_capacity(capacity as u64)
            .shards(2)
            .build()
            .unwrap();
        let cache = Cache::with_options(
            options,
            UnitWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        );
        let items = capacity / 2;
        for i in 0..items {
            cache.insert(i, i);
        }
        assert_eq!(cache.len(), items);
        let mut drain_collected = cache.drain().collect::<Vec<_>>();
        assert_eq!(cache.len(), 0);
        assert_eq!(drain_collected.len(), items);
        drain_collected.sort();
        for (i, v) in drain_collected.into_iter().enumerate() {
            assert_eq!((i, i), v);
        }
    }

    #[test]
    fn test_set_capacity() {
        let cache = Cache::new(100);
        for i in 0..80 {
            cache.insert(i, i);
        }
        let initial_len = cache.len();
        assert!(initial_len <= 80);

        // Set to smaller capacity
        cache.set_capacity(50);
        assert!(cache.len() <= 50);
        assert!(cache.weight() <= 50);

        // Set to larger capacity
        cache.set_capacity(200);
        assert_eq!(cache.capacity(), 200);

        // Insert more items
        for i in 100..180 {
            cache.insert(i, i);
        }
        assert!(cache.len() <= 180);
        assert!(cache.weight() <= 200);
    }

    #[test]
    fn test_remove_if() {
        let cache = Cache::new(100);

        // Insert test data
        cache.insert(1, 10);
        cache.insert(2, 20);
        cache.insert(3, 30);

        // Test removing with predicate that returns true
        let removed = cache.remove_if(&2, |v| *v == 20);
        assert_eq!(removed, Some((2, 20)));
        assert_eq!(cache.get(&2), None);

        // Test removing with predicate that returns false
        let not_removed = cache.remove_if(&3, |v| *v == 999);
        assert_eq!(not_removed, None);
        assert_eq!(cache.get(&3), Some(30));

        // Test removing non-existent key
        let not_found = cache.remove_if(&999, |_| true);
        assert_eq!(not_found, None);
    }

    /// Tests all basic entry actions: Retain, Remove, ReplaceWithGuard, Vacant, mutate+Retain
    #[test]
    fn test_entry_actions() {
        let cache = Cache::new(100);
        cache.insert(1, 10);
        cache.insert(2, 20);

        // Retain returns the value via callback, entry stays
        let result = cache.entry(&1, None, |_k, v| EntryAction::Retain(*v));
        assert!(matches!(result, EntryResult::Retained(10)));
        assert_eq!(cache.get(&1), Some(10));

        // Mutate in place via Retain
        let result = cache.entry(&1, None, |_k, v| {
            *v += 5;
            EntryAction::Retain(())
        });
        assert!(matches!(result, EntryResult::Retained(())));
        assert_eq!(cache.get(&1), Some(15));

        // Remove
        let result = cache.entry(&1, None, |_k, _v| EntryAction::<()>::Remove);
        assert!(matches!(result, EntryResult::Removed(1, 15)));
        assert_eq!(cache.get(&1), None);

        // Remove then re-enter same key → Vacant
        let result = cache.entry(&1, None, |_k, v| EntryAction::Retain(*v));
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(99);
                assert_eq!(cache.get(&1), Some(99));
            }
            _ => panic!("expected Vacant for removed key"),
        }

        // ReplaceWithGuard: capture old value, get guard, insert new
        let mut old_val = 0;
        let result = cache.entry(&2, None, |_k, v| {
            old_val = *v;
            EntryAction::<()>::ReplaceWithGuard
        });
        assert_eq!(old_val, 20);
        match result {
            EntryResult::Replaced(g, old) => {
                assert_eq!(old, 20);
                let _ = g.insert(old_val + 100);
                assert_eq!(cache.get(&2), Some(120));
            }
            _ => panic!("expected Replaced"),
        }

        // ReplaceWithGuard then abandon guard → entry gone
        let result = cache.entry(&2, None, |_k, _v| EntryAction::<()>::ReplaceWithGuard);
        match result {
            EntryResult::Replaced(g, _old) => {
                drop(g);
                assert_eq!(cache.get(&2), None);
            }
            _ => panic!("expected Replaced"),
        }

        // Vacant key → guard
        let result = cache.entry(&3, None, |_k, v| EntryAction::Retain(*v));
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(30);
                assert_eq!(cache.get(&3), Some(30));
            }
            _ => panic!("expected Vacant"),
        }
    }

    /// Tests weight tracking across all entry actions using a string-length weighter
    #[test]
    fn test_entry_weight_tracking() {
        #[derive(Clone)]
        struct StringWeighter;
        impl crate::Weighter<u64, String> for StringWeighter {
            fn weight(&self, _key: &u64, val: &String) -> u64 {
                val.len() as u64
            }
        }

        let cache = Cache::with_weighter(100, 100_000, StringWeighter);
        cache.insert(1, "hello".to_string());
        cache.insert(2, "world".to_string());
        assert_eq!(cache.weight(), 10);

        // Retain without mutation — weight unchanged
        let result = cache.entry(&1, None, |_k, _v| EntryAction::Retain(()));
        assert!(matches!(result, EntryResult::Retained(())));
        assert_eq!(cache.weight(), 10);

        // Mutate to longer string — weight increases
        let result = cache.entry(&1, None, |_k, v| {
            v.push_str(" world");
            EntryAction::Retain(())
        });
        assert!(matches!(result, EntryResult::Retained(())));
        assert_eq!(cache.weight(), 16); // "hello world" (11) + "world" (5)
        assert_eq!(cache.get(&1).unwrap(), "hello world");

        // Mutate to empty string — weight to zero, entry stays
        let result = cache.entry(&1, None, |_k, v| {
            v.clear();
            EntryAction::Retain(())
        });
        assert!(matches!(result, EntryResult::Retained(())));
        assert_eq!(cache.weight(), 5); // "" (0) + "world" (5)
        assert_eq!(cache.get(&1).unwrap(), "");

        // Remove — weight decremented
        let result = cache.entry(&2, None, |_k, _v| EntryAction::<()>::Remove);
        assert!(matches!(result, EntryResult::Removed(2, _)));
        assert_eq!(cache.weight(), 0);
        assert_eq!(cache.len(), 1);

        // ReplaceWithGuard — old weight gone, new weight after insert
        cache.insert(3, "hello".to_string());
        assert_eq!(cache.weight(), 5);
        let result = cache.entry(&3, None, |_k, _v| EntryAction::<()>::ReplaceWithGuard);
        match result {
            EntryResult::Replaced(g, _old) => {
                assert_eq!(cache.weight(), 0);
                let _ = g.insert("hello world!!".to_string());
                assert_eq!(cache.weight(), 13);
            }
            _ => panic!("expected Replaced"),
        }
    }

    /// Tests eviction and zero-capacity edge cases
    #[test]
    fn test_entry_eviction() {
        // Cache with capacity for ~2 items — insert 3rd triggers eviction
        let cache = Cache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        assert_eq!(cache.len(), 2);

        let result = cache.entry(&3, None, |_k, v| EntryAction::Retain(*v));
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(30);
                assert!(cache.len() <= 2);
                assert_eq!(cache.get(&3), Some(30));
            }
            _ => panic!("expected Vacant"),
        }

        // Zero-capacity cache — insert evicts immediately
        let cache = Cache::new(0);
        let result = cache.entry(&1, None, |_k, v| EntryAction::Retain(*v));
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(10);
                assert_eq!(cache.get(&1), None);
            }
            _ => panic!("expected Vacant"),
        }
    }

    /// Tests entry() waiting on existing placeholder: value arrives, guard abandoned
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_concurrent_placeholder_wait() {
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        // Thread holds guard, inserts after delay
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let handle = thread::spawn(move || match cache2.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => {
                barrier2.wait();
                std::thread::sleep(Duration::from_millis(50));
                let _ = g.insert(42);
            }
            _ => panic!("expected guard"),
        });

        barrier.wait();
        let result = cache.entry(&1, None, |_k, v| EntryAction::Retain(*v));
        assert!(matches!(result, EntryResult::Retained(42)));
        handle.join().unwrap();
    }

    /// Tests entry() getting guard when placeholder loader abandons
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_concurrent_placeholder_guard_abandoned() {
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let handle = thread::spawn(move || match cache2.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => {
                barrier2.wait();
                std::thread::sleep(Duration::from_millis(50));
                drop(g);
            }
            _ => panic!("expected guard"),
        });

        barrier.wait();
        let result = cache.entry(&1, None, |_k, v| EntryAction::Retain(*v));
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(99);
                assert_eq!(cache.get(&1), Some(99));
            }
            _ => panic!("expected Vacant after abandoned placeholder"),
        }
        handle.join().unwrap();
    }

    /// Tests zero and nonzero timeouts
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_timeout() {
        let cache = Cache::new(100);

        // Zero timeout — immediate Timeout when placeholder exists
        let guard = match cache.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => g,
            _ => panic!("expected guard"),
        };
        let result = cache.entry(&1, Some(Duration::ZERO), |_k, v| EntryAction::Retain(*v));
        assert!(matches!(result, EntryResult::Timeout));
        let _ = guard.insert(1);

        // Nonzero timeout — guard held longer than timeout
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));
        let cache2 = cache.clone();
        let barrier2 = barrier.clone();
        let holder = thread::spawn(move || {
            let guard = match cache2.get_value_or_guard(&1, None) {
                GuardResult::Guard(g) => g,
                _ => panic!("expected guard"),
            };
            barrier2.wait();
            std::thread::sleep(Duration::from_millis(200));
            let _ = guard.insert(1);
        });

        barrier.wait();
        let result = cache.entry(&1, Some(Duration::from_millis(50)), |_k, v| {
            EntryAction::Retain(*v)
        });
        assert!(matches!(result, EntryResult::Timeout));
        holder.join().unwrap();
    }

    /// Tests multiple waiters all receiving the value
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_concurrent_multiple_waiters() {
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(4)); // 1 loader + 3 waiters

        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let loader = thread::spawn(move || match cache1.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => {
                barrier1.wait();
                std::thread::sleep(Duration::from_millis(50));
                let _ = g.insert(42);
            }
            _ => panic!("expected guard"),
        });

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let cache_c = cache.clone();
            let barrier_c = barrier.clone();
            waiters.push(thread::spawn(move || {
                barrier_c.wait();
                let result = cache_c.entry(&1, None, |_k, v| EntryAction::Retain(*v));
                match result {
                    EntryResult::Retained(v) => v,
                    _ => panic!("expected Value"),
                }
            }));
        }

        loader.join().unwrap();
        for w in waiters {
            assert_eq!(w.join().unwrap(), 42);
        }
    }

    /// Tests ReplaceWithGuard and Remove actions after waiting for a placeholder
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_concurrent_action_after_wait() {
        // ReplaceWithGuard after wait
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let loader = thread::spawn(move || match cache1.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => {
                barrier1.wait();
                std::thread::sleep(Duration::from_millis(50));
                let _ = g.insert(42);
            }
            _ => panic!("expected guard"),
        });

        barrier.wait();
        let result = cache.entry(&1, None, |_k, _v| EntryAction::<()>::ReplaceWithGuard);
        match result {
            EntryResult::Replaced(g, old) => {
                assert_eq!(old, 42);
                let _ = g.insert(100);
                assert_eq!(cache.get(&1), Some(100));
            }
            _ => panic!("expected Replaced"),
        }
        loader.join().unwrap();

        // Remove after wait
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let loader = thread::spawn(move || match cache1.get_value_or_guard(&1, None) {
            GuardResult::Guard(g) => {
                barrier1.wait();
                std::thread::sleep(Duration::from_millis(50));
                let _ = g.insert(42);
            }
            _ => panic!("expected guard"),
        });

        barrier.wait();
        let result = cache.entry(&1, None, |_k, _v| EntryAction::<()>::Remove);
        assert!(matches!(result, EntryResult::Removed(1, 42)));
        assert_eq!(cache.get(&1), None);
        loader.join().unwrap();
    }

    /// Multi-thread stress test for entry()
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_entry_concurrent_stress() {
        const N_THREADS: usize = 8;
        const N_KEYS: usize = 50;
        const N_OPS: usize = 500;

        let cache = Arc::new(Cache::new(1000));
        let barrier = Arc::new(Barrier::new(N_THREADS));

        let mut handles = Vec::new();
        for t in 0..N_THREADS {
            let cache = cache.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..N_OPS {
                    let key = (t * N_OPS + i) % N_KEYS;
                    let result = cache.entry(&key, Some(Duration::from_millis(10)), |_k, v| {
                        EntryAction::Retain(*v)
                    });
                    match result {
                        EntryResult::Retained(_) => {}
                        EntryResult::Vacant(g) => {
                            let _ = g.insert(key * 10);
                        }
                        EntryResult::Replaced(g, _) => {
                            let _ = g.insert(key * 10);
                        }
                        EntryResult::Timeout => {}
                        EntryResult::Removed(_, _) => {}
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert!(cache.len() <= N_KEYS);
        for key in 0..N_KEYS {
            if let Some(v) = cache.get(&key) {
                assert_eq!(v, key * 10);
            }
        }
    }

    // --- Async tests ---

    /// Tests all basic async entry actions in one test
    #[tokio::test]
    async fn test_entry_async_actions() {
        let cache = Cache::new(100);
        cache.insert(1, 10);
        cache.insert(2, 20);

        // Retain
        let result = cache.entry_async(&1, |_k, v| EntryAction::Retain(*v)).await;
        assert!(matches!(result, EntryResult::Retained(10)));
        assert_eq!(cache.get(&1), Some(10));

        // Remove
        let result = cache
            .entry_async(&1, |_k, _v| EntryAction::<()>::Remove)
            .await;
        assert!(matches!(result, EntryResult::Removed(1, 10)));
        assert_eq!(cache.get(&1), None);

        // ReplaceWithGuard
        let result = cache
            .entry_async(&2, |_k, _v| EntryAction::<()>::ReplaceWithGuard)
            .await;
        match result {
            EntryResult::Replaced(g, old) => {
                assert_eq!(old, 20);
                let _ = g.insert(42);
                assert_eq!(cache.get(&2), Some(42));
            }
            _ => panic!("expected Replaced"),
        }

        // Vacant
        let result = cache.entry_async(&3, |_k, v| EntryAction::Retain(*v)).await;
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(99);
                assert_eq!(cache.get(&3), Some(99));
            }
            _ => panic!("expected Vacant"),
        }
    }

    /// Tests async entry waiting on placeholder: value arrives, guard abandoned
    #[tokio::test(flavor = "multi_thread")]
    async fn test_entry_async_concurrent_wait() {
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let holder = thread::spawn(move || {
            let guard = match cache1.get_value_or_guard(&1, None) {
                GuardResult::Guard(g) => g,
                _ => panic!("expected guard"),
            };
            barrier1.wait();
            std::thread::sleep(Duration::from_millis(50));
            let _ = guard.insert(42);
        });

        barrier.wait();
        let result = cache.entry_async(&1, |_k, v| EntryAction::Retain(*v)).await;
        assert!(matches!(result, EntryResult::Retained(42)));
        holder.join().unwrap();
    }

    /// Tests async entry getting guard when placeholder loader abandons
    #[tokio::test(flavor = "multi_thread")]
    async fn test_entry_async_concurrent_guard_abandoned() {
        let cache = Arc::new(Cache::new(100));
        let barrier = Arc::new(Barrier::new(2));

        let cache1 = cache.clone();
        let barrier1 = barrier.clone();
        let holder = thread::spawn(move || {
            let guard = match cache1.get_value_or_guard(&1, None) {
                GuardResult::Guard(g) => g,
                _ => panic!("expected guard"),
            };
            barrier1.wait();
            std::thread::sleep(Duration::from_millis(50));
            drop(guard);
        });

        barrier.wait();
        let result = cache.entry_async(&1, |_k, v| EntryAction::Retain(*v)).await;
        match result {
            EntryResult::Vacant(g) => {
                let _ = g.insert(99);
            }
            _ => panic!("expected Vacant after abandoned placeholder"),
        }
        assert_eq!(cache.get(&1), Some(99));
        holder.join().unwrap();
    }

    /// Multi-task async stress test
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(miri, ignore)]
    async fn test_entry_async_concurrent_stress() {
        const N_TASKS: usize = 16;
        const N_KEYS: usize = 50;
        const N_OPS: usize = 200;

        let cache = Arc::new(Cache::new(1000));
        let barrier = Arc::new(tokio::sync::Barrier::new(N_TASKS));

        let mut handles = Vec::new();
        for t in 0..N_TASKS {
            let cache = cache.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..N_OPS {
                    let key = (t * N_OPS + i) % N_KEYS;
                    // Use get_or_insert_async instead of entry_async to avoid
                    // lifetime issues with tokio::spawn (entry_async borrows &self)
                    let _ = cache
                        .get_or_insert_async(&key, async { Ok::<_, ()>(key * 10) })
                        .await;
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert!(cache.len() <= N_KEYS);
        for key in 0..N_KEYS {
            if let Some(v) = cache.get(&key) {
                assert_eq!(v, key * 10);
            }
        }
    }
}
