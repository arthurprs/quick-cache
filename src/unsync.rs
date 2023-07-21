use crate::{
    options::*,
    shard::{CacheShard, InsertStrategy},
    DefaultHashBuilder, Equivalent, Lifecycle, UnitWeighter, Weighter,
};
use std::hash::{BuildHasher, Hash};

pub struct Cache<
    Key,
    Val,
    We = UnitWeighter,
    B = DefaultHashBuilder,
    L = DefaultLifecycle<Key, Val>,
> {
    shard: CacheShard<Key, Val, We, B, L>,
}

impl<Key: Eq + Hash, Val> Cache<Key, Val> {
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

impl<Key: Eq + Hash, Val, We: Weighter<Key, Val>> Cache<Key, Val, We> {
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

impl<Key: Eq + Hash, Val, We: Weighter<Key, Val>, B: BuildHasher, L: Lifecycle<Key, Val>>
    Cache<Key, Val, We, B, L>
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
    /// use quick_cache::{unsync::{Cache, DefaultLifecycle}, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
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
        let shard = CacheShard::new(
            options.hot_allocation,
            options.ghost_allocation,
            options.estimated_items_capacity,
            options.weight_capacity,
            weighter,
            hash_builder,
            lifecycle,
        );
        Self { shard }
    }

    /// Returns whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.shard.len() == 0
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.shard.len()
    }

    /// Returns the total weight of cached items
    pub fn weight(&self) -> u64 {
        self.shard.weight()
    }

    /// Returns the maximum weight of cached items
    pub fn capacity(&self) -> u64 {
        self.shard.capacity()
    }

    /// Returns the number of misses
    pub fn misses(&self) -> u64 {
        self.shard.misses()
    }

    /// Returns the number of hits
    pub fn hits(&self) -> u64 {
        self.shard.hits()
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        self.shard.reserve(additional);
    }

    /// Fetches an item from the cache. Callers should prefer `get_mut` whenever possible as it's more efficient.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.shard.get(self.shard.hash(key), key)
    }

    /// Fetches an item from the cache.
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.shard.get_mut(self.shard.hash(key), key)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.shard.peek(self.shard.hash(key), key)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.shard.peek_mut(self.shard.hash(key), key)
    }

    /// Remove an item from the cache whose key is `key`.
    /// Returns the removed entry, if any.
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.shard.remove(self.shard.hash(key), key)
    }

    /// Replaces an item in the cache, but only if it already exists.
    /// If `soft` is set, the replace operation won't affect the "hotness" of the key,
    /// even if the value is replaced.
    ///
    /// Returns `Ok` if the entry was admitted and `Err(_)` if it wasn't.
    pub fn replace(&mut self, key: Key, value: Val, soft: bool) -> Result<(), (Key, Val)> {
        let lcs = self.replace_with_lifecycle(key, value, soft)?;
        self.shard.lifecycle.end_request(lcs);
        Ok(())
    }

    /// Replaces an item in the cache, but only if it already exists.
    /// If `soft` is set, the replace operation won't affect the "hotness" of the key,
    /// even if the value is replaced.
    ///
    /// Returns `Ok` if the entry was admitted and `Err(_)` if it wasn't.
    pub fn replace_with_lifecycle(
        &mut self,
        key: Key,
        value: Val,
        soft: bool,
    ) -> Result<L::RequestState, (Key, Val)> {
        let mut lcs = self.shard.lifecycle.begin_request();
        self.shard.insert(
            &mut lcs,
            self.shard.hash(&key),
            key,
            value,
            InsertStrategy::Replace { soft },
        )?;
        Ok(lcs)
    }

    /// Inserts an item in the cache with key `key`.
    pub fn insert(&mut self, key: Key, value: Val) {
        let lcs = self.insert_with_lifecycle(key, value);
        self.shard.lifecycle.end_request(lcs);
    }

    /// Inserts an item in the cache with key `key`.
    pub fn insert_with_lifecycle(&mut self, key: Key, value: Val) -> L::RequestState {
        let mut lcs = self.shard.lifecycle.begin_request();
        let result = self.shard.insert(
            &mut lcs,
            self.shard.hash(&key),
            key,
            value,
            InsertStrategy::Insert,
        );
        // result cannot err with the Insert strategy
        debug_assert!(result.is_ok());
        lcs
    }
}

impl<Key, Val, We, B, L> std::fmt::Debug for Cache<Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}

/// Default `Lifecycle` for the unsync cache.
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
    type RequestState = ();

    #[inline]
    fn begin_request(&self) -> Self::RequestState {}

    #[inline]
    fn on_evict(&self, _state: &mut Self::RequestState, _key: Key, _val: Val) {}
}
