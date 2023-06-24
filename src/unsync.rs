use crate::{
    options::*,
    shard::{Entry, KQCacheShard},
    DefaultHashBuilder, UnitWeighter, Weighter,
};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};

/// A two key cache.
///
/// # Key and Version
/// The key qey pair exists for cases where you want a cache keyed by (K, Q).
/// Other rust maps/caches are accessed via the Borrow trait,
/// so they require the caller to build &(K, Q) which might involve cloning K and/or Q.
pub struct KQCache<Key, Qey, Val, We = UnitWeighter, B = DefaultHashBuilder> {
    shard: KQCacheShard<Key, Qey, Val, We, B>,
}

impl<Key: Eq + Hash, Qey: Eq + Hash, Val> KQCache<Key, Qey, Val, UnitWeighter, DefaultHashBuilder> {
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

impl<Key: Eq + Hash, Qey: Eq + Hash, Val, We: Weighter<Key, Qey, Val>>
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

impl<Key: Eq + Hash, Qey: Eq + Hash, Val, We: Weighter<Key, Qey, Val>, B: BuildHasher>
    KQCache<Key, Qey, Val, We, B>
{
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    ///
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
    /// use quick_cache::{unsync::KQCache, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
    ///
    /// KQCache::<String, u64, String>::with_options(
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
        let shard = KQCacheShard::new(
            options.hot_allocation,
            options.ghost_allocation,
            options.estimated_items_capacity,
            options.weight_capacity,
            weighter,
            hash_builder,
        );
        Self { shard }
    }

    /// Returns whether the cache is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shard.len() == 0
    }

    /// Returns the number of cached items
    #[inline]
    pub fn len(&self) -> usize {
        self.shard.len()
    }

    /// Returns the total weight of cached items
    #[inline]
    pub fn weight(&self) -> u64 {
        self.shard.weight()
    }

    /// Returns the maximum weight of cached items
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.shard.capacity()
    }

    /// Returns the number of misses
    #[inline]
    pub fn misses(&self) -> u64 {
        self.shard.misses()
    }

    /// Returns the number of hits
    #[inline]
    pub fn hits(&self) -> u64 {
        self.shard.hits()
    }

    /// Reserver additional space for `additional` entries.
    ///
    /// Note that this is counted in entries, and is not weighted.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.shard.reserve(additional);
    }

    /// Fetches an item from the cache. Callers should prefer `get_mut` whenever possible as it's more efficient.
    pub fn get<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.get(self.shard.hash(key, qey), key, qey)
    }

    /// Fetches an item from the cache.
    pub fn get_mut<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, qey: &W) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.get_mut(self.shard.hash(key, qey), key, qey)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.peek(self.shard.hash(key, qey), key, qey)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek_mut<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, qey: &W) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.peek_mut(self.shard.hash(key, qey), key, qey)
    }

    /// Remove an item from the cache whose key is `key` and qey is `qey`.
    ///
    /// Returns whether an entry was removed.
    #[inline]
    pub fn remove<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, qey: &W) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.remove_evict(key, qey).is_some()
    }

    /// Remove an item from the cache whose key is `key` and qey is `qey`.
    ///
    /// Returns entry if it was removed.
    pub fn remove_evict<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, qey: &W) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard
            .remove(self.shard.hash(key, qey), key, qey)
            .and_then(|entry| {
                if let Entry::Resident(entry) = entry {
                    Some(entry.value)
                } else {
                    None
                }
            })
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    #[inline]
    pub fn insert(&mut self, key: Key, qey: Qey, value: Val) {
        self.insert_evict(key, qey, value);
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    ///
    /// Returns entries if they were removed. Vector is guarantied to be non empty.
    #[inline]
    pub fn insert_evict(&mut self, key: Key, qey: Qey, value: Val) -> Option<Vec<(Key, Qey, Val)>> {
        self.shard
            .insert(self.shard.hash(&key, &qey), key, qey, value)
            .map(|vec| vec.into_iter().filter_map(Entry::map_to_value).collect())
    }
}

impl<Key, Qey, Val, We, B> std::fmt::Debug for KQCache<Key, Qey, Val, We, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KQCache").finish_non_exhaustive()
    }
}

pub struct Cache<Key, Val, We = UnitWeighter, B = DefaultHashBuilder>(KQCache<Key, (), Val, We, B>);

impl<Key: Eq + Hash, Val> Cache<Key, Val, UnitWeighter, DefaultHashBuilder> {
    /// Creates a new cache with holds up to `items_capacity` items (approximately).
    pub fn new(items_capacity: usize) -> Self {
        Self(KQCache::new(items_capacity))
    }
}

impl<Key: Eq + Hash, Val, We: Weighter<Key, (), Val>> Cache<Key, Val, We, DefaultHashBuilder> {
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    ///
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
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

impl<Key: Eq + Hash, Val, We: Weighter<Key, (), Val>, B: BuildHasher> Cache<Key, Val, We, B> {
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    ///
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

    /// Constructs a cache based on [OptionsBuilder].
    ///
    /// # Example
    ///
    /// ```rust
    /// use quick_cache::{unsync::Cache, OptionsBuilder, UnitWeighter, DefaultHashBuilder};
    ///
    /// Cache::<String, String>::with_options(
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
        Self(KQCache::with_options(options, weighter, hash_builder))
    }

    /// Returns whether the cache is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of cached items
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the total weight of cached items
    #[inline]
    pub fn weight(&self) -> u64 {
        self.0.weight()
    }

    /// Returns the maximum weight of cached items
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.0.capacity()
    }

    /// Returns the number of misses
    #[inline]
    pub fn misses(&self) -> u64 {
        self.0.misses()
    }

    /// Returns the number of hits
    #[inline]
    pub fn hits(&self) -> u64 {
        self.0.hits()
    }

    /// Reserver additional space for `additional` entries.
    ///
    /// Note that this is counted in entries, and is not weighted.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Fetches an item from the cache.
    ///
    /// Callers should prefer `get_mut` whenever possible as it's more efficient.
    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.get(key, &())
    }

    /// Fetches an item from the cache.
    #[inline]
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.get_mut(key, &())
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    #[inline]
    pub fn peek<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.peek(key, &())
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    #[inline]
    pub fn peek_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.peek_mut(key, &())
    }

    /// Remove an item from the cache whose key is `key`.
    ///
    /// Returns whether an entry was removed.
    #[inline]
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove(key, &())
    }

    /// Remove an item from the cache whose key is `key`.
    ///
    /// Returns entry if it was removed.
    #[inline]
    pub fn remove_evict<Q: ?Sized>(&mut self, key: &Q) -> Option<Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove_evict(key, &())
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    #[inline]
    pub fn insert(&mut self, key: Key, value: Val) {
        self.0.insert(key, (), value);
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    ///
    /// Returns entries if they were removed. Vector is guarantied to be non empty.
    #[inline]
    pub fn insert_evict(&mut self, key: Key, value: Val) -> Option<Vec<(Key, Val)>> {
        self.0
            .insert_evict(key, (), value)
            .map(|vec| vec.into_iter().map(|(key, _, val)| (key, val)).collect())
    }
}

impl<Key, Val, We, B> std::fmt::Debug for Cache<Key, Val, We, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}
