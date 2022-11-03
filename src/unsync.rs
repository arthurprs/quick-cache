use crate::{shard::VersionedCacheShard, DefaultHashBuilder, UnitWeighter, Weighter};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};

/// A version aware cache.
///
/// # Key and Version
/// The key version pair exists for cases where you want a cache keyed by (T, U).
/// Other rust maps/caches are accessed via the Borrow trait,
/// so they require the caller to build &(T, U) which might involve cloning T and/or U.
pub struct VersionedCache<Key, Ver, Val, We = UnitWeighter, B = DefaultHashBuilder> {
    shard: VersionedCacheShard<Key, Ver, Val, We, B>,
}

impl<Key: Eq + Hash, Ver: Eq + Hash, Val>
    VersionedCache<Key, Ver, Val, UnitWeighter, DefaultHashBuilder>
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

impl<Key: Eq + Hash, Ver: Eq + Hash, Val, We: Weighter<Key, Ver, Val>, B: BuildHasher>
    VersionedCache<Key, Ver, Val, We, B>
{
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hasher: B,
    ) -> Self {
        let shard =
            VersionedCacheShard::new(estimated_items_capacity, weight_capacity, weighter, hasher);
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
    pub fn get<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.get(self.shard.hash(key, version), key, version)
    }

    /// Fetches an item from the cache.
    pub fn get_mut<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, version: &W) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard
            .get_mut(self.shard.hash(key, version), key, version)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard.peek(self.shard.hash(key, version), key, version)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek_mut<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, version: &W) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        self.shard
            .peek_mut(self.shard.hash(key, version), key, version)
    }

    /// Peeks an item from the cache whose key is `key` and version is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, version: &W) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        matches!(
            self.shard
                .remove(self.shard.hash(key, version), key, version),
            Some(Ok(_))
        )
    }

    /// Inserts an item in the cache with key `key` and version `version`.
    pub fn insert(&mut self, key: Key, version: Ver, value: Val) {
        self.shard
            .insert(self.shard.hash(&key, &version), key, version, value);
    }
}

impl<Key, Ver, Val> std::fmt::Debug for VersionedCache<Key, Ver, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionedCache").finish_non_exhaustive()
    }
}

pub struct Cache<Key, Val, We = UnitWeighter, B = DefaultHashBuilder>(
    VersionedCache<Key, (), Val, We, B>,
);

impl<Key: Eq + Hash, Val> Cache<Key, Val, UnitWeighter, DefaultHashBuilder> {
    /// Creates a new cache with holds up to `items_capacity` items (approximately).
    pub fn new(items_capacity: usize) -> Self {
        Self(VersionedCache::new(items_capacity))
    }
}

impl<Key: Eq + Hash, Val, We: Weighter<Key, (), Val>, B: BuildHasher> Cache<Key, Val, We, B> {
    /// Creates a new cache that can hold up to `weight_capacity` in weight.
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hasher: B,
    ) -> Self {
        Self(VersionedCache::with(
            estimated_items_capacity,
            weight_capacity,
            weighter,
            hasher,
        ))
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
        self.0.reserve(additional);
    }

    /// Fetches an item from the cache.
    /// Callers should prefer `get_mut` whenever possible as it's more efficient.
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.get(key, &())
    }

    /// Fetches an item from the cache.
    pub fn get_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.get_mut(key, &())
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q: ?Sized>(&self, key: &Q) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.peek(key, &())
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek_mut<Q: ?Sized>(&mut self, key: &Q) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.peek_mut(key, &())
    }

    /// Peeks an item from the cache whose key is `key` and version is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove(key, &())
    }

    /// Inserts an item in the cache with key `key` and version `version`.
    pub fn insert(&mut self, key: Key, value: Val) {
        self.0.insert(key, (), value);
    }
}

impl<Key: Eq + Hash, Val> std::fmt::Debug for Cache<Key, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}
