use crate::{shard::KQCacheShard, DefaultHashBuilder, UnitWeighter, Weighter};
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
    ) -> KQCache<Key, Qey, Val, We, DefaultHashBuilder> {
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
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hasher: B,
    ) -> Self {
        let shard = KQCacheShard::new(estimated_items_capacity, weight_capacity, weighter, hasher);
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

    /// Peeks an item from the cache whose key is `key` and qey is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized, W: ?Sized>(&mut self, key: &Q, qey: &W) -> bool
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        matches!(
            self.shard.remove(self.shard.hash(key, qey), key, qey),
            Some(Ok(_))
        )
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    pub fn insert(&mut self, key: Key, qey: Qey, value: Val) {
        self.shard
            .insert(self.shard.hash(&key, &qey), key, qey, value);
    }
}

impl<Key, Qey, Val> std::fmt::Debug for KQCache<Key, Qey, Val> {
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
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with_weighter(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
    ) -> Cache<Key, Val, We, DefaultHashBuilder> {
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
    /// `estimated_items_capacity` is the estimated number of items the cache is expected to hold,
    /// roughly equivalent to `weight_capacity / average item weight`.
    pub fn with(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hasher: B,
    ) -> Self {
        Self(KQCache::with(
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

    /// Peeks an item from the cache whose key is `key` and qey is <= `highest_version`.
    /// Contrary to gets, peeks don't alter the key "hotness".
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        Key: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.0.remove(key, &())
    }

    /// Inserts an item in the cache with key `key` and qey `qey`.
    pub fn insert(&mut self, key: Key, value: Val) {
        self.0.insert(key, (), value);
    }
}

impl<Key: Eq + Hash, Val> std::fmt::Debug for Cache<Key, Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}
