use crate::{shard::VersionedCacheShard, DefaultHashBuilder};
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
pub struct VersionedCache<Key, Ver, Val, B = DefaultHashBuilder> {
    shard: VersionedCacheShard<Key, Ver, Val, B>,
}

impl<Key: Eq + Hash, Ver: Eq + Hash, Val, B: Default + BuildHasher>
    VersionedCache<Key, Ver, Val, B>
{
    /// Creates a new cache with holds up to `capacity` items (approximately).
    pub fn new(initial_capacity: usize, max_capacity: usize) -> Self {
        assert!(initial_capacity <= max_capacity);
        let shard = VersionedCacheShard::new(initial_capacity, max_capacity, B::default());
        Self { shard }
    }

    /// Returns the number of cached items
    pub fn len(&self) -> usize {
        self.shard.len()
    }

    /// Returns the maximum number of cached items
    pub fn capacity(&self) -> usize {
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
                .remove(self.shard.hash(&key, &version), key, version),
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

pub struct Cache<Key, Val, B = DefaultHashBuilder>(VersionedCache<Key, (), Val, B>);

impl<Key: Eq + Hash, Val, B: Default + Clone + BuildHasher> Cache<Key, Val, B> {
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
