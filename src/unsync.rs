use crate::{
    linked_slab::Token,
    options::*,
    shard::{self, CacheShard, InsertStrategy},
    DefaultHashBuilder, Equivalent, Lifecycle, UnitWeighter, Weighter,
};
use std::hash::{BuildHasher, Hash};

/// A non-concurrent cache.
#[derive(Clone)]
pub struct Cache<
    Key,
    Val,
    We = UnitWeighter,
    B = DefaultHashBuilder,
    L = DefaultLifecycle<Key, Val>,
> {
    shard: CacheShard<Key, Val, We, B, L, SharedPlaceholder>,
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
    #[cfg(feature = "stats")]
    pub fn misses(&self) -> u64 {
        self.shard.misses()
    }

    /// Returns the number of hits
    #[cfg(feature = "stats")]
    pub fn hits(&self) -> u64 {
        self.shard.hits()
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        self.shard.reserve(additional);
    }

    /// Check if a key exist in the cache.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard.contains(self.shard.hash(key), key)
    }

    /// Fetches an item from the cache.
    pub fn get<Q>(&self, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard.get(self.shard.hash(key), key)
    }

    /// Fetches an item from the cache.
    ///
    /// Note: Leaking the returned RefMut might cause undefined behavior.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<RefMut<'_, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard.get_mut(self.shard.hash(key), key).map(RefMut)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    pub fn peek<Q>(&self, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard.peek(self.shard.hash(key), key)
    }

    /// Peeks an item from the cache. Contrary to gets, peeks don't alter the key "hotness".
    ///
    /// Note: Leaking the returned RefMut might cause undefined behavior.
    pub fn peek_mut<Q>(&mut self, key: &Q) -> Option<RefMut<'_, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.shard.peek_mut(self.shard.hash(key), key).map(RefMut)
    }

    /// Remove an item from the cache whose key is `key`.
    /// Returns the removed entry, if any.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
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

    /// Retains only the items specified by the predicate.
    /// In other words, remove all items for which `f(&key, &value)` returns `false`. The
    /// elements are visited in arbitrary order.
    pub fn retain<F>(&mut self, f: F)
    where
        F: Fn(&Key, &Val) -> bool,
    {
        self.shard.retain(f);
    }

    /// Gets or inserts an item in the cache with key `key`.
    /// Returns a reference to the inserted `value` if it was admitted to the cache.
    ///
    /// See also `get_ref_or_guard`.
    pub fn get_or_insert_with<Q, E>(
        &mut self,
        key: &Q,
        with: impl FnOnce() -> Result<Val, E>,
    ) -> Result<Option<&Val>, E>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let idx = match self.shard.upsert_placeholder(self.shard.hash(key), key) {
            Ok((idx, _)) => idx,
            Err((plh, _)) => {
                let v = with()?;
                let mut lcs = self.shard.lifecycle.begin_request();
                let replaced = self.shard.replace_placeholder(&mut lcs, &plh, false, v);
                self.shard.lifecycle.end_request(lcs);
                debug_assert!(replaced.is_ok(), "unsync replace_placeholder can't fail");
                plh.idx
            }
        };
        Ok(self.shard.peek_token(idx))
    }

    /// Gets or inserts an item in the cache with key `key`.
    /// Returns a mutable reference to the inserted `value` if it was admitted to the cache.
    ///
    /// See also `get_mut_or_guard`.
    pub fn get_mut_or_insert_with<'a, Q, E>(
        &'a mut self,
        key: &Q,
        with: impl FnOnce() -> Result<Val, E>,
    ) -> Result<Option<RefMut<'a, Key, Val, We, B, L>>, E>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let idx = match self.shard.upsert_placeholder(self.shard.hash(key), key) {
            Ok((idx, _)) => idx,
            Err((plh, _)) => {
                let v = with()?;
                let mut lcs = self.shard.lifecycle.begin_request();
                let replaced = self.shard.replace_placeholder(&mut lcs, &plh, false, v);
                debug_assert!(replaced.is_ok(), "unsync replace_placeholder can't fail");
                self.shard.lifecycle.end_request(lcs);
                plh.idx
            }
        };
        Ok(self.shard.peek_token_mut(idx).map(RefMut))
    }

    /// Gets an item from the cache with key `key` .
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    pub fn get_ref_or_guard<Q>(&mut self, key: &Q) -> Result<&Val, Guard<'_, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        // TODO: this could be using a simpler entry API
        match self.shard.upsert_placeholder(self.shard.hash(key), key) {
            Ok((_, v)) => unsafe {
                // Rustc gets insanely confused about returning from mut borrows
                // Safety: v has the same lifetime as self
                let v: *const Val = v;
                Ok(&*v)
            },
            Err((placeholder, _)) => Err(Guard {
                cache: self,
                placeholder,
                inserted: false,
            }),
        }
    }

    /// Gets an item from the cache with key `key` .
    /// If the corresponding value isn't present in the cache, this functions returns a guard
    /// that can be used to insert the value once it's computed.
    pub fn get_mut_or_guard<'a, Q>(
        &'a mut self,
        key: &Q,
    ) -> Result<Option<RefMut<'a, Key, Val, We, B, L>>, Guard<'a, Key, Val, We, B, L>>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        // TODO: this could be using a simpler entry API
        match self.shard.upsert_placeholder(self.shard.hash(key), key) {
            Ok((idx, _)) => Ok(self.shard.peek_token_mut(idx).map(RefMut)),
            Err((placeholder, _)) => Err(Guard {
                cache: self,
                placeholder,
                inserted: false,
            }),
        }
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

    /// Clear all items from the cache
    pub fn clear(&mut self) {
        self.shard.clear();
    }

    /// Iterator for the items in the cache
    pub fn iter(&self) -> impl Iterator<Item = (&'_ Key, &'_ Val)> + '_ {
        // TODO: add a concrete type, impl trait in the public api is really bad.
        self.shard.iter()
    }

    /// Drain all items from the cache
    ///
    /// The cache will be emptied even if the returned iterator isn't fully consumed.
    pub fn drain(&mut self) -> impl Iterator<Item = (Key, Val)> + '_ {
        // TODO: add a concrete type, impl trait in the public api is really bad.
        self.shard.drain()
    }

    #[cfg(any(fuzzing, test))]
    pub fn validate(&self, accept_overweight: bool) {
        self.shard.validate(accept_overweight);
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

#[derive(Debug, Clone)]
struct SharedPlaceholder {
    hash: u64,
    idx: Token,
}

pub struct Guard<'a, Key, Val, We, B, L> {
    cache: &'a mut Cache<Key, Val, We, B, L>,
    placeholder: SharedPlaceholder,
    inserted: bool,
}

impl<Key: Eq + Hash, Val, We: Weighter<Key, Val>, B: BuildHasher, L: Lifecycle<Key, Val>>
    Guard<'_, Key, Val, We, B, L>
{
    /// Inserts the value into the placeholder
    pub fn insert(self, value: Val) {
        self.insert_internal(value, false);
    }

    /// Inserts the value into the placeholder
    pub fn insert_with_lifecycle(self, value: Val) -> L::RequestState {
        self.insert_internal(value, true).unwrap()
    }

    #[inline]
    fn insert_internal(mut self, value: Val, return_lcs: bool) -> Option<L::RequestState> {
        let mut lcs = self.cache.shard.lifecycle.begin_request();
        let replaced =
            self.cache
                .shard
                .replace_placeholder(&mut lcs, &self.placeholder, false, value);
        debug_assert!(replaced.is_ok(), "unsync replace_placeholder can't fail");
        self.inserted = true;
        if return_lcs {
            Some(lcs)
        } else {
            self.cache.shard.lifecycle.end_request(lcs);
            None
        }
    }
}

impl<Key, Val, We, B, L> Drop for Guard<'_, Key, Val, We, B, L> {
    #[inline]
    fn drop(&mut self) {
        #[cold]
        fn drop_slow<Key, Val, We, B, L>(this: &mut Guard<'_, Key, Val, We, B, L>) {
            this.cache.shard.remove_placeholder(&this.placeholder);
        }
        if !self.inserted {
            drop_slow(self);
        }
    }
}

pub struct RefMut<'cache, Key, Val, We: Weighter<Key, Val>, B, L>(
    crate::shard::RefMut<'cache, Key, Val, We, B, L, SharedPlaceholder>,
);

impl<Key, Val, We: Weighter<Key, Val>, B, L> std::ops::Deref for RefMut<'_, Key, Val, We, B, L> {
    type Target = Val;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.pair().1
    }
}

impl<Key, Val, We: Weighter<Key, Val>, B, L> std::ops::DerefMut for RefMut<'_, Key, Val, We, B, L> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.value_mut()
    }
}

impl shard::SharedPlaceholder for SharedPlaceholder {
    #[inline]
    fn new(hash: u64, idx: Token) -> Self {
        Self { hash, idx }
    }

    #[inline]
    fn same_as(&self, _other: &Self) -> bool {
        true
    }

    #[inline]
    fn hash(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn idx(&self) -> Token {
        self.idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Weighter;

    impl crate::Weighter<u32, u32> for Weighter {
        fn weight(&self, _key: &u32, val: &u32) -> u64 {
            *val as u64
        }
    }

    #[test]
    fn test_zero_weights() {
        let mut cache = Cache::with_weighter(100, 100, Weighter);
        cache.insert(0, 0);
        assert_eq!(cache.weight(), 0);
        for i in 1..100 {
            cache.insert(i, i);
            cache.insert(i, i);
        }
        assert_eq!(cache.get(&0).copied(), Some(0));
        assert!(cache.contains_key(&0));
        let a = cache.weight();
        *cache.get_mut(&0).unwrap() += 1;
        assert_eq!(cache.weight(), a + 1);
        for i in 1..100 {
            cache.insert(i, i);
            cache.insert(i, i);
        }
        assert_eq!(cache.get(&0), None);
        assert!(!cache.contains_key(&0));

        cache.insert(0, 1);
        let a = cache.weight();
        *cache.get_mut(&0).unwrap() -= 1;
        assert_eq!(cache.weight(), a - 1);
        for i in 1..100 {
            cache.insert(i, i);
            cache.insert(i, i);
        }
        assert_eq!(cache.get(&0).copied(), Some(0));
        assert!(cache.contains_key(&0));
    }
}
