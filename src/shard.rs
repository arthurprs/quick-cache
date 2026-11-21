use std::{
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    mem::{self, MaybeUninit},
};

use hashbrown::HashTable;

use crate::{
    linked_slab::{LinkedSlab, Token},
    options::DEFAULT_HOT_ALLOCATION,
    shim::sync::atomic::{self, AtomicU16},
    Equivalent, Lifecycle, MemoryUsed, Weighter,
};

#[cfg(feature = "stats")]
use crate::shim::sync::atomic::AtomicU64;

// Max reference counter, this is 1 in ClockPro and 3 in the S3-FIFO.
const MAX_F: u16 = 2;

pub trait PlaceholderId<Val>: Copy + Eq {
    type Shared;
    type Context: Default;
    type WakeBatch;

    fn new(context: &mut Self::Context) -> Self;
    fn invalidate(shared: &Self::Shared, wakes: &mut Self::WakeBatch);
}

#[derive(Debug, Clone, Copy)]
pub struct PlaceholderLocation<Id> {
    pub hash: u64,
    pub idx: Token,
    pub id: Id,
}

pub enum InsertStrategy {
    Insert,
    Replace { soft: bool },
}

/// What to do with an existing entry after inspection/mutation.
///
/// Used with [`Cache::entry`](crate::sync::Cache::entry) and
/// [`Cache::entry_async`](crate::sync::Cache::entry_async).
pub enum EntryAction<T> {
    /// Retain the entry in the cache. The value may have been mutated in place
    /// before returning this variant.
    ///
    /// Returns [`EntryResult::Retained(T)`](crate::sync::EntryResult::Retained).
    Retain(T),
    /// Remove the entry from the cache.
    ///
    /// Returns [`EntryResult::Removed(Key, Val)`](crate::sync::EntryResult::Removed).
    Remove,
    /// Remove the entry and get a [`PlaceholderGuard`](crate::sync::PlaceholderGuard)
    /// for re-insertion. This is useful for "validate-or-recompute" patterns.
    ///
    /// Returns [`EntryResult::Replaced(PlaceholderGuard, Val)`](crate::sync::EntryResult::Replaced).
    ReplaceWithGuard,
}

/// Result of an entry-or-placeholder operation at the shard level.
pub enum EntryOrPlaceholder<Key, Val, Id, T> {
    /// Callback returned `Retain(T)` — entry is still in the cache.
    Kept(T),
    /// Callback returned `Remove` — entry was removed.
    Removed(Key, Val),
    /// Callback returned `ReplaceWithGuard` — entry was replaced with a placeholder.
    /// The old value is returned so it can be dropped outside the lock.
    Replaced(PlaceholderLocation<Id>, Val),
    /// Found an existing placeholder (another loader is working on this key).
    ExistingPlaceholder(PlaceholderLocation<Id>),
    /// No entry existed, a new placeholder was created.
    NewPlaceholder(PlaceholderLocation<Id>),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ResidentState {
    Hot,
    Cold,
}

#[derive(Debug)]
pub struct Resident<Key, Val> {
    key: Key,
    value: Val,
    state: ResidentState,
    referenced: AtomicU16,
    /// Number of times this item has been accessed (read) since it became resident.
    /// Incremented wherever a cache hit is recorded, unlike `referenced` which is
    /// bounded by the eviction policy. Reset whenever the slot is reused for a new key.
    #[cfg(feature = "stats")]
    access_count: AtomicU64,
}

impl<Key: Clone, Val: Clone> Clone for Resident<Key, Val> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            state: self.state,
            referenced: self.referenced.load(atomic::Ordering::Relaxed).into(),
            #[cfg(feature = "stats")]
            access_count: self.access_count.load(atomic::Ordering::Relaxed).into(),
        }
    }
}

#[derive(Debug, Clone)]
struct Placeholder<Key, Id, Shared> {
    key: Key,
    hot: ResidentState,
    id: Id,
    shared: Option<Shared>,
}

#[derive(Clone)]
enum Entry<Key, Val, Id, Shared = std::convert::Infallible> {
    Resident(Resident<Key, Val>),
    Placeholder(Placeholder<Key, Id, Shared>),
    Ghost(u64),
}

const GHOST_SAMPLE_INTERVAL: u8 = 255;

/// A bounded cache using a modified CLOCK-PRO eviction policy.
/// The implementation allows some parallelism as gets don't require exclusive access.
/// Any evicted items are returned so they can be dropped by the caller, outside the locks.
pub struct CacheShard<Key, Val, We, B, L, Id: PlaceholderId<Val>> {
    hash_builder: B,
    /// Map to an entry in the `entries` slab.
    /// Note that the actual key/value/hash are not stored in the map but in the slab.
    map: HashTable<Token>,
    /// Slab holding entries
    entries: LinkedSlab<Entry<Key, Val, Id, Id::Shared>>,
    pub(crate) placeholder_context: Id::Context,
    /// Head of cold list, containing Cold entries.
    /// Only contains entries of kind `Resident`.
    cold_head: Option<Token>,
    /// Head of hot list, containing Hot entries.
    /// Only contains entries of kind `Resident`.
    hot_head: Option<Token>,
    /// Head of ghost list, containing non-resident/Hash entries.
    /// Only contains entries of kind `Ghost`.
    ghost_head: Option<Token>,
    weight_target_hot: u64,
    weight_capacity: u64,
    weight_hot: u64,
    weight_cold: u64,
    num_hot: usize,
    num_cold: usize,
    num_non_resident: usize,
    capacity_non_resident: usize,
    adaptive_ghost_allocation: Option<f64>,
    ghosts_until_sample: u8,
    #[cfg(feature = "stats")]
    hits: AtomicU64,
    #[cfg(feature = "stats")]
    misses: AtomicU64,
    weighter: We,
    pub(crate) lifecycle: L,
}

impl<Key: Clone, Val: Clone, We: Clone, B: Clone, L: Clone, Id: PlaceholderId<Val>> Clone
    for CacheShard<Key, Val, We, B, L, Id>
where
    Id::Context: Clone,
    Id::Shared: Clone,
{
    fn clone(&self) -> Self {
        Self {
            hash_builder: self.hash_builder.clone(),
            map: self.map.clone(),
            entries: self.entries.clone(),
            placeholder_context: self.placeholder_context.clone(),
            cold_head: self.cold_head,
            hot_head: self.hot_head,
            ghost_head: self.ghost_head,
            weight_target_hot: self.weight_target_hot,
            weight_capacity: self.weight_capacity,
            weight_hot: self.weight_hot,
            weight_cold: self.weight_cold,
            num_hot: self.num_hot,
            num_cold: self.num_cold,
            num_non_resident: self.num_non_resident,
            capacity_non_resident: self.capacity_non_resident,
            adaptive_ghost_allocation: self.adaptive_ghost_allocation,
            ghosts_until_sample: self.ghosts_until_sample,
            #[cfg(feature = "stats")]
            hits: self.hits.load(atomic::Ordering::Relaxed).into(),
            #[cfg(feature = "stats")]
            misses: self.misses.load(atomic::Ordering::Relaxed).into(),
            weighter: self.weighter.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }
}

#[cfg(feature = "stats")]
macro_rules! record_hit {
    ($self: expr) => {{
        $self.hits.fetch_add(1, atomic::Ordering::Relaxed);
    }};
    ($self: expr, $resident: expr) => {{
        $self.hits.fetch_add(1, atomic::Ordering::Relaxed);
        $resident
            .access_count
            .fetch_add(1, atomic::Ordering::Relaxed);
    }};
}
#[cfg(feature = "stats")]
macro_rules! record_hit_mut {
    ($self: expr) => {{
        *$self.hits.get_mut() += 1;
    }};
    ($self: expr, $resident: expr) => {{
        *$self.hits.get_mut() += 1;
        *$resident.access_count.get_mut() += 1;
    }};
}
#[cfg(feature = "stats")]
macro_rules! record_miss {
    ($self: expr) => {{
        $self.misses.fetch_add(1, atomic::Ordering::Relaxed);
    }};
}
#[cfg(feature = "stats")]
macro_rules! record_miss_mut {
    ($self: expr) => {{
        *$self.misses.get_mut() += 1;
    }};
}
#[cfg(not(feature = "stats"))]
macro_rules! record_hit {
    ($self: expr) => {{}};
    ($self: expr, $resident: expr) => {{}};
}
#[cfg(not(feature = "stats"))]
macro_rules! record_hit_mut {
    ($self: expr) => {{}};
    ($self: expr, $resident: expr) => {{}};
}
#[cfg(not(feature = "stats"))]
macro_rules! record_miss {
    ($self: expr) => {{}};
}
#[cfg(not(feature = "stats"))]
macro_rules! record_miss_mut {
    ($self: expr) => {{}};
}

impl<Key, Val, We, B, L, Id: PlaceholderId<Val>> CacheShard<Key, Val, We, B, L, Id> {
    fn placeholder_id(entry: &Entry<Key, Val, Id, Id::Shared>) -> Option<Id> {
        let Entry::Placeholder(p) = entry else {
            return None;
        };
        Some(p.id)
    }

    pub fn shared_placeholder(&self, location: &PlaceholderLocation<Id>) -> Option<&Id::Shared> {
        let (Entry::Placeholder(p), _) = self.entries.get(location.idx)? else {
            return None;
        };
        let shared = p.shared.as_ref()?;
        (p.id == location.id).then_some(shared)
    }

    pub fn share_placeholder(
        &mut self,
        location: &PlaceholderLocation<Id>,
        create: impl FnOnce() -> Id::Shared,
    ) -> &Id::Shared {
        let (entry, _) = self.entries.get_mut(location.idx).unwrap();
        debug_assert!(Self::placeholder_id(entry) == Some(location.id));
        let Entry::Placeholder(p) = entry else {
            unreachable!()
        };
        p.shared.get_or_insert_with(create)
    }

    pub fn remove_placeholder(&mut self, placeholder: &PlaceholderLocation<Id>) {
        if let Ok(entry) = self.map.find_entry(placeholder.hash, |&idx| {
            if idx != placeholder.idx {
                return false;
            }
            let (entry, _) = self.entries.get(idx).unwrap();
            Self::placeholder_id(entry) == Some(placeholder.id)
        }) {
            entry.remove();
            self.entries.remove(placeholder.idx);
        }
    }

    #[cold]
    fn cold_change_weight(&mut self, idx: Token, old_weight: u64, new_weight: u64) {
        let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else {
            unsafe { unreachable_unchecked() };
        };
        let (weight_ptr, target_head) = if resident.state == ResidentState::Hot {
            (&mut self.weight_hot, &mut self.hot_head)
        } else {
            (&mut self.weight_cold, &mut self.cold_head)
        };
        *weight_ptr -= old_weight;
        *weight_ptr += new_weight;

        if old_weight == 0 && new_weight != 0 {
            *target_head = Some(self.entries.link(idx, *target_head));
        } else if old_weight != 0 && new_weight == 0 {
            let next = self.entries.unlink(idx);
            if *target_head == Some(idx) {
                *target_head = next;
            }
        }
    }
}

impl<Key, Val, We, B, L, Id: PlaceholderId<Val>> CacheShard<Key, Val, We, B, L, Id> {
    pub fn memory_used(&self) -> MemoryUsed {
        MemoryUsed {
            entries: self.entries.memory_used(),
            map: self.map.allocation_size(),
        }
    }

    pub fn weight(&self) -> u64 {
        self.weight_hot + self.weight_cold
    }

    pub fn len(&self) -> usize {
        self.num_hot + self.num_cold
    }

    pub fn capacity(&self) -> u64 {
        self.weight_capacity
    }

    #[cfg(feature = "stats")]
    pub fn hits(&self) -> u64 {
        self.hits.load(atomic::Ordering::Relaxed)
    }

    #[cfg(feature = "stats")]
    pub fn misses(&self) -> u64 {
        self.misses.load(atomic::Ordering::Relaxed)
    }

    pub fn clear(&mut self, wakes: &mut Id::WakeBatch) {
        // Mark notifications before dropping keys from the draining entries.
        for entry in self.entries.iter() {
            if let Entry::Placeholder(p) = entry {
                if let Some(shared) = p.shared.as_ref() {
                    Id::invalidate(shared, wakes);
                }
            }
        }
        let _ = self.drain();
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (Key, Val)> + '_ {
        self.cold_head = None;
        self.hot_head = None;
        self.ghost_head = None;
        self.num_hot = 0;
        self.num_cold = 0;
        self.num_non_resident = 0;
        if self.adaptive_ghost_allocation.is_some() {
            self.capacity_non_resident = 0;
            self.ghosts_until_sample = 1;
        }
        self.weight_hot = 0;
        self.weight_cold = 0;
        self.map.clear();
        self.entries.drain().filter_map(|i| match i {
            Entry::Resident(r) => Some((r.key, r.value)),
            Entry::Placeholder(_) | Entry::Ghost(_) => None,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'_ Key, &'_ Val)> + '_ {
        self.entries.iter().filter_map(|i| match i {
            Entry::Resident(r) => Some((&r.key, &r.value)),
            Entry::Placeholder(_) | Entry::Ghost(_) => None,
        })
    }

    pub fn iter_from(
        &self,
        continuation: Option<Token>,
    ) -> impl Iterator<Item = (Token, &'_ Key, &'_ Val)> + '_ {
        self.entries
            .iter_from(continuation)
            .filter_map(|(token, i)| match i {
                Entry::Resident(r) => Some((token, &r.key, &r.value)),
                Entry::Placeholder(_) | Entry::Ghost(_) => None,
            })
    }
}

impl<
        Key: Eq + Hash,
        Val,
        We: Weighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
        Id: PlaceholderId<Val>,
    > CacheShard<Key, Val, We, B, L, Id>
{
    pub fn new(
        hot_allocation: f64,
        ghost_allocation: f64,
        estimated_items_capacity: Option<usize>,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
        lifecycle: L,
    ) -> Self {
        // Clamp to at least 1 when weight_capacity > 0, since the f64 multiplication
        // can truncate to 0 for small capacities, which would reject all inserts as overweight.
        let weight_target_hot = ((weight_capacity as f64 * hot_allocation) as u64)
            .clamp(weight_capacity.min(1), weight_capacity);
        let capacity_non_resident =
            (estimated_items_capacity.unwrap_or_default() as f64 * ghost_allocation) as usize;
        let adaptive_ghost_allocation = (estimated_items_capacity.is_none()
            && ghost_allocation > 0.0)
            .then_some(ghost_allocation);
        Self {
            hash_builder,
            map: HashTable::with_capacity(0),
            entries: LinkedSlab::with_capacity(0),
            placeholder_context: Default::default(),
            weight_capacity,
            #[cfg(feature = "stats")]
            hits: Default::default(),
            #[cfg(feature = "stats")]
            misses: Default::default(),
            cold_head: None,
            hot_head: None,
            ghost_head: None,
            capacity_non_resident,
            adaptive_ghost_allocation,
            ghosts_until_sample: u8::from(adaptive_ghost_allocation.is_some()),
            weight_target_hot,
            num_hot: 0,
            num_cold: 0,
            num_non_resident: 0,
            weight_hot: 0,
            weight_cold: 0,
            weighter,
            lifecycle,
        }
    }

    #[cfg(any(fuzzing, test))]
    pub fn validate(&self, accept_overweight: bool) {
        self.entries.validate();
        let mut num_hot = 0;
        let mut num_cold = 0;
        let mut num_non_resident = 0;
        let mut weight_hot = 0;
        let mut weight_hot_pinned = 0;
        let mut weight_cold = 0;
        let mut weight_cold_pinned = 0;
        for e in self.entries.iter_entries() {
            match e {
                Entry::Resident(r) if r.state == ResidentState::Cold => {
                    num_cold += 1;
                    let weight = self.weighter.weight(&r.key, &r.value);
                    if self.lifecycle.is_pinned(&r.key, &r.value) {
                        weight_cold_pinned += weight;
                    } else {
                        weight_cold += weight;
                    }
                }
                Entry::Resident(r) => {
                    num_hot += 1;
                    let weight = self.weighter.weight(&r.key, &r.value);
                    if self.lifecycle.is_pinned(&r.key, &r.value) {
                        weight_hot_pinned += weight;
                    } else {
                        weight_hot += weight;
                    }
                }
                Entry::Ghost(_) => {
                    num_non_resident += 1;
                }
                Entry::Placeholder(_) => (),
            }
        }
        // eprintln!("-------------");
        // dbg!(
        //     num_hot,
        //     num_cold,
        //     num_non_resident,
        //     weight_hot,
        //     weight_hot_pinned,
        //     weight_cold,
        //     weight_cold_pinned,
        //     self.num_hot,
        //     self.num_cold,
        //     self.num_non_resident,
        //     self.weight_hot,
        //     self.weight_cold,
        //     self.weight_target_hot,
        //     self.weight_capacity,
        //     self.capacity_non_resident
        // );
        assert_eq!(num_hot, self.num_hot);
        assert_eq!(num_cold, self.num_cold);
        assert_eq!(num_non_resident, self.num_non_resident);
        assert_eq!(weight_hot + weight_hot_pinned, self.weight_hot);
        assert_eq!(weight_cold + weight_cold_pinned, self.weight_cold);
        if !accept_overweight {
            assert!(weight_hot + weight_cold <= self.weight_capacity);
        }
        assert!(num_non_resident <= self.capacity_non_resident);
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        // Reserve extra slots for ghost history up to the current ghost limit.
        let additional = additional.saturating_add(additional.min(self.capacity_non_resident));
        self.entries.reserve(additional);
        self.map.reserve(additional, |&idx| {
            let (entry, _) = self.entries.get(idx).unwrap();
            match entry {
                Entry::Resident(Resident { key, .. })
                | Entry::Placeholder(Placeholder { key, .. }) => {
                    Self::hash_static(&self.hash_builder, key)
                }
                Entry::Ghost(non_resident_hash) => *non_resident_hash,
            }
        })
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: Fn(&Key, &Val) -> bool,
    {
        let retained_tokens = self
            .map
            .iter()
            .filter_map(|&idx| match self.entries.get(idx) {
                Some((entry, _idx)) => match entry {
                    Entry::Resident(r) => {
                        if !f(&r.key, &r.value) {
                            let hash = self.hash(&r.key);
                            Some((idx, hash))
                        } else {
                            None
                        }
                    }
                    Entry::Placeholder(_) | Entry::Ghost(_) => None,
                },
                None => None,
            })
            .collect::<Vec<_>>();
        for (idx, hash) in retained_tokens {
            self.remove_internal(hash, idx);
        }
    }

    #[inline]
    fn hash_static<Q>(hasher: &B, key: &Q) -> u64
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        hasher.hash_one(key)
    }

    #[inline]
    pub fn hash<Q>(&self, key: &Q) -> u64
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        Self::hash_static(&self.hash_builder, key)
    }

    #[inline]
    fn search<Q>(&self, hash: u64, k: &Q) -> Option<Token>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let mut ghost = None;
        self.map
            .find(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                match entry {
                    Entry::Resident(Resident { key, .. })
                    | Entry::Placeholder(Placeholder { key, .. }) => k.equivalent(key),
                    Entry::Ghost(ghost_hash) if *ghost_hash == hash => {
                        ghost = Some(idx);
                        false
                    }
                    _ => false,
                }
            })
            .copied()
            .or(ghost)
    }

    #[inline]
    fn search_resident<Q>(&self, hash: u64, k: &Q) -> Option<(Token, &Resident<Key, Val>)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let mut resident = MaybeUninit::uninit();
        self.map
            .find(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                match entry {
                    Entry::Resident(r) if k.equivalent(&r.key) => {
                        resident.write(r);
                        true
                    }
                    _ => false,
                }
            })
            .map(|idx| {
                // Safety: we found a successful entry in `get` above,
                // thus resident is initialized.
                (*idx, unsafe { resident.assume_init() })
            })
    }

    pub fn contains<Q>(&self, hash: u64, key: &Q) -> bool
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.map
            .find(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                matches!(entry, Entry::Resident(r) if key.equivalent(&r.key))
            })
            .is_some()
    }

    pub fn get_key_value<Q>(&self, hash: u64, key: &Q) -> Option<(&Key, &Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        if let Some((_, resident)) = self.search_resident(hash, key) {
            let referenced = resident.referenced.load(atomic::Ordering::Relaxed);
            // Attempt to avoid contention on hot items, which may have their counters maxed out
            if referenced < MAX_F {
                // While extremely unlikely, this increment can theoretically overflow.
                // Even if that happens there's no impact correctness wise.
                resident.referenced.fetch_add(1, atomic::Ordering::Relaxed);
            }
            record_hit!(self, resident);
            Some((&resident.key, &resident.value))
        } else {
            record_miss!(self);
            None
        }
    }

    #[inline]
    pub fn get<Q>(&self, hash: u64, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        self.get_key_value(hash, key).map(|(_k, v)| v)
    }

    pub fn get_mut<Q>(&mut self, hash: u64, key: &Q) -> Option<RefMut<'_, Key, Val, We, B, L, Id>>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let Some((idx, _)) = self.search_resident(hash, key) else {
            record_miss_mut!(self);
            return None;
        };
        let (Entry::Resident(resident), _) = (unsafe { self.entries.get_mut_unchecked(idx) })
        else {
            unsafe { unreachable_unchecked() };
        };
        if *resident.referenced.get_mut() < MAX_F {
            *resident.referenced.get_mut() += 1;
        }
        record_hit_mut!(self, resident);

        let old_weight = self.weighter.weight(&resident.key, &resident.value);
        Some(RefMut {
            guard: WeightGuard {
                shard: self as *mut _,
                idx,
                old_weight,
            },
            _phantom: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn peek_token(&self, token: Token) -> Option<&Val> {
        if let Some((Entry::Resident(resident), _)) = self.entries.get(token) {
            Some(&resident.value)
        } else {
            None
        }
    }

    #[inline]
    pub fn peek_token_mut(&mut self, token: Token) -> Option<RefMut<'_, Key, Val, We, B, L, Id>> {
        if let Some((Entry::Resident(resident), _)) = self.entries.get_mut(token) {
            let old_weight = self.weighter.weight(&resident.key, &resident.value);
            Some(RefMut {
                guard: WeightGuard {
                    shard: self as *mut _,
                    idx: token,
                    old_weight,
                },
                _phantom: std::marker::PhantomData,
            })
        } else {
            None
        }
    }

    pub fn peek<Q>(&self, hash: u64, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (_, resident) = self.search_resident(hash, key)?;
        Some(&resident.value)
    }

    /// Returns per-item statistics for a resident key without affecting its hotness
    /// or access count. Returns `None` if the key is not resident.
    #[cfg(feature = "stats")]
    pub fn item_stats<Q>(&self, hash: u64, key: &Q) -> Option<crate::ItemStats>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (_, resident) = self.search_resident(hash, key)?;
        Some(crate::ItemStats {
            access_count: resident.access_count.load(atomic::Ordering::Relaxed),
        })
    }

    pub fn peek_mut<Q>(&mut self, hash: u64, key: &Q) -> Option<RefMut<'_, Key, Val, We, B, L, Id>>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        let (idx, _) = self.search_resident(hash, key)?;
        self.peek_token_mut(idx)
    }

    pub fn remove<Q>(&mut self, hash: u64, key: &Q, wakes: &mut Id::WakeBatch) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
    {
        // This could be search_resident, but calling `remove` is likely a
        // strong indication that this input isn't important.
        let idx = self.search(hash, key)?;
        if let Some((Entry::Placeholder(p), _)) = self.entries.get(idx) {
            if let Some(shared) = p.shared.as_ref() {
                Id::invalidate(shared, wakes);
            }
        }
        self.remove_internal(hash, idx)
    }

    pub fn remove_if<Q, F>(&mut self, hash: u64, key: &Q, f: F) -> Option<(Key, Val)>
    where
        Q: Hash + Equivalent<Key> + ?Sized,
        F: FnOnce(&Val) -> bool,
    {
        let (idx, resident) = self.search_resident(hash, key)?;
        if f(&resident.value) {
            self.remove_internal(hash, idx)
        } else {
            None
        }
    }

    pub fn remove_token(&mut self, token: Token) -> Option<(Key, Val)> {
        let Some((Entry::Resident(resident), _)) = self.entries.get(token) else {
            return None;
        };
        let hash = Self::hash_static(&self.hash_builder, &resident.key);
        self.remove_internal(hash, token)
    }

    pub fn remove_next(&mut self, continuation: Option<Token>) -> Option<(Token, Key, Val)> {
        let (token, key, _) = self
            .entries
            .iter_from(continuation)
            .filter_map(|(token, i)| match i {
                Entry::Resident(r) => Some((token, &r.key, &r.value)),
                Entry::Placeholder(_) | Entry::Ghost(_) => None,
            })
            .next()?;
        let hash = Self::hash_static(&self.hash_builder, key);
        self.remove_internal(hash, token)
            .map(|(k, v)| (token, k, v))
    }

    fn remove_internal(&mut self, hash: u64, idx: Token) -> Option<(Key, Val)> {
        self.map_remove(hash, idx);
        let mut result = None;
        let (entry, next) = self.entries.remove(idx).unwrap();
        let list_head = match entry {
            Entry::Resident(r) => {
                let weight = self.weighter.weight(&r.key, &r.value);
                result = Some((r.key, r.value));
                if r.state == ResidentState::Hot {
                    self.num_hot -= 1;
                    self.weight_hot -= weight;
                    &mut self.hot_head
                } else {
                    debug_assert!(r.state == ResidentState::Cold);
                    self.num_cold -= 1;
                    self.weight_cold -= weight;
                    &mut self.cold_head
                }
            }
            Entry::Ghost(_) => {
                // Since this an user invoked remove we opt to remove even Ghost entries that could match it.
                self.num_non_resident -= 1;
                &mut self.ghost_head
            }
            Entry::Placeholder(_) => return None,
        };
        if *list_head == Some(idx) {
            *list_head = next;
        }
        result
    }

    /// Advance cold ring, promoting to hot and demoting as needed.
    #[must_use]
    fn advance_cold(&mut self, lcs: &mut L::RequestState) -> bool {
        let Some(mut idx) = self.cold_head else {
            return self.advance_hot(lcs);
        };
        loop {
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else {
                unsafe { unreachable_unchecked() };
            };
            debug_assert_eq!(resident.state, ResidentState::Cold);
            if *resident.referenced.get_mut() != 0 {
                *resident.referenced.get_mut() -= 1;
                resident.state = ResidentState::Hot;
                let weight = self.weighter.weight(&resident.key, &resident.value);
                self.weight_hot += weight;
                self.weight_cold -= weight;
                self.num_hot += 1;
                self.num_cold -= 1;
                self.cold_head = self.entries.unlink(idx);
                self.hot_head = Some(self.entries.link(idx, self.hot_head));
                // evict from hot if overweight
                while self.weight_hot > self.weight_target_hot && self.advance_hot(lcs) {}
                return true;
            }

            if self.lifecycle.is_pinned(&resident.key, &resident.value) {
                if Some(next) == self.cold_head {
                    return self.advance_hot(lcs);
                }
                idx = next;
                continue;
            }

            self.weight_cold -= self.weighter.weight(&resident.key, &resident.value);
            self.lifecycle
                .before_evict(lcs, &resident.key, &mut resident.value);
            if self.weighter.weight(&resident.key, &resident.value) == 0 {
                self.cold_head = self.entries.unlink(idx);
                return true;
            }
            let hash = Self::hash_static(&self.hash_builder, &resident.key);
            let Entry::Resident(evicted) = mem::replace(entry, Entry::Ghost(hash)) else {
                // Safety: we had a mut reference to the Resident inside entry until the previous line
                unsafe { core::hint::unreachable_unchecked() };
            };
            self.cold_head = self.entries.unlink(idx);
            self.ghost_head = Some(self.entries.link(idx, self.ghost_head));
            self.num_cold -= 1;
            self.num_non_resident += 1;
            // Fixed sizing leaves the countdown at zero when it reaches the sampler.
            self.ghosts_until_sample = self.ghosts_until_sample.wrapping_sub(1);
            if self.ghosts_until_sample == 0 {
                self.update_ghost_capacity();
            }
            if self.num_non_resident > self.capacity_non_resident {
                self.advance_ghost();
            }
            self.lifecycle
                .on_evict_cold(lcs, evicted.key, evicted.value);
            return true;
        }
    }

    /// Advance hot ring evicting entries.
    #[must_use]
    fn advance_hot(&mut self, lcs: &mut L::RequestState) -> bool {
        let mut unpinned = 0usize;
        let Some(mut idx) = self.hot_head else {
            return false;
        };
        loop {
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else {
                unsafe { unreachable_unchecked() };
            };
            debug_assert_eq!(resident.state, ResidentState::Hot);
            if self.lifecycle.is_pinned(&resident.key, &resident.value) {
                *resident.referenced.get_mut() = (*resident.referenced.get_mut())
                    .min(MAX_F)
                    .saturating_sub(1);
                if Some(next) == self.hot_head {
                    if unpinned == 0 {
                        // All entries are pinned
                        return false;
                    }
                    // Restart unpinned count
                    unpinned = 0;
                }
                idx = next;
                continue;
            }
            unpinned += 1;
            if *resident.referenced.get_mut() != 0 {
                *resident.referenced.get_mut() = (*resident.referenced.get_mut()).min(MAX_F) - 1;
                idx = next;
                continue;
            }
            self.weight_hot -= self.weighter.weight(&resident.key, &resident.value);
            self.lifecycle
                .before_evict(lcs, &resident.key, &mut resident.value);
            if self.weighter.weight(&resident.key, &resident.value) == 0 {
                self.hot_head = self.entries.unlink(idx);
            } else {
                self.num_hot -= 1;
                let hash = Self::hash_static(&self.hash_builder, &resident.key);
                let Some((Entry::Resident(evicted), next)) = self.entries.remove(idx) else {
                    // Safety: we had a mut reference to the Resident under `idx` until the previous line
                    unsafe { core::hint::unreachable_unchecked() };
                };
                self.hot_head = next;
                self.lifecycle.on_evict_hot(lcs, evicted.key, evicted.value);
                self.map_remove(hash, idx);
            }
            return true;
        }
    }

    #[inline]
    fn advance_ghost(&mut self) {
        debug_assert_ne!(self.num_non_resident, 0);
        let idx = self.ghost_head.unwrap();
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        let Entry::Ghost(hash) = *entry else {
            unsafe { unreachable_unchecked() };
        };
        self.num_non_resident -= 1;
        self.map_remove(hash, idx);
        let (_, next) = self.entries.remove(idx).unwrap();
        self.ghost_head = next;
    }

    fn insert_existing(
        &mut self,
        lcs: &mut L::RequestState,
        idx: Token,
        (key, value): (Key, Val),
        weight: u64,
        strategy: InsertStrategy,
        wakes: &mut Id::WakeBatch,
    ) -> Result<(), (Key, Val)> {
        // caller already handled overweight items, but it could have been pinned
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        let referenced;
        let enter_state;
        match entry {
            Entry::Resident(resident) => {
                enter_state = resident.state;
                referenced = resident
                    .referenced
                    .get_mut()
                    .saturating_add(
                        !matches!(strategy, InsertStrategy::Replace { soft: true }) as u16
                    )
                    .min(MAX_F);
            }
            _ if matches!(strategy, InsertStrategy::Replace { .. }) => {
                return Err((key, value));
            }
            Entry::Ghost(_) => {
                referenced = 0;
                enter_state = ResidentState::Hot;
            }
            Entry::Placeholder(ph) => {
                if let Some(shared) = ph.shared.as_ref() {
                    Id::invalidate(shared, wakes);
                }
                referenced = 1;
                enter_state = ph.hot;
            }
        }

        let evicted = mem::replace(
            entry,
            Entry::Resident(Resident {
                key,
                value,
                state: enter_state,
                referenced: referenced.into(),
                #[cfg(feature = "stats")]
                access_count: Default::default(),
            }),
        );
        match evicted {
            Entry::Resident(evicted) => {
                debug_assert_eq!(evicted.state, enter_state);
                let evicted_weight = self.weighter.weight(&evicted.key, &evicted.value);
                let list_head = if enter_state == ResidentState::Hot {
                    self.weight_hot -= evicted_weight;
                    self.weight_hot += weight;
                    &mut self.hot_head
                } else {
                    self.weight_cold -= evicted_weight;
                    self.weight_cold += weight;
                    &mut self.cold_head
                };
                if evicted_weight == 0 && weight != 0 {
                    *list_head = Some(self.entries.link(idx, *list_head));
                } else if evicted_weight != 0 && weight == 0 {
                    let next = self.entries.unlink(idx);
                    if *list_head == Some(idx) {
                        *list_head = next;
                    }
                }
                match enter_state {
                    ResidentState::Hot => {
                        self.lifecycle.on_evict_hot(lcs, evicted.key, evicted.value)
                    }
                    ResidentState::Cold => {
                        self.lifecycle
                            .on_evict_cold(lcs, evicted.key, evicted.value)
                    }
                }
            }
            Entry::Ghost(_) => {
                self.weight_hot += weight;
                self.num_hot += 1;
                self.num_non_resident -= 1;
                let next_ghost = self.entries.unlink(idx);
                if self.ghost_head == Some(idx) {
                    self.ghost_head = next_ghost;
                }
                if weight != 0 {
                    self.hot_head = Some(self.entries.link(idx, self.hot_head));
                }
            }
            Entry::Placeholder(_) => {
                let list_head = if enter_state == ResidentState::Hot {
                    self.num_hot += 1;
                    self.weight_hot += weight;
                    &mut self.hot_head
                } else {
                    self.num_cold += 1;
                    self.weight_cold += weight;
                    &mut self.cold_head
                };
                if weight != 0 {
                    *list_head = Some(self.entries.link(idx, *list_head));
                }
            }
        }

        while self.weight() > self.weight_capacity && self.advance_cold(lcs) {}
        Ok(())
    }

    #[inline]
    fn map_insert(&mut self, hash: u64, idx: Token) {
        self.map.insert_unique(hash, idx, |&i| {
            let (entry, _) = self.entries.get(i).unwrap();
            match entry {
                Entry::Resident(Resident { key, .. })
                | Entry::Placeholder(Placeholder { key, .. }) => {
                    Self::hash_static(&self.hash_builder, key)
                }
                Entry::Ghost(hash) => *hash,
            }
        });
    }

    #[inline]
    fn map_remove(&mut self, hash: u64, idx: Token) {
        if let Ok(entry) = self.map.find_entry(hash, |&i| i == idx) {
            entry.remove();
            return;
        }
        #[cfg(debug_assertions)]
        panic!("key not found");
    }

    pub fn replace_placeholder(
        &mut self,
        lcs: &mut L::RequestState,
        placeholder: &PlaceholderLocation<Id>,
        referenced: bool,
        mut value: Val,
    ) -> Result<(), Val> {
        // Final owners stop accepting joins while this shard lock is held.
        let entry = match self.entries.get_mut(placeholder.idx) {
            Some((entry, _)) if Self::placeholder_id(entry) == Some(placeholder.id) => entry,
            _ => return Err(value),
        };
        let Entry::Placeholder(Placeholder {
            key, hot, shared, ..
        }) = mem::replace(entry, Entry::Ghost(0))
        else {
            unreachable!()
        };
        let mut placeholder_hot = hot;
        drop(shared);
        let mut weight = self.weighter.weight(&key, &value);
        // don't admit if it won't fit within the budget
        if weight > self.weight_target_hot && !self.lifecycle.is_pinned(&key, &value) {
            self.lifecycle.before_evict(lcs, &key, &mut value);
            weight = self.weighter.weight(&key, &value);
            // check again, it could have changed weight
            if weight > self.weight_target_hot {
                return self.handle_overweight_replace_placeholder(lcs, placeholder, key, value);
            }
        }

        // cache is filling up, admit as hot if possible
        if self.weight_hot + weight <= self.weight_target_hot {
            placeholder_hot = ResidentState::Hot;
        }
        *entry = Entry::Resident(Resident {
            key,
            value,
            state: placeholder_hot,
            referenced: (referenced as u16).into(),
            #[cfg(feature = "stats")]
            access_count: Default::default(),
        });

        let list_head = if placeholder_hot == ResidentState::Hot {
            self.num_hot += 1;
            self.weight_hot += weight;
            &mut self.hot_head
        } else {
            self.num_cold += 1;
            self.weight_cold += weight;
            &mut self.cold_head
        };

        if weight != 0 {
            *list_head = Some(self.entries.link(placeholder.idx, *list_head));
            while self.weight() > self.weight_capacity && self.advance_cold(lcs) {}
        }

        Ok(())
    }

    #[cold]
    fn handle_overweight_replace_placeholder(
        &mut self,
        lcs: &mut L::RequestState,
        placeholder: &PlaceholderLocation<Id>,
        key: Key,
        value: Val,
    ) -> Result<(), Val> {
        self.entries.remove(placeholder.idx);
        self.map_remove(placeholder.hash, placeholder.idx);
        self.lifecycle.on_evict_cold(lcs, key, value);
        Ok(())
    }

    pub fn insert(
        &mut self,
        lcs: &mut L::RequestState,
        hash: u64,
        key: Key,
        mut value: Val,
        strategy: InsertStrategy,
        wakes: &mut Id::WakeBatch,
    ) -> Result<(), (Key, Val)> {
        let mut weight = self.weighter.weight(&key, &value);
        // don't admit if it won't fit within the budget
        if weight > self.weight_target_hot && !self.lifecycle.is_pinned(&key, &value) {
            self.lifecycle.before_evict(lcs, &key, &mut value);
            weight = self.weighter.weight(&key, &value);
            // check again, it could have changed weight
            if weight > self.weight_target_hot {
                return self.handle_insert_overweight(lcs, hash, key, value, strategy);
            }
        }

        if let Some(idx) = self.search(hash, &key) {
            return self.insert_existing(lcs, idx, (key, value), weight, strategy, wakes);
        } else if matches!(strategy, InsertStrategy::Replace { .. }) {
            return Err((key, value));
        }

        // cache is filling up, admit as hot if possible
        let enter_hot = self.weight_hot + weight <= self.weight_target_hot;
        // pre-evict instead of post-evict, this gives sightly more priority to the new item
        while self.weight() + weight > self.weight_capacity && self.advance_cold(lcs) {}

        let (state, list_head) = if enter_hot {
            self.num_hot += 1;
            self.weight_hot += weight;
            (ResidentState::Hot, &mut self.hot_head)
        } else {
            self.num_cold += 1;
            self.weight_cold += weight;
            (ResidentState::Cold, &mut self.cold_head)
        };
        let idx = self.entries.insert(Entry::Resident(Resident {
            key,
            value,
            state,
            referenced: Default::default(),
            #[cfg(feature = "stats")]
            access_count: Default::default(),
        }));
        if weight != 0 {
            *list_head = Some(self.entries.link(idx, *list_head));
        }
        self.map_insert(hash, idx);
        Ok(())
    }

    #[cold]
    fn handle_insert_overweight(
        &mut self,
        lcs: &mut L::RequestState,
        hash: u64,
        key: Key,
        value: Val,
        strategy: InsertStrategy,
    ) -> Result<(), (Key, Val)> {
        // Make sure to remove any existing entry
        if let Some((idx, resident)) = self.search_resident(hash, &key) {
            let prev_state = resident.state;
            if let Some((ek, ev)) = self.remove_internal(hash, idx) {
                match prev_state {
                    ResidentState::Hot => self.lifecycle.on_evict_hot(lcs, ek, ev),
                    ResidentState::Cold => self.lifecycle.on_evict_cold(lcs, ek, ev),
                }
            }
        }
        if matches!(strategy, InsertStrategy::Replace { .. }) {
            return Err((key, value));
        }
        self.lifecycle.on_evict_cold(lcs, key, value);
        Ok(())
    }

    pub fn get_or_placeholder<Q>(
        &mut self,
        hash: u64,
        key: &Q,
    ) -> Result<(Token, &Val), (PlaceholderLocation<Id>, bool)>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let idx = self.search(hash, key);
        if let Some(idx) = idx {
            if let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) {
                if *resident.referenced.get_mut() < MAX_F {
                    *resident.referenced.get_mut() += 1;
                }
                record_hit_mut!(self, resident);
                unsafe {
                    // Rustc gets insanely confused returning references from mut borrows
                    // Safety: value will have the same lifetime as `resident`
                    let value_ptr: *const Val = &resident.value;
                    return Ok((idx, &*value_ptr));
                }
            }
        }
        let (shared, is_new) = unsafe { self.non_resident_to_placeholder(hash, key, idx) };
        Err((shared, is_new))
    }

    /// Entry operation on an existing or missing key.
    ///
    /// If a `Resident` entry exists, calls `on_occupied` with `&mut Val` to decide what to do.
    /// On `Retain`, weight is recalculated after the callback returns.
    /// Otherwise, creates a placeholder or joins an existing one.
    ///
    /// `on_occupied` is taken by mutable reference so the caller retains ownership.
    /// It is called at most once per invocation.
    pub fn entry_or_placeholder<Q, T, F>(
        &mut self,
        hash: u64,
        key: &Q,
        on_occupied: &mut F,
    ) -> EntryOrPlaceholder<Key, Val, Id, T>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
        F: FnMut(&Key, &mut Val) -> EntryAction<T>,
    {
        let idx = self.search(hash, key);
        if let Some(idx) = idx {
            let shard = self as *mut _;
            if let Some((Entry::Resident(r), _)) = self.entries.get_mut(idx) {
                // Call the callback inside a WeightGuard scope: if it panics
                // after mutating the value, the guard recomputes weight on drop.
                // SAFETY: key/value point into the Resident entry at idx, alive
                // for the duration of the callback.
                let action = {
                    let (key_ptr, val_ptr) = (&r.key as *const Key, &mut r.value as *mut Val);
                    let _guard = WeightGuard::<Key, Val, We, B, L, Id> {
                        idx,
                        old_weight: self.weighter.weight(&r.key, &r.value),
                        shard,
                    };
                    on_occupied(unsafe { &*key_ptr }, unsafe { &mut *val_ptr })
                };

                return match action {
                    EntryAction::Retain(t) => {
                        let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else {
                            // SAFETY: we had a mut reference to the Resident under `idx` until the previous line
                            unsafe { unreachable_unchecked() };
                        };
                        if *resident.referenced.get_mut() < MAX_F {
                            *resident.referenced.get_mut() += 1;
                        }
                        record_hit_mut!(self, resident);
                        EntryOrPlaceholder::Kept(t)
                    }
                    EntryAction::Remove => {
                        let (key, val) = self.remove_internal(hash, idx).unwrap();
                        EntryOrPlaceholder::Removed(key, val)
                    }
                    EntryAction::ReplaceWithGuard => {
                        let shared = PlaceholderLocation {
                            hash,
                            idx,
                            id: Id::new(&mut self.placeholder_context),
                        };
                        let Some((Entry::Resident(r), _)) = self.entries.get_mut(idx) else {
                            // SAFETY: we had a mut reference to the Resident under `idx` until the previous line
                            unsafe { unreachable_unchecked() };
                        };
                        let state = r.state;
                        let current_weight = self.weighter.weight(&r.key, &r.value);
                        let list_head = if state == ResidentState::Hot {
                            self.num_hot -= 1;
                            self.weight_hot -= current_weight;
                            &mut self.hot_head
                        } else {
                            self.num_cold -= 1;
                            self.weight_cold -= current_weight;
                            &mut self.cold_head
                        };
                        if current_weight != 0 {
                            let next = self.entries.unlink(idx);
                            if *list_head == Some(idx) {
                                *list_head = next;
                            }
                        }
                        let (entry, _) = unsafe { self.entries.get_mut_unchecked(idx) };
                        let Entry::Resident(r) = mem::replace(entry, Entry::Ghost(0)) else {
                            unsafe { unreachable_unchecked() }
                        };
                        *entry = Entry::Placeholder(Placeholder {
                            key: r.key,
                            hot: state,
                            id: shared.id,
                            shared: None,
                        });
                        EntryOrPlaceholder::Replaced(shared, r.value)
                    }
                };
            }
        }
        let (shared, is_new) = unsafe { self.non_resident_to_placeholder(hash, key, idx) };
        if is_new {
            EntryOrPlaceholder::NewPlaceholder(shared)
        } else {
            EntryOrPlaceholder::ExistingPlaceholder(shared)
        }
    }

    /// Creates or joins a placeholder for a non-Resident entry (Placeholder, Ghost, or missing).
    /// Returns `(shared, true)` for new placeholders, `(shared, false)` for existing ones.
    /// The entry at `idx` must NOT be Resident.
    unsafe fn non_resident_to_placeholder<Q>(
        &mut self,
        hash: u64,
        key: &Q,
        idx: Option<Token>,
    ) -> (PlaceholderLocation<Id>, bool)
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        if let Some(idx) = idx {
            let (entry, _) = unsafe { self.entries.get_mut_unchecked(idx) };
            match entry {
                Entry::Placeholder(_) => {
                    record_hit_mut!(self);
                    (
                        PlaceholderLocation {
                            hash,
                            idx,
                            id: Self::placeholder_id(entry).unwrap(),
                        },
                        false,
                    )
                }
                Entry::Ghost(_) => {
                    let shared = PlaceholderLocation {
                        hash,
                        idx,
                        id: Id::new(&mut self.placeholder_context),
                    };
                    *entry = Entry::Placeholder(Placeholder {
                        key: key.to_owned(),
                        hot: ResidentState::Hot,
                        id: shared.id,
                        shared: None,
                    });
                    self.num_non_resident -= 1;
                    let next = self.entries.unlink(idx);
                    if self.ghost_head == Some(idx) {
                        self.ghost_head = next;
                    }
                    record_miss_mut!(self);
                    (shared, true)
                }
                Entry::Resident(_) => unsafe { unreachable_unchecked() },
            }
        } else {
            let idx = self.entries.next_free();
            let shared = PlaceholderLocation {
                hash,
                idx,
                id: Id::new(&mut self.placeholder_context),
            };
            let idx_ = self.entries.insert(Entry::Placeholder(Placeholder {
                key: key.to_owned(),
                hot: ResidentState::Cold,
                id: shared.id,
                shared: None,
            }));
            debug_assert_eq!(idx, idx_);
            self.map_insert(hash, idx);
            record_miss_mut!(self);
            (shared, true)
        }
    }

    pub fn set_capacity(&mut self, new_weight_capacity: u64, lcs: &mut L::RequestState) {
        let hot_ratio = if self.weight_capacity == 0 {
            DEFAULT_HOT_ALLOCATION
        } else {
            if self.adaptive_ghost_allocation.is_none() {
                let old_new_ratio = new_weight_capacity as f64 / self.weight_capacity as f64;
                self.capacity_non_resident =
                    (self.capacity_non_resident as f64 * old_new_ratio) as usize;
            }
            self.weight_target_hot as f64 / self.weight_capacity as f64
        };
        self.weight_capacity = new_weight_capacity;
        self.weight_target_hot = ((new_weight_capacity as f64 * hot_ratio) as u64)
            .clamp(new_weight_capacity.min(1), new_weight_capacity);

        while self.weight() > self.weight_capacity && self.advance_cold(lcs) {}
        if self.adaptive_ghost_allocation.is_some() {
            let sample_next_ghost = self.num_non_resident == 0 || self.len() == 0;
            self.update_ghost_capacity();
            if sample_next_ghost {
                self.ghosts_until_sample = 1;
            }
        } else {
            self.trim_ghosts();
        }
    }

    #[cold]
    fn update_ghost_capacity(&mut self) {
        let Some(allocation) = self.adaptive_ghost_allocation else {
            return;
        };
        self.capacity_non_resident = if self.weight_capacity == 0 {
            0
        } else {
            (self.len() as f64 * allocation) as usize
        };
        self.ghosts_until_sample = GHOST_SAMPLE_INTERVAL;
        self.trim_ghosts();
    }

    #[inline]
    fn trim_ghosts(&mut self) {
        while self.num_non_resident > self.capacity_non_resident {
            self.advance_ghost();
        }
    }
}

/// Drop guard for `entry_or_placeholder`: if the user callback panics after
/// mutating the value, recomputes weight to keep shard accounting consistent.
struct WeightGuard<Key, Val, We: Weighter<Key, Val>, B, L, Id: PlaceholderId<Val>> {
    shard: *mut CacheShard<Key, Val, We, B, L, Id>,
    idx: Token,
    old_weight: u64,
}

impl<Key, Val, We: Weighter<Key, Val>, B, L, Id: PlaceholderId<Val>> Drop
    for WeightGuard<Key, Val, We, B, L, Id>
{
    fn drop(&mut self) {
        // SAFETY: shard pointer is valid — guard is created and dropped within
        // entry_or_placeholder which holds &mut CacheShard.
        unsafe {
            let shard = &mut *self.shard;
            let (entry, _) = shard.entries.get_unchecked(self.idx);
            let Entry::Resident(r) = entry else {
                unreachable_unchecked()
            };
            let new_weight = shard.weighter.weight(&r.key, &r.value);
            if self.old_weight != new_weight {
                shard.cold_change_weight(self.idx, self.old_weight, new_weight);
            }
        }
    }
}

/// Structure wrapping a mutable reference to a cached item.
/// On drop, recomputes weight via the inner [`WeightGuard`].
pub struct RefMut<'cache, Key, Val, We: Weighter<Key, Val>, B, L, Id: PlaceholderId<Val>> {
    guard: WeightGuard<Key, Val, We, B, L, Id>,
    _phantom: std::marker::PhantomData<&'cache mut CacheShard<Key, Val, We, B, L, Id>>,
}

impl<Key, Val, We: Weighter<Key, Val>, B, L, Id: PlaceholderId<Val>>
    RefMut<'_, Key, Val, We, B, L, Id>
{
    pub(crate) fn pair(&self) -> (&Key, &Val) {
        // SAFETY: RefMut is only constructed from a valid &mut CacheShard with a
        // Resident entry at idx, and we hold exclusive access via the lifetime.
        unsafe {
            let shard = &*self.guard.shard;
            let (entry, _) = shard.entries.get_unchecked(self.guard.idx);
            let Entry::Resident(Resident { key, value, .. }) = entry else {
                core::hint::unreachable_unchecked()
            };
            (key, value)
        }
    }

    pub(crate) fn value_mut(&mut self) -> &mut Val {
        // SAFETY: same as pair(), plus we have &mut self so exclusive access is guaranteed.
        unsafe {
            let shard = &mut *self.guard.shard;
            let (entry, _) = shard.entries.get_mut_unchecked(self.guard.idx);
            let Entry::Resident(Resident { value, .. }) = entry else {
                core::hint::unreachable_unchecked()
            };
            value
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_prefers_exact_key_over_colliding_ghost() {
        #[derive(Default)]
        struct ZeroHasher;

        impl std::hash::Hasher for ZeroHasher {
            fn write(&mut self, _: &[u8]) {}

            fn finish(&self) -> u64 {
                0
            }
        }

        let options = crate::OptionsBuilder::new()
            .hot_allocation(0.5)
            .ghost_allocation(1.0)
            .estimated_items_capacity(4)
            .weight_capacity(2)
            .build()
            .unwrap();
        let mut cache = crate::unsync::Cache::<u64, u64, _, _>::with_options(
            options,
            crate::UnitWeighter,
            std::hash::BuildHasherDefault::<ZeroHasher>::default(),
            crate::unsync::DefaultLifecycle::default(),
        );
        for key in 0..3 {
            cache.insert(key, key);
        }
        assert_eq!(cache.peek(&1), None);
        assert_eq!(cache.remove(&2), Some((2, 2)));
        assert_eq!(cache.remove(&99), None);
        assert_eq!(cache.peek(&0), Some(&0));
    }

    #[test]
    fn learned_ghost_capacity() {
        struct ValueWeighter;
        impl Weighter<u64, u64> for ValueWeighter {
            fn weight(&self, _key: &u64, value: &u64) -> u64 {
                *value
            }
        }

        let mut shard = CacheShard::<_, _, _, _, _, ()>::new(
            0.5,
            0.5,
            None,
            8,
            ValueWeighter,
            crate::DefaultHashBuilder::default(),
            crate::unsync::DefaultLifecycle::default(),
        );
        let insert = |shard: &mut CacheShard<_, _, _, _, _, ()>, key, value| {
            let hash = shard.hash(&key);
            shard
                .insert(&mut (), hash, key, value, InsertStrategy::Insert, &mut ())
                .unwrap();
        };

        for key in 0..=10 {
            insert(&mut shard, key, u64::from(key >= 2));
        }
        assert_eq!(shard.capacity_non_resident, 4);
        assert_eq!(shard.num_non_resident, 1);

        insert(&mut shard, 11, 4);
        assert_eq!(shard.num_non_resident, 4);

        // Stop after 254 ghost additions since the first sample.
        for key in 12..262 {
            insert(&mut shard, key, 4);
        }
        assert_eq!(shard.capacity_non_resident, 4);
        insert(&mut shard, 262, 4);
        assert_eq!(shard.capacity_non_resident, 3);
        assert_eq!(shard.num_non_resident, 3);

        shard.set_capacity(0, &mut ());
        assert_eq!(shard.len(), 2);
        assert_eq!(shard.capacity_non_resident, 0);
        assert_eq!(shard.num_non_resident, 0);

        shard.set_capacity(8, &mut ());
        for key in 20..=28 {
            insert(&mut shard, key, 1);
        }
        assert_eq!(shard.capacity_non_resident, 4);
        assert_eq!(shard.num_non_resident, 1);

        drop(shard.drain());
        assert_eq!(shard.ghosts_until_sample, 1);
        shard.set_capacity(1, &mut ());
        for key in 0..3 {
            insert(&mut shard, key, 1);
        }
        assert_eq!(shard.ghosts_until_sample, GHOST_SAMPLE_INTERVAL);
    }

    #[cfg(not(feature = "stats"))]
    #[test]
    fn reserve_caps_ghost_headroom() {
        // A small reserve on a shard with a large estimated capacity (hence a
        // large `capacity_non_resident`) must not over-allocate the slab by the
        // full ghost cap; the ghost headroom is bounded by `additional`.
        let mut shard = CacheShard::<
            u64,
            u64,
            crate::UnitWeighter,
            crate::DefaultHashBuilder,
            crate::sync::DefaultLifecycle<u64, u64>,
            std::num::NonZeroU64,
        >::new(
            DEFAULT_HOT_ALLOCATION,
            0.5,             // ghost_allocation -> capacity_non_resident = 500_000
            Some(1_000_000), // estimated_items_capacity
            u64::MAX,        // weight_capacity
            crate::UnitWeighter,
            crate::DefaultHashBuilder::default(),
            crate::sync::DefaultLifecycle::default(),
        );
        assert_eq!(shard.capacity_non_resident, 500_000);
        shard.reserve(100);
        // Ghost headroom is min(additional, capacity_non_resident) = 100, so the
        // slab reserves ~200 entries, not 500_000+.
        assert!(
            shard.entries.capacity() < 1_000,
            "slab over-allocated: {}",
            shard.entries.capacity()
        );
    }

    #[test]
    fn entry_overhead() {
        use std::mem::size_of;
        // Pinned-toolchain layout: sync IDs provide a niche; unsync stores no ID.
        #[cfg(feature = "stats")]
        let sync_overhead = 24;
        #[cfg(not(feature = "stats"))]
        let sync_overhead = 16;
        assert_eq!(
            size_of::<
                Entry<
                    u64,
                    u64,
                    std::num::NonZeroU64,
                    crate::shim::sync::Arc<crate::sync_placeholder::Placeholder<u64>>,
                >,
            >() - size_of::<[u64; 2]>(),
            sync_overhead
        );
        #[cfg(not(feature = "stats"))]
        let unsync_overhead = 8;
        #[cfg(feature = "stats")]
        let unsync_overhead = 16;
        assert_eq!(
            size_of::<Entry<u64, u64, ()>>() - size_of::<[u64; 2]>(),
            unsync_overhead
        );
    }
}
