use std::{
    hash::{BuildHasher, Hash, Hasher},
    mem,
    sync::{
        atomic::{self, AtomicBool, AtomicU64},
        Arc,
    },
};

use hashbrown::raw::RawTable;

use crate::{
    linked_slab::{LinkedSlab, Token},
    placeholder::{new_shared_placeholder, SharedPlaceholder},
    Equivalent, Lifecycle,
};

/// Superset of Weighter (weights 1u32..=u32::MAX) that returns the same weight as u64.
/// Since each shard can only hold up to u32::MAX - 1 items its internal weight cannot overflow.
pub trait InternalWeighter<Key, Val> {
    fn weight(&self, key: &Key, val: &Val) -> u64;
}

impl<Key, Val, T> InternalWeighter<Key, Val> for T
where
    T: crate::Weighter<Key, Val>,
{
    #[inline]
    fn weight(&self, key: &Key, val: &Val) -> u64 {
        crate::Weighter::weight(self, key, val).get() as u64
    }
}

pub enum InsertStrategy {
    Insert,
    Replace { soft: bool },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ResidentState {
    Hot,
    ColdInTest,
    ColdDemoted,
}

#[derive(Debug)]
pub struct Resident<Key, Val> {
    key: Key,

    value: Val,
    state: ResidentState,
    referenced: AtomicBool,
}

#[derive(Debug)]
pub struct Placeholder<Key, Val> {
    key: Key,

    hot: bool,
    shared: SharedPlaceholder<Val>,
}

enum Entry<Key, Val> {
    Resident(Resident<Key, Val>),
    Placeholder(Placeholder<Key, Val>),
    Ghost(u64),
}

impl<Key, Val> Entry<Key, Val> {
    fn dbg(&self) -> &'static str {
        match self {
            Entry::Resident(_) => "Resident",
            Entry::Placeholder(_) => "Placeholder",
            Entry::Ghost(_) => "Ghost",
        }
    }
}

/// A bounded cache using a modified CLOCK-PRO eviction policy.
/// The implementation allows some parallelism as gets don't require exclusive access.
/// Any evicted items are returned so they can be dropped by the caller, outside the locks.
pub struct CacheShard<Key, Val, We, B, L> {
    hash_builder: B,
    /// Map to an entry in the `entries` slab.
    /// Note that the actual key/value/hash are not stored in the map but in the slab.
    map: RawTable<Token>,
    /// Slab holding entries
    entries: LinkedSlab<Entry<Key, Val>>,
    /// Head of cold list, containing ColdInTest and ColdDemoted entries.
    cold_head: Option<Token>,
    /// Head of hot list, containing Hot entries.
    hot_head: Option<Token>,
    /// Head of ghost list, containing non-resident/Hash entries.
    ghost_head: Option<Token>,
    weight_target_hot: u64,
    weight_capacity: u64,
    weight_hot: u64,
    weight_cold: u64,
    num_hot: usize,
    num_cold: usize,
    num_non_resident: usize,
    capacity_non_resident: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    weighter: We,
    pub(crate) lifecycle: L,
}

impl<Key, Val, We, B, L> CacheShard<Key, Val, We, B, L> {
    pub fn remove_placeholder(&mut self, placeholder: &SharedPlaceholder<Val>) {
        self.map.remove_entry(placeholder.hash, |&idx| {
            if idx != placeholder.idx {
                return false;
            }
            let (entry, _) = self.entries.get(idx).unwrap();
            matches!(entry, Entry::Placeholder(Placeholder { shared, .. }) if Arc::ptr_eq(shared, placeholder))
        });
    }
}

impl<
        Key: Eq + Hash,
        Val,
        We: InternalWeighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
    > CacheShard<Key, Val, We, B, L>
{
    pub fn new(
        hot_allocation: f64,
        ghost_allocation: f64,
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
        lifecycle: L,
    ) -> Self {
        let weight_target_hot = (weight_capacity as f64 * hot_allocation) as u64;
        let capacity_non_resident = (estimated_items_capacity as f64 * ghost_allocation) as usize;
        Self {
            hash_builder,
            map: RawTable::with_capacity(0),
            entries: LinkedSlab::with_capacity(0),
            weight_capacity,
            hits: Default::default(),
            misses: Default::default(),
            cold_head: None,
            hot_head: None,
            ghost_head: None,
            capacity_non_resident,
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

    #[cfg(fuzzing)]
    pub fn validate(&self) {
        let mut num_hot = 0;
        let mut num_cold = 0;
        let mut num_non_resident = 0;
        let mut weight_hot = 0;
        let mut weight_cold = 0;
        for e in self.entries.iter_entries() {
            match e {
                Entry::Resident(r)
                    if matches!(
                        r.state,
                        ResidentState::ColdDemoted | ResidentState::ColdInTest
                    ) =>
                {
                    num_cold += 1;
                    weight_cold += self.weighter.weight(&r.key, &r.value);
                }
                Entry::Resident(r) => {
                    num_hot += 1;
                    weight_hot += self.weighter.weight(&r.key, &r.value);
                }
                Entry::Ghost(_) => {
                    num_non_resident += 1;
                }
                Entry::Placeholder(_) => (),
            }
        }
        // eprintln!("-------------");
        // dbg!(num_hot, num_cold, num_non_resident, weight_hot, weight_cold);
        // dbg!(
        //     self.num_hot,
        //     self.num_cold,
        //     self.num_non_resident,
        //     self.weight_hot,
        //     self.weight_cold,
        //     self.weight_target_hot,
        //     self.capacity_non_resident
        // );
        assert_eq!(num_hot, self.num_hot);
        assert_eq!(num_cold, self.num_cold);
        assert_eq!(num_non_resident, self.num_non_resident);
        assert_eq!(weight_hot, self.weight_hot);
        assert_eq!(weight_cold, self.weight_cold);
        assert!(weight_hot <= self.weight_target_hot);
        assert!(num_non_resident <= self.capacity_non_resident);
    }

    /// Reserver additional space for `additional` entries.
    /// Note that this is counted in entries, and is not weighted.
    pub fn reserve(&mut self, additional: usize) {
        // extra 50% for non-resident entries
        let additional = additional.saturating_add(additional / 2);
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

    pub fn weight(&self) -> u64 {
        self.weight_hot + self.weight_cold
    }

    pub fn len(&self) -> usize {
        self.num_hot + self.num_cold
    }

    pub fn capacity(&self) -> u64 {
        self.weight_capacity
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(atomic::Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(atomic::Ordering::Relaxed)
    }

    #[inline]
    fn hash_static<Q: ?Sized>(hasher: &B, key: &Q) -> u64
    where
        Q: Hash + Equivalent<Key>,
    {
        let mut hasher = hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    pub fn hash<Q: ?Sized>(&self, key: &Q) -> u64
    where
        Q: Hash + Equivalent<Key>,
    {
        Self::hash_static(&self.hash_builder, key)
    }

    #[inline]
    fn search<Q: ?Sized>(&self, hash: u64, k: &Q) -> Option<Token>
    where
        Q: Hash + Equivalent<Key>,
    {
        // Safety for `RawTable::iter_hash` and `Bucket::as_ref`:
        // * Their outputs do not outlive their HashBrown:
        // The HashBrown instance is alive for the entirety of the function and
        // the content references never leave the function.
        // * HashBrown can't be mutated while being iterated with iter_hash:
        // The HashBrown instance isn't mutated in this method.
        unsafe {
            let mut hash_match = None;
            for bucket in self.map.iter_hash(hash) {
                let idx = *bucket.as_ref();
                let (entry, _) = self.entries.get(idx).unwrap();
                match entry {
                    Entry::Resident(Resident { key, .. })
                    | Entry::Placeholder(Placeholder { key, .. })
                        if k.equivalent(key) =>
                    {
                        return Some(idx);
                    }
                    Entry::Ghost(non_resident_hash) if *non_resident_hash == hash => {
                        hash_match = Some(idx);
                    }
                    _ => (),
                }
            }
            hash_match
        }
    }

    #[inline]
    fn search_resident<Q: ?Sized>(&self, hash: u64, k: &Q) -> Option<Token>
    where
        Q: Hash + Equivalent<Key>,
    {
        self.map
            .get(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                matches!(entry, Entry::Resident(Resident { key,.. }) if k.equivalent(key) )
            })
            .copied()
    }

    pub fn get<Q: ?Sized>(&self, hash: u64, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        if let Some(idx) = self.search_resident(hash, key) {
            let Some((Entry::Resident(resident), _)) = self.entries.get(idx) else {
                unreachable!()
            };
            resident.referenced.store(true, atomic::Ordering::Relaxed);
            self.hits.fetch_add(1, atomic::Ordering::Relaxed);
            return Some(&resident.value);
        }
        self.misses.fetch_add(1, atomic::Ordering::Relaxed);
        None
    }

    pub fn get_mut<Q: ?Sized>(&mut self, hash: u64, key: &Q) -> Option<&mut Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        if let Some(idx) = self.search_resident(hash, key) {
            let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else {
                unreachable!()
            };
            *resident.referenced.get_mut() = true;
            *self.hits.get_mut() += 1;
            return Some(&mut resident.value);
        }
        *self.misses.get_mut() += 1;
        None
    }

    pub fn peek<Q: ?Sized>(&self, hash: u64, key: &Q) -> Option<&Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        let idx = self.search_resident(hash, key)?;
        let Some((Entry::Resident(resident), _)) = self.entries.get(idx) else {
            unreachable!()
        };
        Some(&resident.value)
    }

    pub fn peek_mut<Q: ?Sized>(&mut self, hash: u64, key: &Q) -> Option<&mut Val>
    where
        Q: Hash + Equivalent<Key>,
    {
        let idx = self.search_resident(hash, key)?;
        let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else {
            unreachable!()
        };
        Some(&mut resident.value)
    }

    pub fn remove<Q: ?Sized>(&mut self, lcs: &mut L::RequestState, hash: u64, key: &Q) -> bool
    where
        Q: Hash + Equivalent<Key>,
    {
        let Some(idx) = self.search(hash, key) else {
            return false;
        };
        self.map_remove(hash, idx);
        let (entry, next) = self.entries.remove(idx).unwrap();
        let list_head = match entry {
            Entry::Resident(r) => {
                let weight = self.weighter.weight(&r.key, &r.value);
                self.lifecycle.on_evict(lcs, r.key, r.value);
                if r.state == ResidentState::Hot {
                    self.num_hot -= 1;
                    self.weight_hot -= weight;
                    &mut self.hot_head
                } else {
                    debug_assert!(matches!(
                        r.state,
                        ResidentState::ColdDemoted | ResidentState::ColdInTest
                    ));
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
            Entry::Placeholder(_) => {
                // TODO: this is probably undesirable as it could lead to two placeholders for the same key.
                return false;
            }
        };
        if *list_head == Some(idx) {
            *list_head = next;
        }
        true
    }

    /// Advance cold ring, promoting to hot and demoting as needed.
    /// Returns the evicted entry.
    /// Panics if the cache is empty.
    fn advance_cold(&mut self, lcs: &mut L::RequestState) {
        loop {
            let idx = if let Some(idx) = self.cold_head {
                idx
            } else {
                self.advance_hot(lcs)
            };
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else {
                unreachable!()
            };
            debug_assert!(matches!(
                resident.state,
                ResidentState::ColdDemoted | ResidentState::ColdInTest
            ));
            if *resident.referenced.get_mut() {
                *resident.referenced.get_mut() = false;
                if resident.state == ResidentState::ColdInTest {
                    resident.state = ResidentState::Hot;
                    self.lifecycle
                        .on_change_state(lcs, &resident.key, &resident.value, true);
                    let weight = self.weighter.weight(&resident.key, &resident.value);
                    self.weight_hot += weight;
                    self.weight_cold -= weight;
                    self.num_hot += 1;
                    self.num_cold -= 1;
                    Self::relink(
                        &mut self.entries,
                        idx,
                        &mut self.cold_head,
                        &mut self.hot_head,
                    );
                    // demote hot entries as we go, by advancing both lists together
                    // we keep a similar recency in both lists.
                    while self.weight_hot > self.weight_target_hot {
                        self.advance_hot(lcs);
                    }
                } else {
                    // ColdDemoted
                    debug_assert_eq!(resident.state, ResidentState::ColdDemoted);
                    resident.state = ResidentState::ColdInTest;
                    self.cold_head = Some(next);
                }
                continue;
            }

            let weight = self.weighter.weight(&resident.key, &resident.value);
            let hash = Self::hash_static(&self.hash_builder, &resident.key);
            let Entry::Resident(resident) = mem::replace(entry, Entry::Ghost(hash)) else {
                unreachable!()
            };
            self.num_cold -= 1;
            self.weight_cold -= weight;

            // Register a non-resident entry if ColdInTest
            if resident.state == ResidentState::ColdInTest {
                self.num_non_resident += 1;
                Self::relink(
                    &mut self.entries,
                    idx,
                    &mut self.cold_head,
                    &mut self.ghost_head,
                );
                if self.num_non_resident > self.capacity_non_resident {
                    self.advance_ghost();
                }
            } else {
                self.map_remove(hash, idx);
                let (_, next) = self.entries.remove(idx).unwrap();
                self.cold_head = next;
            }
            self.lifecycle.on_evict(lcs, resident.key, resident.value);
            return;
        }
    }

    /// Advance hot ring demoting entries to cold.
    /// Returns the Token of the new cold entry.
    /// Panics if there are no hot entries.
    #[inline]
    fn advance_hot(&mut self, lcs: &mut L::RequestState) -> Token {
        debug_assert_ne!(self.num_hot, 0);
        loop {
            let idx = self.hot_head.unwrap();
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else {
                unreachable!()
            };
            debug_assert_eq!(resident.state, ResidentState::Hot);
            if *resident.referenced.get_mut() {
                *resident.referenced.get_mut() = false;
                self.hot_head = Some(next);
                continue;
            } else {
                resident.state = ResidentState::ColdDemoted;
            }
            self.lifecycle
                .on_change_state(lcs, &resident.key, &resident.value, false);
            let weight = self.weighter.weight(&resident.key, &resident.value);
            self.weight_hot -= weight;
            self.weight_cold += weight;
            self.num_hot -= 1;
            self.num_cold += 1;
            Self::relink(
                &mut self.entries,
                idx,
                &mut self.hot_head,
                &mut self.cold_head,
            );
            return idx;
        }
    }

    #[inline]
    fn advance_ghost(&mut self) {
        debug_assert_ne!(self.num_non_resident, 0);
        let idx = self.ghost_head.unwrap();
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        let Entry::Ghost(hash) = *entry else {
            unreachable!("{}", entry.dbg())
        };
        self.num_non_resident -= 1;
        self.map_remove(hash, idx);
        let (_, next) = self.entries.remove(idx).unwrap();
        self.ghost_head = next;
    }

    fn insert_existing(
        &mut self,
        lcs: &mut L::RequestState,
        hash: u64,
        idx: Token,
        key: Key,
        value: Val,
        weight: u64,
        strategy: InsertStrategy,
    ) -> Result<(), (Key, Val)> {
        let (entry, _) = self.entries.get_mut(idx).unwrap();

        if weight > self.weight_capacity {
            // don't admit if it won't fit within the budget
            match entry {
                Entry::Resident(_) => {
                    // but also make sure to remove the existing entry
                    self.remove(lcs, hash, &key);
                }
                Entry::Placeholder(_) | Entry::Ghost(_) => (),
            }
            self.lifecycle.on_evict(lcs, key, value);
            return Ok(());
        }

        match entry {
            Entry::Resident(resident) => {
                let evicted_weight = self.weighter.weight(&resident.key, &resident.value);
                if resident.state == ResidentState::Hot {
                    self.weight_hot -= evicted_weight;
                    self.weight_hot += weight;
                } else {
                    self.weight_cold -= evicted_weight;
                    self.weight_cold += weight;
                }
                // re-insert counts as a hit unless it's a soft replace
                let referenced = *resident.referenced.get_mut()
                    || !matches!(strategy, InsertStrategy::Replace { soft: true });
                let new_resident = Resident {
                    key,
                    value,
                    state: resident.state,
                    referenced: AtomicBool::new(referenced),
                };
                let evicted = mem::replace(resident, new_resident);
                self.lifecycle.on_evict(lcs, evicted.key, evicted.value);
                self.lifecycle.on_change_state(
                    lcs,
                    &resident.key,
                    &resident.value,
                    matches!(resident.state, ResidentState::Hot),
                );
            }
            Entry::Placeholder(..) | Entry::Ghost(..)
                if matches!(strategy, InsertStrategy::Replace { .. }) =>
            {
                return Err((key, value));
            }
            Entry::Placeholder(..) | Entry::Ghost(..) => {
                self.lifecycle.on_change_state(lcs, &key, &value, true);
                let evicted = mem::replace(
                    entry,
                    Entry::Resident(Resident {
                        key,
                        value,
                        state: ResidentState::Hot,
                        referenced: Default::default(),
                    }),
                );
                self.num_hot += 1;
                self.weight_hot += weight;
                if matches!(evicted, Entry::Ghost(..)) {
                    self.num_non_resident -= 1;
                    Self::relink(
                        &mut self.entries,
                        idx,
                        &mut self.ghost_head,
                        &mut self.hot_head,
                    );
                }
            }
        }

        // the replacement may have made the hot section/cache too big
        while self.weight_hot > self.weight_target_hot {
            self.advance_hot(lcs);
        }
        while self.weight_hot + self.weight_cold > self.weight_capacity {
            self.advance_cold(lcs);
        }
        Ok(())
    }

    #[inline]
    fn relink<T>(
        ring: &mut LinkedSlab<T>,
        idx: Token,
        source_head: &mut Option<Token>,
        target_head: &mut Option<Token>,
    ) {
        let next = ring.unlink(idx);
        if *source_head == Some(idx) {
            *source_head = next;
        }
        ring.link(idx, *target_head);
        if target_head.is_none() {
            *target_head = Some(idx);
        }
    }

    #[inline]
    fn map_insert(&mut self, hash: u64, idx: Token) {
        self.map.insert(hash, idx, |&i| {
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
        let removed = self.map.erase_entry(hash, |&i| i == idx);
        debug_assert!(removed);
    }

    pub fn replace_placeholder(
        &mut self,
        lcs: &mut L::RequestState,
        placeholder: &SharedPlaceholder<Val>,
        referenced: bool,
        value: Val,
    ) -> Result<(), Val> {
        let found = self.map.find(placeholder.hash, |&idx| {
            if idx != placeholder.idx {
                return false;
            }
            let (entry, _) = self.entries.get(idx).unwrap();
            matches!(entry, Entry::Placeholder(Placeholder { shared, .. }) if Arc::ptr_eq(shared, placeholder))
        }).is_some();
        if !found {
            return Err(value);
        }
        let (entry, _) = self.entries.get_mut(placeholder.idx).unwrap();
        let Entry::Placeholder(Placeholder {
            key,
            hot: placeholder_hot,
            ..
        }) = mem::replace(entry, Entry::Ghost(0))
        else {
            unreachable!()
        };
        let weight = self.weighter.weight(&key, &value);
        if weight > self.weight_capacity {
            // don't admit if it won't fit within the budget
            self.entries.remove(placeholder.idx);
            self.map_remove(placeholder.hash, placeholder.idx);
            self.lifecycle.on_evict(lcs, key, value);
            return Ok(());
        }

        let enter_hot =
            placeholder_hot || self.weight_hot + self.weight_cold + weight <= self.weight_capacity;
        self.lifecycle.on_change_state(lcs, &key, &value, enter_hot);
        let (state, list_head) = if enter_hot {
            self.num_hot += 1;
            self.weight_hot += weight;
            (ResidentState::Hot, &mut self.hot_head)
        } else {
            self.num_cold += 1;
            self.weight_cold += weight;
            (ResidentState::ColdInTest, &mut self.cold_head)
        };
        *entry = Entry::Resident(Resident {
            key,
            value,
            state,
            referenced: referenced.into(),
        });

        self.entries.link(placeholder.idx, *list_head);
        if list_head.is_none() {
            *list_head = Some(placeholder.idx);
        }

        // the replacement may have made the hot section/cache too big
        while self.weight_hot > self.weight_target_hot {
            self.advance_hot(lcs);
        }
        while self.weight_hot + self.weight_cold > self.weight_capacity {
            self.advance_cold(lcs);
        }

        Ok(())
    }

    pub fn insert(
        &mut self,
        lcs: &mut L::RequestState,
        hash: u64,
        key: Key,
        value: Val,
        strategy: InsertStrategy,
    ) -> Result<(), (Key, Val)> {
        let weight = self.weighter.weight(&key, &value);
        if let Some(idx) = self.search(hash, &key) {
            return self.insert_existing(lcs, hash, idx, key, value, weight, strategy);
        } else if matches!(strategy, InsertStrategy::Replace { .. }) {
            return Err((key, value));
        }

        if weight > self.weight_capacity {
            // don't admit if it won't fit within the budget
            self.lifecycle.on_evict(lcs, key, value);
            return Ok(());
        }

        let enter_hot = if self.weight_hot + self.weight_cold + weight > self.weight_capacity {
            // evict until we have enough space for this entry
            loop {
                self.advance_cold(lcs);
                if self.weight_hot + self.weight_cold + weight <= self.weight_capacity {
                    break;
                }
            }
            false
        } else {
            // cache is filling up
            self.weight_hot + weight <= self.weight_target_hot
        };

        self.lifecycle.on_change_state(lcs, &key, &value, enter_hot);
        let (state, list_head) = if enter_hot {
            self.num_hot += 1;
            self.weight_hot += weight;
            (ResidentState::Hot, &mut self.hot_head)
        } else {
            self.num_cold += 1;
            self.weight_cold += weight;
            (ResidentState::ColdInTest, &mut self.cold_head)
        };
        let idx = self.entries.insert(
            Entry::Resident(Resident {
                key,
                value,
                state,
                referenced: Default::default(),
            }),
            *list_head,
        );
        if list_head.is_none() {
            *list_head = Some(idx);
        }
        // insert the new key in the map
        self.map_insert(hash, idx);
        Ok(())
    }

    pub fn get_value_or_placeholder(
        &mut self,
        hash: u64,
        key: Key,
    ) -> Result<Val, (SharedPlaceholder<Val>, bool)>
    where
        Val: Clone,
    {
        if let Some(idx) = self.search(hash, &key) {
            let (entry, _) = self.entries.get_mut(idx).unwrap();
            match entry {
                Entry::Resident(resident) => {
                    *resident.referenced.get_mut() = true;
                    *self.hits.get_mut() += 1;
                    Ok(resident.value.clone())
                }
                Entry::Placeholder(p) => {
                    *self.hits.get_mut() += 1;
                    Err((p.shared.clone(), false))
                }
                Entry::Ghost(..) => {
                    *self.misses.get_mut() += 1;
                    let shared = new_shared_placeholder(hash, idx);
                    *entry = Entry::Placeholder(Placeholder {
                        key,
                        hot: true,
                        shared: shared.clone(),
                    });
                    let next_ghost = self.entries.unlink(idx);
                    if self.ghost_head == Some(idx) {
                        self.ghost_head = next_ghost;
                    }
                    Err((shared, true))
                }
            }
        } else {
            *self.misses.get_mut() += 1;
            let idx = self.entries.next_free();
            let shared = new_shared_placeholder(hash, idx);
            let idx_ = self.entries.insert(
                Entry::Placeholder(Placeholder {
                    key,
                    hot: false,
                    shared: shared.clone(),
                }),
                None,
            );
            debug_assert_eq!(idx, idx_);
            self.map_insert(hash, idx);
            Err((shared, true))
        }
    }
}
