use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    mem,
};

use hashbrown::raw::RawTable;

use crate::std_sync::{
    atomic::{self, AtomicBool, AtomicU64},
    Arc,
};

use crate::{
    linked_slab::{LinkedSlab, Token},
    placeholder::{new_shared_placeholder, SharedPlaceholder},
};

/// Superset of Weighter (weights 1u32..=u32::MAX) that returns the same weight as u64.
/// Since each shard can only hold up to u32::MAX - 1 items its internal weight cannot overflow.
pub trait InternalWeighter<Key, Qey, Val> {
    fn weight(&self, key: &Key, qey: &Qey, val: &Val) -> u64;
}

impl<Key, Qey, Val, T> InternalWeighter<Key, Qey, Val> for T
where
    T: crate::Weighter<Key, Qey, Val>,
{
    #[inline]
    fn weight(&self, key: &Key, qey: &Qey, val: &Val) -> u64 {
        crate::Weighter::weight(self, key, qey, val).get() as u64
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ResidentState {
    Hot,
    ColdInTest,
    ColdDemoted,
}

#[derive(Debug)]
pub struct Resident<Key, Qey, Val> {
    key: Key,
    qey: Qey,
    value: Val,
    state: ResidentState,
    referenced: AtomicBool,
}

#[derive(Debug)]
pub struct Placeholder<Key, Qey, Val> {
    key: Key,
    qey: Qey,
    hot: bool,
    shared: SharedPlaceholder<Val>,
}

pub enum Entry<Key, Qey, Val> {
    Resident(Resident<Key, Qey, Val>),
    Placeholder(Placeholder<Key, Qey, Val>),
    Ghost(u64),
}

impl<Key, Qey, Val> Entry<Key, Qey, Val> {
    fn dbg(&self) -> &'static str {
        match self {
            Entry::Resident(_) => "Resident",
            Entry::Placeholder(_) => "Placeholder",
            Entry::Ghost(_) => "Ghost",
        }
    }
}

/// A qey aware cache using a modified CLOCK-PRO eviction policy.
/// The implementation allows some parallelism as gets don't require exclusive access.
/// Any evicted items are returned so they can be dropped by the caller, outside the locks.
pub struct KQCacheShard<Key, Qey, Val, We, B> {
    hash_builder: B,
    /// Map to an entry in the `entries` slab.
    /// Note that the actual key/qey/value/hash are not stored in the map but in the slab.
    map: RawTable<Token>,
    /// Slab holding entries
    entries: LinkedSlab<Entry<Key, Qey, Val>>,
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
}

impl<Key, Qey, Val, We, B> KQCacheShard<Key, Qey, Val, We, B> {
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

impl<Key: Eq + Hash, Qey: Eq + Hash, Val, We: InternalWeighter<Key, Qey, Val>, B: BuildHasher>
    KQCacheShard<Key, Qey, Val, We, B>
{
    pub fn new(
        hot_allocation: f64,
        ghost_allocation: f64,
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
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
                    weight_cold += self.weighter.weight(&r.key, &r.qey, &r.value);
                }
                Entry::Resident(r) => {
                    num_hot += 1;
                    weight_hot += self.weighter.weight(&r.key, &r.qey, &r.value);
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
                Entry::Resident(Resident { key, qey, .. })
                | Entry::Placeholder(Placeholder { key, qey, .. }) => {
                    Self::hash_static(&self.hash_builder, key, qey)
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
    fn hash_static<Q: ?Sized, W: ?Sized>(hasher: &B, key: &Q, qey: &W) -> u64
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let mut hasher = hasher.build_hasher();
        key.hash(&mut hasher);
        qey.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    pub fn hash<Q: ?Sized, W: ?Sized>(&self, key: &Q, qey: &W) -> u64
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        Self::hash_static(&self.hash_builder, key, qey)
    }

    #[inline]
    fn search<Q: ?Sized, W: ?Sized>(&self, hash: u64, k: &Q, q: &W) -> Option<Token>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
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
                    Entry::Resident(Resident { key, qey, .. })
                    | Entry::Placeholder(Placeholder { key, qey, .. })
                        if key.borrow() == k && qey.borrow() == q =>
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
    fn search_resident<Q: ?Sized, W: ?Sized>(&self, hash: u64, k: &Q, q: &W) -> Option<Token>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        self.map
            .get(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                matches!(entry, Entry::Resident(Resident { key, qey, .. }) if key.borrow() == k && qey.borrow() == q)
            })
            .copied()
    }

    pub fn get<Q: ?Sized, W: ?Sized>(&self, hash: u64, key: &Q, qey: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some(idx) = self.search_resident(hash, key, qey) {
            let Some((Entry::Resident(resident), _)) = self.entries.get(idx) else { unreachable!() };
            resident.referenced.store(true, atomic::Ordering::Relaxed);
            self.hits.fetch_add(1, atomic::Ordering::Relaxed);
            return Some(&resident.value);
        }
        self.misses.fetch_add(1, atomic::Ordering::Relaxed);
        None
    }

    pub fn get_mut<Q: ?Sized, W: ?Sized>(&mut self, hash: u64, key: &Q, qey: &W) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some(idx) = self.search_resident(hash, key, qey) {
            let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else { unreachable!() };
            resident.referenced.set_mut(true);
            self.hits.inc_mut();
            return Some(&mut resident.value);
        }
        self.misses.inc_mut();
        None
    }

    pub fn peek<Q: ?Sized, W: ?Sized>(&self, hash: u64, key: &Q, qey: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search_resident(hash, key, qey)?;
        let Some((Entry::Resident(resident), _)) = self.entries.get(idx) else { unreachable!() };
        Some(&resident.value)
    }

    pub fn peek_mut<Q: ?Sized, W: ?Sized>(
        &mut self,
        hash: u64,
        key: &Q,
        qey: &W,
    ) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search_resident(hash, key, qey)?;
        let Some((Entry::Resident(resident), _)) = self.entries.get_mut(idx) else { unreachable!() };
        Some(&mut resident.value)
    }

    pub fn remove<Q: ?Sized, W: ?Sized>(
        &mut self,
        hash: u64,
        key: &Q,
        qey: &W,
    ) -> Option<Entry<Key, Qey, Val>>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Qey: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search(hash, key, qey)?;
        self.map_remove(hash, idx);
        let (entry, next) = self.entries.remove(idx).unwrap();
        let list_head = match &entry {
            Entry::Resident(r) => {
                let weight = self.weighter.weight(&r.key, &r.qey, &r.value);
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
                // TODO: this is probably undesirable as it could leak to two placeholders for the same key.
                return Some(entry);
            }
        };
        if *list_head == Some(idx) {
            *list_head = next;
        }
        Some(entry)
    }

    /// Advance cold ring, promoting to hot and demoting as needed.
    /// Returns the evicted entry.
    /// Panics if the cache is empty.
    fn advance_cold(&mut self) -> Resident<Key, Qey, Val> {
        loop {
            let idx = if let Some(idx) = self.cold_head {
                idx
            } else {
                self.advance_hot()
            };
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else { unreachable!() };
            debug_assert!(matches!(
                resident.state,
                ResidentState::ColdDemoted | ResidentState::ColdInTest
            ));
            if resident.referenced.load_mut() {
                resident.referenced.set_mut(false);
                if resident.state == ResidentState::ColdInTest {
                    resident.state = ResidentState::Hot;
                    self.num_hot += 1;
                    self.num_cold -= 1;
                    let weight =
                        self.weighter
                            .weight(&resident.key, &resident.qey, &resident.value);
                    self.weight_hot += weight;
                    self.weight_cold -= weight;
                    Self::relink(
                        &mut self.entries,
                        idx,
                        &mut self.cold_head,
                        &mut self.hot_head,
                    );
                    // demote hot entries as we go, by advancing both lists together
                    // we keep a similar recency in both lists.
                    while self.weight_hot > self.weight_target_hot {
                        self.advance_hot();
                    }
                } else {
                    // ColdDemoted
                    debug_assert_eq!(resident.state, ResidentState::ColdDemoted);
                    resident.state = ResidentState::ColdInTest;
                    self.cold_head = Some(next);
                }
                continue;
            }

            let weight = self
                .weighter
                .weight(&resident.key, &resident.qey, &resident.value);
            let hash = Self::hash_static(&self.hash_builder, &resident.key, &resident.qey);
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
            return resident;
        }
    }

    /// Advance hot ring demoting entries to cold.
    /// Returns the Token of the new cold entry.
    /// Panics if there are no hot entries.
    #[inline]
    fn advance_hot(&mut self) -> Token {
        debug_assert_ne!(self.num_hot, 0);
        loop {
            let idx = self.hot_head.unwrap();
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let Entry::Resident(resident) = entry else { unreachable!() };
            debug_assert_eq!(resident.state, ResidentState::Hot);
            if resident.referenced.load_mut() {
                resident.referenced.set_mut(false);
                self.hot_head = Some(next);
                continue;
            } else {
                resident.state = ResidentState::ColdDemoted;
            }
            self.num_hot -= 1;
            self.num_cold += 1;
            let weight = self
                .weighter
                .weight(&resident.key, &resident.qey, &resident.value);
            self.weight_hot -= weight;
            self.weight_cold += weight;
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
        let Entry::Ghost(hash) = *entry else { unreachable!("{}", entry.dbg()) };
        self.num_non_resident -= 1;
        self.map_remove(hash, idx);
        let (_, next) = self.entries.remove(idx).unwrap();
        self.ghost_head = next;
    }

    fn insert_existing(
        &mut self,
        idx: Token,
        key: Key,
        qey: Qey,
        value: Val,
        weight: u64,
    ) -> Entry<Key, Qey, Val> {
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        let mut evicted;
        match entry {
            Entry::Resident(resident) => {
                let evicted_weight =
                    self.weighter
                        .weight(&resident.key, &resident.qey, &resident.value);
                if resident.state == ResidentState::Hot {
                    self.weight_hot -= evicted_weight;
                    self.weight_hot += weight;
                } else {
                    self.weight_cold -= evicted_weight;
                    self.weight_cold += weight;
                }
                let new_resident = Resident {
                    key,
                    qey,
                    value,
                    state: resident.state,
                    referenced: AtomicBool::new(true), // re-insert counts as a hit
                };
                evicted = Entry::Resident(mem::replace(resident, new_resident));
            }
            Entry::Placeholder(..) | Entry::Ghost(..) => {
                evicted = mem::replace(
                    entry,
                    Entry::Resident(Resident {
                        key,
                        qey,
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
            self.advance_hot();
        }
        while self.weight_hot + self.weight_cold > self.weight_capacity {
            evicted = Entry::Resident(self.advance_cold());
        }
        evicted
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
                Entry::Resident(Resident { key, qey, .. })
                | Entry::Placeholder(Placeholder { key, qey, .. }) => {
                    Self::hash_static(&self.hash_builder, key, qey)
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
        placeholder: &SharedPlaceholder<Val>,
        referenced: bool,
        value: Val,
    ) -> Result<Option<Entry<Key, Qey, Val>>, Val> {
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
        let Entry::Placeholder(Placeholder { key, qey, hot: placeholder_hot, .. }) =
            mem::replace(entry, Entry::Ghost(0)) else { unreachable!() };
        let weight = self.weighter.weight(&key, &qey, &value);
        if weight > self.weight_capacity {
            // don't admit if it won't fit within the budget
            return Ok(None);
        }
        let enter_hot =
            placeholder_hot || self.weight_hot + self.weight_cold + weight <= self.weight_capacity;
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
            qey,
            value,
            state,
            referenced: referenced.into(),
        });

        self.entries.link(placeholder.idx, *list_head);
        if list_head.is_none() {
            *list_head = Some(placeholder.idx);
        }

        let mut evicted = None;
        // the replacement may have made the hot section/cache too big
        while self.weight_hot > self.weight_target_hot {
            self.advance_hot();
        }
        while self.weight_hot + self.weight_cold > self.weight_capacity {
            evicted = Some(Entry::Resident(self.advance_cold()));
        }

        Ok(evicted)
    }

    pub fn insert(
        &mut self,
        hash: u64,
        key: Key,
        qey: Qey,
        value: Val,
    ) -> Option<Entry<Key, Qey, Val>> {
        let weight = self.weighter.weight(&key, &qey, &value);
        if weight > self.weight_capacity {
            // don't admit if it won't fit within the budget
            return None;
        }

        if let Some(idx) = self.search(hash, &key, &qey) {
            return Some(self.insert_existing(idx, key, qey, value, weight));
        }

        let mut evicted;
        let enter_hot = if self.weight_hot + self.weight_cold + weight > self.weight_capacity {
            // evict until we have enough space for this entry
            loop {
                evicted = Some(Entry::Resident(self.advance_cold()));
                if self.weight_hot + self.weight_cold + weight <= self.weight_capacity {
                    break;
                }
            }
            false
        } else {
            // cache is filling up
            evicted = None;
            self.weight_hot + weight <= self.weight_target_hot
        };

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
                qey,
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
        evicted
    }

    pub fn get_value_or_placeholder(
        &mut self,
        hash: u64,
        key: Key,
        qey: Qey,
    ) -> Result<Val, (SharedPlaceholder<Val>, bool)>
    where
        Val: Clone,
    {
        if let Some(idx) = self.search(hash, &key, &qey) {
            let (entry, _) = self.entries.get_mut(idx).unwrap();
            match entry {
                Entry::Resident(resident) => {
                    resident.referenced.set_mut(true);
                    self.hits.set_mut(1);
                    Ok(resident.value.clone())
                }
                Entry::Placeholder(p) => {
                    self.hits.inc_mut();
                    Err((p.shared.clone(), false))
                }
                Entry::Ghost(..) => {
                    self.misses.inc_mut();
                    let shared = new_shared_placeholder(hash, idx);
                    *entry = Entry::Placeholder(Placeholder {
                        key,
                        qey,
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
            self.misses.inc_mut();
            let idx = self.entries.next_free();
            let shared = new_shared_placeholder(hash, idx);
            let idx_ = self.entries.insert(
                Entry::Placeholder(Placeholder {
                    key,
                    qey,
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

trait AtomicExt<T> {
    fn set_mut(&mut self, value: T);
    fn load_mut(&mut self) -> T;
    fn inc_mut(&mut self) {
        unimplemented!()
    }
}

#[cfg(not(loom))]
impl AtomicExt<bool> for AtomicBool {
    fn set_mut(&mut self, value: bool) {
        *self.get_mut() = value;
    }

    fn load_mut(&mut self) -> bool {
        *self.get_mut()
    }
}

#[cfg(loom)]
impl AtomicExt<bool> for AtomicBool {
    fn set_mut(&mut self, value: bool) {
        self.store(value, loom::sync::atomic::Ordering::SeqCst);
    }

    fn load_mut(&mut self) -> bool {
        unsafe { self.unsync_load() }
    }
}

#[cfg(not(loom))]
impl AtomicExt<u64> for AtomicU64 {
    fn set_mut(&mut self, value: u64) {
        *self.get_mut() = value;
    }

    fn load_mut(&mut self) -> u64 {
        *self.get_mut()
    }

    fn inc_mut(&mut self) {
        *self.get_mut() += 1;
    }
}

#[cfg(loom)]
impl AtomicExt<u64> for AtomicU64 {
    fn set_mut(&mut self, value: u64) {
        self.with_mut(|v| {
            *v = value;
        });
    }

    fn load_mut(&mut self) -> u64 {
        self.with_mut(|v| *v)
    }

    fn inc_mut(&mut self) {
        self.with_mut(|v| {
            *v += 1;
        })
    }
}
