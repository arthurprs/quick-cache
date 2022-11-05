use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    mem,
    sync::atomic::{self, AtomicBool, AtomicU64},
};

use hashbrown::raw::RawTable;

use crate::{
    linked_slab::{LinkedSlab, Token},
    Weighter,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ResidentState {
    Hot,
    ColdInTest,
    ColdDemoted,
}

#[derive(Debug)]
pub struct Resident<Key, Ver, Val> {
    key: Key,
    version: Ver,
    value: Val,
    state: ResidentState,
    referenced: AtomicBool,
}

/// Entries can be either Resident `Ok(Resident)` or Ghost `Err(hash)`.
pub type Entry<Key, Ver, Val> = Result<Resident<Key, Ver, Val>, u64>;

/// A version aware cache using a modified CLOCK-PRO eviction policy.
/// The implementation allows some parallelism as gets don't require exclusive access.
/// Any evicted items are returned so they can be dropped by the caller, outside the locks.
pub struct VersionedCacheShard<Key, Ver, Val, We, B> {
    hash_builder: B,
    /// Map to an entry in the `entries` slab.
    /// Note that the actual key/version/value/hash are not stored in the map but in the slab.
    map: RawTable<Token>,
    /// Slab holding entries
    entries: LinkedSlab<Entry<Key, Ver, Val>>,
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

impl<Key: Eq + Hash, Ver: Eq + Hash, Val, We: Weighter<Key, Ver, Val>, B: BuildHasher>
    VersionedCacheShard<Key, Ver, Val, We, B>
{
    pub fn new(
        estimated_items_capacity: usize,
        weight_capacity: u64,
        weighter: We,
        hash_builder: B,
    ) -> Self {
        let weight_capacity = weight_capacity.max(2);
        // assign 1% of the capacity to cold items
        let target_hot = weight_capacity - (weight_capacity / 100).max(1);
        assert!(weight_capacity >= 2);
        assert!(target_hot >= 1);
        assert!(weight_capacity - target_hot >= 1);
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
            capacity_non_resident: estimated_items_capacity / 2,
            weight_target_hot: target_hot,
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
        for a in self.entries.iter_entries() {
            match a {
                Ok(r)
                    if matches!(
                        r.state,
                        ResidentState::ColdDemoted | ResidentState::ColdInTest
                    ) =>
                {
                    num_cold += 1;
                    weight_cold += self.weighter.weight(&r.key, &r.version, &r.value) as u64;
                }
                Ok(r) => {
                    num_hot += 1;
                    weight_hot += self.weighter.weight(&r.key, &r.version, &r.value) as u64;
                }
                Err(_) => {
                    num_non_resident += 1;
                }
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
                Ok(r) => Self::hash_static(&self.hash_builder, &r.key, &r.version),
                Err(non_resident_hash) => *non_resident_hash,
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
    fn hash_static<Q: ?Sized, W: ?Sized>(hasher: &B, key: &Q, version: &W) -> u64
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let mut hasher = hasher.build_hasher();
        key.hash(&mut hasher);
        version.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
    pub fn hash<Q: ?Sized, W: ?Sized>(&self, key: &Q, version: &W) -> u64
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        Self::hash_static(&self.hash_builder, key, version)
    }

    #[inline]
    fn search<Q: ?Sized, W: ?Sized>(&self, hash: u64, key: &Q, version: &W) -> Option<Token>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        self.map
            .get(hash, |&idx| {
                let (entry, _) = self.entries.get(idx).unwrap();
                match entry {
                    Ok(r) => r.key.borrow() == key && r.version.borrow() == version,
                    Err(non_resident_hash) => *non_resident_hash == hash,
                }
            })
            .copied()
    }

    #[inline]
    fn search_collision(&self, hash: u64, original_idx: Token) -> Option<Token> {
        self.map
            .get(hash, |&idx| {
                if idx == original_idx {
                    return false;
                }
                let (entry, _) = self.entries.get(idx).unwrap();
                match entry {
                    Ok(r) => self.hash(&r.key, &r.version) == hash,
                    Err(non_resident_hash) => *non_resident_hash == hash,
                }
            })
            .copied()
    }

    pub fn get<Q: ?Sized, W: ?Sized>(&self, hash: u64, key: &Q, version: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some(idx) = self.search(hash, key, version) {
            let (entry, _) = self.entries.get(idx).unwrap();
            if let Ok(resident) = entry {
                resident.referenced.store(true, atomic::Ordering::Relaxed);
                self.hits.fetch_add(1, atomic::Ordering::Relaxed);
                return Some(&resident.value);
            }
        }
        self.misses.fetch_add(1, atomic::Ordering::Relaxed);
        None
    }

    pub fn get_mut<Q: ?Sized, W: ?Sized>(
        &mut self,
        hash: u64,
        key: &Q,
        version: &W,
    ) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        if let Some(idx) = self.search(hash, key, version) {
            let (entry, _) = self.entries.get_mut(idx).unwrap();
            if let Ok(resident) = entry {
                *resident.referenced.get_mut() = true;
                *self.hits.get_mut() += 1;
                return Some(&mut resident.value);
            }
        }
        *self.misses.get_mut() += 1;
        None
    }

    pub fn peek<Q: ?Sized, W: ?Sized>(&self, hash: u64, key: &Q, version: &W) -> Option<&Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search(hash, key, version)?;
        let (entry, _) = self.entries.get(idx).unwrap();
        if let Ok(resident) = entry {
            Some(&resident.value)
        } else {
            None
        }
    }

    pub fn peek_mut<Q: ?Sized, W: ?Sized>(
        &mut self,
        hash: u64,
        key: &Q,
        version: &W,
    ) -> Option<&mut Val>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search(hash, key, version)?;
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        if let Ok(resident) = entry {
            Some(&mut resident.value)
        } else {
            None
        }
    }

    pub fn remove<Q: ?Sized, W: ?Sized>(
        &mut self,
        hash: u64,
        key: &Q,
        version: &W,
    ) -> Option<Entry<Key, Ver, Val>>
    where
        Key: Borrow<Q>,
        Q: Hash + Eq,
        Ver: Borrow<W>,
        W: Hash + Eq,
    {
        let idx = self.search(hash, key, version)?;
        self.remove_from_map(hash, idx);
        let (entry, next) = self.entries.remove(idx).unwrap();
        let list_head = match &entry {
            Ok(r) => {
                let weight = self.weighter.weight(&r.key, &r.version, &r.value) as u64;
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
            Err(_) => {
                self.num_non_resident -= 1;
                &mut self.ghost_head
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
    fn advance_cold(&mut self) -> Resident<Key, Ver, Val> {
        loop {
            let idx = if let Some(idx) = self.cold_head {
                idx
            } else {
                self.advance_hot()
            };
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let resident = entry.as_mut().unwrap();
            debug_assert!(matches!(
                resident.state,
                ResidentState::ColdDemoted | ResidentState::ColdInTest
            ));
            if *resident.referenced.get_mut() {
                *resident.referenced.get_mut() = false;
                if resident.state == ResidentState::ColdInTest {
                    resident.state = ResidentState::Hot;
                    self.num_hot += 1;
                    self.num_cold -= 1;
                    let weight =
                        self.weighter
                            .weight(&resident.key, &resident.version, &resident.value)
                            as u64;
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
                .weight(&resident.key, &resident.version, &resident.value)
                as u64;
            let hash = Self::hash_static(&self.hash_builder, &resident.key, &resident.version);
            let resident = mem::replace(entry, Err(hash)).unwrap();
            self.num_cold -= 1;
            self.weight_cold -= weight;

            // Register a non-resident entry if ColdInTest, the most common case.
            // In the very unlikely event of a hash collision we'll evict a ColdInTest
            // like a ColdDemoted, without registering a non-resident entry.
            if resident.state == ResidentState::ColdInTest
                && self.search_collision(hash, idx).is_none()
            {
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
                self.remove_from_map(hash, idx);
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
            let resident = entry.as_mut().unwrap();
            debug_assert_eq!(resident.state, ResidentState::Hot);
            if *resident.referenced.get_mut() {
                *resident.referenced.get_mut() = false;
                self.hot_head = Some(next);
                continue;
            } else {
                resident.state = ResidentState::ColdDemoted;
            }
            self.num_hot -= 1;
            self.num_cold += 1;
            let weight = self
                .weighter
                .weight(&resident.key, &resident.version, &resident.value)
                as u64;
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
        let hash = *entry.as_ref().err().unwrap();
        self.num_non_resident -= 1;
        self.remove_from_map(hash, idx);
        let (_, next) = self.entries.remove(idx).unwrap();
        self.ghost_head = next;
    }

    fn insert_existing(
        &mut self,
        idx: Token,
        key: Key,
        version: Ver,
        value: Val,
        weight: u64,
    ) -> Option<Resident<Key, Ver, Val>> {
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        let mut evicted;
        if let Ok(resident) = entry {
            let evicted_weight =
                self.weighter
                    .weight(&resident.key, &resident.version, &resident.value)
                    as u64;
            if resident.state == ResidentState::Hot {
                self.weight_hot -= evicted_weight;
                self.weight_hot += weight;
            } else {
                self.weight_cold -= evicted_weight;
                self.weight_cold += weight;
            }
            let new_resident = Resident {
                key,
                version,
                value,
                state: resident.state,
                referenced: AtomicBool::new(true), // re-insert counts as a hit
            };
            evicted = Some(mem::replace(resident, new_resident));
        } else {
            debug_assert_eq!(
                *entry.as_ref().err().unwrap(),
                Self::hash_static(&self.hash_builder, &key, &version)
            );
            *entry = Ok(Resident {
                key,
                version,
                value,
                state: ResidentState::Hot,
                referenced: Default::default(),
            });
            self.num_non_resident -= 1;
            self.num_hot += 1;
            self.weight_hot += weight;
            Self::relink(
                &mut self.entries,
                idx,
                &mut self.ghost_head,
                &mut self.hot_head,
            );
            evicted = None;
        }

        while self.weight_hot > self.weight_target_hot {
            self.advance_hot();
        }
        // the addition above might have made the cache too big
        while self.weight_hot + self.weight_cold > self.weight_capacity {
            evicted = Some(self.advance_cold());
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
    fn remove_from_map(&mut self, hash: u64, idx: Token) {
        let removed = self.map.erase_entry(hash, |&i| i == idx);
        debug_assert!(removed);
    }

    pub fn insert(
        &mut self,
        hash: u64,
        key: Key,
        version: Ver,
        value: Val,
    ) -> Option<Resident<Key, Ver, Val>> {
        let weight = self.weighter.weight(&key, &version, &value) as u64;
        if weight > self.weight_target_hot {
            // don't admit if it won't fit within hot budget
            return None;
        }

        if let Some(idx) = self.search(hash, &key, &version) {
            return self.insert_existing(idx, key, version, value, weight);
        }

        let mut evicted;
        let enter_hot = if self.weight_hot + self.weight_cold + weight > self.weight_capacity {
            // evict until we have enough space for this entry
            loop {
                evicted = Some(self.advance_cold());
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
            Ok(Resident {
                key,
                version,
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
        self.map.insert(hash, idx, |&i| {
            let (entry, _) = self.entries.get(i).unwrap();
            match entry {
                Ok(r) => Self::hash_static(&self.hash_builder, &r.key, &r.version),
                Err(hash) => *hash,
            }
        });
        evicted
    }
}
