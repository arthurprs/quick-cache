use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
    mem,
    sync::atomic::{self, AtomicBool, AtomicU64},
};

use hashbrown::raw::RawTable;

use crate::linked_slab::{LinkedSlab, Token};

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
pub struct VersionedCacheShard<Key, Ver, Val, B> {
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
    target_hot: usize,
    max_hot_scan: usize,
    capacity_resident: usize,
    capacity_non_resident: usize,
    num_hot: usize,
    num_cold: usize,
    num_non_resident: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<Key: Eq + Hash, Ver: Eq + Hash, Val, B: BuildHasher> VersionedCacheShard<Key, Ver, Val, B> {
    pub fn new(initial_capacity: usize, max_capacity: usize, hash_builder: B) -> Self {
        assert!(initial_capacity <= max_capacity);
        let max_capacity = max_capacity.max(2);
        let capacity_resident = max_capacity;
        let capacity_non_resident = max_capacity / 2;
        let preallocated = initial_capacity + initial_capacity / 2;
        // assign 1% of the capacity to cold items
        let target_hot = capacity_resident - (capacity_resident / 100).max(1) as usize;
        // limit hot scan to ~= 100 + 20% max hot count
        let max_hot_scan = 100 + capacity_resident / 5;
        Self {
            hash_builder,
            map: RawTable::with_capacity(preallocated),
            entries: LinkedSlab::with_capacity(preallocated),
            capacity_resident,
            hits: Default::default(),
            misses: Default::default(),
            max_hot_scan,
            cold_head: None,
            hot_head: None,
            ghost_head: None,
            capacity_non_resident,
            target_hot,
            num_hot: 0,
            num_cold: 0,
            num_non_resident: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.num_hot + self.num_cold
    }

    pub fn capacity(&self) -> usize {
        self.capacity_resident
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
            Ok(r) if r.state == ResidentState::Hot => {
                self.num_hot -= 1;
                &mut self.hot_head
            }
            Ok(r) => {
                debug_assert!(matches!(
                    r.state,
                    ResidentState::ColdDemoted | ResidentState::ColdInTest
                ));
                self.num_cold -= 1;
                &mut self.cold_head
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

    fn advance_cold(&mut self) -> Resident<Key, Ver, Val> {
        debug_assert_ne!(self.num_cold, 0);
        loop {
            let idx = self.cold_head.unwrap();
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
                    Self::relink(
                        &mut self.entries,
                        idx,
                        &mut self.cold_head,
                        &mut self.hot_head,
                    );
                    // demote hot entries as we go, by advancing both lists together
                    // we keep a similar recency in both lists.
                    if self.num_hot > self.target_hot {
                        self.advance_hot();
                    }
                } else {
                    // ColdDemoted
                    resident.state = ResidentState::ColdInTest;
                    self.cold_head = Some(next);
                }
                continue;
            }

            let hash = Self::hash_static(&self.hash_builder, &resident.key, &resident.version);
            let resident = mem::replace(entry, Err(hash)).unwrap();
            self.num_cold -= 1;

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

    #[inline]
    fn advance_hot(&mut self) -> bool {
        debug_assert_ne!(self.num_hot, 0);
        let mut scan_left = self.max_hot_scan;
        loop {
            let idx = self.hot_head.unwrap();
            let (entry, next) = self.entries.get_mut(idx).unwrap();
            let resident = entry.as_mut().unwrap();
            debug_assert_eq!(resident.state, ResidentState::Hot);
            if *resident.referenced.get_mut() {
                *resident.referenced.get_mut() = false;
                if scan_left != 0 {
                    scan_left -= 1;
                    self.hot_head = Some(next);
                    continue;
                }
                // scan count exhausted, demote to ColdInTest as a compromise
                resident.state = ResidentState::ColdInTest;
            } else {
                resident.state = ResidentState::ColdDemoted;
            }
            self.num_hot -= 1;
            self.num_cold += 1;
            Self::relink(
                &mut self.entries,
                idx,
                &mut self.hot_head,
                &mut self.cold_head,
            );
            return true;
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
        Self::unlink(&mut self.entries, idx, &mut self.ghost_head);
    }

    fn evict(&mut self) -> Resident<Key, Ver, Val> {
        debug_assert!(self.num_hot <= self.target_hot + 1);
        debug_assert!(self.num_cold <= self.capacity_resident - self.target_hot + 1);
        debug_assert!(self.num_non_resident <= self.capacity_non_resident);
        // Demote a hot entry if there are too many or there are no cold entries
        if self.num_hot > self.target_hot || self.cold_head.is_none() {
            self.advance_hot();
        }
        debug_assert!(self.num_hot <= self.target_hot);
        debug_assert!(self.num_cold != 0);
        debug_assert!(self.num_cold <= self.capacity_resident - self.target_hot + 1);
        let resident = self.advance_cold();
        debug_assert!(self.num_hot <= self.target_hot);
        debug_assert!(self.num_cold <= self.capacity_resident - self.target_hot);
        debug_assert!(self.num_non_resident <= self.capacity_non_resident);
        resident
    }

    fn insert_existing(
        &mut self,
        idx: Token,
        key: Key,
        version: Ver,
        value: Val,
    ) -> Option<Resident<Key, Ver, Val>> {
        let (entry, _) = self.entries.get_mut(idx).unwrap();
        if let Ok(resident) = entry {
            let new_resident = Resident {
                key,
                version,
                value,
                state: resident.state,
                referenced: AtomicBool::new(true), // re-insert counts as a hit
            };
            Some(mem::replace(resident, new_resident))
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
            Self::relink(
                &mut self.entries,
                idx,
                &mut self.ghost_head,
                &mut self.hot_head,
            );
            if self.num_hot + self.num_cold > self.capacity_resident {
                Some(self.evict())
            } else {
                None
            }
        }
    }

    #[inline]
    fn link<T>(ring: &mut LinkedSlab<T>, idx: Token, list_head: &mut Option<Token>) {
        ring.link(idx, *list_head);
        if list_head.is_none() {
            *list_head = Some(idx);
        }
    }

    #[inline]
    fn unlink<T>(ring: &mut LinkedSlab<T>, idx: Token, source_head: &mut Option<Token>) {
        let next = ring.unlink(idx);
        if *source_head == Some(idx) {
            *source_head = next;
        }
    }

    #[inline]
    fn relink<T>(
        clock: &mut LinkedSlab<T>,
        idx: Token,
        source_head: &mut Option<Token>,
        target_head: &mut Option<Token>,
    ) {
        Self::unlink(clock, idx, source_head);
        Self::link(clock, idx, target_head);
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
        if let Some(idx) = self.search(hash, &key, &version) {
            return self.insert_existing(idx, key, version, value);
        }

        let evicted;
        let enter_hot = if self.num_hot + self.num_cold >= self.capacity_resident {
            evicted = Some(self.evict());
            false
        } else {
            evicted = None;
            self.num_hot < self.target_hot
        };

        let (state, list_head) = if enter_hot {
            self.num_hot += 1;
            (ResidentState::Hot, &mut self.hot_head)
        } else {
            self.num_cold += 1;
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
