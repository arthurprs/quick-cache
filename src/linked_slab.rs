pub type Token = std::num::NonZeroU32;

#[derive(Debug, Clone)]
struct Entry<T> {
    item: Option<T>,
    /// The next item in the list (possibly itself).
    /// When `item` is None, `next` points ot the next free token in the slab.
    next: Token,
    /// The previous item in the list (possibly itself).
    /// Unused if `item` is None.
    prev: Token,
}

/// A slab like structure that also maintains circular lists with its items.
#[derive(Debug, Clone)]
pub struct LinkedSlab<T> {
    entries: Vec<Entry<T>>,
    next_free: Token,
}

impl<T> LinkedSlab<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            next_free: Token::new(1).unwrap(),
        }
    }

    #[cfg(fuzzing)]
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|e| e.item.is_some()).count()
    }

    #[cfg(any(fuzzing, test))]
    pub fn iter_entries(&self) -> impl Iterator<Item = &T> + '_ {
        self.entries.iter().filter_map(|e| e.item.as_ref())
    }

    #[cfg(any(fuzzing, test))]
    pub fn validate(&self) {
        let mut freelist = std::collections::HashSet::new();
        let mut next_free = self.next_free;
        while next_free.get() as usize - 1 != self.entries.len() {
            freelist.insert(next_free.get() as usize);
            let e = &self.entries[next_free.get() as usize - 1];
            assert!(e.item.is_none(), "{next_free} is in freelist but has item");
            next_free = e.next;
        }
        for (i, e) in self.entries.iter().enumerate() {
            if e.item.is_some() {
                assert!(!freelist.contains(&(i + 1)));
                assert!(!freelist.contains(&(e.prev.get() as usize)));
                assert!(!freelist.contains(&(e.next.get() as usize)));
            }
        }
    }

    /// Inserts a new entry in the list.
    /// Initially, the item will belong to a list only containing itself.
    ///
    /// # Panics
    /// Panics if number of items exceed `u32::MAX - 1`.
    pub fn insert(&mut self, item: T) -> Token {
        let token = self.next_free;
        // eprintln!("linkedslab::insert token {token} head {head:?}");
        let idx = (token.get() - 1) as usize;
        if idx < self.entries.len() {
            let entry = &mut self.entries[idx];
            self.next_free = entry.next;
            (entry.prev, entry.next) = (token, token);
            debug_assert!(entry.item.is_none());
            entry.item = Some(item);
        } else {
            debug_assert_eq!(idx, self.entries.len());
            self.next_free = Token::new(token.get().wrapping_add(1)).expect("Capacity overflow");
            self.entries.push(Entry {
                next: token,
                prev: token,
                item: Some(item),
            });
        }
        token
    }

    /// Gets an entry and a token to the next entry.
    #[inline]
    pub fn get(&self, index: Token) -> Option<(&T, Token)> {
        if let Some(entry) = self.entries.get((index.get() - 1) as usize) {
            if let Some(v) = &entry.item {
                return Some((v, entry.next));
            }
        }
        None
    }

    /// Gets an entry and a token to the next entry.
    #[inline]
    pub fn get_mut(&mut self, index: Token) -> Option<(&mut T, Token)> {
        if let Some(entry) = self.entries.get_mut((index.get() - 1) as usize) {
            if let Some(v) = &mut entry.item {
                return Some((v, entry.next));
            }
        }
        None
    }

    /// Gets an entry and a token to the next entry w/o checking, thus unsafe.
    #[inline]
    pub unsafe fn get_unchecked(&self, index: Token) -> (&T, Token) {
        let entry = self.entries.get_unchecked((index.get() - 1) as usize);
        let v = entry.item.as_ref().unwrap_unchecked();
        (v, entry.next)
    }

    /// Gets an entry and a token to the next entry w/o checking, thus unsafe.
    #[inline]
    pub unsafe fn get_mut_unchecked(&mut self, index: Token) -> (&mut T, Token) {
        let entry = self.entries.get_unchecked_mut((index.get() - 1) as usize);
        let v = entry.item.as_mut().unwrap_unchecked();
        (v, entry.next)
    }

    /// Links an entry before `target_head`. Returns the item next to the linked item,
    /// which is either the item itself or `target_head`.
    ///
    /// # Panics
    /// Panics on out of bounds access.
    /// Panics (in debug mode) if linking an absent entry.
    pub fn link(&mut self, idx: Token, target_head: Option<Token>) -> Token {
        // eprintln!("linkedslab::link {idx} head {target_head:?}");
        let (prev, next) = if let Some(target_head) = target_head {
            let head = &mut self.entries[(target_head.get() - 1) as usize];
            debug_assert!(head.item.is_some());
            if head.prev == target_head {
                // existing list has a single item linking to itself
                head.prev = idx;
                head.next = idx;
                (target_head, target_head)
            } else {
                let before_head_idx = head.prev;
                head.prev = idx;
                let before_head = &mut self.entries[(before_head_idx.get() - 1) as usize];
                debug_assert!(before_head.item.is_some());
                debug_assert_eq!(before_head.next, target_head);
                before_head.next = idx;
                (before_head_idx, target_head)
            }
        } else {
            (idx, idx)
        };

        let entry = &mut self.entries[(idx.get() - 1) as usize];
        debug_assert!(entry.item.is_some());
        debug_assert_eq!(entry.next, idx);
        debug_assert_eq!(entry.prev, idx);
        (entry.prev, entry.next) = (prev, next);
        next
    }

    /// Unlinks an entry without removing it from the ring
    /// Returns next item in the list, if not self.
    ///
    /// # Panics
    /// Panics on out of bounds access.
    /// Panics (in debug mode) if unlinking an absent entry.
    pub fn unlink(&mut self, idx: Token) -> Option<Token> {
        // eprintln!("linkedslab::unlink {idx}");
        let entry = &mut self.entries[(idx.get() - 1) as usize];
        debug_assert!(entry.item.is_some());
        let (prev_idx, next_idx) = (entry.prev, entry.next);
        if next_idx == idx {
            debug_assert_eq!(prev_idx, idx);
            // single item list, nothing to do
            None
        } else {
            (entry.prev, entry.next) = (idx, idx);
            let next = &mut self.entries[(next_idx.get() - 1) as usize];
            debug_assert!(next.item.is_some());
            debug_assert_eq!(next.prev, idx);
            next.prev = prev_idx;
            let prev = &mut self.entries[(prev_idx.get() - 1) as usize];
            debug_assert!(prev.item.is_some());
            debug_assert_eq!(prev.next, idx);
            prev.next = next_idx;
            Some(next_idx)
        }
    }

    /// Unlinks and removes the entry from the ring.
    /// Returns next item in the list, if not self.
    ///
    /// # Panics
    /// Panics on out of bounds access.
    pub fn remove(&mut self, idx: Token) -> Option<(T, Option<Token>)> {
        // eprintln!("linkedslab::remove {idx}");
        let next = self.unlink(idx);
        let entry = &mut self.entries[(idx.get() - 1) as usize];
        let old_item = entry.item.take()?;
        entry.next = self.next_free;
        self.next_free = idx;
        Some((old_item, next))
    }

    /// The Token that will be returned by the next call to `insert()`
    pub fn next_free(&self) -> Token {
        self.next_free
    }

    /// Drains all items from the slab.
    ///
    /// The slab will be emptied even if the returned iterator isn't fully consumed.
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.next_free = Token::new(1).unwrap();
        self.entries.drain(..).flat_map(|i| i.item)
    }

    /// Iterator for the items in the slab
    pub fn iter(&self) -> impl Iterator<Item = &'_ T> + '_ {
        self.entries.iter().flat_map(|i| i.item.as_ref())
    }

    /// Iterator for the items in the slab, starting after a given token.
    pub fn iter_from(
        &self,
        continuation: Option<Token>,
    ) -> impl Iterator<Item = (Token, &'_ T)> + '_ {
        let skip = continuation.map_or(0, |c| c.get() as usize);
        self.entries
            .iter()
            .skip(skip)
            .enumerate()
            .filter_map(move |(i, e)| {
                if let Some(item) = &e.item {
                    Some((Token::new((skip + i + 1) as u32)?, item))
                } else {
                    None
                }
            })
    }
}
