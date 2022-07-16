pub type Token = std::num::NonZeroU32;

#[derive(Debug)]
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
#[derive(Debug)]
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

    /// Inserts a new entry in the list, link it before `head`.
    /// If `head` is not set the item will belong to a list only containing itself.
    ///
    /// # Panics
    /// Panics if number of items exceed `u32::MAX - 1`.
    pub fn insert(&mut self, item: T, head: Option<Token>) -> Token {
        let token = self.next_free;
        let idx = (token.get() - 1) as usize;
        if idx < self.entries.len() {
            let entry = &mut self.entries[idx];
            assert!(entry.item.is_none());
            entry.item = Some(item);
            self.next_free = entry.next;
        } else {
            self.next_free = Token::new(token.get().wrapping_add(1)).expect("Capacity overflow");
            self.entries.push(Entry {
                next: token,
                prev: token,
                item: Some(item),
            });
        }
        self.link(token, head);
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

    /// Links an entry before `target_head`
    ///
    /// # Panics
    /// Panics on out of bounds access.
    /// Panics (in debug mode) if linking an absent entry.
    pub fn link(&mut self, idx: Token, target_head: Option<Token>) {
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
                before_head.next = idx;
                (before_head_idx, target_head)
            }
        } else {
            (idx, idx)
        };

        let entry = &mut self.entries[(idx.get() - 1) as usize];
        debug_assert!(entry.item.is_some());
        (entry.prev, entry.next) = (prev, next);
    }

    /// Unlinks an entry without removing it from the ring
    /// Returns next item in the list, if not self.
    ///
    /// # Panics
    /// Panics on out of bounds access.
    /// Panics (in debug mode) if unlinking an absent entry.
    pub fn unlink(&mut self, idx: Token) -> Option<Token> {
        let entry = &self.entries[(idx.get() - 1) as usize];
        debug_assert!(entry.item.is_some());
        let (prev_idx, next_idx) = (entry.prev, entry.next);
        if next_idx == idx {
            debug_assert_eq!(prev_idx, idx);
            // single item list, nothing to do
            None
        } else {
            let next = &mut self.entries[(next_idx.get() - 1) as usize];
            next.prev = prev_idx;
            let prev = &mut self.entries[(prev_idx.get() - 1) as usize];
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
        let next = self.unlink(idx);
        let entry = &mut self.entries[(idx.get() - 1) as usize];
        let old_item = entry.item.take()?;
        entry.next = self.next_free;
        self.next_free = idx;
        Some((old_item, next))
    }
}
