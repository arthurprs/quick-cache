use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    mem, pin,
    task::{self, Poll},
    time::{Duration, Instant},
};

use crate::{
    linked_slab::Token,
    shard::CacheShard,
    shim::{
        rw_lock::{RwLock, RwLockWriteGuard},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread, OnceLock,
    },
    Equivalent, Lifecycle, Weighter,
};

pub type SharedPlaceholder<Val> = Arc<Placeholder<Val>>;

impl<Val> crate::shard::SharedPlaceholder for SharedPlaceholder<Val> {
    fn new(hash: u64, idx: Token) -> Self {
        Arc::new(Placeholder {
            hash,
            idx,
            value: OnceLock::new(),
            state: RwLock::new(State {
                waiters: Default::default(),
                loading: LoadingState::Loading,
            }),
        })
    }

    #[inline]
    fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(self, other)
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

#[derive(Debug)]
pub struct Placeholder<Val> {
    hash: u64,
    idx: Token,
    state: RwLock<State>,
    value: OnceLock<Val>,
}

#[derive(Debug)]
pub struct State {
    /// The waiters list
    /// Adding to the list requires holding the outer shard lock to avoid races between
    /// removing the orphan placeholder from the cache and adding a new waiter to it.
    waiters: Vec<Waiter>,
    loading: LoadingState,
}

#[derive(Debug)]
enum LoadingState {
    /// A guard was/will be created and the value might get filled
    Loading,
    /// A value was filled, no more waiters can be added
    Inserted,
}

pub struct PlaceholderGuard<'a, Key, Val, We, B, L> {
    lifecycle: &'a L,
    shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
    shared: SharedPlaceholder<Val>,
    inserted: bool,
}

#[derive(Debug)]
enum Waiter {
    Thread {
        notified: *const AtomicBool,
        thread: thread::Thread,
    },
    Task {
        notified: *const AtomicBool,
        waker: task::Waker,
    },
}

// SAFETY: The AtomicBool is on the waiting thread's stack or pinned future
// and the thread/task will remove itself from waiters before returning
unsafe impl Send for Waiter {}
unsafe impl Sync for Waiter {}

impl Waiter {
    #[inline]
    fn notify(self) {
        match self {
            Waiter::Thread {
                thread, notified, ..
            } => {
                // SAFETY: The AtomicBool is on the waiting thread's stack or pinned future
                // and the thread/task will remove itself from waiters before returning
                unsafe { notified.as_ref().unwrap().store(true, Ordering::Release) };
                thread.unpark();
            }
            Waiter::Task { waker: t, notified } => {
                unsafe { notified.as_ref().unwrap().store(true, Ordering::Release) };
                t.wake();
            }
        }
    }

    #[inline]
    fn is_waiter(&self, other: *const AtomicBool) -> bool {
        matches!(self, Waiter::Task { notified, .. } | Waiter::Thread { notified, .. } if std::ptr::eq(*notified, other))
    }
}

#[derive(Debug)]
pub enum GuardResult<'a, Key, Val, We, B, L> {
    Value(Val),
    Guard(PlaceholderGuard<'a, Key, Val, We, B, L>),
    Timeout,
}

impl<'a, Key, Val, We, B, L> PlaceholderGuard<'a, Key, Val, We, B, L> {
    pub fn start_loading(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        shared: SharedPlaceholder<Val>,
    ) -> Self {
        debug_assert!(matches!(
            shared.state.write().loading,
            LoadingState::Loading
        ));
        PlaceholderGuard {
            lifecycle,
            shard,
            shared,
            inserted: false,
        }
    }

    // Check the state of the placeholder, returning the value if it was loaded
    // or a guard if the caller got the guard.
    #[inline]
    fn handle_notification(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        shared: SharedPlaceholder<Val>,
    ) -> Result<Val, PlaceholderGuard<'a, Key, Val, We, B, L>>
    where
        Val: Clone,
    {
        // Check if the value was loaded, and if it wasn't it means we got the
        // guard and need to start loading the value.
        if let Some(v) = shared.value.get() {
            Ok(v.clone())
        } else {
            Err(PlaceholderGuard::start_loading(lifecycle, shard, shared))
        }
    }

    // Join the waiters list or return the value if it was already loaded
    #[inline]
    fn join_waiters(
        // we require the shard lock to be held to add a new waiter
        _locked_shard: RwLockWriteGuard<'a, CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        shared: &SharedPlaceholder<Val>,
        // a function that returns a waiter if it should be added
        waiter_new: impl FnOnce() -> Option<Waiter>,
    ) -> Option<Val>
    where
        Val: Clone,
    {
        let mut state = shared.state.write();
        // _locked_shard could be released here, it would be sufficient to synchronize with the holder
        // of the guard trying to remove the placeholder from the cache. But if this placeholder is hot,
        // anyone waiting on the shard will immediately hit the state lock. Since the cache is sharded
        // we consider the latter more likely. So we keep the shard lock until we are done with the state.
        match state.loading {
            LoadingState::Loading => {
                if let Some(waiter) = waiter_new() {
                    state.waiters.push(waiter);
                }
                None
            }
            LoadingState::Inserted => unsafe {
                // SAFETY: The value is guaranteed to be set at this point
                drop(state); // Allow cloning outside the lock
                Some(shared.value.get().unwrap_unchecked().clone())
            },
        }
    }
}

impl<
        'a,
        Key: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
    > PlaceholderGuard<'a, Key, Val, We, B, L>
{
    pub fn join<Q>(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        hash: u64,
        key: &Q,
        mut timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Val, We, B, L>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let mut shard_guard = shard.write();
        let shared = match shard_guard.upsert_placeholder(hash, key) {
            Ok((_, v)) => return GuardResult::Value(v.clone()),
            Err((shared, true)) => {
                return GuardResult::Guard(Self::start_loading(lifecycle, shard, shared));
            }
            Err((shared, false)) => shared,
        };

        // Create notified flag on stack - this will live for the entire duration of join
        let notified = pin::pin!(AtomicBool::new(false));
        // Set if the thread was added to the waiters list
        let mut parked_thread = None;
        let maybe_val = Self::join_waiters(shard_guard, &shared, || {
            if timeout.is_some_and(|t| t.is_zero()) {
                None
            } else {
                let thread = thread::current();
                let id = thread.id();
                parked_thread = Some(id);
                Some(Waiter::Thread {
                    thread,
                    notified: &*notified as *const AtomicBool,
                })
            }
        });
        if let Some(v) = maybe_val {
            return GuardResult::Value(v);
        }

        // Track the start time of the timeout, set lazily
        let mut timeout_start = None;
        loop {
            if let Some(remaining) = timeout {
                if remaining.is_zero() {
                    return Self::join_timeout(lifecycle, shard, shared, parked_thread, &notified);
                }
                let start = *timeout_start.get_or_insert_with(Instant::now);
                #[cfg(not(fuzzing))]
                thread::park_timeout(remaining);
                timeout = Some(remaining.saturating_sub(start.elapsed()));
            } else {
                thread::park();
            }
            if notified.load(Ordering::Acquire) {
                return match Self::handle_notification(lifecycle, shard, shared) {
                    Ok(v) => GuardResult::Value(v),
                    Err(g) => GuardResult::Guard(g),
                };
            }
        }
    }

    #[cold]
    fn join_timeout(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, Arc<Placeholder<Val>>>>,
        shared: Arc<Placeholder<Val>>,
        // when timeout is zero, the thread may have not been added to the waiters list
        parked_thread: Option<thread::ThreadId>,
        notified: &AtomicBool,
    ) -> GuardResult<'a, Key, Val, We, B, L> {
        let mut state = shared.state.write();
        match state.loading {
            LoadingState::Loading if notified.load(Ordering::Acquire) => {
                drop(state); // Drop state guard to avoid a deadlock with start_loading
                GuardResult::Guard(PlaceholderGuard::start_loading(lifecycle, shard, shared))
            }
            LoadingState::Loading => {
                if parked_thread.is_some() {
                    // Remove ourselves from the waiters list
                    let waiter_idx = state
                        .waiters
                        .iter()
                        .position(|w| w.is_waiter(notified as _));
                    if let Some(idx) = waiter_idx {
                        state.waiters.swap_remove(idx);
                    } else {
                        unsafe { unreachable_unchecked() };
                    }
                }
                GuardResult::Timeout
            }
            LoadingState::Inserted => unsafe {
                // SAFETY: The value is guaranteed to be set at this point
                GuardResult::Value(shared.value.get().unwrap_unchecked().clone())
            },
        }
    }
}

impl<
        Key: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
    > PlaceholderGuard<'_, Key, Val, We, B, L>
{
    /// Inserts the value into the placeholder
    ///
    /// Returns Err if the placeholder isn't in the cache anymore.
    /// A placeholder can be removed as a result of a `remove` call
    /// or a non-placeholder `insert` with the same key.
    pub fn insert(self, value: Val) -> Result<(), Val> {
        let lifecycle = self.lifecycle;
        let lcs = self.insert_with_lifecycle(value)?;
        lifecycle.end_request(lcs);
        Ok(())
    }

    /// Inserts the value into the placeholder
    ///
    /// Returns Err if the placeholder isn't in the cache anymore.
    /// A placeholder can be removed as a result of a `remove` call
    /// or a non-placeholder `insert` with the same key.
    pub fn insert_with_lifecycle(mut self, value: Val) -> Result<L::RequestState, Val> {
        unsafe { self.shared.value.set(value.clone()).unwrap_unchecked() };
        let referenced;
        {
            // Whoever is already waiting will get notified and hit the fast-path
            // as they will see the value set. Anyone that races trying to add themselves
            // to the waiters list will wait on the state lock.
            let mut state = self.shared.state.write();
            state.loading = LoadingState::Inserted;
            referenced = !state.waiters.is_empty();
            for w in state.waiters.drain(..) {
                w.notify();
            }
        }

        // Set flag to disable drop_uninserted_slow, it has no work to do:
        //   - waiters have already been drained
        //   - no waiters can be added because we set LoadingState::Inserted
        //   - the placeholder will be removed here, if it still exists
        self.inserted = true;

        let mut lcs = self.lifecycle.begin_request();
        self.shard
            .write()
            .replace_placeholder(&mut lcs, &self.shared, referenced, value)?;
        Ok(lcs)
    }
}

impl<Key, Val, We, B, L> PlaceholderGuard<'_, Key, Val, We, B, L> {
    #[cold]
    fn drop_uninserted_slow(&mut self) {
        // Fast path: check if there are other waiters without the shard lock
        // This may or may not be common, but the assumption is that the shard lock is hot
        // and should be avoided if possible.
        {
            let mut state = self.shared.state.write();
            debug_assert!(matches!(state.loading, LoadingState::Loading));
            if let Some(waiter) = state.waiters.pop() {
                waiter.notify();
                return;
            }
        }

        // Slow path: acquire shard lock and re-check
        // By acquiring the shard lock we synchronize with any other threads that might be
        // trying to add themselves to the waiters list.
        let mut shard_guard = self.shard.write();
        let mut state = self.shared.state.write();
        debug_assert!(matches!(state.loading, LoadingState::Loading));
        if let Some(waiter) = state.waiters.pop() {
            drop(shard_guard);
            waiter.notify();
        } else {
            shard_guard.remove_placeholder(&self.shared);
        }
    }
}

impl<Key, Val, We, B, L> Drop for PlaceholderGuard<'_, Key, Val, We, B, L> {
    #[inline]
    fn drop(&mut self) {
        if !self.inserted {
            self.drop_uninserted_slow();
        }
    }
}
impl<Key, Val, We, B, L> std::fmt::Debug for PlaceholderGuard<'_, Key, Val, We, B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaceholderGuard").finish_non_exhaustive()
    }
}

/// Future that results in an Ok(Value) or Err(Guard)
pub struct JoinFuture<'a, 'b, Q: ?Sized, Key, Val, We, B, L> {
    lifecycle: &'a L,
    shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
    state: JoinFutureState<'b, Q, Val>,
    notified: AtomicBool,
}

enum JoinFutureState<'b, Q: ?Sized, Val> {
    Created {
        hash: u64,
        key: &'b Q,
    },
    Pending {
        shared: SharedPlaceholder<Val>,
        waker: task::Waker,
    },
    Done,
}

impl<'a, 'b, Q: ?Sized, Key, Val, We, B, L> JoinFuture<'a, 'b, Q, Key, Val, We, B, L> {
    pub fn new(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        hash: u64,
        key: &'b Q,
    ) -> JoinFuture<'a, 'b, Q, Key, Val, We, B, L> {
        Self {
            lifecycle,
            shard,
            state: JoinFutureState::Created { hash, key },
            notified: Default::default(),
        }
    }

    #[cold]
    fn drop_pending_waiter(&mut self) {
        let JoinFutureState::Pending { shared, .. } =
            mem::replace(&mut self.state, JoinFutureState::Done)
        else {
            unsafe { unreachable_unchecked() }
        };
        let mut state = shared.state.write();
        match state.loading {
            LoadingState::Loading if self.notified.load(Ordering::Acquire) => {
                // The write guard was abandoned elsewhere, this future was notified but didn't get polled.
                // So we get and drop the guard here to handle the side effects.
                drop(state); // Drop state guard to avoid a deadlock with start_loading
                let _ = PlaceholderGuard::start_loading(self.lifecycle, self.shard, shared);
            }
            LoadingState::Loading => {
                // Remove ourselves from the waiters list
                let waiter_idx = state
                    .waiters
                    .iter()
                    .position(|w| w.is_waiter(&self.notified as _));
                if let Some(idx) = waiter_idx {
                    state.waiters.swap_remove(idx);
                } else {
                    // We didn't find ourselves in the waiters list!?
                    unsafe { unreachable_unchecked() }
                }
            }
            LoadingState::Inserted => (), // We were notified but didn't get polled - nothing to do
        }
    }
}

impl<Q: ?Sized, Key, Val, We, B, L> Drop for JoinFuture<'_, '_, Q, Key, Val, We, B, L> {
    #[inline]
    fn drop(&mut self) {
        if matches!(self.state, JoinFutureState::Pending { .. }) {
            self.drop_pending_waiter();
        }
    }
}

impl<
        'a,
        Key: Eq + Hash,
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
        Val: Clone,
        We: Weighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
    > Future for JoinFuture<'a, '_, Q, Key, Val, We, B, L>
{
    type Output = Result<Val, PlaceholderGuard<'a, Key, Val, We, B, L>>;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let lifecycle = this.lifecycle;
        let shard = this.shard;
        match &mut this.state {
            JoinFutureState::Created { hash, key } => {
                debug_assert!(!this.notified.load(Ordering::Acquire));
                let mut shard_guard = shard.write();
                match shard_guard.upsert_placeholder(*hash, *key) {
                    Ok((_, v)) => {
                        this.state = JoinFutureState::Done;
                        Poll::Ready(Ok(v.clone()))
                    }
                    Err((shared, true)) => {
                        let guard = PlaceholderGuard::start_loading(lifecycle, shard, shared);
                        this.state = JoinFutureState::Done;
                        Poll::Ready(Err(guard))
                    }
                    Err((shared, false)) => {
                        let mut waker = None;
                        let maybe_val =
                            PlaceholderGuard::join_waiters(shard_guard, &shared, || {
                                let waker_ = cx.waker().clone();
                                waker = Some(waker_.clone());
                                Some(Waiter::Task {
                                    waker: waker_,
                                    notified: &this.notified as *const AtomicBool,
                                })
                            });
                        if let Some(v) = maybe_val {
                            debug_assert!(waker.is_none());
                            debug_assert!(!this.notified.load(Ordering::Acquire));
                            this.state = JoinFutureState::Done;
                            Poll::Ready(Ok(v))
                        } else {
                            let waker = waker.unwrap();
                            this.state = JoinFutureState::Pending { shared, waker };
                            Poll::Pending
                        }
                    }
                }
            }
            JoinFutureState::Pending { .. } if this.notified.load(Ordering::Acquire) => {
                let JoinFutureState::Pending { shared, .. } =
                    mem::replace(&mut this.state, JoinFutureState::Done)
                else {
                    unsafe { unreachable_unchecked() }
                };
                Poll::Ready(PlaceholderGuard::handle_notification(
                    lifecycle, shard, shared,
                ))
            }
            JoinFutureState::Pending { waker, shared } => {
                // Update waker in case it changed
                let new_waker = cx.waker();
                if !waker.will_wake(new_waker) {
                    let mut state = shared.state.write();
                    if let Some(w) = state
                        .waiters
                        .iter_mut()
                        .find(|w| w.is_waiter(&this.notified as _))
                    {
                        *waker = new_waker.clone();
                        *w = Waiter::Task {
                            waker: new_waker.clone(),
                            notified: &this.notified as *const AtomicBool,
                        };
                    } else {
                        unsafe { unreachable_unchecked() };
                    }
                }
                Poll::Pending
            }
            JoinFutureState::Done => panic!("Polled after ready"),
        }
    }
}
