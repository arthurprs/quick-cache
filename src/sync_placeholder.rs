use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    marker::PhantomPinned,
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

impl<Val> Placeholder<Val> {
    /// Returns the filled value, if any.
    #[inline]
    pub(crate) fn value(&self) -> Option<&Val> {
        self.value.get()
    }
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

/// Result of [`Cache::get_value_or_guard`](crate::sync::Cache::get_value_or_guard).
///
/// See also [`Cache::get_value_or_guard_async`](crate::sync::Cache::get_value_or_guard_async)
/// which returns `Result<Val, PlaceholderGuard>` instead.
#[derive(Debug)]
pub enum GuardResult<'a, Key, Val, We, B, L> {
    /// The value was found in the cache.
    Value(Val),
    /// The key was absent; use the guard to insert a value.
    Guard(PlaceholderGuard<'a, Key, Val, We, B, L>),
    /// Timed out waiting for another loader's placeholder.
    Timeout,
}

// Re-export from shard where it's defined.
pub use crate::shard::OccupiedAction;

/// Result of waiting for a placeholder or [`JoinFuture`].
pub(crate) enum JoinResult<'a, Key, Val, We, B, L> {
    /// Value is available — either found directly in the cache (`None`) or
    /// inside the shared placeholder (`Some`).
    Filled(Option<SharedPlaceholder<Val>>),
    /// Got the guard — caller should load the value.
    Guard(PlaceholderGuard<'a, Key, Val, We, B, L>),
    /// Timed out waiting (sync paths only).
    Timeout,
}

/// Result of an entry operation.
///
/// See [`Cache::entry`](crate::sync::Cache::entry) and
/// [`Cache::entry_async`](crate::sync::Cache::entry_async).
pub enum EntryResult<'a, Key, Val, We, B, L, T> {
    /// Callback returned [`OccupiedAction::Keep`].
    Value(T),
    /// Callback returned [`OccupiedAction::Remove`].
    Removed(Key, Val),
    /// Key was vacant, or callback returned [`OccupiedAction::Replace`].
    Guard(PlaceholderGuard<'a, Key, Val, We, B, L>),
    /// Timed out waiting for another loader's placeholder.
    ///
    /// Only returned by [`Cache::entry`](crate::sync::Cache::entry)
    /// which accepts a `timeout` parameter. For the async variant, use an external timeout
    /// mechanism (e.g. `tokio::time::timeout`).
    Timeout,
}

impl<'a, Key, Val, We, B, L> PlaceholderGuard<'a, Key, Val, We, B, L> {
    #[inline]
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
    ) -> Result<SharedPlaceholder<Val>, PlaceholderGuard<'a, Key, Val, We, B, L>> {
        // Check if the value was loaded, and if it wasn't it means we got the
        // guard and need to start loading the value.
        if shared.value().is_some() {
            Ok(shared)
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
    ) -> bool {
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
                false
            }
            LoadingState::Inserted => true,
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
        timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Val, We, B, L>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let mut shard_guard = shard.write();
        let shared = match shard_guard.get_or_placeholder(hash, key) {
            Ok((_, v)) => return GuardResult::Value(v.clone()),
            Err((shared, true)) => {
                return GuardResult::Guard(Self::start_loading(lifecycle, shard, shared));
            }
            Err((shared, false)) => shared,
        };
        let mut deadline = timeout.map(Ok);
        match Self::wait_for_placeholder(lifecycle, shard, shard_guard, shared, deadline.as_mut()) {
            JoinResult::Filled(shared) => unsafe {
                // SAFETY: Filled means the value was set by the loader.
                GuardResult::Value(shared.unwrap_unchecked().value().unwrap_unchecked().clone())
            },
            JoinResult::Guard(g) => GuardResult::Guard(g),
            JoinResult::Timeout => GuardResult::Timeout,
        }
    }

    /// Waits for an existing placeholder to be filled by another thread.
    ///
    /// Registers the current thread as a waiter (consuming the shard guard to avoid
    /// races with placeholder removal), then parks until notified or timeout.
    ///
    /// `deadline` is `None` for no timeout, or `Some(&mut Ok(duration))` on the first
    /// call. On first use the duration is converted in-place to `Err(instant)` so that
    /// callers that retry (e.g. `entry`) preserve the original deadline across calls.
    pub(crate) fn wait_for_placeholder(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        shard_guard: RwLockWriteGuard<'a, CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        shared: SharedPlaceholder<Val>,
        deadline: Option<&mut Result<Duration, Instant>>,
    ) -> JoinResult<'a, Key, Val, We, B, L> {
        let notified = pin::pin!(AtomicBool::new(false));
        let mut parked_thread = None;
        let already_filled = Self::join_waiters(shard_guard, &shared, || {
            // Skip registering a waiter if the timeout is zero.
            // An already-elapsed Err(instant) deadline is not checked here;
            // the loop below handles it and join_timeout cleans up the waiter.
            if matches!(deadline.as_deref(), Some(Ok(d)) if d.is_zero()) {
                None
            } else {
                let thread = thread::current();
                parked_thread = Some(thread.id());
                Some(Waiter::Thread {
                    thread,
                    notified: &*notified as *const AtomicBool,
                })
            }
        });
        if already_filled {
            return JoinResult::Filled(Some(shared));
        }

        // Lazily convert the duration to a deadline on first call;
        // subsequent retries from entry() reuse the same deadline.
        let deadline = deadline.map(|d| match *d {
            Ok(dur) => {
                let instant = Instant::now() + dur;
                *d = Err(instant);
                instant
            }
            Err(instant) => instant,
        });
        loop {
            if let Some(instant) = deadline {
                let remaining = instant.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Self::join_timeout(lifecycle, shard, shared, parked_thread, &notified);
                }
                #[cfg(not(fuzzing))]
                thread::park_timeout(remaining);
            } else {
                #[cfg(not(fuzzing))]
                thread::park();
            }
            if notified.load(Ordering::Acquire) {
                return match Self::handle_notification(lifecycle, shard, shared) {
                    Ok(shared) => JoinResult::Filled(Some(shared)),
                    Err(g) => JoinResult::Guard(g),
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
    ) -> JoinResult<'a, Key, Val, We, B, L> {
        let mut state = shared.state.write();
        match state.loading {
            LoadingState::Loading if notified.load(Ordering::Acquire) => {
                drop(state); // Drop state guard to avoid a deadlock with start_loading
                JoinResult::Guard(PlaceholderGuard::start_loading(lifecycle, shard, shared))
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
                JoinResult::Timeout
            }
            LoadingState::Inserted => {
                drop(state);
                JoinResult::Filled(Some(shared))
            }
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

/// Future that checks for an existing placeholder and waits for it to be filled.
///
/// The shard lock is acquired as a local variable inside `poll`, never stored
/// in the future state, so the future remains `Send`.
///
/// # Pin safety
///
/// This future is `!Unpin` because `poll` registers `&self.notified` as a raw
/// pointer in the placeholder's waiter list. `Pin` guarantees the future won't
/// be moved after the first poll, keeping that pointer valid. The pointer is
/// cleaned up in `drop_pending_waiter` before the struct is destroyed.
pub(crate) struct JoinFuture<'a, 'b, Q: ?Sized, Key, Val, We, B, L> {
    lifecycle: &'a L,
    shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
    hash: u64,
    key: &'b Q,
    state: JoinFutureState<Val>,
    notified: AtomicBool,
    _pin: PhantomPinned,
}

enum JoinFutureState<Val> {
    Created,
    Pending {
        shared: SharedPlaceholder<Val>,
        waker: task::Waker,
    },
    Done,
}

impl<'a, 'b, Q: ?Sized, Key, Val, We, B, L> JoinFuture<'a, 'b, Q, Key, Val, We, B, L> {
    pub(crate) fn new(
        lifecycle: &'a L,
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, SharedPlaceholder<Val>>>,
        hash: u64,
        key: &'b Q,
    ) -> Self {
        Self {
            lifecycle,
            shard,
            hash,
            key,
            state: JoinFutureState::Created,
            notified: Default::default(),
            _pin: PhantomPinned,
        }
    }
}

impl<Q: ?Sized, Key, Val, We, B, L> JoinFuture<'_, '_, Q, Key, Val, We, B, L> {
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
            LoadingState::Inserted => (), // Notified but didn't get polled - nothing to do
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
        Val,
        We: Weighter<Key, Val>,
        B: BuildHasher,
        L: Lifecycle<Key, Val>,
    > Future for JoinFuture<'a, '_, Q, Key, Val, We, B, L>
{
    type Output = JoinResult<'a, Key, Val, We, B, L>;

    fn poll(self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We never move the struct out of the Pin — only read/write individual
        // fields. The `notified` field's address (registered in the waiter list) stays
        // stable because Pin guarantees the future won't be moved.
        let this = unsafe { self.get_unchecked_mut() };
        let lifecycle = this.lifecycle;
        let shard = this.shard;
        match &mut this.state {
            JoinFutureState::Created => {
                let mut shard_guard = shard.write();
                match shard_guard.get_or_placeholder(this.hash, this.key) {
                    Ok(_) => {
                        this.state = JoinFutureState::Done;
                        Poll::Ready(JoinResult::Filled(None))
                    }
                    Err((shared, true)) => {
                        this.state = JoinFutureState::Done;
                        drop(shard_guard);
                        Poll::Ready(JoinResult::Guard(PlaceholderGuard::start_loading(
                            lifecycle, shard, shared,
                        )))
                    }
                    Err((shared, false)) => {
                        // Register as waiter while holding shard lock — prevents
                        // race with drop_uninserted_slow removing the placeholder.
                        let mut waker = None;
                        let already_filled =
                            PlaceholderGuard::join_waiters(shard_guard, &shared, || {
                                let waker_ = cx.waker().clone();
                                waker = Some(waker_.clone());
                                Some(Waiter::Task {
                                    waker: waker_,
                                    notified: &this.notified as *const AtomicBool,
                                })
                            });
                        if already_filled {
                            this.state = JoinFutureState::Done;
                            Poll::Ready(JoinResult::Filled(Some(shared)))
                        } else {
                            this.state = JoinFutureState::Pending {
                                shared,
                                waker: waker.unwrap(),
                            };
                            Poll::Pending
                        }
                    }
                }
            }
            JoinFutureState::Pending { waker, shared } => {
                if !this.notified.load(Ordering::Acquire) {
                    let new_waker = cx.waker();
                    if waker.will_wake(new_waker) {
                        return Poll::Pending;
                    }
                    let mut state = shared.state.write();
                    // Re-check after acquiring the lock — a concurrent insert
                    // may have drained the waiters list in the meantime.
                    if !this.notified.load(Ordering::Acquire) {
                        let w = unsafe {
                            state
                                .waiters
                                .iter_mut()
                                .find(|w| w.is_waiter(&this.notified as _))
                                .unwrap_unchecked()
                        };
                        *waker = new_waker.clone();
                        *w = Waiter::Task {
                            waker: new_waker.clone(),
                            notified: &this.notified as *const AtomicBool,
                        };
                        return Poll::Pending;
                    }
                }
                let JoinFutureState::Pending { shared, .. } =
                    mem::replace(&mut this.state, JoinFutureState::Done)
                else {
                    unsafe { unreachable_unchecked() }
                };
                Poll::Ready(
                    match PlaceholderGuard::handle_notification(lifecycle, shard, shared) {
                        Ok(shared) => JoinResult::Filled(Some(shared)),
                        Err(g) => JoinResult::Guard(g),
                    },
                )
            }
            JoinFutureState::Done => panic!("Polled after ready"),
        }
    }
}
