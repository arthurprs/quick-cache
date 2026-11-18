use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    marker::PhantomPinned,
    mem,
    num::NonZeroU64,
    pin,
    task::{self, Poll},
    time::{Duration, Instant},
};

use crate::{
    shard::{CacheShard, PlaceholderId, PlaceholderLocation},
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

impl<Val> PlaceholderId<Val> for NonZeroU64 {
    type Shared = Arc<Placeholder<Val>>;
    type Context = u64;
    type WakeBatch = WakeBatch;

    fn new(context: &mut Self::Context) -> Self {
        *context = context
            .checked_add(1)
            .expect("placeholder identity exhausted");
        NonZeroU64::new(*context).unwrap()
    }

    fn invalidate(shared: &Self::Shared, wakes: &mut Self::WakeBatch) {
        let mut waiters = shared.waiters.write();
        if shared.value.get().is_none() {
            unsafe { shared.value.set(None).unwrap_unchecked() };
            waiters.iter().for_each(Waiter::mark_notified);
            if wakes.0.is_empty() {
                wakes.0 = mem::take(&mut *waiters);
            } else {
                wakes.0.append(&mut *waiters);
            }
        }
    }
}

#[derive(Default)]
pub struct WakeBatch(Vec<Waiter>);

impl WakeBatch {
    pub(crate) fn wake(self) {
        for waiter in self.0 {
            waiter.wake.wake();
        }
    }
}

#[derive(Debug)]
pub struct Placeholder<Val> {
    location: PlaceholderLocation<NonZeroU64>,
    // Registration holds the shard lock. Terminal writers set the outcome and
    // mark and detach waiters while holding this lock.
    waiters: RwLock<Vec<Waiter>>,
    // Unset: loading or handoff. None: retry. Some(value): published.
    value: OnceLock<Option<Val>>,
}

impl<Val> Placeholder<Val> {
    /// Returns the filled value, if any.
    #[inline]
    pub(crate) fn value(&self) -> Option<&Val> {
        self.value.get().and_then(Option::as_ref)
    }
}

pub struct PlaceholderGuard<'a, Key, Val, We, B, L> {
    shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
    location: Option<PlaceholderLocation<NonZeroU64>>,
}

#[cfg(test)]
impl<'a, Key, Val, We, B, L> PlaceholderGuard<'a, Key, Val, We, B, L> {
    pub fn location(&self) -> &PlaceholderLocation<NonZeroU64> {
        self.location.as_ref().unwrap()
    }
}

#[derive(Debug)]
struct Waiter {
    notified: *const AtomicBool,
    wake: Wake,
}

// SAFETY: Cancellation unregisters under the waiter lock. Detached records
// access only their owned wake handles after notification flags are marked.
unsafe impl Send for Waiter {}

unsafe impl Sync for Waiter {}

#[derive(Debug)]
enum Wake {
    Thread(thread::Thread),
    Task(task::Waker),
}

impl Wake {
    fn wake(self) {
        match self {
            Self::Thread(thread) => thread.unpark(),
            Self::Task(waker) => waker.wake(),
        }
    }
}

impl Waiter {
    fn mark_notified(&self) {
        // SAFETY: Cancellation removes unnotified waiters under the waiter lock.
        unsafe { (*self.notified).store(true, Ordering::Release) };
    }
}

fn remove_waiter(waiters: &mut Vec<Waiter>, notified: *const AtomicBool) -> Waiter {
    // An unnotified waiter stays registered until it removes itself under this lock.
    let idx = unsafe {
        waiters
            .iter()
            .position(|w| std::ptr::eq(w.notified, notified))
            .unwrap_unchecked()
    };
    waiters.swap_remove(idx)
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
pub use crate::shard::EntryAction;

/// Result of waiting for a placeholder or [`JoinFuture`].
pub(crate) enum JoinResult<'a, Key, Val, We, B, L> {
    /// Retry cache lookup (`None`) or use a published shared value (`Some`).
    Filled(Option<Arc<Placeholder<Val>>>),
    /// Got the guard — caller should load the value.
    Guard(PlaceholderGuard<'a, Key, Val, We, B, L>),
    /// Timed out waiting (sync paths only).
    Timeout,
}

/// Result of an [`entry`](crate::sync::Cache::entry) or
/// [`entry_async`](crate::sync::Cache::entry_async) operation.
#[derive(Debug)]
pub enum EntryResult<'a, Key, Val, We, B, L, T> {
    /// The key existed and the callback returned [`EntryAction::Retain`].
    /// Contains the value `T` returned by the callback.
    Retained(T),
    /// The key existed and the callback returned [`EntryAction::Remove`].
    /// Contains the removed key and value.
    Removed(Key, Val),
    /// The key existed and the callback returned [`EntryAction::ReplaceWithGuard`].
    /// Contains a [`PlaceholderGuard`] for re-insertion and the old value.
    Replaced(PlaceholderGuard<'a, Key, Val, We, B, L>, Val),
    /// The key was absent. Contains a [`PlaceholderGuard`] for inserting a new value.
    Vacant(PlaceholderGuard<'a, Key, Val, We, B, L>),
    /// Timed out waiting for another loader's placeholder.
    ///
    /// Only returned by [`Cache::entry`](crate::sync::Cache::entry),
    /// which accepts a `timeout` parameter. For the async variant, use an external
    /// timeout mechanism (e.g. `tokio::time::timeout`).
    Timeout,
}

impl<'a, Key, Val, We, B, L> PlaceholderGuard<'a, Key, Val, We, B, L> {
    #[inline]
    pub fn start_loading(
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shared: PlaceholderLocation<NonZeroU64>,
    ) -> Self {
        PlaceholderGuard {
            shard,
            location: Some(shared),
        }
    }

    // Check the state of the placeholder, returning the value if it was loaded
    // or a guard if the caller got the guard.
    #[inline]
    fn handle_notification(
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shared: Arc<Placeholder<Val>>,
    ) -> JoinResult<'a, Key, Val, We, B, L> {
        match shared.value.get() {
            None => JoinResult::Guard(Self::start_loading(shard, shared.location)),
            Some(Some(_)) => JoinResult::Filled(Some(shared)),
            Some(None) => JoinResult::Filled(None),
        }
    }

    // The shard lock keeps the placeholder live while the waiter registers.
    #[inline]
    fn join_waiters(
        mut locked_shard: RwLockWriteGuard<'a, CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shared: &PlaceholderLocation<NonZeroU64>,
        waiter: Waiter,
    ) -> Arc<Placeholder<Val>> {
        let shared = locked_shard
            .share_placeholder(shared, || {
                Arc::new(Placeholder {
                    location: *shared,
                    value: OnceLock::new(),
                    waiters: RwLock::new(Vec::new()),
                })
            })
            .clone();
        debug_assert!(shared.value.get().is_none());
        shared.waiters.write().push(waiter);
        shared
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
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        hash: u64,
        key: &Q,
        timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Val, We, B, L>
    where
        Q: Hash + Equivalent<Key> + ToOwned<Owned = Key> + ?Sized,
    {
        let mut deadline = timeout.map(Ok);
        loop {
            let mut shard_guard = shard.write();
            let location = match shard_guard.get_or_placeholder(hash, key) {
                Ok((_, value)) => return GuardResult::Value(value.clone()),
                Err((location, true)) => {
                    return GuardResult::Guard(Self::start_loading(shard, location))
                }
                Err((location, false)) => location,
            };
            match Self::wait_for_placeholder(shard, shard_guard, location, deadline.as_mut()) {
                JoinResult::Filled(Some(shared)) => {
                    return GuardResult::Value(shared.value().unwrap().clone())
                }
                JoinResult::Filled(None) => continue,
                JoinResult::Guard(guard) => return GuardResult::Guard(guard),
                JoinResult::Timeout => return GuardResult::Timeout,
            }
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
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shard_guard: RwLockWriteGuard<'a, CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shared: PlaceholderLocation<NonZeroU64>,
        deadline: Option<&mut Result<Duration, Instant>>,
    ) -> JoinResult<'a, Key, Val, We, B, L> {
        if matches!(deadline.as_deref(), Some(Ok(d)) if d.is_zero()) {
            return JoinResult::Timeout;
        }
        let notified = pin::pin!(AtomicBool::new(false));
        let thread = thread::current();
        let shared = Self::join_waiters(
            shard_guard,
            &shared,
            Waiter {
                notified: &*notified as *const AtomicBool,
                wake: Wake::Thread(thread),
            },
        );
        // Lazily convert the duration to a deadline on first call;
        // subsequent retries from entry() reuse the same deadline.
        let deadline = deadline.and_then(|d| match *d {
            Ok(dur) => match Instant::now().checked_add(dur) {
                Some(instant) => {
                    *d = Err(instant);
                    Some(instant)
                }
                None => None, // overflow → treat as no timeout (wait forever)
            },
            Err(instant) => Some(instant),
        });
        loop {
            if let Some(instant) = deadline {
                let remaining = instant.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Self::join_timeout(shard, shared, &notified);
                }
                #[cfg(not(fuzzing))]
                thread::park_timeout(remaining);
            } else {
                #[cfg(not(fuzzing))]
                thread::park();
            }
            if notified.load(Ordering::Acquire) {
                return Self::handle_notification(shard, shared);
            }
        }
    }

    #[cold]
    fn join_timeout(
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        shared: Arc<Placeholder<Val>>,
        notified: &AtomicBool,
    ) -> JoinResult<'a, Key, Val, We, B, L> {
        let mut waiters = shared.waiters.write();
        if !notified.load(Ordering::Acquire) {
            remove_waiter(&mut waiters, notified);
            return JoinResult::Timeout;
        }
        drop(waiters);
        Self::handle_notification(shard, shared)
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
        let mut lcs = Default::default();
        self.insert_with_lifecycle(value, &mut lcs)
    }

    /// Inserts the value into the placeholder, recording any evicted items into the given
    /// lifecycle request state.
    ///
    /// Returns Err if the placeholder isn't in the cache anymore.
    /// A placeholder can be removed as a result of a `remove` call
    /// or a non-placeholder `insert` with the same key.
    ///
    /// `lcs` is a [`Lifecycle::RequestState`]; construct one with `Default::default()`.
    /// Evicted items are released when `lcs` is dropped.
    pub fn insert_with_lifecycle(
        mut self,
        value: Val,
        lcs: &mut L::RequestState,
    ) -> Result<(), Val> {
        let location = self.location.unwrap();
        let mut shard = self.shard.write();
        let Some(shared) = shard.shared_placeholder(&location).cloned() else {
            self.location = None;
            return shard.replace_placeholder(lcs, &location, false, value);
        };
        // The shard lock serializes cloning and publication with invalidation.
        let shared_value = value.clone();
        let waiters = {
            let mut waiters = shared.waiters.write();
            assert!(shared.value.set(Some(shared_value)).is_ok());
            self.location = None;
            // A wake can panic or cancel another waiter after this lock is released.
            waiters.iter().for_each(Waiter::mark_notified);
            mem::take(&mut *waiters)
        };
        let result = shard.replace_placeholder(lcs, &location, !waiters.is_empty(), value);
        drop(shard);
        for waiter in waiters {
            waiter.wake.wake();
        }
        result
    }
}

impl<Key, Val, We, B, L> PlaceholderGuard<'_, Key, Val, We, B, L> {
    #[cold]
    fn drop_uninserted_slow(&mut self, location: PlaceholderLocation<NonZeroU64>) {
        let mut shard_guard = self.shard.write();
        let wake = shard_guard
            .shared_placeholder(&location)
            .and_then(|shared| {
                let mut waiters = shared.waiters.write();
                debug_assert!(shared.value.get().is_none());
                waiters.pop().map(|waiter| {
                    waiter.mark_notified();
                    waiter.wake
                })
            });
        if let Some(wake) = wake {
            drop(shard_guard);
            wake.wake();
        } else {
            shard_guard.remove_placeholder(&location);
        }
    }
}

impl<Key, Val, We, B, L> Drop for PlaceholderGuard<'_, Key, Val, We, B, L> {
    #[inline]
    fn drop(&mut self) {
        if let Some(location) = self.location {
            self.drop_uninserted_slow(location);
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
    shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
    hash: u64,
    key: &'b Q,
    state: JoinFutureState<Val>,
    notified: AtomicBool,
    _pin: PhantomPinned,
}

enum JoinFutureState<Val> {
    Created,
    Pending {
        shared: Arc<Placeholder<Val>>,
        waker: task::Waker,
    },
    Done,
}

impl<'a, 'b, Q: ?Sized, Key, Val, We, B, L> JoinFuture<'a, 'b, Q, Key, Val, We, B, L> {
    pub(crate) fn new(
        shard: &'a RwLock<CacheShard<Key, Val, We, B, L, NonZeroU64>>,
        hash: u64,
        key: &'b Q,
    ) -> Self {
        Self {
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
        // Keep the cached waker alive until the notification pointer is unregistered.
        let JoinFutureState::Pending {
            shared,
            waker: _waker,
        } = mem::replace(&mut self.state, JoinFutureState::Done)
        else {
            unsafe { unreachable_unchecked() }
        };
        let mut waiters = shared.waiters.write();
        if shared.value.get().is_none() {
            if self.notified.load(Ordering::Acquire) {
                // The selected waiter passes abandoned ownership onward when cancelled.
                drop(waiters);
                let _ = PlaceholderGuard::start_loading(self.shard, shared.location);
            } else {
                let waiter = remove_waiter(&mut waiters, &self.notified);
                drop(waiters);
                drop(waiter);
            }
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
                            shard, shared,
                        )))
                    }
                    Err((shared, false)) => {
                        // Register as waiter while holding shard lock — prevents
                        // race with drop_uninserted_slow removing the placeholder.
                        let waker = cx.waker().clone();
                        let shared = PlaceholderGuard::join_waiters(
                            shard_guard,
                            &shared,
                            Waiter {
                                notified: &this.notified as *const AtomicBool,
                                wake: Wake::Task(waker.clone()),
                            },
                        );
                        this.state = JoinFutureState::Pending { shared, waker };
                        Poll::Pending
                    }
                }
            }
            JoinFutureState::Pending { waker, shared } => {
                if !this.notified.load(Ordering::Acquire) {
                    let new_waker = cx.waker();
                    if waker.will_wake(new_waker) {
                        return Poll::Pending;
                    }
                    // Waker clones and drops can call user code.
                    let next_waker = new_waker.clone();
                    let next_wake = Wake::Task(next_waker.clone());
                    let mut waiters = shared.waiters.write();
                    // Re-check after acquiring the lock — a concurrent insert
                    // may have drained the waiters list in the meantime.
                    if !this.notified.load(Ordering::Acquire) {
                        let w = unsafe {
                            waiters
                                .iter_mut()
                                .find(|w| std::ptr::eq(w.notified, &this.notified))
                                .unwrap_unchecked()
                        };
                        let previous_wake = mem::replace(&mut w.wake, next_wake);
                        drop(waiters);
                        *waker = next_waker;
                        drop(previous_wake);
                        return Poll::Pending;
                    }
                }
                let JoinFutureState::Pending { shared, .. } =
                    mem::replace(&mut this.state, JoinFutureState::Done)
                else {
                    unsafe { unreachable_unchecked() }
                };
                Poll::Ready(PlaceholderGuard::handle_notification(shard, shared))
            }
            JoinFutureState::Done => panic!("Polled after ready"),
        }
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
mod lazy_tests {
    use super::*;
    use crate::{sync::DefaultLifecycle, UnitWeighter};
    use std::{
        collections::hash_map::DefaultHasher,
        hash::BuildHasherDefault,
        panic::{catch_unwind, AssertUnwindSafe},
        sync::atomic::AtomicUsize,
        task::{Context, Wake, Waker},
    };

    type TestHasher = BuildHasherDefault<DefaultHasher>;
    type TestShard<V> =
        RwLock<CacheShard<u64, V, UnitWeighter, TestHasher, DefaultLifecycle<u64, V>, NonZeroU64>>;

    fn hash(key: u64) -> u64 {
        TestHasher::default().hash_one(key)
    }

    fn shard<V>() -> TestShard<V> {
        RwLock::new(CacheShard::new(
            0.9,
            0.5,
            1024,
            1024,
            UnitWeighter,
            TestHasher::default(),
            DefaultLifecycle::default(),
        ))
    }

    struct Noop;

    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    fn clear<V>(shard: &TestShard<V>) {
        let mut wakes = WakeBatch::default();
        shard.write().clear(&mut wakes);
        wakes.wake();
    }

    #[test]
    fn promotion_survives_cancel_rejoin_and_handoff() {
        let shard = shard::<u64>();
        let GuardResult::Guard(owner) = PlaceholderGuard::join(&shard, hash(0), &0, None) else {
            panic!()
        };
        let location = owner.location.unwrap();
        assert!(shard.read().shared_placeholder(&location).is_none());
        drop(JoinFuture::new(&shard, hash(0), &0));
        assert!(matches!(
            PlaceholderGuard::join(&shard, hash(0), &0, Some(Duration::ZERO)),
            GuardResult::Timeout
        ));
        assert!(shard.read().shared_placeholder(&location).is_none());
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let mut first = Box::pin(JoinFuture::new(&shard, hash(0), &0));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        let shared = shard.read().shared_placeholder(&location).unwrap().clone();
        drop(first);
        let mut second = pin::pin!(JoinFuture::new(&shard, hash(0), &0));
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(Arc::ptr_eq(
            &shared,
            shard.read().shared_placeholder(&location).unwrap()
        ));
        let mut third = Box::pin(JoinFuture::new(&shard, hash(0), &0));
        assert!(third.as_mut().poll(&mut cx).is_pending());
        drop(owner);
        drop(third);
        let Poll::Ready(JoinResult::Guard(successor)) = second.as_mut().poll(&mut cx) else {
            panic!()
        };
        successor.insert(8).unwrap();
        assert!(shard.read().shared_placeholder(&location).is_none());
    }

    struct CheckWakerDrop {
        shared: Arc<Placeholder<u64>>,
        lock_available: Arc<AtomicBool>,
    }

    impl Wake for CheckWakerDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for CheckWakerDrop {
        fn drop(&mut self) {
            self.lock_available
                .store(self.shared.waiters.try_write().is_some(), Ordering::Relaxed);
        }
    }

    #[test]
    fn cancellation_and_repoll_drop_wakers_after_unlocking() {
        let lock_available = [true, false].map(|cancel| {
            let shard = shard::<u64>();
            let GuardResult::Guard(owner) = PlaceholderGuard::join(&shard, hash(0), &0, None)
            else {
                panic!()
            };
            let mut waiter = Box::pin(JoinFuture::new(&shard, hash(0), &0));
            let mut cx = Context::from_waker(Waker::noop());
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
            let shared = shard
                .read()
                .shared_placeholder(owner.location())
                .unwrap()
                .clone();
            let lock_available = Arc::new(AtomicBool::new(false));
            let waker = Waker::from(Arc::new(CheckWakerDrop {
                shared,
                lock_available: lock_available.clone(),
            }));
            assert!(waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(waker);
            if cancel {
                drop(waiter);
            } else {
                assert!(waiter.as_mut().poll(&mut cx).is_pending());
            }
            assert!(owner.insert(8).is_ok());
            lock_available.load(Ordering::Relaxed)
        });
        assert_eq!(lock_available, [true, true], "cancellation and repoll");
    }

    #[derive(Debug)]
    struct PublicationProbe {
        shard: std::sync::Weak<TestShard<PublicationProbe>>,
        clones: Arc<AtomicUsize>,
        wakes: Arc<AtomicUsize>,
    }

    impl Clone for PublicationProbe {
        fn clone(&self) -> Self {
            let shard = self.shard.upgrade().unwrap();
            assert!(
                shard.try_write().is_none(),
                "publication clone must serialize with invalidation"
            );
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                shard: self.shard.clone(),
                clones: self.clones.clone(),
                wakes: self.wakes.clone(),
            }
        }
    }

    impl Wake for PublicationProbe {
        fn wake(self: Arc<Self>) {
            let shard = self.shard.upgrade().unwrap();
            assert!(
                shard.try_write().is_some(),
                "publication wake holds the shard lock"
            );
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn publication_serializes_clone_then_wakes_after_unlock() {
        let shard = Arc::new(shard::<PublicationProbe>());
        let clones = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let probe = || PublicationProbe {
            shard: Arc::downgrade(&shard),
            clones: clones.clone(),
            wakes: wakes.clone(),
        };
        let GuardResult::Guard(owner) = PlaceholderGuard::join(&shard, hash(0), &0, None) else {
            panic!()
        };
        let waker = Waker::from(Arc::new(probe()));
        let mut cx = Context::from_waker(&waker);
        let mut waiter = pin::pin!(JoinFuture::new(&shard, hash(0), &0));
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
        owner.insert(probe()).unwrap();
        assert_eq!(clones.load(Ordering::SeqCst), 1);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        clear(&shard);
        assert!(matches!(
            waiter.as_mut().poll(&mut cx),
            Poll::Ready(JoinResult::Filled(Some(_)))
        ));
    }

    struct CheckWake {
        shard: Arc<TestShard<u64>>,
        shared: Arc<Placeholder<u64>>,
        calls: Arc<AtomicUsize>,
        panic: bool,
    }

    impl Wake for CheckWake {
        fn wake(self: Arc<Self>) {
            assert!(
                self.shard.try_write().is_some(),
                "invalidation holds shard lock"
            );
            assert!(
                self.shared.waiters.try_write().is_some(),
                "invalidation holds waiter lock"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "invalidation wake");
        }
    }

    #[test]
    fn invalidation_marks_waiters_before_panicking_wake() {
        let shard = Arc::new(shard::<u64>());
        let GuardResult::Guard(owner) = PlaceholderGuard::join(&shard, hash(0), &0, None) else {
            panic!()
        };
        let location = owner.location.unwrap();
        let noop = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&noop);
        let mut first = pin::pin!(JoinFuture::new(&shard, hash(0), &0));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        let shared = shard.read().shared_placeholder(&location).unwrap().clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let bad = Waker::from(Arc::new(CheckWake {
            shard: shard.clone(),
            shared: shared.clone(),
            calls: calls.clone(),
            panic: true,
        }));
        assert!(first
            .as_mut()
            .poll(&mut Context::from_waker(&bad))
            .is_pending());
        let good = Waker::from(Arc::new(CheckWake {
            shard: shard.clone(),
            shared: shared.clone(),
            calls: calls.clone(),
            panic: false,
        }));
        let mut second = pin::pin!(JoinFuture::new(&shard, hash(0), &0));
        assert!(second
            .as_mut()
            .poll(&mut Context::from_waker(&good))
            .is_pending());
        let error = catch_unwind(AssertUnwindSafe(|| clear(&shard))).unwrap_err();
        assert_eq!(error.downcast_ref::<&str>(), Some(&"invalidation wake"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            first.as_mut().poll(&mut cx),
            Poll::Ready(JoinResult::Filled(None))
        ));
        assert!(matches!(
            second.as_mut().poll(&mut cx),
            Poll::Ready(JoinResult::Filled(None))
        ));
        assert_eq!(owner.insert(7), Err(7));
    }

    fn wait_for_registration(shard: &TestShard<u64>, location: PlaceholderLocation<NonZeroU64>) {
        for _ in 0..10_000 {
            if shard
                .read()
                .shared_placeholder(&location)
                .is_some_and(|shared| !shared.waiters.read().is_empty())
            {
                return;
            }
            std::thread::yield_now();
        }
        panic!("waiter did not register");
    }

    #[test]
    fn synchronous_waiter_retries_then_accepts_handoff() {
        let shard = shard::<u64>();
        let GuardResult::Guard(old) = PlaceholderGuard::join(&shard, hash(0), &0, None) else {
            panic!()
        };
        let old_location = old.location.unwrap();
        std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                let GuardResult::Guard(owner) =
                    PlaceholderGuard::join(&shard, hash(0), &0, Some(Duration::from_secs(10)))
                else {
                    panic!("waiter receives retry handoff")
                };
                owner.insert(9).unwrap();
            });
            wait_for_registration(&shard, old_location);
            let next = {
                let mut wakes = WakeBatch::default();
                let mut locked = shard.write();
                locked.remove(hash(0), &0, &mut wakes);
                let (location, is_new) = locked.get_or_placeholder(hash(0), &0).unwrap_err();
                assert!(is_new);
                let next = PlaceholderGuard::start_loading(&shard, location);
                drop(locked);
                wakes.wake();
                next
            };
            wait_for_registration(&shard, next.location.unwrap());
            assert_eq!(old.insert(7), Err(7));
            drop(next);
            waiter.join().unwrap();
        });
        assert_eq!(shard.read().peek(hash(0), &0), Some(&9));
    }
}
