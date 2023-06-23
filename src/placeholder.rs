use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
    task::Poll,
    thread,
    time::{Duration, Instant},
};

use crate::{
    linked_slab::Token,
    rw_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    shard::KQCacheShard,
    Weighter,
};
use crate::shard::Entry;

pub type SharedPlaceholder<Val> = Arc<Placeholder<Val>>;

pub fn new_shared_placeholder<Val>(hash: u64, idx: Token) -> SharedPlaceholder<Val> {
    Arc::new(Placeholder {
        hash,
        idx,
        state: RwLock::new(State {
            waiters: Default::default(),
            loading: LoadingState::Loading,
        }),
    })
}

#[derive(Debug)]
pub struct Placeholder<Val> {
    pub hash: u64,
    pub idx: Token,
    pub state: RwLock<State<Val>>,
}

#[derive(Debug)]
pub struct State<Val> {
    /// The waiters list
    /// Adding to the list requires holding the outer shard lock to avoid races between
    /// removing the orphan placeholder from the cache and adding a new waiter to it.
    waiters: Vec<Waiter>,
    loading: LoadingState<Val>,
}

#[derive(Debug)]
enum LoadingState<Val> {
    /// A guard was/will be created and the value might get filled
    Loading,
    /// A value was filled, no more waiters can be added
    Inserted(Val),
    /// The placeholder was abandoned w/o any waiters and was removed from the map
    Terminated,
}

pub struct PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
    shared: SharedPlaceholder<Val>,
    inserted: bool,
}

#[derive(Debug)]
pub struct TaskWaiter {
    notified: bool,
    waker: std::task::Waker,
}

type SharedTaskWaiter = Arc<RwLock<TaskWaiter>>;

#[derive(Debug)]
enum Waiter {
    Thread(thread::Thread, Arc<AtomicBool>),
    Task(SharedTaskWaiter),
}

impl Waiter {
    #[inline]
    fn notify(self) {
        match self {
            Waiter::Thread(t, n) => {
                n.store(true, atomic::Ordering::Release);
                t.unpark()
            }
            Waiter::Task(t) => {
                let mut t = t.write();
                t.notified = true;
                t.waker.wake_by_ref()
            }
        }
    }

    #[inline]
    fn is_task(&self, other: &SharedTaskWaiter) -> bool {
        matches!(self, Waiter::Task(t) if Arc::ptr_eq(t, other))
    }

    #[inline]
    fn is_thread(&self, other: thread::ThreadId) -> bool {
        matches!(self, Waiter::Thread(t, _) if t.id() == other)
    }
}

#[derive(Debug)]
pub enum GuardResult<'a, Key, Qey, Val, We, B> {
    Value(Val),
    Guard(PlaceholderGuard<'a, Key, Qey, Val, We, B>),
    Timeout,
}

impl<'a, Key, Qey, Val, We, B> PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    pub fn start_loading(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
    ) -> Self {
        debug_assert!(matches!(
            shared.state.write().loading,
            LoadingState::Loading
        ));
        PlaceholderGuard {
            shard,
            shared,
            inserted: false,
        }
    }

    #[allow(clippy::type_complexity)]
    #[inline]
    fn join_internal(
        // We take shard lock here even if unused, as manipulating the waiters list
        // requires holding it to avoid races.
        _shard_lock: Result<
            RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
            RwLockReadGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        >,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: &SharedPlaceholder<Val>,
        notified: bool,
        waiter_fn: impl FnOnce() -> Waiter,
    ) -> Option<Result<Val, PlaceholderGuard<'a, Key, Qey, Val, We, B>>>
    where
        Val: Clone,
    {
        if notified {
            let state = shared.state.read();
            match &state.loading {
                LoadingState::Loading => {
                    drop(state);
                    Some(Err(Self::start_loading(shard, shared.clone())))
                }
                LoadingState::Inserted(value) => Some(Ok(value.clone())),
                LoadingState::Terminated => unreachable!(),
            }
        } else {
            let mut state = shared.state.write();
            match &state.loading {
                LoadingState::Loading => {
                    state.waiters.push(waiter_fn());
                    None
                }
                LoadingState::Inserted(value) => Some(Ok(value.clone())),
                LoadingState::Terminated => unreachable!(),
            }
        }
    }
}

impl<
        'a,
        Key: Eq + Hash,
        Qey: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Qey, Val>,
        B: BuildHasher,
    > PlaceholderGuard<'a, Key, Qey, Val, We, B>
{
    pub fn join(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        key: Key,
        qey: Qey,
        timeout: Option<Duration>,
    ) -> GuardResult<'a, Key, Qey, Val, We, B> {
        let mut shard_guard = shard.write();
        let shared = match shard_guard.get_value_or_placeholder(hash, key, qey) {
            Ok(v) => return GuardResult::Value(v),
            Err((shared, true)) => return GuardResult::Guard(Self::start_loading(shard, shared)),
            Err((shared, false)) => shared,
        };
        let mut notification: Option<Arc<AtomicBool>> = None;
        let mut notified = false;
        let mut shard_guard = Ok(shard_guard);
        loop {
            if let Some(result) = Self::join_internal(shard_guard, shard, &shared, notified, || {
                Waiter::Thread(
                    thread::current(),
                    notification.get_or_insert_with(Default::default).clone(),
                )
            }) {
                return match result {
                    Ok(v) => GuardResult::Value(v),
                    Err(g) => GuardResult::Guard(g),
                };
            }
            let notification = notification.as_ref().unwrap();
            if let Some(timeout) = timeout {
                let start = Instant::now();
                loop {
                    thread::park_timeout(Instant::now().saturating_duration_since(start));
                    if notification.load(atomic::Ordering::Acquire) {
                        break;
                    }
                    if start.elapsed() < timeout {
                        // spurious unpark
                        continue;
                    }
                    // Lock state and re-check
                    let mut state = shared.state.write();
                    if notification.load(atomic::Ordering::Acquire) {
                        break;
                    }
                    // We really timed out... remove from waiters list
                    let tid = thread::current().id();
                    let waiter_idx = state.waiters.iter().position(|w| w.is_thread(tid));
                    state.waiters.swap_remove(waiter_idx.unwrap());
                    return GuardResult::Timeout;
                }
            } else {
                loop {
                    thread::park();
                    if notification.load(atomic::Ordering::Acquire) {
                        break;
                    }
                }
            }
            notified = true;
            shard_guard = Err(shard.read());
        }
    }
}

impl<
        'a,
        Key: Eq + Hash,
        Qey: Eq + Hash,
        Val: Clone,
        We: Weighter<Key, Qey, Val>,
        B: BuildHasher,
    > PlaceholderGuard<'a, Key, Qey, Val, We, B>
{
    #[inline]
    pub fn insert(self, value: Val) {
        self.insert_evict(value);
    }

    pub fn insert_evict(mut self, value: Val) -> Option<(Key, Qey, Val)> {
        let referenced;
        {
            let mut state = self.shared.state.write();
            state.loading = LoadingState::Inserted(value.clone());
            referenced = !state.waiters.is_empty();
            for w in state.waiters.drain(..) {
                w.notify();
            }
        }

        let result = self
            .shard
            .write()
            .replace_placeholder(&self.shared, referenced, value);
        self.inserted = true;
        match result {
            Ok(Some(Entry::Resident(val))) => Some((val.key, val.qey, val.value)),
            _ => None
        }
    }
}

impl<'a, Key, Qey, Val, We, B> PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    #[cold]
    fn drop_slow(&mut self) {
        // Make sure to acquire the shard lock to prevent races with other threads
        let mut shard = self.shard.write();
        let mut state = self.shared.state.write();
        if let Some(w) = state.waiters.pop() {
            debug_assert!(matches!(state.loading, LoadingState::Loading));
            w.notify();
        } else {
            state.loading = LoadingState::Terminated;
            shard.remove_placeholder(&self.shared);
        }
    }
}

impl<'a, Key, Qey, Val, We, B> Drop for PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        if !self.inserted {
            self.drop_slow();
        }
    }
}
impl<'a, Key, Qey, Val, We, B> std::fmt::Debug for PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaceholderGuard").finish_non_exhaustive()
    }
}

/// Future that results in an Ok(Value) or Err(Guard)
pub enum JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    Created {
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        key: &'b Key,
        qey: &'b Qey,
    },
    Pending {
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
        waiter: Option<SharedTaskWaiter>,
    },
    Done,
}

impl<'a, 'b, Key, Qey, Val, We, B> JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    pub fn new(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        key: &'b Key,
        qey: &'b Qey,
    ) -> JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
        JoinFuture::Created {
            shard,
            hash,
            key,
            qey,
        }
    }

    #[cold]
    fn drop_pending_waiter(&mut self) {
        let Self::Pending { shard, shared, waiter: Some(waiter) } = self else { unreachable!() };
        let mut state = shared.state.write();
        let notified = waiter.read().notified; // Drop waiter guard to avoid a deadlock with start_loading
        if notified {
            if matches!(state.loading, LoadingState::Loading) {
                // The write guard was abandoned elsewhere, this future was notified but didn't get polled.
                // So we get and drop the guard here to handle the side effects.
                drop(state); // Drop state guard to avoid a deadlock with start_loading
                let _ = PlaceholderGuard::start_loading(shard, shared.clone());
            }
        } else {
            let waiter_idx = state.waiters.iter().position(|w| w.is_task(waiter));
            state.waiters.swap_remove(waiter_idx.unwrap());
        }
    }
}

impl<'a, 'b, Key, Qey, Val, We, B> Drop for JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        if matches!(
            self,
            Self::Pending {
                waiter: Some(_),
                ..
            }
        ) {
            self.drop_pending_waiter();
        }
    }
}

impl<
        'a,
        'b,
        Key: Eq + Hash + Clone,
        Qey: Eq + Hash + Clone,
        Val: Clone,
        We: Weighter<Key, Qey, Val>,
        B: BuildHasher,
    > Future for JoinFuture<'a, 'b, Key, Qey, Val, We, B>
{
    type Output = Result<Val, PlaceholderGuard<'a, Key, Qey, Val, We, B>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let shard_guard = match &*self {
            JoinFuture::Created {
                shard,
                hash,
                key,
                qey,
            } => {
                let mut shard_guard = shard.write();
                match shard_guard.get_value_or_placeholder(*hash, Key::clone(key), Qey::clone(qey))
                {
                    Ok(v) => {
                        *self = Self::Done;
                        return Poll::Ready(Ok(v));
                    }
                    Err((shared, true)) => {
                        let guard = PlaceholderGuard::start_loading(shard, shared);
                        *self = Self::Done;
                        return Poll::Ready(Err(guard));
                    }
                    Err((shared, false)) => {
                        *self = Self::Pending {
                            shard,
                            shared,
                            waiter: None,
                        };
                        Ok(shard_guard)
                    }
                }
            }
            JoinFuture::Pending {
                shard,
                waiter: Some(waiter),
                ..
            } => {
                let mut waiter = waiter.write();
                if waiter.notified {
                    waiter.notified = false;
                } else {
                    if !waiter.waker.will_wake(cx.waker()) {
                        waiter.waker = cx.waker().clone();
                    }
                    return Poll::Pending;
                }
                // drop waiter first to avoid deadlock due to lock order
                drop(waiter);
                Err(shard.read())
            }
            JoinFuture::Pending { .. } => unreachable!(),
            JoinFuture::Done => panic!("Polled after ready"),
        };

        let Self::Pending { shard, shared, waiter } = &mut *self else { unreachable!() };
        // If we reach here and waiter is some, it means we got a notification.
        match PlaceholderGuard::join_internal(shard_guard, shard, shared, waiter.is_some(), || {
            let task_waiter = waiter
                .get_or_insert_with(|| {
                    Arc::new(RwLock::new(TaskWaiter {
                        notified: false,
                        waker: cx.waker().clone(),
                    }))
                })
                .clone();
            Waiter::Task(task_waiter)
        }) {
            Some(result) => {
                *waiter = None;
                *self = Self::Done;
                Poll::Ready(result)
            }
            None => {
                debug_assert!(waiter.is_some());
                Poll::Pending
            }
        }
    }
}
