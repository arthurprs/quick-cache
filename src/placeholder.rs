use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    mem,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use crate::{
    linked_slab::Token,
    rw_lock::{RwLock, RwLockWriteGuard},
    shard::KQCacheShard,
    Weighter,
};

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
struct TaskWaiter {
    notified: bool,
    waker: std::task::Waker,
}

type SharedTaskWaiter = Arc<RwLock<TaskWaiter>>;

#[derive(Debug)]
enum Waiter {
    Thread(std::thread::Thread, Arc<AtomicBool>),
    Task(SharedTaskWaiter),
}

impl Waiter {
    #[inline]
    fn notify(self) {
        match self {
            Waiter::Thread(t, n) => {
                n.store(true, std::sync::atomic::Ordering::Release);
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
}

pub enum JoinResult<'a, Key, Qey, Val, We, B> {
    Value(Val),
    Guard(PlaceholderGuard<'a, Key, Qey, Val, We, B>),
    Timeout,
}

impl<'a, Key, Qey, Val, We, B> PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    pub fn start_loading_unchecked(
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

    pub fn start_loading(
        _shard_lock: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
    ) -> Self {
        Self::start_loading_unchecked(shard, shared)
    }

    #[allow(clippy::type_complexity)]
    fn join_internal(
        shard_lock: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: &SharedPlaceholder<Val>,
        notified: bool,
        waiter_fn: impl FnOnce() -> Waiter,
    ) -> Option<Result<Val, PlaceholderGuard<'a, Key, Qey, Val, We, B>>>
    where
        Val: Clone,
    {
        let mut state = shared.state.write();
        match &state.loading {
            LoadingState::Loading if notified => {
                drop(state);
                Some(Err(Self::start_loading(shard_lock, shard, shared.clone())))
            }
            LoadingState::Loading => {
                state.waiters.push(waiter_fn());
                None
            }
            LoadingState::Inserted(value) => Some(Ok(value.clone())),
            LoadingState::Terminated => unreachable!(),
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
    ) -> JoinResult<'a, Key, Qey, Val, We, B> {
        let mut shard_guard = shard.write();
        let shared = match shard_guard.value_or_placeholder(hash, key, qey) {
            Ok(v) => return JoinResult::Value(v),
            Err((shared, true)) => {
                return JoinResult::Guard(Self::start_loading(shard_guard, shard, shared))
            }
            Err((shared, false)) => shared,
        };
        let mut notification: Option<Arc<AtomicBool>> = None;
        let mut notified = false;
        loop {
            if let Some(result) = Self::join_internal(shard_guard, shard, &shared, notified, || {
                Waiter::Thread(
                    std::thread::current(),
                    notification.get_or_insert_with(Default::default).clone(),
                )
            }) {
                return match result {
                    Ok(v) => JoinResult::Value(v),
                    Err(g) => JoinResult::Guard(g),
                };
            }
            let notification = notification.as_ref().unwrap();
            if let Some(timeout) = timeout {
                loop {
                    let start = Instant::now();
                    std::thread::park_timeout(timeout);
                    if notification.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                    if start.elapsed() >= timeout {
                        return JoinResult::Timeout;
                    }
                }
            } else {
                loop {
                    std::thread::park();
                    if notification.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }
                }
            }
            notified = true;
            shard_guard = shard.write();
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
    pub fn insert(mut self, value: Val) {
        let waiters = {
            let mut state = self.shared.state.write();
            state.loading = LoadingState::Inserted(value.clone());
            mem::take(&mut state.waiters)
        };
        let referenced = !waiters.is_empty();
        for w in waiters {
            w.notify();
        }
        let _result = self
            .shard
            .write()
            .replace_placeholder(&self.shared, referenced, value);
        self.inserted = true;
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

/// Future that results in an Ok(Value) or Err(Guard)
///
/// Possible states for (hkq, shared, waiter):
/// (some, none, none) => never polled
/// (some, _    , _  ) => invalid
/// (none, some, some) => pending, waiter added to state
/// (none, none, none) => after returning Ready in the 1st poll
/// (none, some, none) => after returning Ready in the 2nd+ poll
/// (none, none, some) => invalid
pub struct JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
    hkq: Option<(u64, &'b Key, &'b Qey)>,
    shared: Option<SharedPlaceholder<Val>>,
    waiter: Option<SharedTaskWaiter>,
}

impl<'a, 'b, Key, Qey, Val, We, B> JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    pub fn new(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        key: &'b Key,
        qey: &'b Qey,
    ) -> JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
        JoinFuture {
            shard,
            hkq: Some((hash, key, qey)),
            shared: None,
            waiter: None,
        }
    }

    #[cold]
    fn drop_slow(&mut self, waiter: SharedTaskWaiter) {
        let shared = self.shared.as_ref().unwrap();
        let mut state = shared.state.write();
        if waiter.read().notified {
            if matches!(state.loading, LoadingState::Loading) {
                // The write guard was abandoned and the future was notified but didn't get polled.
                // So the next waiter, if any, will get notified.
                drop(state); // Drop state to avoid a deadlock
                let _ = PlaceholderGuard::start_loading_unchecked(self.shard, shared.clone());
            }
        } else {
            let waiter_idx = state.waiters.iter().position(|w| w.is_task(&waiter));
            state.waiters.swap_remove(waiter_idx.unwrap());
        }
    }
}

impl<'a, 'b, Key, Qey, Val, We, B> Drop for JoinFuture<'a, 'b, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            self.drop_slow(waiter)
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
    ) -> std::task::Poll<Self::Output> {
        let mut shard_guard = self.shard.write();
        if let Some((hash, key, qey)) = self.hkq.take() {
            match shard_guard.value_or_placeholder(hash, key.clone(), qey.clone()) {
                Ok(v) => return std::task::Poll::Ready(Ok(v)),
                Err((shared, true)) => {
                    let guard = PlaceholderGuard::start_loading(shard_guard, self.shard, shared);
                    return std::task::Poll::Ready(Err(guard));
                }
                Err((shared, false)) => self.shared = Some(shared),
            }
        }

        let mut waiter_opt = self.waiter.take();
        if let Some(waiter) = &waiter_opt {
            if !waiter.write().notified {
                // Lock waiters, double check and adjust waker if needed
                let state = self.shared.as_ref().unwrap().state.write();
                let mut waiter = waiter.write();
                if !waiter.notified {
                    if !waiter.waker.will_wake(cx.waker()) {
                        waiter.waker = cx.waker().clone();
                    }
                    drop(state);
                    drop(waiter);
                    self.waiter = waiter_opt;
                    return std::task::Poll::Pending;
                }
            }
        }

        // If we reach here and waiter_opt is some, it means we got a notification.
        let notified = waiter_opt.is_some();
        match PlaceholderGuard::join_internal(
            shard_guard,
            self.shard,
            self.shared.as_ref().unwrap(),
            notified,
            || {
                let task_waiter = waiter_opt
                    .get_or_insert_with(|| {
                        Arc::new(RwLock::new(TaskWaiter {
                            notified: false,
                            waker: cx.waker().clone(),
                        }))
                    })
                    .clone();
                Waiter::Task(task_waiter)
            },
        ) {
            Some(result) => std::task::Poll::Ready(result),
            None => {
                debug_assert!(waiter_opt.is_some());
                self.waiter = waiter_opt;
                std::task::Poll::Pending
            }
        }
    }
}
