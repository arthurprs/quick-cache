use std::{
    future::Future,
    hash::{BuildHasher, Hash},
    mem,
    sync::{atomic::AtomicBool, Arc},
    time::{Instant, Duration},
};

use crate::{
    linked_slab::Token,
    rw_lock::{RwLock, RwLockWriteGuard},
    shard::KQCacheShard,
    Weighter,
};

pub type SharedPlaceholder<Val> = Arc<Placeholder<Val>>;

pub fn new_shared_placeholder<'a, Val>(hash: u64, idx: Token) -> SharedPlaceholder<Val> {
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

// #[derive(Debug)]
pub struct State<Val> {
    waiters: Vec<Waiter>,
    loading: LoadingState<Val>,
}

// #[derive(Debug)]
enum LoadingState<Val> {
    /// A guard was created and the value might get filled
    Loading,
    /// A value was filled, no more waiters can be added
    Inserted(Val),
    /// The placeholder was abandoned w/o any waiters and was removed from the map
    Terminated,
}
impl<Val> std::fmt::Debug for State<Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("waiters", &self.waiters)
            .field("loading", &self.loading)
            .finish()
    }
}

impl<Val> std::fmt::Debug for LoadingState<Val> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loading => write!(f, "Loading"),
            Self::Inserted(_arg0) => write!(f, "Inserted"),
            Self::Terminated => write!(f, "Terminated"),
        }
    }
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

impl<'a, Key, Qey, Val: Clone, We, B> PlaceholderGuard<'a, Key, Qey, Val, We, B> {
    pub fn start_loading(
        _shard_lock: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
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

    fn join_internal(
        shard_lock: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
        notified: bool,
        waiter_fn: impl FnOnce() -> Waiter,
    ) -> Option<Result<Val, PlaceholderGuard<'a, Key, Qey, Val, We, B>>> {
        let mut state = shared.state.write();
        match &state.loading {
            LoadingState::Loading if notified => {
                drop(state);
                Some(Err(Self::start_loading(
                    shard_lock, shard, shared,
                )))
            }
            LoadingState::Loading => {
                state.waiters.push(waiter_fn());
                None
            }
            LoadingState::Inserted(value) => Some(Ok(value.clone())),
            LoadingState::Terminated => unreachable!(),
        }
    }

    pub fn join(
        shard_guard: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
        timeout: Option<Duration>,
    ) -> JoinResult<'a, Key, Qey, Val, We, B> {
        let mut notification: Option<Arc<AtomicBool>> = None;
        let mut notified = false;
        let mut shard_guard = Some(shard_guard);
        loop {
            let shard_guard = shard_guard.take().unwrap_or_else(|| shard.write());
            if let Some(result) =
                Self::join_internal(shard_guard, shard, shared.clone(), notified, || {
                    Waiter::Thread(
                        std::thread::current(),
                        notification.get_or_insert_with(Default::default).clone(),
                    )
                })
            {
                return match result {
                    Ok(v) => JoinResult::Value(v),
                    Err(g) => JoinResult::Guard(g),
                };
            } else {
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
            }
        }
    }

    pub fn join_future(
        _shard_guard: RwLockWriteGuard<'a, KQCacheShard<Key, Qey, Val, We, B>>,
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        shared: SharedPlaceholder<Val>,
    ) -> JoinFuture<'a, Key, Qey, Val, We, B> {
        JoinFuture {
            shard,
            shared,
            waker: None,
            pending: false,
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
    #[inline(never)]
    pub fn drop_slow(&mut self) {
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

pub struct JoinFuture<'a, Key, Qey, Val: Clone, We, B> {
    shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
    shared: SharedPlaceholder<Val>,
    waker: Option<SharedTaskWaiter>,
    pending: bool,
}

impl<'a, Key, Qey, Val: Clone, We, B> Drop for JoinFuture<'a, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        let Some(waker) = self.waker.take() else { return };
        if !self.pending {
            return;
        }
        let mut state = self.shared.state.write();
        let waiter_idx = state.waiters.iter().position(|w| w.is_task(&waker));
        if let Some(idx) = waiter_idx {
            state.waiters.swap_remove(idx);
        } else if matches!(state.loading, LoadingState::Loading) {
            debug_assert!(waker.read().notified);
            // The write guard was abandoned and the future was notified but didn't get polled.
            // So the next waiter, if any, will get notified.
            drop(state);
            let _ = PlaceholderGuard::start_loading(
                self.shard.write(),
                self.shard,
                self.shared.clone(),
            );
        }
    }
}

impl<'a, Key, Qey, Val: Clone, We, B> Future for JoinFuture<'a, Key, Qey, Val, We, B> {
    type Output = Result<Val, PlaceholderGuard<'a, Key, Qey, Val, We, B>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let notified = if self.pending {
            let waker = self.waker.as_ref().unwrap();
            if !waker.write().notified {
                // Lock waiters, double check and adjust waker if needed
                let _state = self.shared.state.write();
                let mut waker = waker.write();
                if !waker.notified {
                    if !waker.waker.will_wake(cx.waker()) {
                        waker.waker = cx.waker().clone();
                    }
                    return std::task::Poll::Pending;
                }
            }
            // We got a notification!
            self.pending = false;
            true
        } else {
            false
        };

        match PlaceholderGuard::join_internal(
            self.shard.write(),
            self.shard,
            self.shared.clone(),
            notified,
            || {
                let task_waiter = self
                    .waker
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
                self.pending = true;
                std::task::Poll::Pending
            }
        }
    }
}
