use std::sync::Arc;
use std::{future::Future, mem};

use crate::{rw_lock::RwLock, shard::KQCacheShard};

pub type SentinelShared<Val> = Arc<RwLock<SentinelInner<Val>>>;

#[derive(Debug)]
pub struct SentinelInner<Val> {
    waiters: Vec<Waiter>,
    state: SentinelState<Val>,
}

pub struct SentinelWriteGuard<'a, Key, Qey, Val, We, B> {
    hash: u64,
    shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
    sentinel: SentinelShared<Val>,
    inserted: bool,
}

#[derive(Debug)]
enum SentinelState<Val> {
    /// The sentinel was just created and will start loading,
    /// or a waiter was notified to start loading.
    Abandoned,
    /// A guard was created and the value might get filled
    Loading,
    /// A value was filled
    Inserted(Val),
    /// The sentinel was abandoned without any waiters to take over
    Terminated,
}

pub enum JoinResult<'a, Key, Qey, Val, We, B> {
    Guard(SentinelWriteGuard<'a, Key, Qey, Val, We, B>),
    Value(Val),
    Retry,
}

impl<Val> Default for SentinelInner<Val> {
    fn default() -> Self {
        SentinelInner {
            waiters: Default::default(),
            state: SentinelState::Abandoned,
        }
    }
}

#[derive(Debug)]
pub enum Waiter {
    Thread(std::thread::Thread),
    Task(Arc<std::task::Waker>),
}

impl<'a, Key, Qey, Val: Clone, We, B> SentinelWriteGuard<'a, Key, Qey, Val, We, B> {
    pub fn join_internal(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        sentinel: SentinelShared<Val>,
        add_waiter: impl FnOnce(&mut Vec<Waiter>),
    ) -> Option<JoinResult<'a, Key, Qey, Val, We, B>> {
        let mut locked = sentinel.write();
        match &locked.state {
            SentinelState::Abandoned => {
                locked.state = SentinelState::Loading;
                drop(locked);
                return Some(JoinResult::Guard(Self {
                    hash,
                    shard,
                    sentinel,
                    inserted: false,
                }));
            }
            SentinelState::Inserted(value) => {
                return Some(JoinResult::Value(value.clone()));
            }
            SentinelState::Terminated => return Some(JoinResult::Retry),
            SentinelState::Loading => (),
        }
        add_waiter(&mut locked.waiters);
        None
    }

    pub fn join(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        sentinel: SentinelShared<Val>,
    ) -> JoinResult<'a, Key, Qey, Val, We, B> {
        let mut registered_thread: Option<std::thread::Thread> = None;
        loop {
            match Self::join_internal(shard, hash, sentinel.clone(), |ws| {
                let thread = match &registered_thread {
                    Some(thread) => {
                        if ws
                            .iter()
                            .find(|w| matches!(w, Waiter::Thread(t) if t.id() == thread.id()))
                            .is_some()
                        {
                            // Spurious unpark!?
                            return;
                        }
                        thread.clone()
                    }
                    None => registered_thread.insert(std::thread::current()).clone(),
                };
                ws.push(Waiter::Thread(thread));
            }) {
                Some(result) => return result,
                None => std::thread::park(),
            }
        }
    }

    pub fn join_async(
        shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
        hash: u64,
        sentinel: SentinelShared<Val>,
    ) -> JoinFuture<Key, Qey, Val, We, B> {
        JoinFuture {
            shard,
            hash,
            sentinel,
            registered_waker: None,
        }
    }

    pub fn mark_inserted(mut self, value: Val) -> usize {
        let waiters = {
            let mut sentinel = self.sentinel.write();
            sentinel.state = SentinelState::Inserted(value);
            mem::take(&mut sentinel.waiters)
        };
        self.inserted = true;
        let waiters_len = waiters.len();
        for w in waiters {
            w.notify();
        }
        waiters_len
    }
}

impl<'a, Key, Qey, Val, We, B> Drop for SentinelWriteGuard<'a, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        if self.inserted {
            return;
        }
        let mut sentinel = self.sentinel.write();
        if let Some(w) = sentinel.waiters.pop() {
            sentinel.state = SentinelState::Abandoned;
            w.notify();
        } else {
            sentinel.state = SentinelState::Terminated;
            self.shard
                .write()
                .remove_sentinel(self.hash, &self.sentinel);
        }
    }
}

impl Waiter {
    pub fn notify(self) {
        match self {
            Waiter::Thread(t) => t.unpark(),
            Waiter::Task(t) => t.wake_by_ref(),
        }
    }
}

pub struct JoinFuture<'a, Key, Qey, Val, We, B> {
    shard: &'a RwLock<KQCacheShard<Key, Qey, Val, We, B>>,
    hash: u64,
    sentinel: SentinelShared<Val>,
    registered_waker: Option<Arc<std::task::Waker>>,
}

impl<'a, Key, Qey, Val, We, B> Drop for JoinFuture<'a, Key, Qey, Val, We, B> {
    fn drop(&mut self) {
        if let Some(registered_waker) = self.registered_waker.take() {
            let mut sentinel = self.sentinel.write();
            let waiter_idx = sentinel.waiters.iter().position(
                |waiter| matches!(waiter, Waiter::Task(w) if Arc::ptr_eq(w, &registered_waker)),
            );
            if let Some(idx) = waiter_idx {
                sentinel.waiters.swap_remove(idx);
            } else if matches!(sentinel.state, SentinelState::Abandoned) {
                // The write guard was abandoned then the future was notified but didn't get polled.
                // So the next waiter, if any, will get notified.
                let next_waiter = sentinel.waiters.pop();
                if let Some(w) = next_waiter {
                    w.notify();
                } else {
                    sentinel.state = SentinelState::Terminated;
                    self.shard
                        .write()
                        .remove_sentinel(self.hash, &self.sentinel);
                }
            }
        }
    }
}

impl<'a, Key, Qey, Val: Clone, We, B> Future for JoinFuture<'a, Key, Qey, Val, We, B> {
    type Output = JoinResult<'a, Key, Qey, Val, We, B>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(mut registered_waker) = self.registered_waker.take() {
            if Arc::get_mut(&mut registered_waker).is_some() {
                // We definitely got notified, call join_internal again
            } else {
                // Lock waiters and double check
                let mut sentinel = self.sentinel.write();
                let mut new_registered_waker = None;
                if Arc::get_mut(&mut registered_waker).is_some() {
                    // We got a notification!
                } else if cx.waker().will_wake(&registered_waker) {
                    new_registered_waker = Some(registered_waker);
                } else {
                    // Futures can migrate to other tasks, which is responsible for most of the complexity.
                    for waiter in &mut sentinel.waiters {
                        match waiter {
                            Waiter::Task(w) if Arc::ptr_eq(w, &registered_waker) => {
                                drop(registered_waker);
                                let waker = cx.waker().clone();
                                *Arc::get_mut(w).unwrap() = waker;
                                new_registered_waker = Some(w.clone());
                                break;
                            }
                            _ => (),
                        }
                    }
                }
                drop(sentinel);
                if new_registered_waker.is_some() {
                    self.registered_waker = new_registered_waker;
                    return std::task::Poll::Pending;
                }
                // If we have no new_registered_waker we must have been notified
            }
        }

        debug_assert!(self.registered_waker.is_none());
        match SentinelWriteGuard::join_internal(self.shard, self.hash, self.sentinel.clone(), |w| {
            let waker = self
                .registered_waker
                .insert(Arc::new(cx.waker().clone()))
                .clone();
            w.push(Waiter::Task(waker));
        }) {
            Some(result) => std::task::Poll::Ready(result),
            None => std::task::Poll::Pending,
        }
    }
}
