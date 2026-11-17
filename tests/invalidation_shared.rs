#![cfg(not(feature = "shuttle"))]

use quick_cache::{
    sync::{Cache, EntryAction, EntryResult, GuardResult},
    UnitWeighter,
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn cache() -> Cache<u64, u64> {
    Cache::with_options(
        quick_cache::OptionsBuilder::new()
            .estimated_items_capacity(64)
            .weight_capacity(64)
            .shards(1)
            .build()
            .unwrap(),
        UnitWeighter,
        Default::default(),
        Default::default(),
    )
}
#[test]
fn remove_wakes_waiters_to_retry_without_old_value() {
    let cache = cache();
    let GuardResult::Guard(old) = cache.get_value_or_guard(&0, None) else {
        panic!("old owner")
    };
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first = pin!(cache.get_value_or_guard_async(&0));
    let mut second = pin!(cache.get_value_or_guard_async(&0));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(cache.remove(&0), None);
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        2,
        "invalidation wakes registered waiters"
    );
    let Poll::Ready(Err(new)) = first.as_mut().poll(&mut cx) else {
        panic!("invalidated waiter retries and becomes owner")
    };
    assert!(second.as_mut().poll(&mut cx).is_pending());
    assert_eq!(old.insert(7), Err(7));
    assert!(second.as_mut().poll(&mut cx).is_pending());
    new.insert(9).unwrap();
    assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(Ok(9))));
    assert_eq!(cache.get(&0), Some(9));
}
#[test]
fn clear_multiple_pending_loads_wakes_every_waiter() {
    let cache = cache();
    let keys = [0, 1, 2, 3, 4, 5];
    let owners: Vec<_> = keys
        .iter()
        .map(|key| {
            let GuardResult::Guard(owner) = cache.get_value_or_guard(key, None) else {
                panic!()
            };
            owner
        })
        .collect();
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut waiters: Vec<_> = keys
        .iter()
        .map(|key| Box::pin(cache.get_value_or_guard_async(key)))
        .collect();
    for waiter in &mut waiters {
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
    }
    cache.clear();
    assert_eq!(wakes.0.load(Ordering::SeqCst), keys.len());
    for ((key, old), waiter) in keys.iter().zip(owners).zip(&mut waiters) {
        let Poll::Ready(Err(owner)) = waiter.as_mut().poll(&mut cx) else {
            panic!("invalidated waiter retries")
        };
        assert_eq!(old.insert(7), Err(7));
        owner.insert(key + 10).unwrap();
        assert_eq!(cache.get(key), Some(key + 10));
    }
}

#[test]
fn async_loader_runs_once_after_invalidation_retry() {
    let cache = cache();
    let GuardResult::Guard(old) = cache.get_value_or_guard(&0, None) else {
        panic!()
    };
    let waker = Waker::from(Arc::new(Wakes::default()));
    let mut cx = Context::from_waker(&waker);
    let calls = AtomicUsize::new(0);
    let mut waiting = pin!(cache.get_or_insert_async(&0, async {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok::<_, ()>(23)
    }));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    cache.remove(&0);
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(Ok(23))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(old.insert(7), Err(7));
}

#[test]
fn entry_callback_runs_once_on_replacement_after_retry() {
    let cache = cache();
    let GuardResult::Guard(old) = cache.get_value_or_guard(&0, None) else {
        panic!()
    };
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let calls = AtomicUsize::new(0);
    let mut waiting = pin!(cache.entry_async(&0, |_, value| {
        calls.fetch_add(1, Ordering::SeqCst);
        EntryAction::Retain(*value)
    }));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    cache.insert(0, 99);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(EntryResult::Retained(99))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(old.insert(7), Err(7));
    assert_eq!(cache.get(&0), Some(99));
}

#[test]
fn clear_panic_leaves_invalidated_waiters_safe_to_drop() {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::atomic::AtomicBool,
    };
    static PANIC: AtomicBool = AtomicBool::new(false);
    #[derive(Clone, PartialEq, Eq, Hash)]
    struct Key(u64);
    impl Drop for Key {
        fn drop(&mut self) {
            assert!(!PANIC.swap(false, Ordering::SeqCst), "key drop");
        }
    }
    let cache = Cache::<Key, u64>::with_options(
        quick_cache::OptionsBuilder::new()
            .estimated_items_capacity(16)
            .weight_capacity(16)
            .shards(1)
            .build()
            .unwrap(),
        UnitWeighter,
        Default::default(),
        Default::default(),
    );
    let first_key = Key(0);
    let second_key = Key(1);
    let GuardResult::Guard(first_owner) = cache.get_value_or_guard(&first_key, None) else {
        panic!()
    };
    let GuardResult::Guard(second_owner) = cache.get_value_or_guard(&second_key, None) else {
        panic!()
    };
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first = pin!(cache.get_value_or_guard_async(&first_key));
    let mut second = pin!(cache.get_value_or_guard_async(&second_key));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    PANIC.store(true, Ordering::SeqCst);
    let error = catch_unwind(AssertUnwindSafe(|| cache.clear())).unwrap_err();
    assert_eq!(error.downcast_ref::<&str>(), Some(&"key drop"));
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    // Loader handles contain metadata; their Drop cannot use a poisoned shard.
    std::mem::forget(first_owner);
    std::mem::forget(second_owner);
}

type PendingWaiter = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
struct CancelWake {
    next: Arc<std::sync::Mutex<Option<PendingWaiter>>>,
}
impl Wake for CancelWake {
    fn wake(self: Arc<Self>) {
        self.next.lock().unwrap().take();
    }
}
#[test]
fn invalidation_wake_can_drop_a_later_waiter() {
    let cache = Arc::new(cache());
    let GuardResult::Guard(old) = cache.get_value_or_guard(&0, None) else {
        panic!()
    };
    let next = Arc::new(std::sync::Mutex::new(None));
    let first_waker = Waker::from(Arc::new(CancelWake { next: next.clone() }));
    let mut first = pin!(cache.get_value_or_guard_async(&0));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&first_waker))
        .is_pending());
    let later_wakes = Arc::new(Wakes::default());
    let later_waker = Waker::from(later_wakes.clone());
    let owned = cache.clone();
    let mut later: PendingWaiter = Box::pin(async move {
        let _ = owned.get_value_or_guard_async(&0).await;
    });
    assert!(later
        .as_mut()
        .poll(&mut Context::from_waker(&later_waker))
        .is_pending());
    *next.lock().unwrap() = Some(later);
    cache.remove(&0);
    assert!(next.lock().unwrap().is_none());
    assert_eq!(later_wakes.0.load(Ordering::SeqCst), 1);
    let Poll::Ready(Err(owner)) = first.as_mut().poll(&mut Context::from_waker(&first_waker))
    else {
        panic!("surviving waiter retries")
    };
    assert_eq!(old.insert(7), Err(7));
    owner.insert(9).unwrap();
}
