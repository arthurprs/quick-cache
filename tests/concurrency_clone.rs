#![cfg(not(feature = "shuttle"))]
use quick_cache::sync::{Cache, GuardResult};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[derive(Debug)]
struct Value(Arc<AtomicUsize>);
impl Clone for Value {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::SeqCst);
        Self(self.0.clone())
    }
}
fn guard<'a>(
    cache: &'a Cache<u64, Value>,
) -> quick_cache::sync::PlaceholderGuard<
    'a,
    u64,
    Value,
    quick_cache::UnitWeighter,
    quick_cache::DefaultHashBuilder,
    quick_cache::sync::DefaultLifecycle<u64, Value>,
> {
    match cache.get_value_or_guard(&0, None) {
        GuardResult::Guard(g) => g,
        _ => unreachable!(),
    }
}
#[test]
fn uncontended_guard_avoids_shared_value_clone() {
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    guard(&cache).insert(Value(count.clone())).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[test]
fn overwritten_guard_returns_original_without_clone() {
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    let old = guard(&cache);
    cache.insert(0, Value(count.clone()));
    assert!(old.insert(Value(count.clone())).is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[test]
fn convenience_loader_clones_for_cache_ownership() {
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    cache
        .get_or_insert_with(&0, || Ok::<_, ()>(Value(count.clone())))
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
struct Noop;
impl std::task::Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
#[test]
fn detached_placeholder_still_delivers_to_registered_waiter() {
    use std::future::Future;
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    let owner = guard(&cache);
    let mut waiter = Box::pin(cache.get_value_or_guard_async(&0));
    let waker = std::task::Waker::from(Arc::new(Noop));
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    assert!(cache.remove(&0).is_none());
    assert!(owner.insert(Value(count.clone())).is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        waiter.as_mut().poll(&mut cx),
        std::task::Poll::Ready(Ok(_))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 2);
}
#[test]
fn racing_registration_receives_value() {
    use std::future::Future;
    let cache = Cache::<u64, u64>::new(16);
    for round in 0..1000 {
        cache.remove(&0);
        let owner = match cache.get_value_or_guard(&0, None) {
            GuardResult::Guard(g) => g,
            _ => unreachable!(),
        };
        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::yield_now();
                owner.insert(round).unwrap();
            });
            let mut waiter = Box::pin(cache.get_value_or_guard_async(&0));
            let waker = std::task::Waker::from(Arc::new(Noop));
            let mut cx = std::task::Context::from_waker(&waker);
            loop {
                if let std::task::Poll::Ready(result) = waiter.as_mut().poll(&mut cx) {
                    assert_eq!(result.unwrap(), round);
                    break;
                }
                std::thread::yield_now();
            }
        });
    }
}
#[derive(Clone)]
struct Heavy;
impl quick_cache::Weighter<u64, Value> for Heavy {
    fn weight(&self, _: &u64, _: &Value) -> u64 {
        1000
    }
}
#[test]
fn rejected_heavy_value_needs_no_shared_clone() {
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::with(
        16,
        16,
        Heavy,
        quick_cache::DefaultHashBuilder::default(),
        quick_cache::sync::DefaultLifecycle::default(),
    );
    let owner = match cache.get_value_or_guard(&0, None) {
        GuardResult::Guard(g) => g,
        _ => unreachable!(),
    };
    owner.insert(Value(count.clone())).unwrap();
    assert!(!cache.contains_key(&0));
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[test]
fn cancelled_waiters_need_no_shared_clone() {
    use std::future::Future;
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    let owner = guard(&cache);
    let mut waiters: Vec<_> = (0..8)
        .map(|_| Box::pin(cache.get_value_or_guard_async(&0)))
        .collect();
    let waker = std::task::Waker::from(Arc::new(Noop));
    let mut cx = std::task::Context::from_waker(&waker);
    for waiter in &mut waiters {
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
    }
    drop(waiters);
    owner.insert(Value(count.clone())).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[test]
fn stacked_waiters_receive_shared_value_after_cancellation() {
    use std::future::Future;
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    let owner = guard(&cache);
    let mut waiters: Vec<_> = (0..8)
        .map(|_| Box::pin(cache.get_value_or_guard_async(&0)))
        .collect();
    let waker = std::task::Waker::from(Arc::new(Noop));
    let mut cx = std::task::Context::from_waker(&waker);
    for waiter in &mut waiters {
        assert!(waiter.as_mut().poll(&mut cx).is_pending());
    }
    drop(waiters.pop());
    owner.insert(Value(count.clone())).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for waiter in &mut waiters {
        assert!(matches!(
            waiter.as_mut().poll(&mut cx),
            std::task::Poll::Ready(Ok(_))
        ));
    }
    assert_eq!(count.load(Ordering::SeqCst), 8);
}
