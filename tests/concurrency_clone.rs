#![cfg(not(feature = "shuttle"))]

use quick_cache::sync::{Cache, GuardResult};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Debug)]
struct Value(Arc<AtomicUsize>);

impl Clone for Value {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::SeqCst);
        Self(self.0.clone())
    }
}

#[test]
fn uncontended_guard_avoids_shared_value_clone() {
    let count = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::new(16);
    let GuardResult::Guard(owner) = cache.get_value_or_guard(&0, None) else {
        panic!("expected loader")
    };
    owner.insert(Value(count.clone())).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct Heavy;

impl quick_cache::Weighter<u64, Value> for Heavy {
    fn weight(&self, _: &u64, _: &Value) -> u64 {
        1000
    }
}

#[test]
fn shared_value_reaches_waiters_after_cancellation_and_rejection() {
    for (capacity, cached) in [(16, false), (4096, true)] {
        let count = Arc::new(AtomicUsize::new(0));
        let cache = Cache::<u64, Value, Heavy>::with(
            16,
            capacity,
            Heavy,
            Default::default(),
            Default::default(),
        );
        let GuardResult::Guard(owner) = cache.get_value_or_guard(&0, None) else {
            panic!("expected loader")
        };
        let mut waiters: Vec<_> = (0..3)
            .map(|_| Box::pin(cache.get_value_or_guard_async(&0)))
            .collect();
        let mut cx = Context::from_waker(Waker::noop());
        for waiter in &mut waiters {
            assert!(waiter.as_mut().poll(&mut cx).is_pending());
        }
        drop(waiters.pop());
        owner.insert(Value(count.clone())).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.remove(&0).is_some(), cached);
        for waiter in &mut waiters {
            assert!(matches!(waiter.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
        }
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }
}

struct Noop;

impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn panicking_wake_keeps_other_waiter_safe_to_repoll() {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("wake callback");
        }
    }

    let cache = Cache::<u64, u64>::new(16);
    let GuardResult::Guard(owner) = cache.get_value_or_guard(&0, None) else {
        panic!("expected loader")
    };
    let bad = Waker::from(Arc::new(PanicWake));
    let good = Waker::from(Arc::new(Noop));
    let changed = Waker::from(Arc::new(Noop));
    assert!(!good.will_wake(&changed));
    let mut first = Box::pin(cache.get_value_or_guard_async(&0));
    let mut second = Box::pin(cache.get_value_or_guard_async(&0));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&bad))
        .is_pending());
    assert!(second
        .as_mut()
        .poll(&mut Context::from_waker(&good))
        .is_pending());
    assert!(catch_unwind(AssertUnwindSafe(|| owner.insert(7))).is_err());
    assert!(matches!(
        second.as_mut().poll(&mut Context::from_waker(&changed)),
        Poll::Ready(Ok(7))
    ));
    assert_eq!(cache.peek(&0), Some(7));
}

// parking_lot allows the lock probe after unwind releases the guard.
#[cfg(feature = "parking_lot")]
#[test]
fn publication_panic_drops_shared_value_after_unlock() {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        pin::pin,
        sync::{OnceLock, Weak},
    };

    #[derive(Clone)]
    struct PanicWeight;

    type TestCache = Cache<u64, Arc<DropProbe>, PanicWeight>;

    struct DropProbe {
        cache: Weak<TestCache>,
        unlocked: Arc<OnceLock<bool>>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let cache = self.cache.upgrade().unwrap();
            let _ = self.unlocked.set(cache.try_contains_key(&0).is_ok());
        }
    }

    impl quick_cache::Weighter<u64, Arc<DropProbe>> for PanicWeight {
        fn weight(&self, _: &u64, _: &Arc<DropProbe>) -> u64 {
            panic!("weigher panic")
        }
    }

    let cache = Arc::new(TestCache::with(
        16,
        16,
        PanicWeight,
        Default::default(),
        Default::default(),
    ));
    let GuardResult::Guard(owner) = cache.get_value_or_guard(&0, None) else {
        panic!("expected owner");
    };
    {
        let mut waiter = pin!(cache.get_value_or_guard_async(&0));
        assert!(waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
    }

    let unlocked = Arc::new(OnceLock::new());
    let value = Arc::new(DropProbe {
        cache: Arc::downgrade(&cache),
        unlocked: unlocked.clone(),
    });
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = owner.insert(value);
    }))
    .unwrap_err();

    assert_eq!(panic.downcast_ref::<&str>(), Some(&"weigher panic"));
    assert_eq!(unlocked.get(), Some(&true));
}
