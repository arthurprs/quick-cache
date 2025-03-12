//! Lightweight, high performance concurrent cache. It allows very fast access to the cached items
//! with little overhead compared to a plain concurrent hash table. No allocations are ever performed
//! unless the cache internal state table needs growing (which will eventually stabilize).
//!
//! # Eviction policy
//!
//! The current eviction policy is a modified version of the Clock-PRO algorithm, very similar to the
//! later published S3-FIFO algorithm. It's "scan resistent" and provides high hit rates,
//! significantly better than a LRU eviction policy and comparable to other state-of-the art algorithms
//! like W-TinyLFU.
//!
//! # Thread safety and Concurrency
//!
//! Both `sync` (thread-safe) and `unsync` (non thread-safe) implementations are provided. The latter
//! offers slightly better performance when thread safety is not required.
//!
//! # Equivalent keys
//!
//! The cache uses the [`Equivalent`](https://docs.rs/equivalent/1.0.1/equivalent/trait.Equivalent.html) trait
//! for gets/removals. It can helps work around the `Borrow` limitations.
//! For example, if the cache key is a tuple `(K, Q)`, you wouldn't be access to access such keys without
//! building a `&(K, Q)` and thus potentially cloning `K` and/or `Q`.
//!
//! # User defined weight
//!
//! By implementing the [Weighter] trait the user can define different weights for each cache entry.
//!
//! # Atomic operations
//!
//! By using the `get_or_insert` or `get_value_or_guard` family of functions (both sync and async variants
//! are available, they can be mix and matched) the user can coordinate the insertion of entries, so only
//! one value is "computed" and inserted after a cache miss.
//!
//! # Lifecycle hooks
//!
//! A user can optionally provide a custom [Lifecycle] implementation to hook into the lifecycle of cache entries.
//!
//! Example use cases:
//! * item pinning, so even if the item occupies weight but isn't allowed to be evicted
//! * send evicted items to a channel, achieving the equivalent to an eviction listener feature.
//! * zero out item weights so they are left in the cache instead of evicted.
//!
//! # Hasher
//!
//! By default the crate uses [ahash](https://crates.io/crates/ahash), which is enabled (by default) via
//! a crate feature with the same name. If the `ahash` feature is disabled the crate defaults to the std lib
//! implementation instead (currently Siphash13). Note that a custom hasher can also be provided if desirable.
//!
//! # Synchronization primitives
//!
//! By default the crate uses [parking_lot](https://crates.io/crates/parking_lot), which is enabled (by default) via
//! a crate feature with the same name. If the `parking_lot` feature is disabled the crate defaults to the std lib
//! implementation instead.
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[cfg(not(fuzzing))]
mod linked_slab;
#[cfg(fuzzing)]
pub mod linked_slab;
mod options;
#[cfg(not(feature = "shuttle"))]
mod rw_lock;
mod shard;
mod shim;
/// Concurrent cache variants that can be used from multiple threads.
pub mod sync;
mod sync_placeholder;
/// Non-concurrent cache variants.
pub mod unsync;
pub use equivalent::Equivalent;

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

pub use options::{Options, OptionsBuilder};

#[cfg(feature = "ahash")]
pub type DefaultHashBuilder = ahash::RandomState;
#[cfg(not(feature = "ahash"))]
pub type DefaultHashBuilder = std::collections::hash_map::RandomState;

/// Defines the weight of a cache entry.
///
/// # Example
///
/// ```
/// use quick_cache::{sync::Cache, Weighter};
///
/// #[derive(Clone)]
/// struct StringWeighter;
///
/// impl Weighter<u64, String> for StringWeighter {
///     fn weight(&self, _key: &u64, val: &String) -> u64 {
///         // Be cautious out about zero weights!
///         val.len() as u64
///     }
/// }
///
/// let cache = Cache::with_weighter(100, 100_000, StringWeighter);
/// cache.insert(1, "1".to_string());
/// ```
pub trait Weighter<Key, Val> {
    /// Returns the weight of the cache item.
    ///
    /// For performance reasons this function should be trivially cheap as
    /// it's called during the cache eviction routine.
    /// If weight is expensive to calculate, consider caching it alongside the value.
    ///
    /// Zero (0) weight items are allowed and will be ignored when looking for eviction
    /// candidates. Such items can only be manually removed or overwritten.
    ///
    /// Note that this it's undefined behavior for a cache item to change its weight.
    /// The only exception to this is when Lifecycle::before_evict is called.
    ///
    /// It's also undefined behavior in release mode if summing of weights overflow,
    /// although this is unlikely to be a problem in pratice.
    fn weight(&self, key: &Key, val: &Val) -> u64;
}

/// Each cache entry weights exactly `1` unit of weight.
#[derive(Debug, Clone, Default)]
pub struct UnitWeighter;

impl<Key, Val> Weighter<Key, Val> for UnitWeighter {
    #[inline]
    fn weight(&self, _key: &Key, _val: &Val) -> u64 {
        1
    }
}

/// Hooks into the lifetime of the cache items.
///
/// The functions should be small and very fast, otherwise the cache performance might be negatively affected.
pub trait Lifecycle<Key, Val> {
    type RequestState;

    /// Returns whether the item is pinned. Items that are pinned can't be evicted.
    /// Note that a pinned item can still be replaced with get_mut, insert, replace and similar APIs.
    ///
    /// Compared to zero (0) weight items, pinned items still consume (non-zero) weight even if they can't
    /// be evicted. Furthermore, zero (0) weight items are separated from the other entries, which allows
    /// having a large number of them without impacting performance, but moving them in/out or the evictable
    /// section has a small cost. Pinning on the other hand doesn't separate entries, so during eviction
    /// the cache may visit pinned entries but will ignore them.
    #[allow(unused_variables)]
    #[inline]
    fn is_pinned(&self, key: &Key, val: &Val) -> bool {
        false
    }

    /// Called before the insert request starts, e.g.: insert, replace.
    fn begin_request(&self) -> Self::RequestState;

    /// Called when a cache item is about to be evicted.
    /// Note that value replacement (e.g. insertions for the same key) won't call this method.
    ///
    /// This is the only time the item can change its weight. If the item weight becomes zero (0) it
    /// will be left in the cache, otherwise it'll still be removed. Zero (0) weight items aren't evictable
    /// and are kept separated from the other items so it's possible to have a large number of them without
    /// negatively affecting eviction performance.
    #[allow(unused_variables)]
    #[inline]
    fn before_evict(&self, state: &mut Self::RequestState, key: &Key, val: &mut Val) {}

    /// Called when an item is evicted.
    fn on_evict(&self, state: &mut Self::RequestState, key: Key, val: Val);

    /// Called after a request finishes, e.g.: insert, replace.
    ///
    /// Notes:
    /// This will _not_ be called when using `_with_lifecycle` apis, which will return the RequestState instead.
    /// This will _not_ be called if the request errored (e.g. a replace didn't find a value to replace).
    /// If needed, Drop for RequestState can be used to detect these cases.
    #[allow(unused_variables)]
    #[inline]
    fn end_request(&self, state: Self::RequestState) {}
}

#[cfg(test)]
mod tests {
    use std::{
        hash::Hash,
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use super::*;
    #[derive(Clone)]
    struct StringWeighter;

    impl Weighter<u64, String> for StringWeighter {
        fn weight(&self, _key: &u64, val: &String) -> u64 {
            val.len() as u64
        }
    }

    #[test]
    fn test_new() {
        sync::Cache::<(u64, u64), u64>::new(0);
        sync::Cache::<(u64, u64), u64>::new(1);
        sync::Cache::<(u64, u64), u64>::new(2);
        sync::Cache::<(u64, u64), u64>::new(3);
        sync::Cache::<(u64, u64), u64>::new(usize::MAX);
        sync::Cache::<u64, u64>::new(0);
        sync::Cache::<u64, u64>::new(1);
        sync::Cache::<u64, u64>::new(2);
        sync::Cache::<u64, u64>::new(3);
        sync::Cache::<u64, u64>::new(usize::MAX);
    }

    #[test]
    fn test_custom_cost() {
        let cache = sync::Cache::with_weighter(100, 100_000, StringWeighter);
        cache.insert(1, "1".to_string());
        cache.insert(54, "54".to_string());
        cache.insert(1000, "1000".to_string());
        assert_eq!(cache.get(&1000).unwrap(), "1000");
    }

    #[test]
    fn test_change_get_mut_change_weight() {
        let mut cache = unsync::Cache::with_weighter(100, 100_000, StringWeighter);
        cache.insert(1, "1".to_string());
        assert_eq!(cache.get(&1).unwrap(), "1");
        assert_eq!(cache.weight(), 1);
        let _old = {
            cache
                .get_mut(&1)
                .map(|mut v| std::mem::replace(&mut *v, "11".to_string()))
        };
        let _old = {
            cache
                .get_mut(&1)
                .map(|mut v| std::mem::replace(&mut *v, "".to_string()))
        };
        assert_eq!(cache.get(&1).unwrap(), "");
        assert_eq!(cache.weight(), 0);
        cache.validate(false);
    }

    #[derive(Debug, Hash)]
    pub struct Pair<A, B>(pub A, pub B);

    impl<A, B, C, D> PartialEq<(A, B)> for Pair<C, D>
    where
        C: PartialEq<A>,
        D: PartialEq<B>,
    {
        fn eq(&self, rhs: &(A, B)) -> bool {
            self.0 == rhs.0 && self.1 == rhs.1
        }
    }

    impl<A, B, X> Equivalent<X> for Pair<A, B>
    where
        Pair<A, B>: PartialEq<X>,
        A: Hash + Eq,
        B: Hash + Eq,
    {
        fn equivalent(&self, other: &X) -> bool {
            *self == *other
        }
    }

    #[test]
    fn test_equivalent() {
        let mut cache = unsync::Cache::new(5);
        cache.insert(("square".to_string(), 2022), "blue".to_string());
        cache.insert(("square".to_string(), 2023), "black".to_string());
        assert_eq!(cache.get(&Pair("square", 2022)).unwrap(), "blue");
    }

    #[test]
    fn test_borrow_keys() {
        let cache = sync::Cache::<(Vec<u8>, Vec<u8>), u64>::new(0);
        cache.get(&Pair(&b""[..], &b""[..]));
        let cache = sync::Cache::<(String, String), u64>::new(0);
        cache.get(&Pair("", ""));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_get_or_insert() {
        use rand::prelude::*;
        for _i in 0..2000 {
            dbg!(_i);
            let mut entered = AtomicUsize::default();
            let cache = sync::Cache::<(u64, u64), u64>::new(100);
            const THREADS: usize = 100;
            let wg = std::sync::Barrier::new(THREADS);
            let solve_at = rand::rng().random_range(0..THREADS);
            std::thread::scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        wg.wait();
                        let result = cache.get_or_insert_with(&(1, 1), || {
                            let before = entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if before == solve_at {
                                Ok(1)
                            } else {
                                Err(())
                            }
                        });
                        assert!(matches!(result, Ok(1) | Err(())));
                    });
                }
            });
            assert_eq!(*entered.get_mut(), solve_at + 1);
        }
    }

    #[test]
    fn test_get_or_insert_unsync() {
        let mut cache = unsync::Cache::<u64, u64>::new(100);
        let guard = cache.get_ref_or_guard(&0).unwrap_err();
        guard.insert(0);
        assert_eq!(cache.get_ref_or_guard(&0).ok().copied(), Some(0));
        let guard = cache.get_mut_or_guard(&1).err().unwrap();
        guard.insert(1);
        let v = *cache.get_mut_or_guard(&1).ok().unwrap().unwrap();
        assert_eq!(v, 1);
        let result = cache.get_or_insert_with::<_, ()>(&0, || panic!());
        assert_eq!(result, Ok(Some(&0)));
        let result = cache.get_or_insert_with::<_, ()>(&1, || panic!());
        assert_eq!(result, Ok(Some(&1)));
        let result = cache.get_or_insert_with::<_, ()>(&3, || Ok(3));
        assert_eq!(result, Ok(Some(&3)));
        let result = cache.get_or_insert_with::<_, ()>(&4, || Err(()));
        assert_eq!(result, Err(()));
    }

    #[tokio::test]
    async fn test_get_or_insert_sync() {
        use crate::sync::*;
        let cache = sync::Cache::<u64, u64>::new(100);
        let GuardResult::Guard(guard) = cache.get_value_or_guard(&0, None) else {
            panic!();
        };
        guard.insert(0).unwrap();
        let GuardResult::Value(v) = cache.get_value_or_guard(&0, None) else {
            panic!();
        };
        assert_eq!(v, 0);
        let Err(guard) = cache.get_value_or_guard_async(&1).await else {
            panic!();
        };
        guard.insert(1).unwrap();
        let Ok(v) = cache.get_value_or_guard_async(&1).await else {
            panic!();
        };
        assert_eq!(v, 1);

        let result = cache.get_or_insert_with::<_, ()>(&0, || panic!());
        assert_eq!(result, Ok(0));
        let result = cache.get_or_insert_with::<_, ()>(&3, || Ok(3));
        assert_eq!(result, Ok(3));
        let result = cache.get_or_insert_with::<_, ()>(&4, || Err(()));
        assert_eq!(result, Err(()));
        let result = cache
            .get_or_insert_async::<_, ()>(&0, async { panic!() })
            .await;
        assert_eq!(result, Ok(0));
        let result = cache
            .get_or_insert_async::<_, ()>(&4, async { Err(()) })
            .await;
        assert_eq!(result, Err(()));
        let result = cache
            .get_or_insert_async::<_, ()>(&4, async { Ok(4) })
            .await;
        assert_eq!(result, Ok(4));
    }

    #[test]
    fn test_retain_unsync() {
        let mut cache = unsync::Cache::<u64, u64>::new(100);
        let ranges = 0..10;
        for i in ranges.clone() {
            let guard = cache.get_ref_or_guard(&i).unwrap_err();
            guard.insert(i);
            assert_eq!(cache.get_ref_or_guard(&i).ok().copied(), Some(i));
        }
        let small = 3;
        cache.retain(|&key, &val| val > small && key > small);
        for i in ranges.clone() {
            let actual = cache.get(&i);
            if i > small {
                assert!(actual.is_some());
                assert_eq!(*actual.unwrap(), i);
            } else {
                assert!(actual.is_none());
            }
        }
        let big = 7;
        cache.retain(|&key, &val| val < big && key < big);
        for i in ranges {
            let actual = cache.get(&i);
            if i > small && i < big {
                assert!(actual.is_some());
                assert_eq!(*actual.unwrap(), i);
            } else {
                assert!(actual.is_none());
            }
        }
    }

    #[tokio::test]
    async fn test_retain_sync() {
        use crate::sync::*;
        let cache = Cache::<u64, u64>::new(100);
        let ranges = 0..10;
        for i in ranges.clone() {
            let GuardResult::Guard(guard) = cache.get_value_or_guard(&i, None) else {
                panic!();
            };
            guard.insert(i).unwrap();
            let GuardResult::Value(v) = cache.get_value_or_guard(&i, None) else {
                panic!();
            };
            assert_eq!(v, i);
        }
        let small = 4;
        cache.retain(|&key, &val| val > small && key > small);
        for i in ranges.clone() {
            let actual = cache.get(&i);
            if i > small {
                assert!(actual.is_some());
                assert_eq!(actual.unwrap(), i);
            } else {
                assert!(actual.is_none());
            }
        }
        let big = 8;
        cache.retain(|&key, &val| val < big && key < big);
        for i in ranges {
            let actual = cache.get(&i);
            if i > small && i < big {
                assert!(actual.is_some());
                assert_eq!(actual.unwrap(), i);
            } else {
                assert!(actual.is_none());
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_value_or_guard() {
        use crate::sync::*;
        use rand::prelude::*;
        for _i in 0..2000 {
            dbg!(_i);
            let mut entered = AtomicUsize::default();
            let cache = sync::Cache::<(u64, u64), u64>::new(100);
            const THREADS: usize = 100;
            let wg = std::sync::Barrier::new(THREADS);
            let solve_at = rand::rng().random_range(0..THREADS);
            std::thread::scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        wg.wait();
                        loop {
                            match cache.get_value_or_guard(&(1, 1), Some(Duration::from_millis(1)))
                            {
                                GuardResult::Value(v) => assert_eq!(v, 1),
                                GuardResult::Guard(g) => {
                                    let before =
                                        entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    if before == solve_at {
                                        g.insert(1).unwrap();
                                    }
                                }
                                GuardResult::Timeout => continue,
                            }
                            break;
                        }
                    });
                }
            });
            assert_eq!(*entered.get_mut(), solve_at + 1);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(miri, ignore)]
    async fn test_get_or_insert_async() {
        use rand::prelude::*;
        for _i in 0..5000 {
            dbg!(_i);
            let entered = Arc::new(AtomicUsize::default());
            let cache = Arc::new(sync::Cache::<(u64, u64), u64>::new(100));
            const TASKS: usize = 100;
            let wg = Arc::new(tokio::sync::Barrier::new(TASKS));
            let solve_at = rand::rng().random_range(0..TASKS);
            let mut tasks = Vec::new();
            for _ in 0..TASKS {
                let cache = cache.clone();
                let wg = wg.clone();
                let entered = entered.clone();
                let task = tokio::spawn(async move {
                    wg.wait().await;
                    let result = cache
                        .get_or_insert_async(&(1, 1), async {
                            let before = entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if before == solve_at {
                                Ok(1)
                            } else {
                                Err(())
                            }
                        })
                        .await;
                    assert!(matches!(result, Ok(1) | Err(())));
                });
                tasks.push(task);
            }
            for task in tasks {
                task.await.unwrap();
            }
            assert_eq!(cache.get(&(1, 1)), Some(1));
            assert_eq!(
                entered.load(std::sync::atomic::Ordering::Relaxed),
                solve_at + 1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(miri, ignore)]
    async fn test_value_or_guard_async() {
        use rand::prelude::*;
        for _i in 0..5000 {
            dbg!(_i);
            let entered = Arc::new(AtomicUsize::default());
            let cache = Arc::new(sync::Cache::<(u64, u64), u64>::new(100));
            const TASKS: usize = 100;
            let wg = Arc::new(tokio::sync::Barrier::new(TASKS));
            let solve_at = rand::rng().random_range(0..TASKS);
            let mut tasks = Vec::new();
            for _ in 0..TASKS {
                let cache = cache.clone();
                let wg = wg.clone();
                let entered = entered.clone();
                let task = tokio::spawn(async move {
                    wg.wait().await;
                    loop {
                        match tokio::time::timeout(
                            Duration::from_millis(1),
                            cache.get_value_or_guard_async(&(1, 1)),
                        )
                        .await
                        {
                            Ok(Ok(r)) => assert_eq!(r, 1),
                            Ok(Err(g)) => {
                                let before =
                                    entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if before == solve_at {
                                    g.insert(1).unwrap();
                                }
                            }
                            Err(_) => continue,
                        }
                        break;
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                task.await.unwrap();
            }
            assert_eq!(cache.get(&(1, 1)), Some(1));
            assert_eq!(
                entered.load(std::sync::atomic::Ordering::Relaxed),
                solve_at + 1
            );
        }
    }
}
