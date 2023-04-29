//! Lightweight, high performance concurrent cache. It allows very fast access to the cached items
//! with little overhead compared to a plain concurrent hash table. No allocations are ever performed
//! unless the cache internal state table needs growing (which will eventually stabilize).
//!
//! # Eviction policy
//!
//! The current eviction policy is a modified version of the Clock-PRO algorithm. It's "scan resistent"
//! and provides high hit rates, significantly better than a LRU eviction policy and comparable to
//! other state-of-the art algorithms like W-TinyLFU.
//!
//! # Thread safety and Concurrency
//!
//! Both `sync` (thread-safe) and `unsync` (non thread-safe) implementations are provided. The latter
//! offers slightly better performance when thread safety is not required.
//!
//! # Two keys or QK keys
//!
//! In addition to the standard `key->value` cache, a "two keys" cache `(key, qey)->value` is also
//! available for cases where you want a cache keyed by a tuple like `(K, Q)`. But due to limitations
//! of the `Borrow` trait you cannot access such keys without building the tuple and thus potentially
//! cloning `K` and/or `Q`.
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

use std::num::NonZeroU32;

#[cfg(loom)]
pub(crate) use loom::sync as std_sync;
#[cfg(loom)]
pub(crate) use loom::thread as std_thread;
#[cfg(not(loom))]
pub(crate) use std::sync as std_sync;
#[cfg(not(loom))]
pub(crate) use std::thread as std_thread;

#[cfg(not(fuzzing))]
mod linked_slab;
#[cfg(fuzzing)]
pub mod linked_slab;
#[cfg(not(fuzzing))]
mod options;
#[cfg(fuzzing)]
pub mod options;
mod placeholder;
mod rw_lock;
mod shard;
/// Concurrent cache variants that can be used from multiple threads.
pub mod sync;
/// Non-concurrent cache variants.
pub mod unsync;

pub use options::{Options, OptionsBuilder};
pub use placeholder::{GuardResult, PlaceholderGuard};

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
/// use std::num::NonZeroU32;
///
/// #[derive(Clone)]
/// struct StringWeighter;
///
/// impl Weighter<u64, (), String> for StringWeighter {
///     fn weight(&self, _key: &u64, _qey: &(), val: &String) -> NonZeroU32 {
///         NonZeroU32::new(val.len().clamp(1, u32::MAX as usize) as u32).unwrap()
///     }
/// }
///
/// let cache = Cache::with_weighter(100, 100_000, StringWeighter);
/// cache.insert(1, "1".to_string());
/// ```
pub trait Weighter<Key, Qey, Val> {
    /// Returns the weight of the cache item.
    /// Note that this it's undefined behavior for a cache item to change its weight.
    ///
    /// For performance reasons this function should be trivially cheap as
    /// it's called during the cache eviction routine.
    /// If weight is expensive to calculate, consider caching it alongside the value.
    fn weight(&self, key: &Key, qey: &Qey, val: &Val) -> NonZeroU32;
}

/// Each cache entry weights exactly `1` unit of weight.
#[derive(Debug, Clone)]
pub struct UnitWeighter;

impl<Key, Qey, Val> Weighter<Key, Qey, Val> for UnitWeighter {
    #[inline]
    fn weight(&self, _key: &Key, _qey: &Qey, _val: &Val) -> NonZeroU32 {
        NonZeroU32::new(1).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use super::*;

    #[test]
    fn test_new() {
        sync::KQCache::<u64, u64, u64>::new(0);
        sync::KQCache::<u64, u64, u64>::new(1);
        sync::KQCache::<u64, u64, u64>::new(2);
        sync::KQCache::<u64, u64, u64>::new(3);
        sync::KQCache::<u64, u64, u64>::new(usize::MAX);
        sync::Cache::<u64, u64>::new(0);
        sync::Cache::<u64, u64>::new(1);
        sync::Cache::<u64, u64>::new(2);
        sync::Cache::<u64, u64>::new(3);
        sync::Cache::<u64, u64>::new(usize::MAX);
    }

    #[test]
    fn test_custom_cost() {
        #[derive(Clone)]
        struct StringWeighter;

        impl Weighter<u64, (), String> for StringWeighter {
            fn weight(&self, _key: &u64, _qey: &(), val: &String) -> NonZeroU32 {
                NonZeroU32::new(val.len().clamp(1, u32::MAX as usize) as u32).unwrap()
            }
        }

        let cache = sync::Cache::with_weighter(100, 100_000, StringWeighter);
        cache.insert(1, "1".to_string());
        cache.insert(54, "54".to_string());
        cache.insert(1000, "1000".to_string());
        assert_eq!(cache.get(&1000).unwrap(), "1000");
    }

    #[test]
    fn test_kq() {
        let mut cache = unsync::KQCache::new(5);
        cache.insert("square".to_string(), 2022, "blue".to_string());
        cache.insert("square".to_string(), 2023, "black".to_string());
        assert_eq!(cache.get("square", &2022).unwrap(), "blue");
    }

    #[test]
    fn test_borrow_keys() {
        let cache = sync::KQCache::<Vec<u8>, Vec<u8>, u64>::new(0);
        cache.get(&b""[..], &b""[..]);
        let cache = sync::KQCache::<String, String, u64>::new(0);
        cache.get("", "");
    }

    #[test]
    fn test_get_or_insert() {
        use rand::prelude::*;
        for _i in 0..2000 {
            dbg!(_i);
            let mut entered = AtomicUsize::default();
            let cache = sync::KQCache::<u64, u64, u64>::new(100);
            const THREADS: usize = 100;
            let wg = std::sync::Barrier::new(THREADS);
            let solve_at = rand::thread_rng().gen_range(0..THREADS);
            std::thread::scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        wg.wait();
                        let result = cache.get_or_insert_with(&1, &1, || {
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

    #[cfg(loom)]
    #[test]
    fn test_value_or_guard_loom() {
        use crate::{
            std_sync::{
                atomic::{AtomicUsize, Ordering},
                Arc, Barrier,
            },
            std_thread as thread,
        };

        fn fun_name(cache: Arc<sync::KQCache<u64, u64, u64>>, entered: Arc<AtomicUsize>, solve_at: usize) {
            loop {
                match cache.get_value_or_guard(&1, &1, None) {
                    GuardResult::Value(v) => assert_eq!(v, 1),
                    GuardResult::Guard(g) => {
                        let before = entered.fetch_add(1, Ordering::Relaxed);
                        if before == solve_at {
                            g.insert(1);
                        }
                    }
                    GuardResult::Timeout => continue,
                }
                break;
            }
        }


        use rand::prelude::*;
        const THREADS: usize = 3;
        let solve_at = 2;
        // let rng = rand::rngs::SmallRng::from_entropy();
        loom::model(move || {
            let entered = Arc::new(AtomicUsize::default());
            let cache = Arc::new(sync::KQCache::<u64, u64, u64>::new(100));
            let wg: Arc<rw_lock::RwLock<()>> = Arc::new(rw_lock::RwLock::new(()));
            let wg_lock = wg.write();
            let mut threads = Vec::new();
            for _ in 0..THREADS {
                let solve_at = solve_at;
                let entered = entered.clone();
                let cache = cache.clone();
                let wg = wg.clone();
                let thread = thread::spawn(move || {
                    wg.read();
                    fun_name(cache, entered, solve_at);
                });
                threads.push(thread);
            }
            drop(wg_lock);
            fun_name(cache, entered.clone(), solve_at);
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(entered.load(Ordering::SeqCst), solve_at + 1);
        });
    }


    #[test]
    fn test_value_or_guard() {
        use rand::prelude::*;
        for _i in 0..2000 {
            dbg!(_i);
            let mut entered = AtomicUsize::default();
            let cache = sync::KQCache::<u64, u64, u64>::new(100);
            const THREADS: usize = 100;
            let wg = std::sync::Barrier::new(THREADS);
            let solve_at = rand::thread_rng().gen_range(0..THREADS);
            std::thread::scope(|s| {
                for _ in 0..THREADS {
                    s.spawn(|| {
                        wg.wait();
                        loop {
                            match cache.get_value_or_guard(&1, &1, Some(Duration::from_millis(1))) {
                                GuardResult::Value(v) => assert_eq!(v, 1),
                                GuardResult::Guard(g) => {
                                    let before =
                                        entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    if before == solve_at {
                                        g.insert(1);
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
    async fn test_get_or_insert_async() {
        use rand::prelude::*;
        for _i in 0..5000 {
            dbg!(_i);
            let entered = Arc::new(AtomicUsize::default());
            let cache = Arc::new(sync::KQCache::<u64, u64, u64>::new(100));
            const TASKS: usize = 100;
            let wg = Arc::new(tokio::sync::Barrier::new(TASKS));
            let solve_at = rand::thread_rng().gen_range(0..TASKS);
            let mut tasks = Vec::new();
            for _ in 0..TASKS {
                let cache = cache.clone();
                let wg = wg.clone();
                let entered = entered.clone();
                let task = tokio::spawn(async move {
                    wg.wait().await;
                    let result = cache
                        .get_or_insert_async(&1, &1, async {
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
            assert_eq!(cache.get(&1, &1), Some(1));
            assert_eq!(
                entered.load(std::sync::atomic::Ordering::Relaxed),
                solve_at + 1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_value_or_guard_async() {
        use rand::prelude::*;
        for _i in 0..5000 {
            dbg!(_i);
            let entered = Arc::new(AtomicUsize::default());
            let cache = Arc::new(sync::KQCache::<u64, u64, u64>::new(100));
            const TASKS: usize = 100;
            let wg = Arc::new(tokio::sync::Barrier::new(TASKS));
            let solve_at = rand::thread_rng().gen_range(0..TASKS);
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
                            cache.get_value_or_guard_async(&1, &1),
                        )
                        .await
                        {
                            Ok(Ok(r)) => assert_eq!(r, 1),
                            Ok(Err(g)) => {
                                let before =
                                    entered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if before == solve_at {
                                    g.insert(1);
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
            assert_eq!(cache.get(&1, &1), Some(1));
            assert_eq!(
                entered.load(std::sync::atomic::Ordering::Relaxed),
                solve_at + 1
            );
        }
    }
}
