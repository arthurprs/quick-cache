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
//! # Double keys or Versioned keys
//!
//! In addition to the standard `key->value` cache, a "versioned" cache `(key, version)->value` is also
//! available for cases where you want a cache keyed by a tuple like `(T, U)`. But due to limitations
//! of the `Borrow` trait you cannot access such keys without building the tuple and thus potentially
//! cloning `T` and/or `U`.
//!
//! # Hasher
//!
//! By default the crate uses [ahash](https://crates.io/crates/ahash), which is enabled (by default) via
//! a crate feature with the same name. If the `ahash` feature is disabled the crate defaults to the std lib
//! implementation instead (currently Siphash13). Note that a custom hasher can also be provided if desirable.

#[cfg(not(fuzzing))]
mod linked_slab;
#[cfg(fuzzing)]
pub mod linked_slab;
mod shard;
/// Concurrent cache variants that can be used from multiple threads.
pub mod sync;
/// Non-concurrent cache variants.
pub mod unsync;

#[cfg(feature = "ahash")]
type DefaultHashBuilder = ahash::RandomState;
#[cfg(not(feature = "ahash"))]
type DefaultHashBuilder = std::collections::hash_map::RandomState;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        sync::VersionedCache::<u64, u64, u64>::new(0, 0);
        sync::VersionedCache::<u64, u64, u64>::new(0, 1);
        sync::VersionedCache::<u64, u64, u64>::new(0, 2);
        sync::VersionedCache::<u64, u64, u64>::new(0, 3);
        sync::VersionedCache::<u64, u64, u64>::new(0, usize::MAX);
        sync::Cache::<u64, u64>::new(0, 0);
        sync::Cache::<u64, u64>::new(0, 1);
        sync::Cache::<u64, u64>::new(0, 2);
        sync::Cache::<u64, u64>::new(0, 3);
        sync::Cache::<u64, u64>::new(0, usize::MAX);
    }

    #[test]
    fn test_borrow_keys() {
        let cache = sync::VersionedCache::<Vec<u8>, Vec<u8>, u64>::new(0, 0);
        cache.get(&b""[..], &b""[..]);
        let cache = sync::VersionedCache::<String, String, u64>::new(0, 0);
        cache.get("", "");
    }
}
