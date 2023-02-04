# Quick Cache

Lightweight and high performance concurrent cache optimized for low cache overhead.

* Small overhead compared to a concurrent hash table
* Scan resistent and high hit rate caching policy
* User defined weight per item
* Scales well with the number of threads
* Doesn't use background threads
* One usage of unsafe, trivially verifiable
* Small dependency tree

The implementation is optimized for use cases where the cache access times and overhead can add up to be a significant cost.
Features like: time to live, event listeners and others; are not current implemented in Quick Cache.
If you need these features you may want to take a look at the [Moka](https://crates.io/crates/moka) crate.

## Examples

Basic usage

```rust
use quick_cache::unsync::Cache;

fn main() {
    let cache = Cache::new(5);
    cache.insert("square", "blue");
    cache.insert("circle", "black");
    assert_eq!(*cache.get(&"square").unwrap(), "blue");
    assert_eq!(*cache.get(&"circle").unwrap(), "black");
}
```

A cache with custom item weights. In this case according to the string length of the value.

```rust
use quick_cache::{Weighter, sync::Cache};

#[derive(Clone)]
struct StringWeighter;

impl Weighter<u64, (), String> for StringWeighter {
    fn weight(&self, _key: &u64, _qey: &(), val: &String) -> NonZeroU32 {
        NonZeroU32::new(val.len().clamp(1, u32::MAX as usize) as u32).unwrap()
    }
}

fn main() {
    let cache = Cache::with_weighter(100, 100_000, StringWeighter);
    cache.insert(1, "1".to_string());
    cache.insert(54, "54".to_string());
    cache.insert(1000, "1000".to_string());
    assert_eq!(*cache.get(&"1000").unwrap(), "1000");
}
```

Two keys variant (KQCache), which allows the caller to avoid making expensive keys on gets.

```rust
use quick_cache::sync::KQCache;

fn main() {
    let cache = KQCache::new(5);
    // Normally the cache key would be the tuple (String, u32), which could force
    // the caller to clone/allocate the string in order to form the tuple key.
    // That is not the case with the KQCache.
    cache.insert("square".to_string(), 2022, "blue");
    cache.insert("square".to_string(), 2023, "black");
    assert_eq!(cache.get("square", &2022).unwrap(), "blue");
}
```

## Benchmarks

Since this crate is performance oriented it needs some comparisons.
That said, benchmarks can be misleading so take everything with a pinch of salt.

Benchmarks performed with a modified version of [mokabench](https://github.com/arthurprs/mokabench/tree/reduce-overhead-add-quick-cache) in a x64 Linux OS + AMD 4900HS CPU system.

### Trace 1 (S3 from the Arc paper)

| Cache | Max Capacity | Clients | Hit Ratio | Duration Secs |
|---|---|---|---|---|
| QuickCache | 100000 | 1 | 12.763 | 7.959 |
| QuickCache | 100000 | 4 | 12.765 | 3.333 |
| QuickCache | 100000 | 8 | 12.764 | 2.785 |
| Moka Sync Cache | 100000 | 1 | 10.653 | 43.169 |
| Moka Sync Cache | 100000 | 4 | 10.504 | 44.138 |
| Moka Sync Cache | 100000 | 8 | 10.547 | 42.064 |
| Moka Dash Cache | 100000 | 1 | 10.594 | 29.46 |
| Moka Dash Cache | 100000 | 4 | 10.558 | 22.415 |
| Moka Dash Cache | 100000 | 8 | 10.573 | 21.411 |
| Moka SegmentedCache(8) | 100000 | 1 | 10.099 | 36.963 |
| Moka SegmentedCache(8) | 100000 | 4 | 10.158 | 19.809 |
| Moka SegmentedCache(8) | 100000 | 8 | 10.181 | 15.021 |
| QuickCache | 400000 | 1 | 42.066 | 10.25 |
| QuickCache | 400000 | 4 | 42.066 | 3.644 |
| QuickCache | 400000 | 8 | 42.066 | 3.171 |
| Moka Sync Cache | 400000 | 1 | 42.361 | 34.572 |
| Moka Sync Cache | 400000 | 4 | 41.137 | 32.692 |
| Moka Sync Cache | 400000 | 8 | 40.972 | 34.646 |
| Moka Dash Cache | 400000 | 1 | 41.487 | 24.216 |
| Moka Dash Cache | 400000 | 4 | 41.054 | 17.642 |
| Moka Dash Cache | 400000 | 8 | 41.106 | 17.443 |
| Moka SegmentedCache(8) | 400000 | 1 | 42.391 | 32.021 |
| Moka SegmentedCache(8) | 400000 | 4 | 42.369 | 19.14 |
| Moka SegmentedCache(8) | 400000 | 8 | 42.382 | 16.229 |
| QuickCache | 800000 | 1 | 66.804 | 11.022 |
| QuickCache | 800000 | 4 | 66.804 | 3.223 |
| QuickCache | 800000 | 8 | 66.804 | 2.753 |
| Moka Sync Cache | 800000 | 1 | 69.997 | 21.171 |
| Moka Sync Cache | 800000 | 4 | 65.976 | 18.575 |
| Moka Sync Cache | 800000 | 8 | 65.329 | 18.081 |
| Moka Dash Cache | 800000 | 1 | 70.119 | 15.257 |
| Moka Dash Cache | 800000 | 4 | 65.624 | 10.837 |
| Moka Dash Cache | 800000 | 8 | 65.843 | 10.172 |
| Moka SegmentedCache(8) | 800000 | 1 | 70.325 | 23.146 |
| Moka SegmentedCache(8) | 800000 | 4 | 70.318 | 12.466 |
| Moka SegmentedCache(8) | 800000 | 8 | 69.981 | 10.56 |

### Trace 2 (DS1 from the Arc paper)

| Cache | Max Capacity | Clients | Hit Ratio | Duration Secs |
|---|---|---|---|---|
| QuickCache | 1000000 | 1 | 15.042 | 26.685 |
| QuickCache | 1000000 | 4 | 15.068 | 9.55 |
| QuickCache | 1000000 | 8 | 15.061 | 6.435 |
| Moka Dash Cache | 1000000 | 1 | 14.264 | 77.287 |
| Moka Dash Cache | 1000000 | 4 | 12.909 | 79.323 |
| Moka Dash Cache | 1000000 | 8 | 12.654 | 74.825 |
| Moka Sync Cache | 1000000 | 1 | 15.793 | 158.375 |
| Moka Sync Cache | 1000000 | 4 | 13.11 | 152.436 |
| Moka Sync Cache | 1000000 | 8 | 12.984 | 152.025 |
| Moka SegmentedCache(8) | 1000000 | 1 | 14.968 | 139.847 |
| Moka SegmentedCache(8) | 1000000 | 4 | 14.945 | 90.105 |
| Moka SegmentedCache(8) | 1000000 | 8 | 15.34 | 80.393 |
| QuickCache | 4000000 | 1 | 44.57 | 24.924 |
| QuickCache | 4000000 | 4 | 44.587 | 9.245 |
| QuickCache | 4000000 | 8 | 44.544 | 5.611 |
| Moka Dash Cache | 4000000 | 1 | 44.484 | 50.966 |
| Moka Dash Cache | 4000000 | 4 | 39.112 | 44.108 |
| Moka Dash Cache | 4000000 | 8 | 36.784 | 44.211 |
| Moka Sync Cache | 4000000 | 1 | 44.158 | 104.393 |
| Moka Sync Cache | 4000000 | 4 | 37.699 | 106.608 |
| Moka Sync Cache | 4000000 | 8 | 39.013 | 103.128 |
| Moka SegmentedCache(8) | 4000000 | 1 | 45.322 | 91.684 |
| Moka SegmentedCache(8) | 4000000 | 4 | 45.32 | 62.892 |
| Moka SegmentedCache(8) | 4000000 | 8 | 44.975 | 56.42 |
| QuickCache | 8000000 | 1 | 71.127 | 16.265 |
| QuickCache | 8000000 | 4 | 71.139 | 5.35 |
| QuickCache | 8000000 | 8 | 71.147 | 3.488 |
| Moka Dash Cache | 8000000 | 1 | 68.285 | 32.656 |
| Moka Dash Cache | 8000000 | 4 | 64.365 | 25.279 |
| Moka Dash Cache | 8000000 | 8 | 63.948 | 24.945 |
| Moka Sync Cache | 8000000 | 1 | 67.654 | 43.469 |

### Trace 3 (OLTP from the Arc paper)

This is a much smaller dataset and since Moka Dash Cache and Sync Cache over-allocate several thousants of entries (internal write buffer) they are not comparable in this test.

| Cache | Max Capacity | Clients | Hit Ratio | Duration Secs |
|---|---|---|---|---|
| Moka Unsync Cache | 256 | 1 | 21.244 | 0.313 |
| QuickCache | 256 | 1 | 18.349 | 0.29 |
| Moka Unsync Cache | 512 | 1 | 28.207 | 0.317 |
| QuickCache | 512 | 1 | 27.099 | 0.268 |
| Moka Unsync Cache | 1000 | 1 | 34.2 | 0.326 |
| QuickCache | 1000 | 1 | 35.932 | 0.276 |
| Moka Unsync Cache | 2000 | 1 | 39.868 | 0.322 |
| QuickCache | 2000 | 1 | 42.405 | 0.276 |

## References

* [CLOCK-Pro: An Effective Improvement of the CLOCK Replacement. (PDF)](https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf)
* [Caffeine (source code)](https://github.com/ben-manes/caffeine)
* [Cache2k (source code)](https://github.com/cache2k/cache2k)

## License

This project is licensed under the MIT license.
