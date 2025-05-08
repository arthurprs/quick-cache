# Quick Cache

[![Crates.io](https://img.shields.io/crates/v/quick_cache.svg)](https://crates.io/crates/quick_cache)
[![Docs](https://docs.rs/quick_cache/badge.svg)](https://docs.rs/quick_cache/latest)
[![CI](https://github.com/arthurprs/quick-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/arthurprs/quick-cache/actions/workflows/ci.yml)

Lightweight and high performance concurrent cache optimized for low cache overhead.

* Small overhead compared to a concurrent hash table
* Scan resistant and high hit rate caching policy (S3-FIFO)
* User defined weight per item
* Scales well with the number of threads
* Atomic operations with `get_or_insert` and `get_value_or_guard` functions
* Atomic async operations with `get_or_insert_async` and `get_value_or_guard_async` functions
* Supports item pinning
* Iteration and draining
* Handles zero weight items efficiently
* Allows for customizable lifecycle hooks (e.g. can be used to implement eviction listeners)
* Doesn't use background threads
* Only trivially verifiable usages of unsafe
* Small dependency tree

The implementation is optimized for use cases where the cache access times and overhead can add up to be a significant cost.
Features like: time to live, event listeners and others; are partially or not implemented in Quick Cache.
If you need these features you may want to take a look at the [Moka](https://crates.io/crates/moka) crate.

## Examples

Basic usage

```rust
use quick_cache::unsync::Cache;

fn main() {
    let mut cache = Cache::new(5);
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

impl Weighter<u64, String> for StringWeighter {
    fn weight(&self, _key: &u64, val: &String) -> u64 {
        // Be cautions out about zero weights!
        val.len() as u64
    }
}

fn main() {
    let cache = Cache::with_weighter(100, 100_000, StringWeighter);
    cache.insert(1, "1".to_string());
    cache.insert(54, "54".to_string());
    cache.insert(1000, "1000".to_string());
    assert_eq!(cache.get(&1000).unwrap(), "1000");
}
```

Using the `Equivalent` trait for complex keys

```rust
use quick_cache::{sync::Cache, Equivalent};

#[derive(Debug, Hash)]
pub struct Pair<A, B>(pub A, pub B);

impl<A, B, C, D> Equivalent<(C, D)> for Pair<A, B>
where
    A: PartialEq<C>,
    B: PartialEq<D>,
{
    fn equivalent(&self, rhs: &(C, D)) -> bool {
        self.0 == rhs.0 && self.1 == rhs.1
    }
}

fn main() {
    let cache: Cache<(String, i32), String> = Cache::new(5);
    cache.insert(("square".to_string(), 2022), "blue".to_string());
    cache.insert(("square".to_string(), 2023), "black".to_string());
    assert_eq!(cache.get(&Pair("square", 2022)).unwrap(), "blue");
}
```

## Benchmarks

Since this crate is performance oriented it needs some comparisons.
That said, benchmarks can be misleading so take everything with a pinch of salt.

Benchmarks performed with [mokabench](https://github.com/moka-rs/mokabench) in a x64 Linux OS + Intel i9-12900H CPU.

### Trace 1 (S3 from the Arc paper)

| Cache | Max Capacity | Clients | Inserts | Reads | Hit Ratio | Duration Secs |
|---|---|---|---|---|---|---|
| **QuickCache**  | 100000 | 1 | 14300769 | 16407702 | 12.841 | 2.196 |
| **QuickCache**  | 100000 | 3 | 14301124 | 16407702 | 12.839 | 1.279 |
| **QuickCache**  | 100000 | 6 | 14300809 | 16407702 | 12.841 | 0.798 |
| LRU+Mutex | 100000 | 1 | 16025830 | 16407702 | 2.327 | 2.422 |
| TinyUFO | 100000 | 1 | 15685351 | 16407702 | 4.403 | 11.641 |
| TinyUFO | 100000 | 3 | 15900217 | 16407702 | 3.093 | 8.828 |
| TinyUFO | 100000 | 6 | 15918936 | 16407702 | 2.979 | 8.104 |
| Mini Moka  | 100000 | 1 | 14695340 | 16407702 | 10.436 | 9.399 |
| Mini Moka  | 100000 | 3 | 14679119 | 16407702 | 10.535 | 8.490 |
| Mini Moka  | 100000 | 6 | 14706822 | 16407702 | 10.366 | 8.064 |
| **QuickCache**  | 400000 | 1 | 9435745 | 16407702 | 42.492 | 2.537 |
| **QuickCache**  | 400000 | 3 | 9437141 | 16407702 | 42.483 | 1.323 |
| **QuickCache**  | 400000 | 6 | 9436549 | 16407702 | 42.487 | 0.899 |
| LRU+Mutex | 400000 | 1 | 14432404 | 16407702 | 12.039 | 2.766 |
| TinyUFO | 400000 | 1 | 11455971 | 16407702 | 30.179 | 14.234 |
| TinyUFO | 400000 | 3 | 12405638 | 16407702 | 24.391 | 8.695 |
| TinyUFO | 400000 | 6 | 12551335 | 16407702 | 23.503 | 7.008 |
| Mini Moka  | 400000 | 1 | 9427172 | 16407702 | 42.544 | 8.511 |
| Mini Moka  | 400000 | 3 | 9584914 | 16407702 | 41.583 | 6.624 |
| Mini Moka  | 400000 | 6 | 9656084 | 16407702 | 41.149 | 6.613 |
| **QuickCache**  | 800000 | 1 | 5184786 | 16407702 | 68.400 | 3.207 |
| **QuickCache**  | 800000 | 3 | 5185210 | 16407702 | 68.398 | 1.353 |
| **QuickCache**  | 800000 | 6 | 5185337 | 16407702 | 68.397 | 0.743 |
| LRU+Mutex | 800000 | 1 | 7120978 | 16407702 | 56.600 | 2.613 |
| TinyUFO | 800000 | 1 | 5598215 | 16407702 | 65.881 | 10.043 |
| TinyUFO | 800000 | 3 | 6007956 | 16407702 | 63.383 | 5.077 |
| TinyUFO | 800000 | 6 | 6071806 | 16407702 | 62.994 | 3.789 |
| Mini Moka  | 800000 | 1 | 4872358 | 16407702 | 70.304 | 8.574 |
| Mini Moka  | 800000 | 3 | 5012764 | 16407702 | 69.449 | 5.363 |
| Mini Moka  | 800000 | 6 | 5529311 | 16407702 | 66.301 | 4.551 |

### Trace 2 (DS1 from the Arc paper)

| Cache | Max Capacity | Clients | Inserts | Reads | Hit Ratio | Duration Secs |
|---|---|---|---|---|---|---|
| **QuickCache**  | 1000000 | 1 | 37208683 | 43704979 | 14.864 | 9.883 |
| **QuickCache**  | 1000000 | 3 | 37196302 | 43704979 | 14.892 | 4.546 |
| **QuickCache**  | 1000000 | 6 | 37196402 | 43704979 | 14.892 | 3.585 |
| LRU+Mutex | 1000000 | 1 | 42356290 | 43704979 | 3.086 | 8.901 |
| TinyUFO | 1000000 | 1 | 42093670 | 43704979 | 3.687 | 60.135 |
| TinyUFO | 1000000 | 3 | 42203137 | 43704979 | 3.436 | 35.306 |
| TinyUFO | 1000000 | 6 | 42214889 | 43704979 | 3.409 | 25.440 |
| Mini Moka  | 1000000 | 1 | 37188446 | 43704979 | 14.910 | 25.833 |
| Mini Moka  | 1000000 | 3 | 37332123 | 43704979 | 14.582 | 23.078 |
| Mini Moka  | 1000000 | 6 | 38104018 | 43704979 | 12.815 | 23.571 |
| **QuickCache**  | 4000000 | 1 | 24156843 | 43704979 | 44.727 | 9.497 |
| **QuickCache**  | 4000000 | 3 | 24159591 | 43704979 | 44.721 | 4.257 |
| **QuickCache**  | 4000000 | 6 | 24204044 | 43704979 | 44.619 | 2.777 |
| LRU+Mutex | 4000000 | 1 | 34856997 | 43704979 | 20.245 | 10.735 |
| TinyUFO | 4000000 | 1 | 32607709 | 43704979 | 25.391 | 49.380 |
| TinyUFO | 4000000 | 3 | 32810383 | 43704979 | 24.928 | 26.440 |
| TinyUFO | 4000000 | 6 | 32840752 | 43704979 | 24.858 | 20.390 |
| Mini Moka  | 4000000 | 1 | 23863892 | 43704979 | 45.398 | 26.103 |
| Mini Moka  | 4000000 | 3 | 23865870 | 43704979 | 45.393 | 19.896 |
| Mini Moka  | 4000000 | 6 | 25152629 | 43704979 | 42.449 | 17.912 |
| **QuickCache**  | 8000000 | 1 | 13405437 | 43704979 | 69.327 | 7.703 |
| **QuickCache**  | 8000000 | 3 | 13406862 | 43704979 | 69.324 | 3.598 |
| **QuickCache**  | 8000000 | 6 | 13406780 | 43704979 | 69.324 | 2.243 |
| LRU+Mutex | 8000000 | 1 | 24896662 | 43704979 | 43.035 | 8.995 |
| TinyUFO | 8000000 | 1 | 20143204 | 43704979 | 53.911 | 28.245 |
| TinyUFO | 8000000 | 3 | 20339486 | 43704979 | 53.462 | 14.967 |
| TinyUFO | 8000000 | 6 | 20364270 | 43704979 | 53.405 | 11.206 |
| Mini Moka  | 8000000 | 1 | 14227354 | 43704979 | 67.447 | 29.082 |
| Mini Moka  | 8000000 | 3 | 13762562 | 43704979 | 68.510 | 17.060 |
| Mini Moka  | 8000000 | 6 | 14926093 | 43704979 | 65.848 | 12.294 |

### Trace 3 (SPC1 from the Arc paper)

| Cache | Max Capacity | Clients | Inserts | Reads | Hit Ratio | Duration Secs |
|---|---|---|---|---|---|---|
| **QuickCache**  | 500000 | 1 | 36994843 | 41351279 | 10.535 | 7.880 |
| LRU+Mutex | 500000 | 1 | 39942249 | 41351279 | 3.407 | 9.820 |
| TinyUFO | 500000 | 1 | 39197208 | 41351279 | 5.209 | 49.395 |
| Mini Moka  | 500000 | 1 | 36231669 | 41351279 | 12.381 | 25.467 |
| **QuickCache**  | 2000000 | 1 | 24123918 | 41351279 | 41.661 | 14.611 |
| LRU+Mutex | 2000000 | 1 | 30181071 | 41351279 | 27.013 | 10.768 |
| TinyUFO | 2000000 | 1 | 27173449 | 41351279 | 34.286 | 46.476 |
| Mini Moka  | 2000000 | 1 | 24032624 | 41351279 | 41.882 | 31.064 |
| **QuickCache**  | 30000000 | 1 | 6050363 | 41351279 | 85.368 | 8.170 |
| LRU+Mutex | 30000000 | 1 | 6050363 | 41351279 | 85.368 | 8.594 |
| TinyUFO | 30000000 | 1 | 6050363 | 41351279 | 85.368 | 14.065 |
| Mini Moka  | 30000000 | 1 | 6050363 | 41351279 | 85.368 | 34.452 |

Notes:

* LRU+Mutex: hashlink crate + std::sync::Mutex
* SPC1 only includes 1 client to save space, it follows the same trend as other traces.
* Other Moka variants not included to save space, they perform similarly or worse than Mini Moka. [Full results](https://github.com/arthurprs/quick-cache/issues/49#issuecomment-2323322103)

## References

* [CLOCK-Pro: An Effective Improvement of the CLOCK Replacement (pdf)](https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf)
* [FIFO queues are all you need for cache eviction (pdf)](https://dl.acm.org/doi/10.1145/3600006.3613147)
* [Caffeine (source code)](https://github.com/ben-manes/caffeine)
* [Cache2k (source code)](https://github.com/cache2k/cache2k)

## License

This project is licensed under the MIT license.
