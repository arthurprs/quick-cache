# Quick Cache

Lightweight and high performance concurrent cache optimized for low cache overhead.

* Small overhead compared to a concurrent hash table
* Scan resistent and high hit rate caching policy
* User defined weight per item
* Scales well with the number of threads
* Atomic operations with `get_or_insert` and `get_value_or_guard` functions
* Atomic async operations with `get_or_insert_async` and `get_value_or_guard_async` functions
* Doesn't use background threads
* One trivially verifiable usage of unsafe
* Small dependency tree

The implementation is optimized for use cases where the cache access times and overhead can add up to be a significant cost.
Features like: time to live, iterators, event listeners and others; are not currently implemented in Quick Cache.
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

impl Weighter<u64, String> for StringWeighter {
    fn weight(&self, _key: &u64, val: &String) -> u32 {
        // Be cautions out about zero weights!
        val.len().clamp(1, u32::MAX as usize) as u32
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

Using the `Equivalent` trait for complex keys

```rust
use std::hash::Hash;
use quick_cache::{Equivalent, sync::Cache};

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

fn main() {
    let cache = Cache::new(5);
    cache.insert(("square".to_string(), 2022), "blue");
    // Normally the cache key would be the tuple (String, u32), which could force
    // the caller to clone/allocate the string in order to form the tuple key.
    assert_eq!(cache.get(&Pair("square", 2022)).unwrap(), "blue");
}
```

## Benchmarks

Since this crate is performance oriented it needs some comparisons.
That said, benchmarks can be misleading so take everything with a pinch of salt.

Benchmarks performed with [mokabench](https://github.com/moka-rs/mokabench) in a x64 Linux OS + Intel i9-12900H CPU.

### Trace 1 (S3 from the Arc paper)

| Cache                   | Max Capacity | Clients | Hit Ratio(%) | Elapsed time(s) |
|-------------------------|--------------|---------|--------------|-----------------|
| HashLink (LRU w/ Mutex) |  100000      |  1      |  2.327       |  2.479          |
| HashLink (LRU w/ Mutex) |  100000      |  3      |  2.327       |  6.171          |
| HashLink (LRU w/ Mutex) |  100000      |  6      |  2.328       |  12.467         |
| **QuickCache Sync Cache**   |  100000      |  1      |  12.764      |  2.465          |
| **QuickCache Sync Cache**   |  100000      |  3      |  12.765      |  1.387          |
| **QuickCache Sync Cache**   |  100000      |  6      |  12.764      |  0.835          |
| Mini Moka Sync Cache    |  100000      |  1      |  10.436      |  10.473         |
| Mini Moka Sync Cache    |  100000      |  3      |  10.541      |  8.439          |
| Mini Moka Sync Cache    |  100000      |  6      |  10.366      |  8.008          |
| HashLink (LRU w/ Mutex) |  400000      |  1      |  12.039      |  2.956          |
| HashLink (LRU w/ Mutex) |  400000      |  3      |  12.041      |  6.939          |
| HashLink (LRU w/ Mutex) |  400000      |  6      |  12.043      |  13.503         |
| **QuickCache Sync Cache**   |  400000      |  1      |  42.044      |  3.250          |
| **QuickCache Sync Cache**   |  400000      |  3      |  42.044      |  1.299          |
| **QuickCache Sync Cache**   |  400000      |  6      |  42.044      |  0.777          |
| Mini Moka Sync Cache    |  400000      |  1      |  42.544      |  9.671          |
| Mini Moka Sync Cache    |  400000      |  3      |  41.525      |  7.052          |
| Mini Moka Sync Cache    |  400000      |  6      |  41.007      |  6.618          |
| HashLink (LRU w/ Mutex) |  800000      |  1      |  56.600      |  3.221          |
| HashLink (LRU w/ Mutex) |  800000      |  3      |  56.603      |  5.921          |
| HashLink (LRU w/ Mutex) |  800000      |  6      |  56.605      |  12.768         |
| **QuickCache Sync Cache**   |  800000      |  1      |  66.801      |  3.980          |
| **QuickCache Sync Cache**   |  800000      |  3      |  66.802      |  1.418          |
| **QuickCache Sync Cache**   |  800000      |  6      |  66.803      |  0.798          |
| Mini Moka Sync Cache    |  800000      |  1      |  70.304      |  10.613         |
| Mini Moka Sync Cache    |  800000      |  3      |  68.054      |  4.754          |
| Mini Moka Sync Cache    |  800000      |  6      |  66.288      |  4.137          |

### Trace 2 (DS1 from the Arc paper)

| Cache                   | Max Capacity | Clients |  Hit Ratio(%) |  Elapsed time(s) |
|-------------------------|--------------|---------|---------------|------------------|
| HashLink (LRU w/ Mutex) |  1000000     |  1      |  3.086        |  10.097          |
| HashLink (LRU w/ Mutex) |  1000000     |  3      |  3.085        |  22.443          |
| HashLink (LRU w/ Mutex) |  1000000     |  6      |  3.082        |  41.351          |
| **QuickCache Sync Cache**   |  1000000     |  1      |  15.052       |  9.426           |
| **QuickCache Sync Cache**   |  1000000     |  3      |  15.059       |  4.139           |
| **QuickCache Sync Cache**   |  1000000     |  6      |  15.078       |  2.467           |
| Mini Moka Sync Cache    |  1000000     |  1      |  14.910       |  28.974          |
| Mini Moka Sync Cache    |  1000000     |  3      |  13.257       |  24.681          |
| Mini Moka Sync Cache    |  1000000     |  6      |  13.518       |  22.759          |
| HashLink (LRU w/ Mutex) |  4000000     |  1      |  20.245       |  9.863           |
| HashLink (LRU w/ Mutex) |  4000000     |  3      |  20.250       |  19.418          |
| HashLink (LRU w/ Mutex) |  4000000     |  6      |  20.259       |  36.932          |
| **QuickCache Sync Cache**   |  4000000     |  1      |  44.565       |  9.083           |
| **QuickCache Sync Cache**   |  4000000     |  3      |  44.574       |  4.272           |
| **QuickCache Sync Cache**   |  4000000     |  6      |  44.569       |  2.449           |
| Mini Moka Sync Cache    |  4000000     |  1      |  45.398       |  31.108          |
| Mini Moka Sync Cache    |  4000000     |  3      |  44.236       |  21.381          |
| Mini Moka Sync Cache    |  4000000     |  6      |  41.723       |  18.719          |
| HashLink (LRU w/ Mutex) |  8000000     |  1      |  43.035       |  9.751           |
| HashLink (LRU w/ Mutex) |  8000000     |  3      |  43.034       |  16.554          |
| HashLink (LRU w/ Mutex) |  8000000     |  6      |  43.031       |  34.978          |
| **QuickCache Sync Cache**   |  8000000     |  1      |  71.124       |  7.675           |
| **QuickCache Sync Cache**   |  8000000     |  3      |  71.139       |  3.198           |
| **QuickCache Sync Cache**   |  8000000     |  6      |  71.143       |  2.348           |
| Mini Moka Sync Cache    |  8000000     |  1      |  67.447       |  28.462          |
| Mini Moka Sync Cache    |  8000000     |  3      |  67.995       |  17.054          |
| Mini Moka Sync Cache    |  8000000     |  6      |  64.738       |  12.929          |

## References

* [CLOCK-Pro: An Effective Improvement of the CLOCK Replacement. (PDF)](https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf)
* [Caffeine (source code)](https://github.com/ben-manes/caffeine)
* [Cache2k (source code)](https://github.com/cache2k/cache2k)

## License

This project is licensed under the MIT license.
