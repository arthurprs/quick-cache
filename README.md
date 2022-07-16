# Quick Cache

Lightweight and high performance concurrent cache optimized for low cache overhead.

* Small overhead compared to a concurrent hash table
* Scan resistent and high hit rate caching policy
* Scales well with the number of threads
* Doesn't use background threads
* 100% safe code in the crate
* Small dependency tree

The implementation is optimized for use cases where the cache access times and overhead can add up to be a significant cost.
Features like: time to live, cost weighting, or event listeners; are not current implemented in Quick Cache.
If you need these features you may want to take a look at the [Moka](https://crates.io/crates/moka) or [Stretto](https://crates.io/crates/stretto) crates.

## Example

```rust
use quick_cache::sync::Cache;

fn main() {
    let cache = Cache::new(0, 5);
    cache.insert("square", "blue");
    cache.insert("circle", "black");
    assert_eq!(*cache.get(&"square").unwrap(), "blue");
    assert_eq!(*cache.get(&"circle").unwrap(), "black");
}
```

## Benchmarks

Since this crate is performance oriented it needs some comparisons.
That said, benchmarks can be misleading so take everything with a pinch of salt.

### Throughput

TODO

### Hit rates

TODO

## References

* [CLOCK-Pro: An Effective Improvement of the CLOCK Replacement. (PDF)](https://www.usenix.org/legacy/events/usenix05/tech/general/full_papers/jiang/jiang.pdf)
* [Caffeine (source code)](https://github.com/ben-manes/caffeine)
* [Cache2k (source code)](https://github.com/cache2k/cache2k)

## License

This project is licensed under the MIT license.
