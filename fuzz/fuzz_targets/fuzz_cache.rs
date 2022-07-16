#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::sync::VersionedCache;

fuzz_target!(|ops: Vec<u16>| {
    if ops.len() < 2 {
        return;
    }
    let initial_capacity = ops[0] as usize;
    let max_capacity = (ops[1] as usize).max(initial_capacity).max(2);
    let cache = VersionedCache::new(initial_capacity, max_capacity);
    for op in &ops {
        if cache.get(op, op).is_none() {
            cache.insert(*op, *op, ());
        }
    }
});
