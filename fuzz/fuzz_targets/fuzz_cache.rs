#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::sync::VersionedCache;
use quick_cache::{DefaultHashBuilder, Weighter};

#[derive(Clone)]
struct MyWeighter;

impl Weighter<u16, u16, ()> for MyWeighter {
    fn weight(&self, key: &u16, _ver: &u16, _val: &()) -> u32 {
        (*key).max(1u16) as _
    }
}

fuzz_target!(|ops: Vec<u16>| {
    if ops.len() < 2 {
        return;
    }
    let initial_capacity = ops[0] as usize * 1000;
    let max_capacity = (ops[1] as usize).max(initial_capacity).max(2) * 1000;
    let cache = VersionedCache::with(
        initial_capacity,
        max_capacity,
        MyWeighter,
        DefaultHashBuilder::default(),
    );
    for op in &ops {
        if cache.get(op, op).is_none() {
            cache.insert(*op, *op, ());
        }
    }
});
