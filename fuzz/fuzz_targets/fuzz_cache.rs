#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::sync::KQCache;
use quick_cache::{DefaultHashBuilder, Weighter};
use std::num::NonZeroU32;

#[derive(Clone)]
struct MyWeighter;

impl Weighter<u16, u16, ()> for MyWeighter {
    fn weight(&self, key: &u16, _qey: &u16, _val: &()) -> NonZeroU32 {
        (*key as u32).max(1).try_into().unwrap()
    }
}

fuzz_target!(|ops: Vec<u16>| {
    if ops.len() < 3 {
        return;
    }
    let estimated_items_capacity = ops[0] as usize;
    let max_capacity = ops[0] as u64 * ops[1] as u64 * ops[2].min(1000) as u64;
    let cache = KQCache::with(
        estimated_items_capacity,
        max_capacity,
        MyWeighter,
        DefaultHashBuilder::default(),
    );
    for op in &ops {
        if cache.get(op, op).is_none() {
            cache.insert(*op, *op, ());
            cache.validate();
        }
    }
    cache.validate();
});
