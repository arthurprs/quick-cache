#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::{
    sync::{Cache, DefaultLifecycle},
    DefaultHashBuilder, OptionsBuilder, Weighter,
};
use std::num::NonZeroU32;

#[derive(Clone)]
struct MyWeighter;

impl Weighter<u16, ()> for MyWeighter {
    fn weight(&self, key: &u16, _val: &()) -> NonZeroU32 {
        (*key as u32).max(1).try_into().unwrap()
    }
}

fuzz_target!(|ops: Vec<u16>| {
    if ops.len() < 6 {
        return;
    }
    let estimated_items_capacity = ops[0] as usize;
    let weight_capacity = ops[0] as u64 * ops[1] as u64 * ops[2].min(1000) as u64;
    let hot_allocation = ops[3] as f64 / (u16::MAX as f64);
    let ghost_allocation = ops[4] as f64 / (u16::MAX as f64);
    let shards = (ops[5] as usize) % 10;
    let options = OptionsBuilder::new()
        .estimated_items_capacity(estimated_items_capacity)
        .weight_capacity(weight_capacity)
        .hot_allocation(hot_allocation)
        .ghost_allocation(ghost_allocation)
        .shards(shards)
        .build()
        .unwrap();
    let cache = Cache::with_options(
        options,
        MyWeighter,
        DefaultHashBuilder::default(),
        DefaultLifecycle::default(),
    );
    for op in &ops {
        if cache.get(op).is_none() {
            cache.insert(*op, ());
            cache.validate();
        }
    }
    cache.validate();
});
