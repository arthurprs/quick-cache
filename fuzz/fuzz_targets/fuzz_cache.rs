#![no_main]
use libfuzzer_sys::fuzz_target;
use quick_cache::{sync::Cache, Lifecycle, OptionsBuilder, Weighter};

#[derive(Clone)]
struct MyWeighter;

#[derive(Clone)]
struct MyLifecycle;

impl Weighter<u16, (u16, u16)> for MyWeighter {
    fn weight(&self, _key: &u16, val: &(u16, u16)) -> u32 {
        val.1 as u32
    }
}

impl Lifecycle<u16, (u16, u16)> for MyLifecycle {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {
        ()
    }

    fn before_evict(&self, _state: &mut Self::RequestState, _key: &u16, val: &mut (u16, u16)) {
        if val.0 % 5 == 0 {
            val.1 = 0;
        }
        // eprintln!("Before evict {_key} {val:?}");
    }

    fn on_evict(&self, _state: &mut Self::RequestState, _key: u16, _val: (u16, u16)) {
        // eprintln!("Evicted {_key}");
    }
}

fuzz_target!(|ops: Vec<u16>| {
    if ops.len() < 6 {
        return;
    }
    let hasher =
        ahash::RandomState::with_seeds(ops[0] as u64, ops[1] as u64, ops[2] as u64, ops[3] as u64);
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
    let cache = Cache::with_options(options, MyWeighter, hasher, MyLifecycle);
    for (i, op) in ops.iter().enumerate() {
        if i % 8 == 0 {
            // eprintln!("remove {op}");
            cache.remove(op);
            assert!(cache.get(op).is_none());
        } else if i % 10 == 0 {
            let v = i as u16;
            // eprintln!("replace {op} {v}");
            if cache.replace(*op, (v, v), false).is_ok() {
                let get = cache.get(op);
                assert!(get.is_none() || get.unwrap().0 == v);
            } else {
                assert!(cache.get(op).is_none());
            }
        } else {
            let v = i as u16;
            // eprintln!("insert {op} {v}");
            cache.insert(*op, (v, v));
            let get = cache.get(op);
            assert!(get.is_none() || get.unwrap().0 == v);
        }
    }
    cache.validate();
});
