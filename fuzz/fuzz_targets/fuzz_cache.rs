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
    type RequestState = Vec<(u16, (u16, u16))>;

    fn begin_request(&self) -> Self::RequestState {
        Default::default()
    }

    fn before_evict(&self, _state: &mut Self::RequestState, _key: &u16, val: &mut (u16, u16)) {
        // eprintln!("Before evict {_key} {val:?}");
        if val.0 % 5 == 0 {
            val.1 = 0;
        }
    }

    fn on_evict(&self, state: &mut Self::RequestState, key: u16, val: (u16, u16)) {
        // eprintln!("Evicted {key}");
        state.push((key, val));
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
            if let Some((rem_k, _)) = cache.remove(op) {
                assert_eq!(rem_k, *op);
            }
            assert!(cache.get(op).is_none());
        } else if i % 10 == 0 {
            let v = i as u16;
            // eprintln!("replace {op} {v}");
            if let Ok(evicted) = cache.replace_with_lifecycle(*op, (v, v), false) {
                // if k is present it must have value v
                let get = cache.get(op);
                assert!(get.is_none() || get.unwrap().0 == v);
                check_evicted(*op, get, evicted);
            } else {
                assert!(cache.get(op).is_none());
            }
        } else {
            let v = i as u16;
            // eprintln!("insert {op} {v}");
            let evicted = cache.insert_with_lifecycle(*op, (v, v));
            // if k is present it must have value v
            let get = cache.get(op);
            assert!(get.is_none() || get.unwrap().0 == v);
            check_evicted(*op, get, evicted);
        }
    }
    cache.validate();
});

fn check_evicted(key: u16, get: Option<(u16, u16)>, evicted: Vec<(u16, (u16, u16))>) {
    let mut evicted_hm = ahash::HashSet::default();
    evicted_hm.reserve(evicted.len());
    for (k, (_, w)) in evicted {
        // we can't evict a 0 weight item, unless it was for the same key
        assert!(k == key || w != 0);
        // we can't evict something twice, except if the insert displaced an old old value but the new value also got evicted
        assert!(evicted_hm.insert(k) || (k == key && get.is_none()));
    }
}
