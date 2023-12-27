#![no_main]
use std::time::Duration;

use ahash::{HashMap, HashSet};
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use quick_cache::{sync::Cache, GuardResult, Lifecycle, OptionsBuilder, Weighter};

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

#[derive(Debug, Arbitrary)]
enum Op {
    Insert(u16, u16),
    Replace(u16, u16),
    Placeholder(u16),
    Remove(u16),
}

#[derive(Debug, Arbitrary)]
struct Input {
    ops: [u8; 6],
    operations: Vec<Op>,
}

fuzz_target!(|input: Input| {
    run(input);
});

fn run(input: Input) {
    let Input { ops, operations } = input;
    let hasher =
        ahash::RandomState::with_seeds(ops[0] as u64, ops[1] as u64, ops[2] as u64, ops[3] as u64);
    let estimated_items_capacity = ops[0] as usize;
    let weight_capacity = ops[0] as u64 * ops[1] as u64 * ops[2] as u64;
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
    let mut placeholders: HashMap<u16, _> = HashMap::default();
    for op in operations {
        match op {
            Op::Insert(k, v) => {
                // eprintln!("insert {k} {v}");
                placeholders.remove(&k);
                let evicted = cache.insert_with_lifecycle(k, (v, v));
                // if k is present it must have value v
                let get = cache.get(&k);
                assert!(get.is_none() || get.unwrap().0 == v);
                check_evicted(k, get, evicted);
            }
            Op::Replace(k, v) => {
                // eprintln!("replace {k} {v}");
                placeholders.remove(&k);
                if let Ok(evicted) = cache.replace_with_lifecycle(k, (v, v), false) {
                    // if k is present it must have value v
                    let get = cache.get(&k);
                    assert!(get.is_none() || get.unwrap().0 == v);
                    check_evicted(k, get, evicted);
                } else {
                    assert!(cache.get(&k).is_none());
                }
            }
            Op::Placeholder(k) => {
                // eprintln!("get_value_or_guard {k} {v}");
                match cache.get_value_or_guard(&k, Some(Duration::default())) {
                    GuardResult::Value(_gv) => {
                        // assert_eq!(gv.0, v);
                    }
                    GuardResult::Guard(g) => {
                        placeholders.insert(k, g);
                    }
                    GuardResult::Timeout => assert!(placeholders.contains_key(&k)),
                }
            }
            Op::Remove(k) => {
                // eprintln!("remove {k}");
                if let Some((rem_k, _)) = cache.remove(&k) {
                    placeholders.remove(&k);
                    assert_eq!(rem_k, k);
                }
                assert!(cache.get(&k).is_none());
            }
        }
    }
    cache.validate();
}

fn check_evicted(key: u16, get: Option<(u16, u16)>, evicted: Vec<(u16, (u16, u16))>) {
    let mut evicted_hm = HashSet::default();
    evicted_hm.reserve(evicted.len());
    for (ek, (_, ew)) in evicted {
        // we can't evict a 0 weight item, unless it was for the same key
        assert!(ew != 0 || ek == key);
        // we can't evict something twice, except if the insert displaced an old old value but the new value also got evicted
        assert!(evicted_hm.insert(ek) || (ek == key && get.is_none()));
    }
}
