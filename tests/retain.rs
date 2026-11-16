use quick_cache::unsync::Cache;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    panic::{catch_unwind, AssertUnwindSafe},
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}
struct CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() && TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        }
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() && TRACK.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn retain_removes_without_allocating_a_removal_list() {
    let mut cache = Cache::new(1000);
    for key in 0..1000u64 {
        cache.insert(key, key);
    }
    ALLOCS.set(0);
    TRACK.set(true);
    cache.retain(|_, _| false);
    TRACK.set(false);
    let allocations = ALLOCS.get();
    assert!(cache.is_empty());
    assert_eq!(allocations, 0, "retain allocates a temporary removal list");
}

#[test]
fn retain_visits_residents_once_and_keeps_matching_entries() {
    let mut cache = Cache::new(1000);
    for key in 0..1000u64 {
        cache.insert(key, key * 2);
    }
    let visited = std::cell::RefCell::new(std::collections::HashSet::new());
    cache.retain(|key, value| {
        assert_eq!(*value, *key * 2);
        assert!(visited.borrow_mut().insert(*key));
        key % 3 == 0
    });
    assert_eq!(visited.borrow().len(), 1000);
    assert_eq!(cache.len(), 334);
    for key in 0..1000u64 {
        assert_eq!(cache.peek(&key).is_some(), key % 3 == 0);
    }
    cache.insert(1001, 2002);
    assert_eq!(cache.get(&1001), Some(&2002));
}

#[test]
fn retain_remains_usable_after_predicate_panic() {
    let mut cache = Cache::new(1000);
    for key in 0..1000u64 {
        cache.insert(key, key);
    }
    let calls = Cell::new(0);
    assert!(catch_unwind(AssertUnwindSafe(|| cache.retain(|_, _| {
        calls.set(calls.get() + 1);
        assert!(calls.get() != 20, "predicate panic");
        false
    })))
    .is_err());
    let remaining = cache.len();
    assert_eq!(cache.iter().count(), remaining);
    cache.clear();
    cache.insert(42, 42);
    assert_eq!(cache.get(&42), Some(&42));
}

#[derive(Clone)]
struct ValueWeight;
impl quick_cache::Weighter<u64, u64> for ValueWeight {
    fn weight(&self, _: &u64, value: &u64) -> u64 {
        *value % 5
    }
}
#[derive(Default)]
struct Collide;
impl std::hash::Hasher for Collide {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}
#[test]
fn retain_handles_ghosts_zero_weights_and_hash_collisions() {
    use std::collections::BTreeMap;
    for cap in [0, 1, 2, 32] {
        for hot in [0.0, 0.5, 0.97, 1.0] {
            let options = quick_cache::OptionsBuilder::new()
                .estimated_items_capacity(32)
                .weight_capacity(cap)
                .hot_allocation(hot)
                .ghost_allocation(0.5)
                .build()
                .unwrap();
            let mut cache = Cache::with_options(
                options,
                ValueWeight,
                std::hash::BuildHasherDefault::<Collide>::default(),
                quick_cache::unsync::DefaultLifecycle::default(),
            );
            for key in 0..128u64 {
                cache.insert(key, key);
            }
            let expected = cache
                .iter()
                .filter(|(k, _)| **k % 2 == 0)
                .map(|(k, v)| (*k, *v))
                .collect::<BTreeMap<_, _>>();
            cache.retain(|key, _| key % 2 == 0);
            assert_eq!(
                cache
                    .iter()
                    .map(|(k, v)| (*k, *v))
                    .collect::<BTreeMap<_, _>>(),
                expected
            );
            assert_eq!(cache.len(), expected.len());
            assert_eq!(
                cache.weight(),
                expected.values().map(|v| v % 5).sum::<u64>()
            );
            cache.retain(|_, _| false);
            assert!(cache.is_empty());
            assert_eq!(cache.weight(), 0);
        }
    }
}
