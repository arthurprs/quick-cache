use quick_cache::{
    unsync::{Cache, DefaultLifecycle},
    DefaultHashBuilder, OptionsBuilder, Weighter,
};
#[derive(Clone)]
struct ValueWeight;
impl Weighter<u64, u64> for ValueWeight {
    fn weight(&self, _: &u64, value: &u64) -> u64 {
        *value
    }
}
fn check(hot: bool, replace: bool, head_item: bool) {
    let options = OptionsBuilder::new()
        .estimated_items_capacity(8)
        .weight_capacity(8)
        .hot_allocation(if hot { 1.0 } else { 0.5 })
        .build()
        .unwrap();
    let mut cache = Cache::with_options(
        options,
        ValueWeight,
        DefaultHashBuilder::default(),
        DefaultLifecycle::default(),
    );
    for key in 0..8u64 {
        cache.insert(key, 1);
    }
    let head = if hot { 0 } else { 4 };
    let key = if head_item { head } else { head + 2 };
    if replace {
        cache.replace(key, 0, true).unwrap();
    } else {
        *cache.peek_mut(&key).unwrap() = 0;
    }
    assert_eq!(cache.weight(), 7);
    cache.insert(8, 1);
    cache.insert(9, 1);
    let victim = if head_item { head + 1 } else { head };
    assert!(
        !cache.contains_key(&victim),
        "zeroing key {key} skips the earlier eviction candidate {victim}"
    );
    assert!(cache.contains_key(&key));
    assert_eq!(cache.weight(), 8);
}
#[test]
fn hot_nonhead_replace_preserves_clock_hand() {
    check(true, true, false)
}
#[test]
fn hot_nonhead_mutate_preserves_clock_hand() {
    check(true, false, false)
}
#[test]
fn cold_nonhead_replace_preserves_clock_hand() {
    check(false, true, false)
}
#[test]
fn cold_nonhead_mutate_preserves_clock_hand() {
    check(false, false, false)
}
#[test]
fn hot_head_replace_advances_clock_hand() {
    check(true, true, true)
}
#[test]
fn hot_head_mutate_advances_clock_hand() {
    check(true, false, true)
}
#[test]
fn cold_head_replace_advances_clock_hand() {
    check(false, true, true)
}
#[test]
fn cold_head_mutate_advances_clock_hand() {
    check(false, false, true)
}
