use quick_cache::{sync::Cache, Weighter};

#[derive(Clone)]
struct StringWeighter;

impl Weighter<u64, String> for StringWeighter {
    fn weight(&self, _key: &u64, val: &String) -> u64 {
        // Be cautions out about zero weights!
        val.len() as u64
    }
}

fn main() {
    let cache = Cache::with_weighter(100, 100_000, StringWeighter);
    cache.insert(1, "1".to_string());
    cache.insert(54, "54".to_string());
    cache.insert(1000, "1000".to_string());
    assert_eq!(cache.get(&1000).unwrap(), "1000");
}
