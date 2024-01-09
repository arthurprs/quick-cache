use quick_cache::{sync::Cache, Equivalent};

#[derive(Debug, Hash)]
pub struct Pair<A, B>(pub A, pub B);

impl<A, B, C, D> Equivalent<(C, D)> for Pair<A, B>
where
    A: PartialEq<C>,
    B: PartialEq<D>,
{
    fn equivalent(&self, rhs: &(C, D)) -> bool {
        self.0 == rhs.0 && self.1 == rhs.1
    }
}

fn main() {
    let cache: Cache<(String, i32), String> = Cache::new(5);
    cache.insert(("square".to_string(), 2022), "blue".to_string());
    cache.insert(("square".to_string(), 2023), "black".to_string());
    assert_eq!(cache.get(&Pair("square", 2022)).unwrap(), "blue");
}
