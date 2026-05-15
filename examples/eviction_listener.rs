use quick_cache::{sync::Cache, DefaultHashBuilder, Lifecycle, UnitWeighter};
use std::{sync::mpsc, thread};

#[derive(Debug, Clone)]
struct EvictionListener(mpsc::Sender<(u64, u64)>);

impl Lifecycle<u64, u64> for EvictionListener {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {}

    fn on_evict(&self, _state: &mut Self::RequestState, key: u64, val: u64) {
        let _ = self.0.send((key, val));
    }
}

fn main() {
    let (tx, rx) = mpsc::channel();

    let bg_thread = thread::spawn(move || {
        for evicted in rx {
            println!("Evicted {evicted:?}");
        }
    });

    let cache = Cache::with(
        100,
        100,
        UnitWeighter,
        DefaultHashBuilder::default(),
        EvictionListener(tx),
    );
    for i in 0u64..110 {
        cache.insert(i, i);
    }
    drop(cache);

    bg_thread.join().unwrap();
}
