use crate::{
    shim::{
        sync::{self, atomic, Arc},
        thread,
    },
    sync::GuardResult,
};

use shuttle::{
    future::spawn,
    rand::{self, Rng},
};

#[test]
fn test_guard_works() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    let check_determinism = std::env::var("CHECK_DETERMINISM").is_ok_and(|s| !s.is_empty());
    if let Ok(seed) = std::env::var("SEED") {
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_guard_works_stub);
    } else {
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(1000);
        let scheduler = shuttle::scheduler::RandomScheduler::new(max_iterations);
        if check_determinism {
            let scheduler =
                shuttle::scheduler::UncontrolledNondeterminismCheckScheduler::new(scheduler);
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(test_guard_works_stub);
        } else {
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(test_guard_works_stub);
        }
    }
}

fn test_guard_works_stub() {
    shuttle::future::block_on(test_guard_works_stub_async())
}

async fn test_guard_works_stub_async() {
    const PAIRS: usize = 10;
    let entered_: Arc<atomic::AtomicUsize> = Arc::new(atomic::AtomicUsize::default());
    let cache_ = Arc::new(crate::sync::Cache::<u64, u64>::new(100));
    let wg = Arc::new(tokio::sync::Barrier::new(PAIRS));
    let sync_wg = Arc::new(sync::Barrier::new(PAIRS));
    let solve_at = rand::thread_rng().gen_range(0..PAIRS * 3);
    let mut tasks = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..PAIRS {
        let cache = cache_.clone();
        let wg = wg.clone();
        let entered = entered_.clone();
        let task = spawn(async move {
            wg.wait().await;
            loop {
                let yields = rand::thread_rng().gen_range(0..PAIRS * 2);
                // a dummy timeout like future to race with the cache future in a select
                let timeout_fut = std::pin::pin!(async {
                    for _ in 0..yields {
                        shuttle::future::yield_now().await;
                    }
                });
                let cache_fut = std::pin::pin!(cache.get_value_or_guard_async(&0));
                let cache_fut_res = tokio::select! {
                    // biased is important for determinism
                    biased;
                    cache_fut_res = cache_fut => cache_fut_res,
                    _ = timeout_fut => {
                        if rand::thread_rng().gen_bool(0.1) {
                            cache.insert(0, 0);
                        }
                        continue;
                    },
                };
                match cache_fut_res {
                    Ok(v) => {
                        if v == 1 {
                            break;
                        }
                        shuttle::future::yield_now().await;
                        if rand::thread_rng().gen_bool(0.5) {
                            cache.remove(&0);
                        }
                    }
                    Err(g) => {
                        shuttle::future::yield_now().await;
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before >= solve_at {
                            let _ = g.insert(1);
                        }
                    }
                }
            }
        });
        tasks.push(task);

        let cache = cache_.clone();
        let wg = sync_wg.clone();
        let entered = entered_.clone();
        let thread = thread::spawn(move || {
            wg.wait();
            loop {
                // node that the actual duration is ignored during shuttle tests
                match cache.get_value_or_guard(&0, Some(Default::default())) {
                    GuardResult::Value(v) => {
                        if v == 1 {
                            break;
                        }
                        shuttle::thread::yield_now();
                        if rand::thread_rng().gen_bool(0.5) {
                            cache.remove(&0);
                        }
                    }
                    GuardResult::Guard(g) => {
                        shuttle::thread::yield_now();
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before >= solve_at {
                            let _ = g.insert(1);
                        }
                    }
                    GuardResult::Timeout => {
                        if rand::thread_rng().gen_bool(0.1) {
                            cache.insert(0, 0);
                        }
                    }
                }
            }
        });
        threads.push(thread);
    }
    for task in tasks {
        task.await.unwrap();
    }
    for task in threads {
        task.join().unwrap()
    }
    assert_eq!(cache_.get(&0), Some(1));
}
