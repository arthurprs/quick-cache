use crate::{
    shim::{
        sync::{self, atomic, Arc},
        thread,
    },
    GuardResult,
};
use std::{future::Future, task::Poll, time::Duration};

use shuttle::{
    future::spawn,
    rand::{self, Rng},
};

#[test]
fn test_guard_works() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    let check_determinism = std::env::var("CHECK_DETERMINISM").map_or(false, |s| !s.is_empty());
    if let Ok(seed) = std::env::var("SEED") {
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_guard_works_stub);
    } else {
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(100);
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
    let entered_ = Arc::new(atomic::AtomicUsize::default());
    let cache_ = Arc::new(crate::sync::Cache::<(u64, u64), u64>::new(100));
    const PAIRS: usize = 50;
    let wg = Arc::new(tokio::sync::Barrier::new(PAIRS));
    let sync_wg = Arc::new(sync::Barrier::new(PAIRS));
    let solve_at = rand::thread_rng().gen_range(0..PAIRS * 2);
    let mut tasks = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..PAIRS {
        let cache = cache_.clone();
        let wg = wg.clone();
        let entered = entered_.clone();
        let task = spawn(async move {
            wg.wait().await;
            loop {
                let mut countdown = rand::thread_rng().gen_range(0..1_000usize);
                let mut inner_fut = std::pin::pin!(cache.get_value_or_guard_async(&(1, 1)));
                let fut = std::future::poll_fn(move |ctx| match inner_fut.as_mut().poll(ctx) {
                    Poll::Pending if countdown == 0 => Poll::Ready(Err(())),
                    Poll::Pending => {
                        countdown -= 1;
                        Poll::Pending
                    }
                    Poll::Ready(r) => Poll::Ready(Ok(r)),
                });
                match fut.await {
                    Ok(Ok(r)) => assert_eq!(r, 1),
                    Ok(Err(g)) => {
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before == solve_at {
                            g.insert(1).unwrap();
                        }
                    }
                    Err(_) => continue,
                }
                break;
            }
        });
        tasks.push(task);

        let cache = cache_.clone();
        let wg = sync_wg.clone();
        let entered = entered_.clone();
        let thread = thread::spawn(move || {
            wg.wait();
            loop {
                match cache.get_value_or_guard(&(1, 1), Some(Duration::from_millis(1))) {
                    GuardResult::Value(v) => assert_eq!(v, 1),
                    GuardResult::Guard(g) => {
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before == solve_at {
                            g.insert(1).unwrap();
                        }
                    }
                    GuardResult::Timeout => continue,
                }
                break;
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
    assert_eq!(cache_.get(&(1, 1)), Some(1));
    assert_eq!(entered_.load(atomic::Ordering::Relaxed), solve_at + 1);
}
