use std::future::Future;
use std::task::Poll;

use crate::{
    shim::{
        sync::{self, atomic, Arc},
        thread,
    },
    sync::{EntryAction, EntryResult, GuardResult},
};

use shuttle::{
    future::spawn,
    rand::{self, Rng},
};

fn noop_waker(id: usize) -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable};
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(|data| RawWaker::new(data, &VTABLE), |_| {}, |_| {}, |_| {});
    unsafe { std::task::Waker::from_raw(RawWaker::new(id as *const (), &VTABLE)) }
}

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
    let solve_at = rand::thread_rng().gen_range(0..100);
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
                    _ = timeout_fut => {
                        if rand::thread_rng().gen_bool(0.1) {
                            cache.insert(0, 0);
                        }
                        continue;
                    },
                    cache_fut_res = cache_fut => cache_fut_res,
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
                let timeout = match rand::thread_rng().gen_range(0..3) {
                    0 => None,
                    1 => Some(std::time::Duration::default()),
                    _ => Some(std::time::Duration::from_millis(100)),
                };
                match cache.get_value_or_guard(&0, timeout) {
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

#[test]
fn test_waker_change_race() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    if let Ok(seed) = std::env::var("SEED") {
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_waker_change_race_stub);
    } else {
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(1000);
        let scheduler = shuttle::scheduler::RandomScheduler::new(max_iterations);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_waker_change_race_stub);
    }
}

fn test_waker_change_race_stub() {
    let cache = Arc::new(crate::sync::Cache::<u64, u64>::new(100));

    // Acquire a placeholder guard so the next async access becomes a waiter.
    let guard = match cache.get_value_or_guard(&0, None) {
        GuardResult::Guard(g) => g,
        _ => unreachable!(),
    };

    // Create the async future for the same key — will register as a waiter.
    let mut fut = std::pin::pin!(cache.get_value_or_guard_async(&0));

    // First poll with waker W1 → Pending (registered in waiters list).
    let w1 = noop_waker(1);
    let mut cx1 = std::task::Context::from_waker(&w1);
    assert!(fut.as_mut().poll(&mut cx1).is_pending());

    // Use a scoped thread so we can move the guard (which borrows cache)
    // into a separate thread while re-polling with a different waker.
    thread::scope(|s| {
        s.spawn(|| {
            let _ = guard.insert(42);
        });

        // Re-poll with a different waker W2 — exercises the will_wake() == false path.
        let w2 = noop_waker(2);
        let mut cx2 = std::task::Context::from_waker(&w2);
        loop {
            match fut.as_mut().poll(&mut cx2) {
                Poll::Ready(result) => {
                    assert_eq!(result.unwrap(), 42);
                    break;
                }
                Poll::Pending => {
                    shuttle::thread::yield_now();
                }
            }
        }
    });
}

#[test]
fn test_entry_works() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    let check_determinism = std::env::var("CHECK_DETERMINISM").is_ok_and(|s| !s.is_empty());
    if let Ok(seed) = std::env::var("SEED") {
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_entry_works_stub);
    } else {
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(1000);
        let scheduler = shuttle::scheduler::RandomScheduler::new(max_iterations);
        if check_determinism {
            let scheduler =
                shuttle::scheduler::UncontrolledNondeterminismCheckScheduler::new(scheduler);
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(test_entry_works_stub);
        } else {
            let runner = shuttle::Runner::new(scheduler, config);
            runner.run(test_entry_works_stub);
        }
    }
}

fn test_entry_works_stub() {
    shuttle::future::block_on(test_entry_works_stub_async())
}

async fn test_entry_works_stub_async() {
    const PAIRS: usize = 10;
    let entered_: Arc<atomic::AtomicUsize> = Arc::new(atomic::AtomicUsize::default());
    let cache_ = Arc::new(crate::sync::Cache::<u64, u64>::new(100));
    let wg = Arc::new(tokio::sync::Barrier::new(PAIRS));
    let sync_wg = Arc::new(sync::Barrier::new(PAIRS));
    let solve_at = rand::thread_rng().gen_range(0..100);
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
                let action = rand::thread_rng().gen_range(0..3u8);
                let cache_fut = std::pin::pin!(cache.entry_async(&0, move |_k, v| {
                    // Always keep the terminal value to ensure termination
                    if *v == 1 {
                        return EntryAction::Retain(*v);
                    }
                    match action {
                        0 => EntryAction::Retain(*v),
                        1 => EntryAction::Remove,
                        _ => EntryAction::ReplaceWithGuard,
                    }
                }));
                let cache_fut_res = tokio::select! {
                    biased;
                    _ = timeout_fut => {
                        if rand::thread_rng().gen_bool(0.1) {
                            cache.insert(0, 0);
                        }
                        continue;
                    },
                    result = cache_fut => result,
                };
                match cache_fut_res {
                    EntryResult::Retained(v) => {
                        if v == 1 {
                            break;
                        }
                        shuttle::future::yield_now().await;
                        if rand::thread_rng().gen_bool(0.5) {
                            cache.remove(&0);
                        }
                    }
                    EntryResult::Removed(_, _) => {
                        shuttle::future::yield_now().await;
                    }
                    EntryResult::Vacant(g) | EntryResult::Replaced(g, _) => {
                        shuttle::future::yield_now().await;
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before >= solve_at {
                            let _ = g.insert(1);
                        }
                    }
                    EntryResult::Timeout => unreachable!(),
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
                // note that the actual duration is ignored during shuttle tests
                let timeout = match rand::thread_rng().gen_range(0..3) {
                    0 => None,
                    1 => Some(std::time::Duration::default()),
                    _ => Some(std::time::Duration::from_millis(100)),
                };
                let action = rand::thread_rng().gen_range(0..3u8);
                match cache.entry(&0, timeout, |_k, v| {
                    if *v == 1 {
                        return EntryAction::Retain(*v);
                    }
                    match action {
                        0 => EntryAction::Retain(*v),
                        1 => EntryAction::Remove,
                        _ => EntryAction::ReplaceWithGuard,
                    }
                }) {
                    EntryResult::Retained(v) => {
                        if v == 1 {
                            break;
                        }
                        shuttle::thread::yield_now();
                        if rand::thread_rng().gen_bool(0.5) {
                            cache.remove(&0);
                        }
                    }
                    EntryResult::Removed(_, _) => {
                        shuttle::thread::yield_now();
                    }
                    EntryResult::Vacant(g) | EntryResult::Replaced(g, _) => {
                        shuttle::thread::yield_now();
                        let before = entered.fetch_add(1, atomic::Ordering::Relaxed);
                        if before >= solve_at {
                            let _ = g.insert(1);
                        }
                    }
                    EntryResult::Timeout => {
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
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(cache_.get(&0), Some(1));
}

#[test]
fn test_entry_waker_change_race() {
    let mut config = shuttle::Config::default();
    config.max_steps = shuttle::MaxSteps::None;
    if let Ok(seed) = std::env::var("SEED") {
        let seed = std::fs::read_to_string(&seed).unwrap_or(seed.clone());
        let scheduler = shuttle::scheduler::ReplayScheduler::new_from_encoded(&seed);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_entry_waker_change_race_stub);
    } else {
        let max_iterations: usize = std::env::var("MAX_ITERATIONS")
            .map(|s| s.parse().unwrap())
            .unwrap_or(1000);
        let scheduler = shuttle::scheduler::RandomScheduler::new(max_iterations);
        let runner = shuttle::Runner::new(scheduler, config);
        runner.run(test_entry_waker_change_race_stub);
    }
}

fn test_entry_waker_change_race_stub() {
    let cache = Arc::new(crate::sync::Cache::<u64, u64>::new(100));

    // Acquire a placeholder guard via entry() on a vacant key.
    let guard = match cache.entry(&0, None, |_k, _v| -> EntryAction<()> { unreachable!() }) {
        EntryResult::Vacant(g) => g,
        _ => unreachable!(),
    };

    // Create entry_async future — will find existing placeholder and wait.
    // When the value arrives, entry_async loops back and the callback runs.
    let mut fut = std::pin::pin!(cache.entry_async(&0, |_k, v| EntryAction::Retain(*v)));

    // First poll with waker W1 → Pending (registered in waiters list).
    let w1 = noop_waker(1);
    let mut cx1 = std::task::Context::from_waker(&w1);
    assert!(fut.as_mut().poll(&mut cx1).is_pending());

    // Scoped thread: insert value via guard while re-polling with different waker.
    thread::scope(|s| {
        s.spawn(|| {
            let _ = guard.insert(42);
        });

        // Re-poll with a different waker W2 — exercises the will_wake() == false path.
        let w2 = noop_waker(2);
        let mut cx2 = std::task::Context::from_waker(&w2);
        loop {
            match fut.as_mut().poll(&mut cx2) {
                Poll::Ready(result) => {
                    match result {
                        EntryResult::Retained(v) => assert_eq!(v, 42),
                        _ => panic!("expected EntryResult::Retained"),
                    }
                    break;
                }
                Poll::Pending => {
                    shuttle::thread::yield_now();
                }
            }
        }
    });
}
