use criterion::{criterion_group, criterion_main, Criterion};
use quick_cache::sync::Cache;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use tokio::sync::Barrier;

fn placeholder_async_contention_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("placeholder_async_contention");

    // Number of iterations each task will perform
    const ITERATIONS: usize = 100;

    // Test various contention scenarios
    for num_tasks in [4, 8, 16, 32] {
        group.bench_function(format!("{}", num_tasks), |b| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let cache = Arc::new(Cache::new(1000));
                    let barrier = Arc::new(Barrier::new(num_tasks));
                    let mut handles = vec![];

                    // Spawn tasks that will perform multiple iterations
                    for _ in 0..num_tasks {
                        let cache = cache.clone();
                        let barrier = barrier.clone();
                        let handle = tokio::spawn(async move {
                            barrier.wait().await;

                            // Each task performs multiple cache operations
                            for i in 0..ITERATIONS {
                                match cache
                                    .get_or_insert_async(&i, async { Ok::<_, ()>(i) })
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(e) => panic!("Unexpected error: {:?}", e),
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    // Wait for all tasks to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
    }

    group.finish();
}

fn placeholder_async_handoff_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("placeholder_async_handoff");

    // Number of handoff iterations to perform
    const ITERATIONS: usize = 100;

    // Test handoff efficiency - measures how quickly guards are handed off between tasks
    for num_waiters in [4, 8, 16, 32] {
        group.bench_function(format!("{}", num_waiters), |b| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let cache = Arc::new(Cache::new(10000));
                    let barrier = Arc::new(Barrier::new(num_waiters));
                    let counters = Arc::new([const { AtomicUsize::new(0) }; ITERATIONS]);

                    let mut handles = vec![];
                    // Spawn waiter tasks that will queue up
                    for _ in 0..num_waiters {
                        let cache = cache.clone();
                        let barrier = barrier.clone();
                        let counters = counters.clone();
                        let handle = tokio::spawn(async move {
                            barrier.wait().await;
                            for i in 0..ITERATIONS {
                                loop {
                                    match cache.get_value_or_guard_async(&i).await {
                                        Ok(_v) => break,
                                        Err(g) => {
                                            if counters[i].fetch_add(1, atomic::Ordering::Relaxed)
                                                == num_waiters - 1
                                            {
                                                g.insert(i).unwrap();
                                            }
                                        }
                                    }
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    // Wait for all tasks
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    placeholder_async_contention_bench,
    placeholder_async_handoff_bench
);
criterion_main!(benches);
