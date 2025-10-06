use criterion::{Criterion, criterion_group, criterion_main};
use quick_cache::sync::{Cache, GuardResult};
use std::sync::Barrier;
use std::sync::atomic::{self, AtomicUsize};
use std::thread;

fn placeholder_contention_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("placeholder_contention");

    // Number of iterations each thread will perform
    const ITERATIONS: usize = 100;

    // Test various contention scenarios
    for num_threads in [4, 8, 16, 32] {
        group.bench_function(format!("{}", num_threads), |b| {
            b.iter(|| {
                let cache = Cache::new(1000);
                let barrier = Barrier::new(num_threads);

                thread::scope(|s| {
                    // Spawn threads that will perform multiple iterations
                    for _ in 0..num_threads {
                        s.spawn(|| {
                            barrier.wait();

                            // Each thread performs multiple cache operations
                            for i in 0..ITERATIONS {
                                match cache.get_value_or_guard(&i, None) {
                                    GuardResult::Value(_) => {}
                                    GuardResult::Guard(g) => {
                                        g.insert(i).unwrap();
                                    }
                                    GuardResult::Timeout => panic!("Unexpected timeout"),
                                };
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

fn placeholder_handoff_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("placeholder_handoff");

    // Number of handoff iterations to perform
    const ITERATIONS: usize = 100;

    // Test handoff efficiency - measures how quickly guards are handed off between threads
    for num_waiters in [4, 8, 16, 32] {
        group.bench_function(format!("{}", num_waiters), |b| {
            b.iter(|| {
                let cache = Cache::new(10000);
                let barrier = Barrier::new(num_waiters);
                let counters = [const { AtomicUsize::new(0) }; ITERATIONS];

                thread::scope(|s| {
                    // Spawn waiter threads that will queue up
                    for _ in 0..num_waiters {
                        s.spawn(|| {
                            barrier.wait();
                            for i in 0..ITERATIONS {
                                loop {
                                    match cache.get_value_or_guard(&i, None) {
                                        GuardResult::Value(_v) => break,
                                        GuardResult::Guard(g) => {
                                            if counters[i].fetch_add(1, atomic::Ordering::Relaxed)
                                                == num_waiters - 1
                                            {
                                                g.insert(i).unwrap();
                                            }
                                        }
                                        GuardResult::Timeout => panic!("Unexpected timeout"),
                                    }
                                }
                            }
                        });
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    placeholder_contention_bench,
    placeholder_handoff_bench
);
criterion_main!(benches);
