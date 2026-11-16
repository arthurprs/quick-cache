#![cfg(not(feature = "shuttle"))]
use quick_cache::sync::{Cache, EntryAction, EntryResult, GuardResult};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Wake, Waker},
    time::Duration,
};
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn step(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed >> 32
}
#[test]
fn mixed_sync_async_cancellation_and_replacement() {
    let iterations = if cfg!(miri) { 64 } else { 1000 };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let cache = Arc::new(Cache::<u64,u64>::new(16));
        let mut threads = Vec::new();
        for worker in 0..4 {
            let cache = cache.clone();
            threads.push(std::thread::spawn(move || {
                let mut seed = 0x6105 + worker;
                for _ in 0..iterations {
                    let key = step(&mut seed)%8;
                    match step(&mut seed)%5 {
                        0 => cache.insert(key,key),
                        1 => { if let Some((k,v)) = cache.remove(&key) { assert_eq!((k,v),(key,key)); } }
                        2 => match cache.get_value_or_guard(&key,Some(Duration::from_micros(100))) {
                            GuardResult::Value(v) => assert_eq!(v,key),
                            GuardResult::Guard(g) => { std::thread::yield_now(); if step(&mut seed)%2==0 { let _=g.insert(key); } }
                            GuardResult::Timeout => ()
                        },
                        3 => {
                            let mut calls = 0;
                            let result = cache.entry(&key,Some(Duration::ZERO),|k,v| {
                                calls+=1; assert_eq!((*k,*v),(key,key));
                                match step(&mut seed)%3 { 0=>EntryAction::Remove, 1=>EntryAction::ReplaceWithGuard, _=>EntryAction::Retain(*v) }
                            });
                            assert!(calls<=1);
                            match result {
                                EntryResult::Vacant(g)|EntryResult::Replaced(g,_) => { if step(&mut seed)%2==0 { let _=g.insert(key); } }
                                EntryResult::Retained(v) => assert_eq!(v,key),
                                EntryResult::Removed(k,v) => assert_eq!((k,v),(key,key)),
                                EntryResult::Timeout=>()
                            }
                        }
                        _ => { if let Some(v)=cache.get(&key) { assert_eq!(v,key); } }
                    }
                }
            }));
        }
        let mut tasks = Vec::new();
        for worker in 0..4 {
            let cache = cache.clone();
            tasks.push(tokio::spawn(async move {
                let mut seed = 0x9205 + worker;
                let w1=Waker::from(Arc::new(Noop));
                let w2=Waker::from(Arc::new(Noop));
                for _ in 0..iterations {
                    let key=step(&mut seed)%8;
                    if step(&mut seed)%3==0 {
                        let mut future=Box::pin(cache.get_value_or_guard_async(&key));
                        if future.as_mut().poll(&mut Context::from_waker(&w1)).is_pending() {
                            let _=future.as_mut().poll(&mut Context::from_waker(&w2));
                        }
                        drop(future);
                        tokio::task::yield_now().await;
                    } else if step(&mut seed)%2==0 {
                        tokio::select! {
                            biased;
                            result=cache.get_value_or_guard_async(&key)=> match result {
                                Ok(v)=>assert_eq!(v,key),
                                Err(g)=> { tokio::task::yield_now().await; if step(&mut seed)%2==0 { let _=g.insert(key); } }
                            },
                            _=tokio::task::yield_now()=>()
                        }
                    } else {
                        let mut calls=0;
                        let future=cache.entry_async(&key,|k,v| { calls+=1; assert_eq!((*k,*v),(key,key)); EntryAction::Retain(*v) });
                        tokio::select! {
                            biased;
                            result=future=>match result {
                                EntryResult::Retained(v)=>assert_eq!(v,key),
                                EntryResult::Vacant(g)=> { let _=g.insert(key); }
                                _=>panic!("unexpected entry action")
                            },
                            _=tokio::task::yield_now()=>()
                        }
                        assert!(calls<=1);
                    }
                }
            }));
        }
        for task in tasks { tokio::time::timeout(Duration::from_secs(30),task).await.unwrap().unwrap(); }
        for thread in threads { thread.join().unwrap(); }
    });
}
#[test]
fn single_loader_across_threads_and_tasks() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    for round in 0..32 {
        let cache = Arc::new(Cache::<u64, u64>::new(16));
        let loads = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for _ in 0..4 {
            let cache = cache.clone();
            let loads = loads.clone();
            threads.push(std::thread::spawn(move || {
                assert_eq!(
                    cache
                        .get_or_insert_with(&round, || {
                            loads.fetch_add(1, Ordering::SeqCst);
                            std::thread::yield_now();
                            Ok::<_, ()>(round)
                        })
                        .unwrap(),
                    round
                );
            }));
        }
        runtime.block_on(async {
            let mut tasks = Vec::new();
            for _ in 0..4 {
                let cache = cache.clone();
                let loads = loads.clone();
                tasks.push(tokio::spawn(async move {
                    assert_eq!(
                        cache
                            .get_or_insert_async(&round, async {
                                loads.fetch_add(1, Ordering::SeqCst);
                                tokio::task::yield_now().await;
                                Ok::<_, ()>(round)
                            })
                            .await
                            .unwrap(),
                        round
                    );
                }));
            }
            for task in tasks {
                tokio::time::timeout(Duration::from_secs(10), task)
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }
}
