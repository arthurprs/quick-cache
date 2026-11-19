#![cfg(feature = "stats")]

use quick_cache::{sync, unsync};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

fn counts(cache: &sync::Cache<u64, u64>) -> (u64, u64) {
    (cache.hits(), cache.misses())
}

#[test]
fn loader_counts_internal_probes_and_placeholder_joins() {
    let cache = sync::Cache::new(10);
    let guard = match cache.get_value_or_guard(&1, None) {
        sync::GuardResult::Guard(g) => g,
        _ => panic!(),
    };
    assert_eq!(counts(&cache), (0, 2));
    assert!(matches!(
        cache.get_value_or_guard(&1, Some(Duration::ZERO)),
        sync::GuardResult::Timeout
    ));
    assert_eq!(counts(&cache), (1, 3));
    guard.insert(7).unwrap();
    assert!(matches!(
        cache.get_value_or_guard(&1, None),
        sync::GuardResult::Value(7)
    ));
    assert_eq!(counts(&cache), (2, 3));
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 1);
}

#[test]
fn failed_load_retries_and_unsync_loads_have_different_units() {
    let cache = sync::Cache::<u64, u64>::new(10);
    assert_eq!(
        cache.get_or_insert_with(&1, || Err::<u64, _>("failed")),
        Err("failed")
    );
    assert_eq!(counts(&cache), (0, 2));
    assert_eq!(cache.get_or_insert_with(&1, || Ok::<_, ()>(9)), Ok(9));
    assert_eq!(counts(&cache), (0, 4));
    let mut other = unsync::Cache::<u64, u64>::new(10);
    assert_eq!(
        other.get_or_insert_with(&1, || Ok::<_, ()>(9)).unwrap(),
        Some(&9)
    );
    assert_eq!((other.hits(), other.misses()), (0, 1));
}

#[test]
fn async_waiter_counts_join_without_counting_filled_value_as_access() {
    let cache = sync::Cache::<u64, u64>::new(10);
    let mut context = Context::from_waker(Waker::noop());
    let mut owner = pin!(cache.get_value_or_guard_async(&1));
    let guard = match owner.as_mut().poll(&mut context) {
        Poll::Ready(Err(g)) => g,
        _ => panic!(),
    };
    assert_eq!(counts(&cache), (0, 2));
    let mut waiter = pin!(cache.get_value_or_guard_async(&1));
    assert!(waiter.as_mut().poll(&mut context).is_pending());
    assert_eq!(counts(&cache), (1, 3));
    assert!(waiter.as_mut().poll(&mut context).is_pending());
    assert_eq!(counts(&cache), (1, 3));
    guard.insert(9).unwrap();
    assert!(matches!(
        waiter.as_mut().poll(&mut context),
        Poll::Ready(Ok(9))
    ));
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 0);
    assert_eq!(counts(&cache), (1, 3));
}

#[test]
fn entry_actions_and_residency_reset() {
    let cache = sync::Cache::<u64, u64>::new(10);
    let guard = match cache.entry(&1, None, |_, _| sync::EntryAction::Retain(())) {
        sync::EntryResult::Vacant(g) => g,
        _ => panic!(),
    };
    assert_eq!(counts(&cache), (0, 1));
    assert!(matches!(
        cache.entry(&1, Some(Duration::ZERO), |_, _| sync::EntryAction::Retain(
            ()
        )),
        sync::EntryResult::Timeout
    ));
    assert_eq!(counts(&cache), (1, 1));
    guard.insert(9).unwrap();
    assert!(matches!(
        cache.entry(&1, None, |_, _| sync::EntryAction::Retain(())),
        sync::EntryResult::Retained(())
    ));
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 1);
    assert!(matches!(
        cache.entry::<_, ()>(&1, None, |_, _| sync::EntryAction::Remove),
        sync::EntryResult::Removed(1, 9)
    ));
    assert_eq!(counts(&cache), (2, 1));
    cache.insert(1, 9);
    let guard = match cache.entry::<_, ()>(&1, None, |_, _| sync::EntryAction::ReplaceWithGuard) {
        sync::EntryResult::Replaced(g, 9) => g,
        _ => panic!(),
    };
    assert_eq!(counts(&cache), (2, 1));
    guard.insert(8).unwrap();
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 0);
    let _ = cache.get(&1);
    cache.insert(1, 10);
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 0);
    cache.clear();
    assert_eq!(counts(&cache), (3, 1));
}
#[test]
fn async_entry_counts_both_placeholder_probes() {
    let cache = sync::Cache::<u64, u64>::new(10);
    let guard = match cache.get_value_or_guard(&1, None) {
        sync::GuardResult::Guard(g) => g,
        _ => panic!(),
    };
    let mut context = Context::from_waker(Waker::noop());
    let calls = std::cell::Cell::new(0);
    let mut waiter = pin!(cache.entry_async(&1, |_, _| {
        calls.set(calls.get() + 1);
        sync::EntryAction::Retain(())
    }));
    assert!(waiter.as_mut().poll(&mut context).is_pending());
    assert_eq!(counts(&cache), (2, 2));
    assert_eq!(calls.get(), 0);
    assert!(waiter.as_mut().poll(&mut context).is_pending());
    assert_eq!(counts(&cache), (2, 2));
    guard.insert(9).unwrap();
    assert!(matches!(
        waiter.as_mut().poll(&mut context),
        Poll::Ready(sync::EntryResult::Retained(()))
    ));
    assert_eq!(calls.get(), 1);
    assert_eq!(counts(&cache), (3, 2));
    assert_eq!(cache.item_stats(&1).unwrap().access_count, 1);
}
