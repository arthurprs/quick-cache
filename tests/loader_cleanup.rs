use quick_cache::unsync::Cache;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

#[test]
fn failed_ref_loader_releases_key() {
    let mut cache = Cache::<Arc<str>, u64>::new(100);
    let key: Arc<str> = Arc::from("failed-ref-load");
    assert_eq!(
        cache.get_or_insert_with(&key, || Err::<u64, _>("failed")),
        Err("failed")
    );
    assert_eq!(cache.len(), 0);
    assert_eq!(
        Arc::strong_count(&key),
        1,
        "failed loader retains its owned key"
    );
    assert_eq!(
        cache.get_or_insert_with(&key, || Ok::<_, ()>(42)),
        Ok(Some(&42))
    );
}

#[test]
fn failed_mut_loader_releases_key() {
    let mut cache = Cache::<Arc<str>, u64>::new(100);
    let key: Arc<str> = Arc::from("failed-mut-load");
    assert!(cache
        .get_mut_or_insert_with(&key, || Err::<u64, _>("failed"))
        .is_err());
    assert_eq!(cache.len(), 0);
    assert_eq!(
        Arc::strong_count(&key),
        1,
        "failed mutable loader retains its owned key"
    );
    assert_eq!(
        cache
            .get_mut_or_insert_with(&key, || Ok::<_, ()>(42))
            .unwrap()
            .map(|v| *v),
        Some(42)
    );
}

#[test]
fn panicking_ref_loader_releases_key() {
    let mut cache = Cache::<Arc<str>, u64>::new(100);
    let key: Arc<str> = Arc::from("panicking-ref-load");
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = cache.get_or_insert_with(&key, || -> Result<u64, ()> { panic!("loader panic") });
    }));
    assert!(result.is_err());
    assert_eq!(
        Arc::strong_count(&key),
        1,
        "panicking loader retains its owned key"
    );
    assert_eq!(
        cache.get_or_insert_with(&key, || Ok::<_, ()>(42)),
        Ok(Some(&42))
    );
}

#[test]
fn panicking_mut_loader_releases_key() {
    let mut cache = Cache::<Arc<str>, u64>::new(100);
    let key: Arc<str> = Arc::from("panicking-mut-load");
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ =
            cache.get_mut_or_insert_with(&key, || -> Result<u64, ()> { panic!("loader panic") });
    }));
    assert!(result.is_err());
    assert_eq!(
        Arc::strong_count(&key),
        1,
        "panicking mutable loader retains its owned key"
    );
    assert_eq!(
        cache
            .get_mut_or_insert_with(&key, || Ok::<_, ()>(42))
            .unwrap()
            .map(|v| *v),
        Some(42)
    );
}
