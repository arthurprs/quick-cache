#[cfg(not(feature = "shuttle"))]
pub use std::{sync, thread};

#[cfg(feature = "shuttle")]
pub use shuttle::{sync, thread};

// Shuttle does not model OnceLock. The cache uses it without blocking.
pub use std::sync::OnceLock;

#[cfg(all(feature = "parking_lot", not(feature = "shuttle")))]
pub(crate) use parking_lot::{Mutex, RwLock, RwLockWriteGuard};

#[cfg(any(not(feature = "parking_lot"), feature = "shuttle"))]
pub(crate) use locks::{Mutex, RwLock, RwLockWriteGuard};

#[cfg(any(not(feature = "parking_lot"), feature = "shuttle"))]
mod locks {
    use super::sync;
    use std::sync::{TryLockError, TryLockResult};

    #[cfg(all(feature = "sharded-lock", not(feature = "shuttle")))]
    pub(crate) use crossbeam_utils::sync::{
        ShardedLock as InnerRwLock, ShardedLockReadGuard as RwLockReadGuard,
        ShardedLockWriteGuard as RwLockWriteGuard,
    };
    #[cfg(any(not(feature = "sharded-lock"), feature = "shuttle"))]
    pub(crate) use sync::{RwLock as InnerRwLock, RwLockReadGuard, RwLockWriteGuard};

    fn try_guard<G>(result: TryLockResult<G>) -> Option<G> {
        match result {
            Ok(guard) => Some(guard),
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Poisoned(_)) if cfg!(feature = "shuttle") => None,
            Err(TryLockError::Poisoned(err)) => panic!("{err}"),
        }
    }

    #[derive(Debug)]
    pub(crate) struct Mutex<T>(sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub(crate) const fn new(value: T) -> Self {
            Self(sync::Mutex::new(value))
        }

        #[inline]
        pub(crate) fn lock(&self) -> sync::MutexGuard<'_, T> {
            self.0.lock().unwrap()
        }

        #[cfg(all(test, not(feature = "shuttle")))]
        pub(crate) fn try_lock(&self) -> Option<sync::MutexGuard<'_, T>> {
            try_guard(self.0.try_lock())
        }
    }

    #[derive(Debug, Default)]
    #[repr(transparent)]
    pub struct RwLock<T: ?Sized>(InnerRwLock<T>);

    impl<T> RwLock<T> {
        pub(crate) fn new(value: T) -> Self {
            Self(InnerRwLock::new(value))
        }
    }

    impl<T: ?Sized> RwLock<T> {
        #[inline]
        pub(crate) fn read(&self) -> RwLockReadGuard<'_, T> {
            self.0.read().unwrap()
        }

        #[inline]
        pub(crate) fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.0.write().unwrap()
        }

        #[inline]
        pub(crate) fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            try_guard(self.0.try_read())
        }

        #[inline]
        pub(crate) fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            try_guard(self.0.try_write())
        }
    }
}
