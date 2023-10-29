#![allow(dead_code)]

#[cfg(not(feature = "shuttle"))]
pub(crate) use crate::rw_lock;
#[cfg(not(feature = "shuttle"))]
pub use std::{sync, thread};

#[cfg(feature = "shuttle")]
pub use shuttle::*;

#[cfg(feature = "shuttle")]
pub mod rw_lock {
    use std::ops::{Deref, DerefMut};

    #[derive(Default, Debug)]
    pub struct RwLock<T>(::shuttle::sync::RwLock<T>);

    #[derive(Debug)]
    pub struct RwLockReadGuard<'rwlock, T>(::shuttle::sync::RwLockReadGuard<'rwlock, T>);

    #[derive(Debug)]
    pub struct RwLockWriteGuard<'rwlock, T>(::shuttle::sync::RwLockWriteGuard<'rwlock, T>);

    impl<T> RwLock<T> {
        pub const fn new(t: T) -> Self {
            Self(::shuttle::sync::RwLock::new(t))
        }

        pub fn into_inner(self) -> T {
            self.0.into_inner().unwrap()
        }

        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            RwLockReadGuard(self.0.read().unwrap())
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            RwLockWriteGuard(self.0.write().unwrap())
        }

        pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
            self.0.try_write().map(RwLockWriteGuard).ok()
        }

        pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
            self.0.try_read().map(RwLockReadGuard).ok()
        }
    }

    impl<T> Deref for RwLockReadGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<T> Deref for RwLockWriteGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<T> DerefMut for RwLockWriteGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
}
