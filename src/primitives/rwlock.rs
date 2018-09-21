//! Basic RAII read-write lock implementation with reading preference. The read-write lock has the
//! ability to produce a read-only version of itself. The RAII guards both implement deref.
use self::lock::*;
use std::cell::{Ref, RefCell, RefMut};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use super::*;

struct State<T> {
    r: Lock<FIFO>,
    w: Lock<FIFO>,
    cell: RefCell<T>,
}

/// Basic read-write lock exposing 2 `read()`/`write()` methods returning guards holding a reference
/// to the underlying value. The one obtained via `write()` is a mutable reference. The lock is
/// released once the guard drops.
pub struct RWLock<T> {
    state: Arc<State<T>>,
}

/// Same as the `RWLock` except it does not expose `write()` plus is not constructible. The only
/// way to get it is to derive it from a `RWLock` instance.
pub struct ROLock<T> {
    state: Arc<State<T>>,
}

unsafe impl<T> Send for RWLock<T> {}

unsafe impl<T> Sync for RWLock<T> {}

unsafe impl<T> Send for ROLock<T> {}

unsafe impl<T> Sync for ROLock<T> {}

pub struct ReadGuard<'a, T>
where
    T: 'a,
{
    r: &'a Lock<FIFO>,
    w: &'a Lock<FIFO>,
    inner: Option<Ref<'a, T>>,
}

impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        if let Some(ref val) = self.inner {
            &*val
        } else {
            unreachable!()
        }
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) -> () {

        //
        // - hold the read lock
        // - reset the option to drop the Ref
        // - if we're the last reader release the write lock
        // - release the read lock
        //
        self.r.lock(|n| n - 1);
        self.inner = None;
        if self.r.tag() == 0 {
            self.w.unlock(|n| n);
        }
        self.r.unlock(|n| n);
    }
}

pub struct WriteGuard<'a, T>
where
    T: 'a,
{
    w: &'a Lock<FIFO>,
    inner: Option<RefMut<'a, T>>,
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        if let Some(ref val) = self.inner {
            &*val
        } else {
            unreachable!()
        }
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        if let Some(ref mut val) = self.inner {
            &mut *val
        } else {
            panic!();
        }
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) -> () {

        //
        // - reset the option to drop the RefMut
        // - release the write lock
        //
        self.inner = None;
        self.w.unlock(|n| n);
    }
}

impl<T> RWLock<T> {
    #[inline]
    pub fn from(inner: T) -> Self {
        RWLock {
            state: Arc::new(State {
                r: Lock::new(),
                w: Lock::new(),
                cell: RefCell::new(inner),
            }),
        }
    }

    #[inline]
    pub fn read_only(&self) -> ROLock<T> {
        ROLock { state: self.state.clone() }
    }

    #[inline]
    pub fn readers(&self) -> u32 {
        self.state.r.tag()
    }

    #[inline]
    pub fn read(&self) -> ReadGuard<'_, T> {

        //
        // - hold the read lock
        // - if we are the first reader hold the write lock
        // - borrow the cell
        // - release the read lock and increment the readers count
        //
        self.state.r.lock(|n| n);
        if self.state.r.tag() == 0 {
            self.state.w.lock(|n| n);
        }
        let inner = self.state.cell.borrow();
        self.state.r.unlock(|n| n + 1);
        ReadGuard {
            r: &self.state.r,
            w: &self.state.w,
            inner: Some(inner),
        }
    }

    #[inline]
    pub fn write(&self) -> WriteGuard<'_, T> {

        // - hold the write lock
        // - borrow the cell mutably
        //
        self.state.w.lock(|n| n);
        let inner = self.state.cell.borrow_mut();
        WriteGuard {
            w: &self.state.w,
            inner: Some(inner),
        }
    }
}

impl<T> ROLock<T> {
    #[inline]
    pub fn readers(&self) -> u32 {
        self.state.r.tag()
    }

    #[inline]
    pub fn read(&self) -> ReadGuard<'_, T> {

        //
        // - hold the read lock
        // - if we are the first reader hold the write lock
        // - borrow the cell
        // - release the read lock and increment the readers count
        //
        self.state.r.lock(|n| n);
        if self.state.r.tag() == 0 {
            self.state.w.lock(|n| n);
        }
        let inner = self.state.cell.borrow();
        self.state.r.unlock(|n| n + 1);
        ReadGuard {
            r: &self.state.r,
            w: &self.state.w,
            inner: Some(inner),
        }
    }
}
