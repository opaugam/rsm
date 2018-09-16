//! Basic read-write lock implementation with no write preference. The read-write lock has the
//! ability to produce a read-only version of itself.
use self::lock::*;
use std::cell::{Ref, RefCell, RefMut};
use std::sync::Arc;
use super::*;

struct State<T> {
    r: Lock<FIFO>,
    w: Lock<FIFO>,
    cell: RefCell<T>,
}

/// Basic read-write lock exposing 2 read()/write() methods returning guards holding a reference
/// to the underlying value. The one obtained via write() is a mutable reference. The lock is
/// released once the guard drops.
pub struct RWLock<T> {
    state: Arc<State<T>>,
}

/// Same as the RWLock except it does not expose write() plus is not constructible. The only
/// way to get it is to derive it from a RWLock instance.
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
    pub val: Ref<'a, T>,
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) -> () {
        self.r.lock(|n| n - 1);
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
    pub val: RefMut<'a, T>,
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) -> () {
        self.w.unlock(|n| n);
    }
}

impl<T> RWLock<T> {
    #[inline]
    pub fn from(val: T) -> Self {
        RWLock {
            state: Arc::new(State {
                r: Lock::new(),
                w: Lock::new(),
                cell: RefCell::new(val),
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
        self.state.r.lock(|n| n + 1);
        if self.state.r.tag() == 1 {
            self.state.w.lock(|n| n);
        }
        self.state.r.unlock(|n| n);
        ReadGuard {
            r: &self.state.r,
            w: &self.state.w,
            val: self.state.cell.borrow(),
        }
    }

    #[inline]
    pub fn write(&self) -> WriteGuard<'_, T> {
        self.state.w.lock(|n| n);
        WriteGuard {
            w: &self.state.w,
            val: self.state.cell.borrow_mut(),
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
        self.state.r.lock(|n| n + 1);
        if self.state.r.tag() == 1 {
            self.state.w.lock(|n| n);
        }
        self.state.r.unlock(|n| n);
        ReadGuard {
            r: &self.state.r,
            w: &self.state.w,
            val: self.state.cell.borrow(),
        }
    }
}
