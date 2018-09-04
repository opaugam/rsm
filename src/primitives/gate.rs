//! A basic critical section that runs the specified closure. The gate can be opened or closed in
//! which case a thread trying to enter will park. The gate can be closed/opened at any time and
//! keeps track of the number of times enter() is invoked since it last opened.
//!
//! The closure receives the current number of entries since last opened and returns a boolean to
//! indicate if the gate should remain open or not. This mechanism allows to build primitives akin
//! to barriers to synchronize on a certain number of operations.
//!
//! Please note the gate is closed by default, e.g an initial call to open() must be made to allow
//! threads to enter.
//!
use self::lock::*;
use std::sync::atomic::{AtomicBool, Ordering};
use super::*;

/// Gate wrapping its underlying FIFO lock and keeping track of an atomic bool to guarantee unlock()
/// can't be invoked concurrently.
pub struct Gate {
    lock: Lock<FIFO>,
    closed: AtomicBool,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    #[inline]
    pub fn new() -> Self {
        let lock = Lock::new();
        lock.lock(|_| 0);
        Gate {
            lock,
            closed: AtomicBool::new(true),
        }
    }

    #[inline]
    pub fn entries(&self) -> u32 {
        self.lock.tag()
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn open(&self) -> () {
        let closed = self.closed.load(Ordering::Acquire);
        if closed {
            self.lock.unlock(|_| {
                self.closed.store(false, Ordering::Release);
                0
            });
        }
    }

    #[inline]
    pub fn close(&self) -> () {
        self.lock.lock(|n| n);
        self.closed.store(true, Ordering::Release);
    }

    #[inline]
    pub fn enter<F>(&self, f: F) -> ()
    where
        F: Fn(u32) -> bool,
    {
        self.lock.lock(|n| n + 1);
        if f(self.lock.tag()) {
            self.lock.unlock(|n| n);
        } else {
            self.closed.store(true, Ordering::Release);
        }
    }
}
