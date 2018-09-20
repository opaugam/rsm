//! A simple "once" (e.g a singleton) owning a closure. This construct is typically used to
//! run some initialization code or create some shared object. It holds a value that is constructed
//! by the closure and remains accessible throughout its lifetime. The closure will not be invoked
//! if and only if `run()` is never invoked before the once drops.
//!
//! The once can also be reset at anytime which will drop any data it holds.
use self::lock::*;
use std::cell::RefCell;
use std::ptr;
use std::sync::atomic::AtomicUsize;
use super::*;

pub struct Once<T> {
    lock: Lock<FIFO>,
    cell: RefCell<*mut Wrapper<T>>,
}

struct Wrapper<T> {
    val: T,
}

impl<T> Once<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            lock: Lock {
                tag: AtomicUsize::new(0),
                queue: FIFO { head: UnsafeCell::new(ptr::null_mut()) },
            },
            cell: RefCell::new(ptr::null_mut()),
        }
    }

    pub fn run<'a, F>(&self, f: F) -> &'a T
    where
        F: Fn() -> T,
    {
        self.lock.lock(|n| n);

        //
        // - run the closure upon the first lock
        // - box and keep track of the pointer using the cell
        // - don't forget to increment when releasing the lock
        //
        if self.lock.tag() == 0 {
            let p = Box::into_raw(Box::new(Wrapper { val: f() }));
            *self.cell.borrow_mut() = p;
        }
        self.lock.unlock(|n| n + 1);
        unsafe {
            let p = *self.cell.borrow();
            &(*p).val
        }
    }

    #[inline]
    pub fn reset(&self) -> () {

        //
        // - release the pointer if and only if we have a value
        //
        self.lock.lock(|n| n);
        if self.lock.tag() > 0 {
            let p = *self.cell.borrow();
            debug_assert!(!p.is_null());
            let _: Box<Wrapper<T>> = unsafe { Box::from_raw(p) };
        }

        //
        // - reset the count to 0
        //
        self.lock.unlock(|_| 0);
    }
}

unsafe impl<T> Sync for Once<T> {}

unsafe impl<T> Send for Once<T> {}

impl<T> Drop for Once<T> {
    fn drop(&mut self) -> () {
        self.reset();
    }
}
