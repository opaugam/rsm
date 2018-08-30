//! A simple user space non-recursive lock built on atomics. The fast path amounts to
//! 1 load+cas for both lock() and unlock(). The cold path relies on an additional cas
//! loop and uses a standard condition variable to park/unpark threads. Pending threads
//! are awaken in LIFO order. The first cas loop is optimistic and will spin.
//!
//! The lock does not allow for strict barging in the sense that unlock() will use
//! the cold path if ever there is at least one pending thread waiting.
//!
//! The cost per lock is 16 bytes (state + user payload / linked list head).
//!
//! Please note each lock may carry 32bits of user payload. This can be handy when
//! building higher level synchronization primitives (or just debug).

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering, spin_loop_hint};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

const LOCK: usize = 1;
const BUSY: usize = 2;
const LIFO: usize = 4;

struct Node {
    synchro: Arc<(Mutex<bool>, Condvar)>,
    nxt: UnsafeCell<*mut Node>,
}

impl Node {
    fn new(nxt: *mut Node) -> *mut Node {
        Box::into_raw(Box::new(Node {
            synchro: Arc::new((Mutex::new(true), Condvar::new())),
            nxt: UnsafeCell::new(nxt),
        }))
    }
}

pub struct FIFOLock {
    tag: AtomicUsize,
    head: UnsafeCell<*mut Node>,
}

unsafe impl Send for FIFOLock {}

unsafe impl Sync for FIFOLock {}

impl Default for FIFOLock {
    fn default() -> Self {
         Self::new()
    }
}

impl FIFOLock {
    #[inline]
    pub fn new() -> Self {
        FIFOLock::tagged_as(0)
    }

    #[inline]
    pub fn tagged_as(tag: u32) -> Self {
        let mut tag = tag as usize;
        tag <<= 32;
        FIFOLock {
            tag: AtomicUsize::new(tag),
            head: UnsafeCell::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn tag(&self) -> u32 {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur >> 32) as u32
    }

    #[inline]
    pub fn lock(&self) -> () {

        //
        // - fast lock path (load + CAS)
        // - we just need to flip the LOCK bit
        // - any failure defaults to the slow path
        //
        let cur = self.tag.load(Ordering::Relaxed);
        match self.tag.compare_exchange_weak(
            cur & !LOCK,
            cur | LOCK,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            _ => unsafe {
                self.lock_cold();
            },
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn lock_cold(&self) -> () {
        loop {

            //
            // - attempt to spin and flip the LOCK bit
            // - failure after the initial period will proceed
            //   to enqueue/freeze the thread
            //
            let yield_and_give_up = |_: &AtomicUsize| {
                thread::yield_now();
                false
            };

            if FIFOLock::set_or_spin(&self.tag, 0, LOCK, LOCK, yield_and_give_up).is_none()
            {
                //
                // - the LOCK bit is still set in theory
                // - attempt to spin and flip the BUSY bit as long as LOCK is set
                // - flip also the LIFO bit at the same time
                // - failure to do so after the initial period will loop back
                //   to the first CAS (for instance if LOCK happens to be unset
                //   now)
                //
                if FIFOLock::set_or_spin(&self.tag, LOCK, BUSY, BUSY | LIFO, |_| false).is_some() {

                    //
                    // - we are holding the BUSY bit, e.g we own the queue
                    // - spawn a new node
                    // - chain to the left of the head
                    //
                    let empty = (*self.head.get()).is_null();
                    let node = Node::new(if empty {
                        ptr::null_mut()
                    } else {
                        *self.head.get()
                    });
                    *self.head.get() = node;

                    //
                    // - clone the inner mutex+condvar
                    // - lock the mutex and release the queue by flipping the BUSY bit
                    // - at this point other lock()/unlock() invokations may proceed
                    //
                    let synchro = (*node).synchro.clone();
                    let mut parked = synchro.0.lock().unwrap();
                    let _ = self.tag.fetch_sub(BUSY, Ordering::Release);

                    //
                    // - freeze and wait on the condvar as long as the mutex
                    //   is set to true
                    //
                    *parked = true;
                    while *parked {
                        parked = synchro.1.wait(parked).unwrap();
                    }

                    //
                    // - we got notified
                    // - by design the LOCK bit must be set, e.g lock ownership
                    //   was transferred to us
                    //
                    debug_assert!(self.tag.load(Ordering::Acquire) & LOCK > 0);
                    break;
                }
            } else {
                break;
            }
        }
    }

    #[inline]
    pub fn unlock(&self) -> () {

        //
        // - fast unlock path (load + CAS)
        // - the LOCK bit...
        // - any failure defaults to the slow path
        //
        let cur = self.tag.load(Ordering::Relaxed);
        match self.tag.compare_exchange_weak(
            (cur | LOCK) & !(LIFO | BUSY),
            cur & !LOCK,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            _ => unsafe {
                self.unlock_cold();
            },
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn unlock_cold(&self) -> () {

        //
        // - spin until we flip the BUSY bit on
        // - the LOCK bit is on by design and LIFO may also be set
        //
        let tag = FIFOLock::set_or_spin(&self.tag, LOCK, BUSY, BUSY, |_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        //
        if tag & LIFO > 0 {

            //
            // - dequeue the left-most node (e.g LIFO behavior)
            // - if this is the last pending queue node we have to
            //   unset the LIFO bit
            //
            let nxt = *self.head.get();
            *self.head.get() = *(*nxt).nxt.get();
            let mask = if (*self.head.get()).is_null() {
                BUSY | LIFO
            } else {
                BUSY
            };

            //
            // - release the queue by unsetting the BUSY bit
            //
            let _ = self.tag.fetch_sub(mask, Ordering::Release);

            //
            // - lock the mutex and unset it
            // - notify the condvar at which point the owning thread will be
            //   scheduling again
            //
            let mut parked = (*nxt).synchro.0.lock().unwrap();
            *parked = false;
            (*nxt).synchro.1.notify_one();

            //
            // - drop the node
            // - the mutex/condvar owning arc will then unref and drop
            //
            let _: Box<Node> = Box::from_raw(nxt);

        } else {

            debug_assert!((*self.head.get()).is_null());
            let _ = self.tag.fetch_sub(LOCK | BUSY, Ordering::Release);
        }
    }

    fn set_or_spin<F>(
        state: &AtomicUsize,
        on: usize,
        off: usize,
        set: usize,
        cold: F,
    ) -> Option<usize>
    where
        F: Fn(&AtomicUsize) -> bool,
    {
        //
        // - the thread will spin up to 30 times total in the worst
        //   case before invoking the 'cold' closure
        //
        let mut n = 2;
        let mut cur = state.load(Ordering::Relaxed);
        loop {
            let expected = (cur | on) & !off;
            match state.compare_exchange_weak(
                expected,
                expected | set,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(expected | set);
                }
                Err(prv) => {
                    cur = prv;
                    if n > 16 {
                        if !cold(state) {
                            return None;
                        }
                    } else {
                        for _ in 0..n {
                            spin_loop_hint();
                        }
                        n <<= 1;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    extern crate rand;

    use lock::FIFOLock;
    use lock::tests::rand::{Rng, thread_rng};
    use std::sync::Arc;
    use std::sync::atomic::spin_loop_hint;
    use std::thread;

    #[test]
    fn loop_32x4() {
        let locks: Arc<Vec<_>> = Arc::new((0..4).map(|n| {FIFOLock::tagged_as(n)} ).collect());
        let mut threads = Vec::new();
        for _ in 0..32 {
            let locks = locks.clone();
            let tid = thread::spawn(move || {
                let mut rng = thread_rng();
                for _ in 0..1024 {
                    let lock = rng.choose(&locks).unwrap();
                    lock.lock();
                    for _ in 0..rng.gen_range(0, 40) {
                        spin_loop_hint();
                    }
                    lock.unlock();
                }
            });
            threads.push(tid);
        }

        for tid in threads {
            tid.join().unwrap();
        }
    }
}
