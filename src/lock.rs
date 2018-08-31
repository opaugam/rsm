//! A simple user space non-recursive lock built on atomics. The fast path amounts to
//! 1 load+cas for both lock() and unlock(). The cold path relies on an additional cas
//! loop and uses a standard condition variable to park/unpark threads. Pending threads
//! are awaken according to the specified strategy. The first cas loop is optimistic
//! and will spin, attempting to flip the lock without going to sleep.
//!
//! The lock does not allow for strict barging in the sense that unlock() will use
//! the cold path if ever there is at least one pending thread waiting. Micro contention
//! will be handled however due to the initial spin (e.g 2 competing threads will not
//! necessarily take the cold path).
//!
//! The LIFO strategy is not fair in the sense 2+ threads could hog the lock. The
//! FIFO strategy is fair and will guarantee each thread gets the same exposure.
//!
//! The locks are slightly heavier memory wise than for instance what's described in
//! https://webkit.org/blog/6161/locking-in-webkit/ but the code is drastically
//! simpler. The cost per lock is 16 bytes (state + 1 pointer).
//!
//! Please note each lock may carry 32bits of user payload. This can be handy when
//! building higher level synchronization primitives (or just debug). In addition the
//! lock tracks the count of pending threads, which is also precious informaton in
//! some situations.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering, spin_loop_hint};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

const LOCK: usize = 1;
const BUSY: usize = 2;
const PENDING: usize = 4;
const COUNTER_MASK: usize = ((1 << 24) - 1) << 8;

///
/// Internal double linked list node holding mutex/condvar pairs. Those
/// are transient and only allocated upon contention. Note the LIFO strategy
/// will only use one pointer.
///
struct Node {
    synchro: Arc<(Mutex<bool>, Condvar)>,
    p: UnsafeCell<*mut Node>,
    n: UnsafeCell<*mut Node>,
}

impl Node {
    fn new() -> *mut Node {
        Box::into_raw(Box::new(Node {
            synchro: Arc::new((Mutex::new(true), Condvar::new())),
            p: UnsafeCell::new(ptr::null_mut()),
            n: UnsafeCell::new(ptr::null_mut()),
        }))
    }
}

pub trait Strategy {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)>;
    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>);
}

/// Simple LIFO queue, e.g a stack. This is very light although not really
/// conducing to fairness. A handful of threads are likely to hog the lock
/// and starve the others.
pub struct LIFO {
    head: UnsafeCell<*mut Node>,
}

impl Default for LIFO {
    fn default() -> Self {
        LIFO { head: UnsafeCell::new(ptr::null_mut()) }
    }
}

impl Strategy for LIFO {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)> {

        //
        // - allocate a new single pointer node
        // - push to the left of the head
        // - note the 2nd pointer is not used
        //
        let ptr = Node::new();
        let head = *self.head.get();
        *(*ptr).p.get() = head;

        //
        // - update the head
        //
        *self.head.get() = ptr;
        return (*ptr).synchro.clone();
    }

    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>) {

        //
        // - simply pop the head
        //
        debug_assert!(!(*self.head.get()).is_null());
        let nxt = *self.head.get();
        *self.head.get() = *(*nxt).p.get();

        //
        // - drop the node
        // - the mutex/condvar owning arc will then unref and drop later
        //
        let _: Box<Node> = Box::from_raw(nxt);
        return ((*self.head.get()).is_null(), (*nxt).synchro.clone());
    }
}

/// Simple FIFO queue. This is a bit heavier in terms of code but is
/// guaranteed to be fair. Threads will be awaken based on their waiting
/// order.
pub struct FIFO {
    head: UnsafeCell<*mut Node>,
}

impl Default for FIFO {
    fn default() -> Self {
        FIFO { head: UnsafeCell::new(ptr::null_mut()) }
    }
}

impl Strategy for FIFO {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)> {

        //
        // - allocate a node and append it to the tail
        //
        let ptr = Node::new();
        let head = *self.head.get();
        if head.is_null() {

            //
            // - chain the node to itself (e.g head == tail)
            //
            *(*ptr).p.get() = ptr;
            *(*ptr).n.get() = ptr;

        } else {

            //
            // - we have 1+ nodes already
            // - chain to the right of the tail
            //
            *(*ptr).p.get() = *(*head).p.get();
            *(*ptr).n.get() = head;
            *(*head).p.get() = ptr;
        };

        //
        // - update the head
        //
        *self.head.get() = ptr;
        return (*ptr).synchro.clone();
    }

    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>) {

        //
        // - pop the tail
        //
        let head = *self.head.get();
        let tail = *(*head).p.get();
        let last = if head == tail {

            //
            // - head == tail -> last node
            // - reset the head
            //
            *self.head.get() = ptr::null_mut();
            true

        } else {

            //
            // - we have at least 2 nodes
            // - re-link the penultimate node (tail.p)
            //
            let penultimate = *(*tail).p.get();
            *(*head).p.get() = penultimate;
            *(*penultimate).n.get() = head;
            false
        };

        //
        // - drop the node
        // - the mutex/condvar owning arc will then unref and drop later
        //
        let _: Box<Node> = Box::from_raw(tail);
        return (last, (*tail).synchro.clone());
    }
}

/// Raw lock storing its state in a atomic usize and maintaining a parking
/// queue according to the specified strategy. The lock is able to carry user
/// payload (as a u32) as well as a counter tracking the number of pending
/// threads.
///
/// The state usize is laid out as follows:
///
///    |         32          |        24        |  8  |
///             user                counter       bits
///
pub struct Lock<T>
where
    T: Strategy,
{
    tag: AtomicUsize,
    queue: T,
}

unsafe impl<T> Send for Lock<T>
where
    T: Strategy,
{
}

unsafe impl<T> Sync for Lock<T>
where
    T: Strategy,
{
}

impl<T> Default for Lock<T>
where
    T: Default + Strategy,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Lock<T>
where
    T: Default + Strategy,
{
    #[inline]
    pub fn new() -> Self {
        Lock::tagged_as(0)
    }

    #[inline]
    pub fn tagged_as(tag: u32) -> Self {
        let mut tag = tag as usize;
        tag <<= 32;
        Lock {
            tag: AtomicUsize::new(tag),
            queue: Default::default(),
        }
    }

    #[inline]
    pub fn tag(&self) -> u32 {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur >> 32) as u32
    }

    #[inline]
    pub fn pending(&self) -> usize {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur & COUNTER_MASK) >> 8
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

            if set_or_spin(&self.tag, 0, LOCK, LOCK, 0, |c| c, yield_and_give_up).is_none() {

                //
                // - the LOCK bit is still set in theory
                // - attempt to spin and flip the BUSY bit as long as LOCK is set
                // - flip also the PENDING bit at the same time
                // - increment the pending counter by 1
                // - failure to do so after the initial period will loop back
                //   to the first CAS (for instance if LOCK happens to be unset
                //   now)
                //
                if set_or_spin(
                    &self.tag,
                    LOCK,
                    BUSY,
                    BUSY | PENDING,
                    0,
                    |c| c + 1,
                    |_| false,
                ).is_some()
                {

                    //
                    // - we are holding the BUSY bit, e.g we own the queue
                    // - enqueue a new mutex+condvar
                    //
                    let synchro = self.queue.push();

                    //
                    // - lock the mutex and release the queue by flipping the BUSY bit
                    // - at this point other lock()/unlock() invokations may proceed
                    //
                    let mut parked = synchro.0.lock().unwrap();
                    let _ = self.tag.fetch_sub(BUSY, Ordering::Release);

                    //
                    // - freeze and wait on the condvar as long as the mutex
                    //   is set to true (in case of spurious wakeups)
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
            (cur | LOCK) & !(PENDING | BUSY),
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
        // - the LOCK bit is on by design and PENDING may also be set
        //
        let tag = set_or_spin(&self.tag, LOCK, BUSY, BUSY, 0, |c| c, |_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        //
        if tag & PENDING > 0 {

            //
            // - dequeue the left-most node (e.g PENDING behavior)
            // - if this is the last pending queue node we have to
            //   unset the PENDING bit
            //
            let (last_one, synchro) = self.queue.pop();
            let mask = if last_one { BUSY | PENDING } else { BUSY };

            //
            // - release the queue by unsetting the mask
            // - decrement the pending counter by 1 (this is why we need
            //   a CAS)
            //
            let _ = set_or_spin(&self.tag, LOCK | BUSY, 0, 0, mask, |c| c - 1, |_| true);

            //
            // - lock the mutex and unset it
            // - notify the condvar at which point the owning thread will be
            //   scheduling again
            //
            let mut parked = synchro.0.lock().unwrap();
            *parked = false;
            synchro.1.notify_one();

        } else {

            //
            // - we just failed the first round of spinning
            // - there is no pending thread to transfer ownership to
            // - unset both LOCK and BUSY
            //
            let _ = self.tag.fetch_sub(LOCK | BUSY, Ordering::Release);
        }
    }
}

fn set_or_spin<F, G>(
    state: &AtomicUsize,
    on: usize,
    off: usize,
    set: usize,
    unset: usize,
    incr: F,
    cold: G,
) -> Option<usize>
where
    F: Fn(usize) -> usize,
    G: Fn(&AtomicUsize) -> bool,
{
    //
    // - the thread will spin up to 30 times total in the worst
    //   case before invoking the 'cold' closure
    //
    let mut n = 2;
    let mut cur = state.load(Ordering::Relaxed);
    loop {

        //
        // - the expected value must match the specified on/off bits
        // - extract the counter bits into a usize
        //
        let expected = (cur | on) & !off;
        let mut cnt = (expected & COUNTER_MASK) >> 8;

        //
        // - run the increment closure
        // - shift the counter bits back to where they should be
        // - attempt a CAS with the target value properly constructed
        //
        cnt = incr(cnt) << 8;
        match state.compare_exchange_weak(
            expected,
            (expected & !(unset | COUNTER_MASK)) | set | cnt,
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

#[cfg(test)]
mod tests {

    extern crate rand;

    use lock::*;
    use lock::tests::rand::{Rng, thread_rng};
    use std::sync::Arc;
    use std::sync::atomic::spin_loop_hint;
    use std::thread;

    #[test]
    fn lifo_loop_32x4() {
        let locks: Arc<Vec<_>> = Arc::new((0..4).map(|n| Lock::<LIFO>::tagged_as(n)).collect());
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

        for lock in locks.iter() {
            assert!(lock.pending() == 0);
        }
    }

    #[test]
    fn fifo_loop_32x4() {
        let locks: Arc<Vec<_>> = Arc::new((0..4).map(|n| Lock::<FIFO>::tagged_as(n)).collect());
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

        for lock in locks.iter() {
            assert!(lock.pending() == 0);
        }
    }
}
