//! A simple user space non-recursive lock built on atomics. The fast path amounts to
//! 1 load+cas for both lock() and unlock(). The cold path relies on 1+ additional cas
//! loops and uses a standard condition variable to park/unpark threads. Pending threads
//! are awaken according to the specified strategy. The first cas loop is optimistic
//! and will spin, attempting to flip the lock without going to sleep.
//!
//! The locking/unlocking paths allow for barging, e.g lock preemption while in unlock().
//! This allows to manage high micro-contention and minimizes calls to wait()/notify_one().
//!
//! The LIFO strategy is not fair in the sense 2+ threads could hog the lock. The
//! FIFO strategy is fair and will guarantee each thread gets the same exposure.
//!
//! The locks are slightly heavier memory wise than for instance what's described in
//! https://webkit.org/blog/6161/locking-in-webkit/ but the code is drastically
//! simpler. The cost per lock is 16 bytes (state + 1 pointer).
//!
//! Please note each lock may carry 32bits of user payload. Both lock() and unlock()
//! specify a closure that allows to atomically update this payload. This can be handy when
//! building higher level synchronization primitives (or just debugging). In addition the
//! lock tracks the count of pending threads, which is also precious informaton in
//! some situations.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use super::*;

const LOCK: usize = 1;
const BUSY: usize = 2;
const PENDING: usize = 4;

/// Raw lock storing its state in a atomic usize and maintaining a parking
/// queue according to the specified strategy. The lock is able to carry user
/// payload (as a u32) as well as a counter tracking the number of pending
/// threads (maximum queue of 16M threads).
///
/// The state usize is laid out as follows:
///
///    |         32          |        24        |  8  |
///             user                counter       bits
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
        Lock::with(0)
    }

    #[inline]
    pub fn with(tag: u32) -> Self {
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
        (cur & CNT_MSK) >> 8
    }

    #[inline]
    pub fn lock<F>(&self, update: F) -> ()
    where
        F: Fn(u32) -> u32,
    {
        //
        // - fast lock path (load + CAS)
        // - we just need to flip the LOCK bit and potentially update the user payload
        // - any failure defaults to the slow path
        //
        let cur = self.tag.load(Ordering::Relaxed);
        let user = update((cur >> 32) as u32);
        match self.tag.compare_exchange_weak(
            cur & !LOCK,
            (cur & !USR_MSK) | ((user as usize) << 32) | LOCK,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            _ => unsafe {
                self.lock_cold(update);
            },
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn lock_cold<F>(&self, update: F) -> ()
    where
        F: Fn(u32) -> u32,
    {
        loop {

            //
            // - attempt to spin and flip the LOCK bit
            // - failure after the initial period will proceed to enqueue/freeze the thread
            // - pass the update closure to update the user payload should we succeed to
            //   flip the bit
            //
            let yield_and_give_up = |_: &AtomicUsize| {
                thread::yield_now();
                false
            };

            if set_or_spin(
                &self.tag,
                0,
                LOCK,
                LOCK,
                0,
                &update,
                &|c| c,
                &yield_and_give_up,
            ).is_err()
            {

                //
                // - the LOCK bit is still set in theory
                // - attempt to spin and flip the BUSY bit as long as LOCK is set
                // - flip also the PENDING bit at the same time
                // - increment the pending counter by 1
                // - pass the update closure to potentially update the user payload
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
                    &|n| n,
                    &|c| c + 1,
                    &|_| false,
                ).is_ok()
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
                    // - in theory the LOCK bit must be unset
                    // - loop back to flip it
                    //
                }
            } else {
                break;
            }
        }
    }

    #[inline]
    pub fn unlock<F>(&self, update: F) -> ()
    where
        F: Fn(u32) -> u32,
    {
        //
        // - fast unlock path (load + CAS)
        // - same as lock() except we want to just have to unset the LOCK bit
        // - any failure defaults to the slow path (meaning that having PENDING
        //   or BUSY set will force us that way)
        //
        let cur = self.tag.load(Ordering::Relaxed);
        let user = update((cur >> 32) as u32);
        match self.tag.compare_exchange_weak(
            (cur & !PENDING) | LOCK,
            (cur & !(LOCK | USR_MSK)) | ((user as usize) << 32),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            _ => unsafe {
                self.unlock_cold(update);
            },
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn unlock_cold<F>(&self, update: F) -> ()
    where
        F: Fn(u32) -> u32,
    {
        //
        // - spin until we flip the BUSY bit on and unset the LOCK bit as the same time
        // - this will effectively release the lock while we're clear to start fiddling
        //   with the queue in case we have pending threads
        //
        let cur = set_or_spin(&self.tag, LOCK, BUSY, BUSY, LOCK, &update, &|c| c, &|_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - yield to promote preempting
        // - this will reduce drastically the number of calls to wai() and favor
        //   hot threads, typically during micro-contention
        //
        thread::yield_now();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        // - LOCK has been unset but may have been preempted by another thread
        //   running either lock() or lock_cold()
        //
        let preempted = self.tag.load(Ordering::Relaxed) & LOCK > 0;
        if !preempted && (cur & PENDING > 0) {

            //
            // - dequeue one pending thread
            // - if this is the last pending queue node we have to
            //   unset the PENDING bit
            //
            let (last_one, synchro) = self.queue.pop();
            let mask = if last_one { BUSY | PENDING } else { BUSY };

            //
            // - release the queue by unsetting the mask (which could also
            //   clear the PENDING bit if the queue is now empty)
            // - decrement the pending counter by 1
            // - pass the update closure to potentially update the user payload
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(&self.tag, BUSY, 0, 0, mask, &|n| n, &|c| c - 1, &|_| true);

            //
            // - lock the mutex and unset it
            // - notify the condvar at which point the corresponding thread will be
            //   scheduling again (and attempt to acquire the lock in lock_cold())
            //
            let mut parked = synchro.0.lock().unwrap();
            *parked = false;
            synchro.1.notify_one();

        } else {

            //
            // - we just failed the first round of spinning
            // - there is no pending thread to transfer ownership to
            // - unset the BUSY bit to release the queue
            // - pass the update closure to potentially update the user payload
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(&self.tag, BUSY, 0, 0, BUSY, &|n| n, &|c| c, &|_| true);
        }
    }
}
