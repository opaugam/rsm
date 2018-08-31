//! A simple user space non-recursive lock built on atomics. The fast path amounts to
//! 1 load+cas for both lock() and unlock(). The cold path relies on 1+ additional cas
//! loops and uses a standard condition variable to park/unpark threads. Pending threads
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
            // - pass the update closure to potentially update the user payload
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
            ).is_none()
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
                    &update,
                    &|c| c + 1,
                    &|_| false,
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
            (cur & !(PENDING | BUSY)) | LOCK,
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
        // - spin until we flip the BUSY bit on
        // - the LOCK bit is on by design and PENDING may also be set
        //
        let cur = set_or_spin(&self.tag, LOCK, BUSY, BUSY, 0, &|n| n, &|c| c, &|_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        //
        if cur & PENDING > 0 {

            //
            // - dequeue one pending thread
            // - if this is the last pending queue node we have to
            //   unset the PENDING bit
            //
            let (last_one, synchro) = self.queue.pop();
            let mask = if last_one { BUSY | PENDING } else { BUSY };

            //
            // - release the queue by unsetting the mask
            // - decrement the pending counter by 1
            // - pass the update closure to potentially update the user payload
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(
                &self.tag,
                LOCK | BUSY,
                0,
                0,
                mask,
                &update,
                &|c| c - 1,
                &|_| true,
            );

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
            // - this should not spin unless upon a spurious CAS failure
            // - pass the update closure to potentially update the user payload
            //
            let _ = set_or_spin(
                &self.tag,
                LOCK | BUSY,
                0,
                0,
                LOCK | BUSY,
                &update,
                &|c| c - 1,
                &|_| true,
            );
        }
    }
}