//! A simple user space semaphore built on atomics. It allows to signal/wait with a
//! fast path. The cold path relies on 1+ additional cas loops and uses a standard
//! condition variable to park/unpark threads. The internal queueing is done in LIFO
//! order. The cost per sempahore is 16 bytes (state + 1 pointer).
//!
//! Please note each lock may carry 32bits of user payload.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use super::*;

const BUSY: usize = 1;
const OPEN: usize = 2;
const CLOSED: usize = 4;

/// Semaphore storing its state in a atomic usize and maintaining a LIFO parking
/// queue. The semaphore has a positive count (maximum value of 16M) and is able
/// to carry user payload (as a u32). It has 3 modes:
///
///   o open:  signal() +1,            wait() -1
///   o reset: signal() +1,            wait() +1/blocking
///   o close: signal() -1/unblocking, wait() +1/blocking
///
/// The counter is always >= 0 and has a different meaning based on the OPEN and
/// CLOSED bits. The reset mode maps to both OPEN and CLOSED being unset. An upper
/// cap may be specified when signaling (handy to express specific constructs such
/// as an auto-reset event). By design up to 16M threads could park on one semaphore.
///
/// The state usize is laid out as follows:
///
///    |         32          |        24        |  8  |
///             user                counter       bits
pub struct Semaphore {
    tag: AtomicUsize,
    queue: LIFO,
}

unsafe impl Send for Semaphore {}

unsafe impl Sync for Semaphore {}

impl Default for Semaphore {
    fn default() -> Self {
        Self::new()
    }
}

impl Semaphore {
    #[inline]
    pub fn new() -> Self {
        Semaphore::with(0)
    }

    #[inline]
    pub fn with(tag: u32) -> Self {
        let mut tag = tag as usize;
        tag <<= 32;
        Semaphore {
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
    pub fn is_open(&self) -> bool {
        let cur = self.tag.load(Ordering::Relaxed);
        cur & OPEN > 0
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        let cur = self.tag.load(Ordering::Relaxed);
        cur & CLOSED > 0
    }

    #[inline]
    pub fn count(&self) -> usize {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur & CNT_MSK) >> 8
    }

    #[inline]
    pub fn signal(&self) -> () {
        self.signal_under(CNT_MSK >> 8)
    }

    #[inline]
    pub fn signal_under(&self, cap: usize) -> () {

        //
        // - fast fail if we have reached the cap and
        //   if we are open
        //
        let cur = self.tag.load(Ordering::Relaxed);
        let cnt = (cur & CNT_MSK) >> 8;
        if cnt == cap && cur & OPEN > 0 {
            return;
        }

        //
        // - fast signal path (load + CAS)
        // - attempt to increment the counter bits if and only if the
        //   BUSY and CLOSED bits are not set (e.g we're either reset or
        //   open and nobody is locking the queue)
        // - any failure defaults to the slow path
        //
        match self.tag.compare_exchange_weak(
            (cur & !(BUSY | CLOSED)) | OPEN,
            (cur & !CNT_MSK) | ((cnt + 1) << 8),
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            _ => unsafe {
                self.signal_cold();
            },
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn signal_cold(&self) -> () {

        //
        // - spin until we flip the BUSY bit on
        // - the CLOSED bit may be set
        //
        let cur = set_or_spin(&self.tag, 0, BUSY, BUSY, 0, &|user| user, &|c| c, &|_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        //
        if cur & CLOSED > 0 {

            //
            // - dequeue one pending thread
            // - if this is the last pending queue node we have to
            //   unset the CLOSED bit at which point the semaphore will
            //   be reset (counter == 0)
            //
            let (last_one, synchro) = self.queue.pop();
            let mask = if last_one { BUSY | CLOSED } else { BUSY };

            //
            // - release the queue by unsetting the mask
            // - decrement the counter by 1
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(
                &self.tag,
                BUSY | CLOSED,
                0,
                0,
                mask,
                &|user| user,
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
            // - release the queue by unsetting the BUSY bit
            // - increment the counter by 1
            // - we can now set the OPEN bit since the counter is now > 0
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(
                &self.tag,
                BUSY,
                CLOSED,
                OPEN,
                BUSY,
                &|user| user,
                &|c| c + 1,
                &|_| true,
            );
        }
    }

    #[inline]
    pub fn wait(&self) -> () {

        //
        // - fast wait path (load + CAS)
        // - attempt to decrement the counter bits if and only if the
        //   BUSY bit is not set, the OPEN bit is (e.g we're open and
        //   nobody is locking the queue)
        // - if count is dropping to zero use the slow path to remove the OPEN bit
        // - any failure defaults to the slow path
        //
        let cur = self.tag.load(Ordering::Relaxed);
        let cnt = (cur & CNT_MSK) >> 8;
        if cnt > 1 {
            match self.tag.compare_exchange_weak(
                (cur & !BUSY) | OPEN,
                (cur & !CNT_MSK) | ((cnt - 1) << 8),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {}
                _ => unsafe {
                    self.wait_cold();
                },
            }

        } else {
            unsafe {
                self.wait_cold();
            }
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn wait_cold(&self) -> () {

        //
        // - spin until we flip the BUSY bit on
        // - the CLOSED bit may be set
        //
        let cur = set_or_spin(&self.tag, 0, BUSY, BUSY, 0, &|user| user, &|c| c, &|_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit, e.g we own the queue
        //
        let cnt = (cur & CNT_MSK) >> 8;
        if cur & CLOSED > 0 || cnt == 0 {

            //
            // - enqueue a new mutex+condvar
            //
            let synchro = self.queue.push();

            //
            // - lock the mutex and release the queue by flipping the BUSY bit
            // - at this point other lock()/unlock() invokations may proceed
            //
            let mut parked = synchro.0.lock().unwrap();

            //
            // - release the queue by unsetting the BUSY bit
            // - force the CLOSED bit since we now have at least one thread waiting
            // - increment the counter by 1
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(
                &self.tag,
                BUSY,
                0,
                CLOSED,
                BUSY,
                &|user| user,
                &|c| c + 1,
                &|_| true,
            );

            //
            // - freeze and wait on the condvar as long as the mutex
            //   is set to true (in case of spurious wakeups)
            //
            *parked = true;
            while *parked {
                parked = synchro.1.wait(parked).unwrap();
            }

        } else {

            //
            // - release the queue by unsetting the BUSY bit
            // - decrement the counter by 1
            // - if we reach 0 unset the OPEN bit as well
            // - this should not spin unless upon a spurious CAS failure
            //
            let mask = if cnt == 1 { BUSY | OPEN } else { BUSY };
            let _ = set_or_spin(
                &self.tag,
                BUSY,
                0,
                0,
                mask,
                &|user| user,
                &|c| c - 1,
                &|_| true,
            );
        }
    }
}
