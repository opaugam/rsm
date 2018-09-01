//! A simple user space once-like construct built on atomics. It allows to run a
//! closure once and only once after its counter drops to 0. Its fast path is a
//! single cas. The cold path relies on 1+ additional cas loops.
//! The cost per countdown is 8 bytes (state).
//!
//! A traditional 'once' can be built using an initial counter of 1.
//!
//! Please note each lock may carry 32bits of user payload.
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use super::*;

const BUSY: usize = 1;
const DONE: usize = 2;
const POISON: usize = 4;

/// Countdown storing its state in a atomic usize. It has a positive count (maximum
/// value of 16M) and is able to carry user payload (as a u32).
///
/// Each run() invokation decrements the counter. Once it drops to zero the closure
/// is run after which the countdown is disabled. A panic in the closure will set
/// the POISON bit at which point has_failed() would return true. Please note that
/// the closure is run at most once with no retries (e.g even if it panics).
///
/// The state usize is laid out as follows:
///
///    |         32          |        24        |  8  |
///             user                counter       bits
pub struct Countdown {
    tag: AtomicUsize,
}

unsafe impl Send for Countdown {}

unsafe impl Sync for Countdown {}

impl Countdown {
    #[inline]
    pub fn new(count: usize) -> Self {

        //
        // - new() defaults to a 'once' construct
        //
        Countdown::with(count, 1)
    }

    #[inline]
    pub fn with(count: usize, tag: u32) -> Self {
        assert!(count >= 1);
        let mut tag = tag as usize;
        tag <<= 32;
        tag |= (count - 1) << 8;
        Countdown { tag: AtomicUsize::new(tag) }
    }

    #[inline]
    pub fn tag(&self) -> u32 {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur >> 32) as u32
    }

    #[inline]
    pub fn has_run(&self) -> bool {
        let cur = self.tag.load(Ordering::Relaxed);
        cur & (DONE | POISON) > 0
    }

    #[inline]
    pub fn has_failed(&self) -> bool {
        let cur = self.tag.load(Ordering::Relaxed);
        cur & POISON > 0
    }

    #[inline]
    pub fn count(&self) -> usize {
        let cur = self.tag.load(Ordering::Relaxed);
        (cur & CNT_MSK) >> 8
    }

    #[inline]
    pub fn run<F>(&self, cb: F) -> ()
    where
        F: Fn() -> (),
    {

        //
        // - fast run path (load + CAS)
        // - attempt to decrement the counter bits if and only if the
        //   BUSY bit is not set and the OPEN bit is (e.g we're open
        //   and nobody is locking the queue)
        // - comparing on the OPEN bit takes care of a reset semaphore
        //   (e.g count is zero)
        // - don't worry about underflowing cnt (the CAS would fail anyway)
        // - any failure defaults to the slow path
        //
        let cur = self.tag.load(Ordering::Relaxed);
        if cur & DONE > 0 {
            return;
        }
        let mut cnt = (cur & CNT_MSK) >> 8;
        if cnt > 0 {
            cnt -= 1;
            match self.tag.compare_exchange_weak(
                cur & !(DONE | BUSY),
                (cur & !CNT_MSK) | (cnt << 8),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {}
                Err(prv) => unsafe {
                    if prv & DONE == 0 {
                        self.run_cold(cb);
                    }
                },
            }
        } else {
            unsafe {
                self.run_cold(cb);
            }
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn run_cold<F>(&self, cb: F) -> ()
    where
        F: Fn() -> (),
    {

        //
        // - spin until we flip the BUSY bit on
        // - the DONE bit may be set already
        //
        let cur = set_or_spin(&self.tag, 0, BUSY, BUSY, 0, &|user| user, &|c| c, &|_| {
            thread::yield_now();
            true
        }).unwrap();

        //
        // - we are holding the BUSY bit
        //
        let cnt = (cur & CNT_MSK) >> 8;
        if cnt > 0 {

            debug_assert!(cur & (DONE | POISON) == 0);

            //
            // - we just failed the first round of spinning
            // - the count is still > 0, decrement it
            // - unset BUSY
            // - this should not spin unless upon a spurious CAS failure
            //
            let _ = set_or_spin(
                &self.tag,
                BUSY,
                DONE | POISON,
                0,
                BUSY,
                &|user| user,
                &|c| c - 1,
                &|_| true,
            );

        } else if cur & DONE > 0 {

            //
            // - the DONE bit is already set, nothing to do
            // - unset the BUSY bit
            //
            self.tag.fetch_sub(BUSY, Ordering::Acquire);

        } else {

            //
            // - run the closure
            // - finalize by setting the DONE bit
            // - unset the BUSY bit
            //
            struct _Guard<'a>(&'a AtomicUsize);
            impl<'a> Drop for _Guard<'a> {
                fn drop(&mut self) {
                    let _ = set_or_spin(
                        &self.0,
                        BUSY,
                        DONE | POISON,
                        DONE | POISON,
                        BUSY,
                        &|user| user,
                        &|c| c,
                        &|_| true,
                    );
                }
            }
            let guard = _Guard(&self.tag);
            cb();
            mem::forget(guard);
            let _ = set_or_spin(
                &self.tag,
                BUSY,
                DONE | POISON,
                DONE,
                BUSY,
                &|user| user,
                &|c| c,
                &|_| true,
            );
        }
    }
}