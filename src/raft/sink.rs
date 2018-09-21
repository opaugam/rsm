//! Notification sink coupled with the raft automaton. It allows client code to receive updates
//! whenever the state changes, commits are received, etc. The sink has a built-in capacity beyond
//! which new notifications will be dropped.
use primitives::semaphore::*;
use fsm::mpsc::MPSC;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Events emitted by the state machine. Those are available for the user to react
/// to membership changes, commits, etc.
#[derive(Debug)]
pub enum Notification {
    FOLLOWING,
    LEADING,
    IDLE,
    COMMIT(u64, Vec<u8>),
    CHECKPOINT(u64),
    EXIT,
}

/// Simple blocking notification sink consuming from a MPSC. Once signaled with no
/// content the sink will disable itself and always fail.
pub struct Sink {
    pub(super) sem: Semaphore,
    pub(super) fifo: MPSC<Notification>,
    len: AtomicUsize,
}

impl Sink {
    const CAPACITY: usize = 4096;

    #[allow(dead_code)]
    pub fn next(&self) -> Option<Notification> {

        //
        // - wait/pop, this will fast-fail on None as soon as we disable the semaphore, e.g
        //   when the state-machine exits
        //
        self.sem.wait();
        self.len.fetch_sub(1, Ordering::Release);
        self.fifo.pop().ok()
    }

    pub(super) fn new() -> Self {

        Self {
            sem: Semaphore::new(),
            fifo: MPSC::new(),
            len: AtomicUsize::new(0),
        }
    }

    pub(super) fn push(&self, n: Notification) -> () {
        if self.len.load(Ordering::Relaxed) < Self::CAPACITY {

            //
            // - push to the fifo and signal the semaphore
            // - increment the count (note the capacity check is not meant to be exact)
            //
            self.fifo.push(n);
            self.sem.signal();
            self.len.fetch_add(1, Ordering::Release);
        }
    }
}
