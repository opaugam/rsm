//! Notification sink coupled with the raft automaton. It allows client code to receive updates
//! whenever the state changes, commits are received, etc.
use primitives::semaphore::*;
use fsm::mpsc::MPSC;

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
}

impl Sink {
    #[allow(dead_code)]
    pub fn next(&self) -> Option<Notification> {

        //
        // - wait/pop, this will fast-fail on None as soon as we disable the semaphore, e.g
        //   when the state-machine exits
        //
        self.sem.wait();
        self.fifo.pop().ok()
    }

    pub(super) fn new() -> Self {

        Sink {
            sem: Semaphore::new(),
            fifo: MPSC::new(),
        }
    }
}
