//! Minimalistic automaton (e.g finite state machine) backed by an event loop running on a
//! dedicated thread.
use primitives::event::*;
use self::mpsc::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::mem;
use super::*;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Mode {
    IDLE = 0,
    RUNNING = 1,
    SIGNALED = 2,
    DRAINING = 3,
    DEAD = 4,
}

#[derive(Debug)]
pub enum Errors {
    InvalidMode(Mode),
}

#[derive(Debug)]
pub enum Opcode<T, U> 
where U: PartialEq
{
    START,
    INPUT(T),
    TRANSITION(U),
    DRAIN,
    EXIT,
}

use self::Errors::*;
use self::Mode::*;
use self::Opcode::*;

/// User defined handler specifying the current state and opcode to process.
/// The state will be updated to whatever value is returned.
pub trait Recv<T, U>: Send
where
    T: Send,
    U: PartialEq
{
    fn recv(&mut self, this: &Arc<Automaton<T>>, state: U, opcode: Opcode<T, U>) -> U;
}

/// Automaton (e.g finite state machine) maintaining an incoming queue of commands
/// to process plus a mode (e.g running, dead, etc.). The mode itself is a simple
/// state-machine managing transitions, for instance to switch to draining.
/// It maintains an underlying thread running its event loop (e.g dequeueing from the
/// inbox and invoking a user specified handler).
pub struct Automaton<T>
where
    T: Send + 'static,
{
    event: Event,
    inbox: MPSC<T>,
    mode: AtomicUsize,
}

impl<T> Automaton<T>
where
    T: Send + 'static,
{
    #[inline]
    pub fn spawn<U>(guard: Arc<Guard>, body: Box<dyn Recv<T, U>>) -> Arc<Automaton<T>>
    where
        U: Send + Copy + Default + PartialEq + 'static,
    {
        let fsm = Automaton::with(guard, body);
        fsm.start();
        fsm
    }

    #[inline]
    pub fn with<U>(guard: Arc<Guard>, mut body: Box<dyn Recv<T, U>>) -> Arc<Automaton<T>>
    where
        U: Send + Copy + Default + PartialEq + 'static,
    {
        let fsm = Arc::new(Automaton {
            inbox: MPSC::new(),
            event: Event::new(),
            mode: AtomicUsize::new(0),
        });

        {
            //
            // - allocate the underlying event loop
            // - it is synchronized via the specified guard
            //
            let fsm = fsm.clone();
            let _ = thread::spawn(move || {

                //
                // - the mode is IDLE
                // - block until start() is invoked
                // - recv on START and assign the initial state
                //
                fsm.event.wait();
                let mut draining = false;
                let mut state = Default::default();
                let _ = body.recv(&fsm, state, START);
                loop {

                    //
                    // - we are either RUNNING or SIGNALED
                    // - if SIGNALED (e.g drain() was invoked) transition to DRAINING and
                    //   recv() on DRAIN
                    //
                    draining |=
                        fsm.transition_if(SIGNALED, || { let _ = body.recv(&fsm, state, DRAIN); });
                    if let Ok(msg) = fsm.inbox.pop() {

                        //
                        // - we dequeued the next command
                        // - recv() on INPUT and optional on TRANSITION if the
                        //   state is deemed differnt (see below)
                        //
                        let next = body.recv(&fsm, state, INPUT(msg));
                        if next != state {
                            body.recv(&fsm, next, TRANSITION(state));
                        }

                        //
                        // - assign the state (please note it could implement
                        //   PartialEq and be different in reality, espcially if
                        //   it carries some internal payload)
                        //
                        state = next;

                    } else if draining {

                        //
                        // - we've been draining and the input queue is now empty
                        // - transition a last time and recv() on EXIT
                        // - we're done, exit the processing loop
                        //
                        fsm.mode.fetch_add(1, Ordering::Release);
                        let _ = body.recv(&fsm, state, EXIT);
                        break;

                    } else {

                        //
                        // - we're running but are choking on the input queue
                        // - park ourselves pending new commands
                        //
                        fsm.event.wait();
                    }
                }

                //
                // - the automaton is now dead
                // - drop the guard to signal the underlying event
                // - exit the thread
                //
                drop(guard);
            });
        }
        fsm.start();
        fsm
    }

    #[inline]
    pub fn start(&self) -> () {

        //
        // - attempt to transition from IDLE to RUNNING
        // - signal the internal event upon success to start the
        //   underlying event loop
        //
        let _ = self.transition_if(IDLE, || self.event.signal());
    }

    #[inline]
    pub fn drain(&self) -> () {

        //
        // - attempt to transition from RUNNING to SIGNALED
        // - signal the internal event upon success to potentially wake
        //   the event loop up in case the queue is empty
        //
        let _ = self.transition_if(RUNNING, || self.event.signal());
    }

    #[inline]
    pub fn mode(&self) -> Mode {
        let mode = self.mode.load(Ordering::Relaxed);
        unsafe { mem::transmute(mode as u8) }
    }

    #[inline]
    pub fn post(&self, msg: T) -> Result<(), Errors> {

        //
        // - only allow pushing to the queue if the mode is RUNNING
        //
        //
        match self.mode.load(Ordering::Relaxed) {
            1 => {
                self.inbox.push(msg);
                self.event.signal();
                Ok(())
            }
            mode => Err(InvalidMode(unsafe { mem::transmute(mode as u8) })),
        }
    }

    #[inline]
    fn transition_if<F>(&self, expected: Mode, mut f: F) -> bool
    where
        F: FnMut() -> (),
    {
        let expected = expected as usize;
        loop {

            //
            // - attempt to increment the mode if and only it is presently at the
            //   specified value and run the closure upon success
            // - looping is only allowed upon a spurious CAS failure
            //
            match self.mode.compare_exchange_weak(
                expected,
                expected + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    f();
                    return true;
                }
                Err(prv) if prv != expected => return false,
                _ => {}
            }
        }
    }
}
