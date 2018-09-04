//! Minimalistic automaton (e.g finite state machine) backed by an event loop running on a
//! dedicated thread.
use fsm::automaton::{Automaton, Opcode, Recv};
use primitives::event::*;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub enum CMD<T> {
    TICK,
    FORWARD(T, u64),
}

#[derive(Debug, Copy, Clone)]
enum State {
    OPERATING,
}

impl Default for State {
    fn default() -> State {
        State::OPERATING
    }
}

use self::CMD::*;
use self::State::*;

///
pub struct Clock<T>
where
    T: Send + 'static,
{
    _slots: Vec<T>,
}

impl<T> Recv<CMD<T>, State> for Clock<T>
where
    T: Send + 'static,
{
    fn recv(&mut self, _this: &Automaton<CMD<T>>, state: State, opcode: Opcode<CMD<T>>) -> State {
        match (state, opcode) {
            (OPERATING, Opcode::INPUT(TICK)) => {}
            _ => {}
        }
        state
    }
}

impl<T> Clock<T>
where
    T: Send + 'static,
{
    pub fn spawn(guard: Arc<Guard>) -> Arc<Automaton<CMD<T>>> {

        let fsm = Automaton::spawn(guard.clone(), Box::new(Clock { _slots: Vec::new() }));

        {
            //
            // - allocate an internal thread that will periodically post a TICK
            //   command back to the automaton
            //
            let fsm = fsm.clone();
            let _ = thread::spawn(move || {

                //
                // - the thread will stop spinning as soon as it cannot post anymore
                // - this is safe as the automaton is already started by now
                //
                let cv = Condvar::new();
                let mtx = Mutex::new(());
                while fsm.post(TICK).is_ok() {

                    //
                    // - lock the mutex and wait on the condition variable for up
                    //   to 250ms
                    //
                    let lock = mtx.lock().unwrap();
                    let _ = cv.wait_timeout(lock, Duration::from_millis(250)).unwrap();
                }
                drop(guard);
            });
        }

        fsm
    }
}
