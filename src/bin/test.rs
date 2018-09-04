
extern crate rsm;
extern crate rand;

use std::thread;
use rsm::fsm::automaton::*;
use rsm::fsm::timer::*;
use rsm::primitives::FIFO;
use rsm::primitives::countdown::*;
use rsm::primitives::gate::*;
use rsm::primitives::lock::*;
use rsm::primitives::once::*;
use rsm::primitives::semaphore::*;
use rsm::primitives::event::*;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::sync::atomic::spin_loop_hint;

fn main() {


    #[derive(Debug)]
    enum CMD {
        PING,
    }

    #[derive(Debug, Copy, Clone)]
    enum State {
        RESET,
    }

    impl Default for State {
        fn default() -> State {
            State::RESET
        }
    }

    struct A {
        cnt: usize,
    }

    impl Recv<CMD, State> for A {
        fn recv(&mut self, this: &Automaton<CMD>, state: State, opcode: Opcode<CMD>) -> State {
            self.cnt += 1;
            println!("#{}", self.cnt);
            if self.cnt > 8 {
                this.drain();
            }
            state
        }
    }

    let event = Event::new();
    let guard = event.guard();
    let fsm = Automaton::spawn(guard.clone(), Box::new(A { cnt: 0 }));
    let clock = Clock::<CMD>::spawn(guard.clone());

    {
        let fsm = fsm.clone();
        let guard = guard.clone();
        thread::spawn(move || {
            while fsm.post(CMD::PING).is_ok() {
            }
            drop(guard);
        });
    }

    drop(guard);
    clock.drain();
    event.wait();
}
