
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
use std::time::Duration;

fn random_work(spins: usize) -> () {
    let mut rng = thread_rng();
    for _ in 0..rng.gen_range(0, spins) {
        spin_loop_hint();
    }
}

fn main() {

        let gate = Arc::new(Gate::new());
        let event = Arc::new(Event::new());
        for _ in 0..64 {

            let gate = gate.clone();
            let event = event.clone();
            let _ = thread::spawn(move || {

                random_work(40);
                gate.enter(|n| if n == 8 {
                    event.signal();
                    false
                } else {
                    true
                });
            });
        }

        gate.open();
        for _ in 0..8 {
            event.wait();
            assert!(gate.entries() == 8);
            gate.open();
        }

}
