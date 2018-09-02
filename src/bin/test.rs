
extern crate rsm;
extern crate rand;

use std::thread;
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

    let gate = Arc::new(Gate::new());
    let event = Arc::new(Event::new());

    {
        let guard = event.guard();
        for n in 0..64 {

            let gate = gate.clone();
            let guard = guard.clone();
            let n = thread::spawn(move || {

                gate.enter(|n| {
                    println!("{} entries so far", n);
                    n < 4
                });
                println!("> {}", n);
                drop(guard);
            });
        }
    }

    gate.open();
    event.wait();
    assert!(gate.entries() == 64);
}
