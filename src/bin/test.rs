
extern crate rsm;
extern crate rand;

use std::thread;
use rsm::primitives::FIFO;
use rsm::primitives::countdown::*;
use rsm::primitives::lock::*;
use rsm::primitives::semaphore::*;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::sync::atomic::spin_loop_hint;

fn main() {

    let lock = Arc::new(Lock::<FIFO>::new());
    let mut threads = Vec::new();
    for t in 0..128 {
        let lock = lock.clone();
        let tid = thread::spawn(move || {
            let mut rng = thread_rng();
            loop {
                lock.lock(|n| n + 1);
                for _ in 0..rng.gen_range(0, 4096) {
                    spin_loop_hint();
                }
                println!("#{} got the lock -> {}", t, lock.tag());
                lock.unlock(|n| n - 1);
            }
        });
        threads.push(tid);
    }

    for tid in threads {
        tid.join().unwrap();
    }

    println!("{}", lock.tag());
}
