
extern crate rsm;
extern crate rand;

use std::thread;
use rsm::lock::LIFOLock;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::sync::atomic::spin_loop_hint;

fn main() {

    let locks: Arc<Vec<_>> = Arc::new((0..8).map(|n| {LIFOLock::tagged_as(n)} ).collect());
    let mut threads = Vec::new();
    for n in 0..512 {
        let locks = locks.clone();
        let tid = thread::spawn(move || {
            let mut rng = thread_rng();
            loop {
                let lock = rng.choose(&locks).unwrap();
                lock.lock();
                println!("thread {} | lock {}", n, lock.tag());
                for _ in 0..rng.gen_range(0, 40) {
                    spin_loop_hint();
                }
                lock.unlock();
            }
        });
        threads.push(tid);
    }

    for tid in threads {
        let _ = tid.join().unwrap();
    }
}
