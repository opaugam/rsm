
extern crate rsm;
extern crate rand;

use std::thread;
use rsm::lock::*;
use rand::{Rng, thread_rng};
use std::sync::Arc;
use std::sync::atomic::spin_loop_hint;

fn main() {

    let locks: Arc<Vec<_>> = Arc::new((0..1).map(|n| Lock::<FIFO>::tagged_as(n)).collect());
    let mut threads = Vec::new();
    for n in 0..512 {
        let locks = locks.clone();
        let tid = thread::spawn(move || {
            let mut rng = thread_rng();
            loop {
                let lock = rng.choose(&locks).unwrap();
                lock.lock();
                println!(
                    "thread {} | lock {} ({} pending)",
                    n,
                    lock.tag(),
                    lock.pending()
                );
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
