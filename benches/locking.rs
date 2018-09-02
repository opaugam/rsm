#[macro_use]
extern crate criterion;
extern crate rsm;
extern crate rand;

use criterion::Criterion;
use rsm::primitives::*;
use rsm::primitives::lock::Lock;
use rsm::primitives::event::*;
use std::thread;
use std::sync::Arc;

fn lock_1k_n<T: 'static + Strategy + Default>(lock: Lock<T>, n: usize, slow: bool) {

    let lock = Arc::new(lock);
    let event = Arc::new(Event::new());
    {
        let guard = event.guard();
        for _ in 0..n {

            let lock = lock.clone();
            let guard = guard.clone();
            let _ = thread::spawn(move || {
                for _ in 0..1000 {

                    lock.lock(|n| n);
                    if slow {
                        thread::yield_now();
                    }
                    lock.unlock(|n| n);
                }
                drop(guard);
            });
        }
    }

    event.wait();
}

fn benchmark(c: &mut Criterion) {

    let sizes = vec![1, 2, 4];
    for n in sizes {
        c.bench_function(&format!("lock (lifo, 1K X {})", n), move |b| {
            b.iter(|| lock_1k_n(Lock::<LIFO>::new(), n, false))
        });
        c.bench_function(&format!("lock (lifo [yield], 1K X {})", n), move |b| {
            b.iter(|| lock_1k_n(Lock::<LIFO>::new(), n, true))
        });
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
