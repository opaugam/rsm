#[macro_use]
extern crate criterion;
extern crate lock;
extern crate rand;

use criterion::Criterion;
use lock::lock::LIFOLock;
use std::thread;
use std::sync::Arc;

fn lock_1k_fast_path() {
    let lock = Arc::new(LIFOLock::new());
    for _ in 0..1000 {
        lock.lock();
        lock.unlock();
    }
}

fn lock_1k(size: usize) {
    let lock = Arc::new(LIFOLock::new());
    let mut threads = Vec::new();
    for _ in 0..size {
        let lock = lock.clone();
        let tid = thread::spawn(move || {
            for _ in 0..1024 {
                lock.lock();
                lock.unlock();
            }
        });
        threads.push(tid);
    }

    for tid in threads {
        tid.join();
    }
}

fn benchmark(c: &mut Criterion) {
    c.bench_function("lock (1K fast path)", |b| b.iter(|| lock_1k_fast_path()));
    c.bench_function("lock (1K X 4)", |b| b.iter(|| lock_1k(4)));
//    c.bench_function("lock (1K X 16)", |b| b.iter(|| lock_1K(16)));
//    c.bench_function("lock (1K X 128)", |b| b.iter(|| lock_1K(128)));
//    c.bench_function("lock (1K X 512)", |b| b.iter(|| lock_1K(512)));
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
