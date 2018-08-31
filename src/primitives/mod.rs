use std::cell::UnsafeCell;
use std::ptr;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering, spin_loop_hint};

pub mod countdown;
pub mod lock;
pub mod semaphore;

const CNT_MSK: usize = 0xFFFF_FF00;
const USR_MSK: usize = 0xFFFF_FFFF_0000_0000;

///
/// Internal double linked list node holding mutex/condvar pairs. Those
/// are transient and only allocated upon contention. Note the LIFO strategy
/// will only use one pointer.
///
struct Node {
    synchro: Arc<(Mutex<bool>, Condvar)>,
    p: UnsafeCell<*mut Node>,
    n: UnsafeCell<*mut Node>,
}

impl Node {
    fn new() -> *mut Node {
        Box::into_raw(Box::new(Node {
            synchro: Arc::new((Mutex::new(true), Condvar::new())),
            p: UnsafeCell::new(ptr::null_mut()),
            n: UnsafeCell::new(ptr::null_mut()),
        }))
    }
}

pub trait Strategy {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)>;
    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>);
}

/// Simple LIFO queue, e.g a stack. This is very light although not really
/// conducing to fairness. A handful of threads are likely to hog the lock
/// and starve the others.
pub struct LIFO {
    head: UnsafeCell<*mut Node>,
}

impl Default for LIFO {
    fn default() -> Self {
        LIFO { head: UnsafeCell::new(ptr::null_mut()) }
    }
}

impl Strategy for LIFO {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)> {

        //
        // - allocate a new single pointer node
        // - push to the left of the head
        // - note the 2nd pointer is not used
        //
        let ptr = Node::new();
        let head = *self.head.get();
        *(*ptr).p.get() = head;

        //
        // - update the head
        //
        *self.head.get() = ptr;
        (*ptr).synchro.clone()
    }

    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>) {

        //
        // - simply pop the head
        //
        debug_assert!(!(*self.head.get()).is_null());
        let nxt = *self.head.get();
        *self.head.get() = *(*nxt).p.get();

        //
        // - drop the node
        // - the mutex/condvar owning arc will then unref and drop later
        //
        let _: Box<Node> = Box::from_raw(nxt);
        ((*self.head.get()).is_null(), (*nxt).synchro.clone())
    }
}

/// Simple FIFO queue. This is a bit heavier in terms of code but is
/// guaranteed to be fair. Threads will be awaken based on their waiting
/// order.
pub struct FIFO {
    head: UnsafeCell<*mut Node>,
}

impl Default for FIFO {
    fn default() -> Self {
        FIFO { head: UnsafeCell::new(ptr::null_mut()) }
    }
}

impl Strategy for FIFO {
    unsafe fn push(&self) -> Arc<(Mutex<bool>, Condvar)> {

        //
        // - allocate a node and append it to the tail
        //
        let ptr = Node::new();
        let head = *self.head.get();
        if head.is_null() {

            //
            // - chain the node to itself (e.g head == tail)
            //
            *(*ptr).p.get() = ptr;
            *(*ptr).n.get() = ptr;

        } else {

            //
            // - we have 1+ nodes already
            // - chain to the right of the tail
            //
            *(*ptr).p.get() = *(*head).p.get();
            *(*ptr).n.get() = head;
            *(*head).p.get() = ptr;
        };

        //
        // - update the head
        //
        *self.head.get() = ptr;
        (*ptr).synchro.clone()
    }

    unsafe fn pop(&self) -> (bool, Arc<(Mutex<bool>, Condvar)>) {

        //
        // - pop the tail
        //
        let head = *self.head.get();
        let tail = *(*head).p.get();
        let last = if head == tail {

            //
            // - head == tail -> last node
            // - reset the head
            //
            *self.head.get() = ptr::null_mut();
            true

        } else {

            //
            // - we have at least 2 nodes
            // - re-link the penultimate node (tail.p)
            //
            let penultimate = *(*tail).p.get();
            *(*head).p.get() = penultimate;
            *(*penultimate).n.get() = head;
            false
        };

        //
        // - drop the node
        // - the mutex/condvar owning arc will then unref and drop later
        //
        let _: Box<Node> = Box::from_raw(tail);
        (last, (*tail).synchro.clone())
    }
}

pub fn set_or_spin<E, F, G>(
    state: &AtomicUsize,
    on: usize,
    off: usize,
    set: usize,
    unset: usize,
    user: &E,
    incr: &F,
    cold: &G,
) -> Option<usize>
where
    E: Fn(u32) -> u32,
    F: Fn(usize) -> usize,
    G: Fn(&AtomicUsize) -> bool,
{
    //
    // - the thread will spin up to 30 times total in the worst
    //   case before invoking the 'cold' closure
    //
    let mut n = 2;
    let mut cur = state.load(Ordering::Relaxed);
    loop {

        //
        // - the expected value must match the specified on/off bits
        // - extract the counter bits into a usize
        //
        let payload = user((cur >> 32) as u32);
        let expected = (cur | on) & !off;
        let mut cnt = (cur & CNT_MSK) >> 8;

        //
        // - run the increment closure
        // - shift the counter bits back to where they should be
        // - attempt a CAS with the target value properly constructed
        //
        cnt = incr(cnt) << 8;
        match state.compare_exchange_weak(
            expected,
            (expected & !(unset | CNT_MSK | USR_MSK)) | ((payload as usize) << 32) | set |
                cnt,
            Ordering::Acquire,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                return Some(expected | set);
            }
            Err(prv) => {
                cur = prv;
                if n > 16 {
                    if !cold(state) {
                        return None;
                    }
                } else {
                    for _ in 0..n {
                        spin_loop_hint();
                    }
                    n <<= 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    extern crate rand;

    use primitives::*;
    use primitives::countdown::*;
    use primitives::lock::*;
    use primitives::semaphore::*;
    use primitives::tests::rand::{Rng, thread_rng};
    use std::sync::Arc;
    use std::sync::atomic::spin_loop_hint;
    use std::thread;

    #[test]
    fn lifo_loop_32x4() {
        let locks: Arc<Vec<_>> = Arc::new((0..4).map(|_| Lock::<LIFO>::new()).collect());
        let mut threads = Vec::new();
        for _ in 0..32 {
            let locks = locks.clone();
            let tid = thread::spawn(move || {
                let mut rng = thread_rng();
                for _ in 0..1024 {
                    let lock = rng.choose(&locks).unwrap();
                    lock.lock(|n| n + 1);
                    for _ in 0..rng.gen_range(0, 40) {
                        spin_loop_hint();
                    }
                    lock.unlock(|n| n);
                }
            });
            threads.push(tid);
        }

        for tid in threads {
            tid.join().unwrap();
        }

        let mut total = 0;
        for lock in locks.iter() {
            assert!(lock.pending() == 0);
            total += lock.tag();
        }
        assert!(total == 32 * 1024);
    }

    #[test]
    fn fifo_loop_32x4() {
        let locks: Arc<Vec<_>> = Arc::new((0..4).map(|_| Lock::<FIFO>::new()).collect());
        let mut threads = Vec::new();
        for _ in 0..32 {
            let locks = locks.clone();
            let tid = thread::spawn(move || {
                let mut rng = thread_rng();
                for _ in 0..1024 {
                    let lock = rng.choose(&locks).unwrap();
                    lock.lock(|n| n + 1);
                    for _ in 0..rng.gen_range(0, 40) {
                        spin_loop_hint();
                    }
                    lock.unlock(|n| n);
                }
            });
            threads.push(tid);
        }

        for tid in threads {
            tid.join().unwrap();
        }

        let mut total = 0;
        for lock in locks.iter() {
            assert!(lock.pending() == 0);
            total += lock.tag();
        }
        assert!(total == 32 * 1024);
    }

    #[test]
    fn synchro_512() {
        let sem = Arc::new(Semaphore::new());
        let then = Arc::new(Countdown::new(512));
        let lock = Arc::new(Lock::<FIFO>::new());
        let mut threads = Vec::new();
        for _ in 0..512 {
            let sem = sem.clone();
            let then = then.clone();
            let lock = lock.clone();
            let tid = thread::spawn(move || {
                lock.lock(|n| n + 1);
                lock.unlock(|n| n);
                then.run(|| sem.signal());
            });
            threads.push(tid);
        }
    
        sem.wait();
        assert!(then.has_run());
        assert!(lock.tag() == 512);
    }
}
