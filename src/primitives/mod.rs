use std::cell::UnsafeCell;
use std::ptr;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering, spin_loop_hint};

pub mod countdown;
pub mod event;
pub mod gate;
pub mod lock;
pub mod once;
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
) -> Result<usize, usize>
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
        let target = (expected & !(unset | CNT_MSK | USR_MSK)) | ((payload as usize) << 32) |
            set | cnt;
        match state.compare_exchange_weak(expected, target, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => {
                return Ok(expected | set);
            }
            Err(prv) => {
                cur = prv;
                if n > 16 {
                    if !cold(state) {
                        return Err(cur);
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
    use primitives::event::*;
    use primitives::gate::*;
    use primitives::lock::*;
    use primitives::once::*;
    use primitives::tests::rand::{Rng, thread_rng};
    use std::sync::Arc;
    use std::sync::atomic::spin_loop_hint;
    use std::thread;

    fn random_work(spins: usize) -> () {
        let mut rng = thread_rng();
        for _ in 0..rng.gen_range(0, spins) {
            spin_loop_hint();
        }
    }

    #[test]
    fn synchro_event() {

        let lock = Arc::new(Lock::<FIFO>::new());
        let event = Arc::new(Event::new());

        {
            let guard = event.guard();
            for _ in 0..64 {

                let lock = lock.clone();
                let guard = guard.clone();
                let _ = thread::spawn(move || {

                    lock.lock(|n| n);
                    random_work(40);
                    lock.unlock(|n| n + 1);

                    drop(guard);
                });
            }
        }

        event.wait();
        assert!(lock.tag() == 64);
        assert!(lock.pending() == 0);
    }

    #[test]
    fn synchro_once() {

        let event = Arc::new(Event::new());
        let once = {
            let event = event.clone();
            Arc::new(Once::from(move || { event.signal(); }))
        };

        let _ = thread::spawn(move || { drop(once); });

        event.wait();
    }

    #[test]
    fn synchro_once_2() {

        let lock = Arc::new(Lock::<FIFO>::new());
        let event = Arc::new(Event::new());
        let once = {
            let lock = lock.clone();
            Arc::new(Once::from(move || {
                lock.lock(|n| n);
                lock.unlock(|n| n + 1);
            }))
        };

        {
            let guard = event.guard();
            for _ in 0..64 {

                let once = once.clone();
                let guard = guard.clone();
                let _ = thread::spawn(move || {

                    once.run();
                    drop(guard);
                });
            }
        }

        event.wait();
        assert!(lock.tag() == 1);
    }

    #[test]
    fn synchro_gate() {

        let gate = Arc::new(Gate::new());
        let event = Arc::new(Event::new());

        {
            let guard = event.guard();
            for _ in 0..64 {

                let gate = gate.clone();
                let guard = guard.clone();
                let _ = thread::spawn(move || {

                    gate.enter(|_| true);
                    drop(guard);
                });
            }
        }

        gate.open();
        event.wait();
        assert!(gate.entries() == 64);
    }


    #[test]
    fn synchro_gate_2() {

        let gate = Arc::new(Gate::new());
        let event = Arc::new(Event::new());
        for _ in 0..64 {

            let gate = gate.clone();
            let event = event.clone();
            let _ = thread::spawn(move || {

                random_work(40);
                let open = gate.enter(|n| n < 8);
                if !open {
                    event.signal();
                }
            });
        }

        gate.open();
        for _ in 0..8 {
            event.wait();
            assert!(gate.entries() == 8);
            gate.open();
        }
    }
}
