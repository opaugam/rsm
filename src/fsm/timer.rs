//! Minimalistic automaton (e.g finite state machine) backed by an event loop running on a
//! dedicated thread.
use fsm::automaton::{Automaton, Opcode, Recv};
use primitives::event::*;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

pub enum Command<T>
where
    T: Send + 'static,
{
    TICK,
    SCHEDULE(Arc<Automaton<T>>, T, Duration),
}

#[derive(Copy, Clone, PartialEq)]
enum State {
    DEFAULT,
}

impl Default for State {
    fn default() -> State {
        State::DEFAULT
    }
}

use self::Command::*;

//
// - number of hashed wheel slots
// - our clock frequency is 25ms, our granularity is 64ms: pick 256
// - note this must be a power of 2
//
const SLOTS: usize = 1 << 8;

pub struct Timer<T>
where
    T: Send + 'static,
{
    pub fsm: Arc<Automaton<Command<T>>>,
}

struct Slot<T>
where
    T: Send + 'static,
{
    cnt: u32,
    heap: BinaryHeap<Pending<T>>,
}

struct FSM<T>
where
    T: Send + 'static,
{
    n: usize,
    epoch: Instant,
    slots: Vec<Slot<T>>,
}

struct Pending<T>
where
    T: Send + 'static,
{
    n: u32,
    tick: u64,
    to: Weak<Automaton<T>>,
    msg: T,
}

impl<T> Ord for Pending<T>
where
    T: Send + 'static,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.tick.cmp(&other.tick).reverse()
    }
}

impl<T> PartialOrd for Pending<T>
where
    T: Send + 'static,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Pending<T>
where
    T: Send + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        (self.n, self.tick) == (other.n, other.tick)
    }
}

impl<T> Eq for Pending<T>
where
    T: Send + 'static,
{
}

impl<T> Recv<Command<T>, State> for FSM<T>
where
    T: Send + 'static,
{
    fn recv(
        &mut self,
        _this: &Arc<Automaton<Command<T>>>,
        state: State,
        opcode: Opcode<Command<T>, State>,
    ) -> State {
        match (state, opcode) {
            (_, Opcode::INPUT(TICK)) => {

                //
                // - timer evaluation: the current tick in milliseconds is mapped to a
                //   slot in the hashed wheel (e.g circular buffer)
                // - walk from the last slot to this one and peek into each
                // - pop as many entries as possible for each (e.g pop any entry whose tick
                //   is expired) and post the attached payload
                //
                let (until, ms) = self.tick_to_slot(Instant::now());
                while self.n != until {
                    let ref mut slot = self.slots[self.n];
                    loop {
                        match slot.heap.peek() {
                            Some(item) if item.tick <= ms => {}
                            _ => break,
                        }
                        let pending = slot.heap.pop().unwrap();
                        if let Some(ref fsm) = pending.to.upgrade() {
                            let _ = fsm.post(pending.msg);
                        }
                    }

                    self.n = (self.n + 1) & (SLOTS - 1);
                }
            }
            (_, Opcode::INPUT(SCHEDULE(to, msg, lapse))) => {

                let (n, ms) = self.tick_to_slot(Instant::now() + lapse);
                let ref mut slot = self.slots[n];
                slot.heap.push(Pending {
                    n: slot.cnt,
                    tick: ms,
                    to: Arc::downgrade(&to),
                    msg,
                });
                slot.cnt += 1;
            }
            _ => {}
        }
        state
    }
}

impl<T> FSM<T>
where
    T: Send + 'static,
{
    #[inline]
    fn tick_to_slot(&self, tick: Instant) -> (usize, u64) {

        //
        // - drop the tick on a timeline starting when we got instantiated
        // - convert to ms and bucket given some granularity (2^6, e.g 64ms)
        // - modulo into our slot array
        //
        let lapse = tick - self.epoch;
        let ms = (1e3 * (lapse.as_secs() as f64 + lapse.subsec_nanos() as f64 * 1e-9)) as u64;
        let n = (ms >> 6) as usize;
        (n & (SLOTS - 1), ms)
    }
}

impl<T> Timer<T>
where
    T: Send + 'static,
{
    pub fn spawn(guard: Arc<Guard>) -> Timer<T> {

        let slots = (0..SLOTS)
            .map(|_| {
                Slot {
                    cnt: 0,
                    heap: BinaryHeap::new(),
                }
            })
            .collect();

        let fsm = Automaton::spawn(
            guard.clone(),
            Box::new(FSM {
                n: 0,
                epoch: Instant::now(),
                slots,
            }),
        );

        {
            //
            // - allocate an internal thread that will periodically post a TICK
            //   command back to the automaton
            //
            let fsm = fsm.clone();
            let _ = thread::spawn(move || {

                //
                // - the thread will stop spinning as soon as it cannot post anymore
                // - this is safe as the automaton is already started by now
                //
                let cv = Condvar::new();
                let mtx = Mutex::new(());
                while fsm.post(TICK).is_ok() {

                    //
                    // - lock the mutex and wait on the condition variable for up
                    //   to 25ms
                    //
                    let lock = mtx.lock().unwrap();
                    let _ = cv.wait_timeout(lock, Duration::from_millis(25)).unwrap();
                }
                drop(guard);
            });
        }

        Timer { fsm }
    }

    pub fn schedule(&self, to: Arc<Automaton<T>>, msg: T, lapse: Duration) -> () {
        let _ = self.fsm.post(Command::SCHEDULE(to, msg, lapse));
    }
}

impl<T> Drop for Timer<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) -> () {
        self.fsm.drain();
    }
}
