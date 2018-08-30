use std::boxed::Box;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

#[derive(Debug)]
pub enum Errors {
    Empty,
    CapacityExceeded,
}

use self::Errors::*;

//
// - the queue is a linked list growing to the left and shrinking from the right
// - the option is required to represent the empty marker
//
struct Node<T> {
    prv: AtomicPtr<Node<T>>,
    val: Option<T>,
}

///
/// Basic unbounded MPSC FIFO queue.
///
///            head <- node <- ... <- tail
/// push() ->   x
///                                     x -> pop()
///
/// Also check out http://www.1024cores.net/home/lock-free-algorithms/queues for more
/// interesting ideas.
///
pub struct MPSC<T> {
    head: AtomicPtr<Node<T>>,
    tail: UnsafeCell<*mut Node<T>>,
}

impl<T> Node<T> {
    unsafe fn new(val: Option<T>) -> *mut Node<T> {
        Box::into_raw(Box::new(Node {
            prv: AtomicPtr::new(ptr::null_mut()),
            val,
        }))
    }
}

//
// - the struct must be Sync+Send
// - it only exposes the push and pop operations
// - pop() *must* only ever be used by one thread at a time
//
unsafe impl<T: Send> Send for MPSC<T> {}
unsafe impl<T: Send> Sync for MPSC<T> {}
impl<T> Drop for MPSC<T> {
    fn drop(&mut self) {
        unsafe {
            let mut node = *self.tail.get();
            while !node.is_null() {
                let prv = (*node).prv.load(Ordering::Relaxed);

                //
                // - release the node via a ::from_raw() + drop checker
                //
                let _: Box<Node<T>> = Box::from_raw(node);
                node = prv;
            }
        }
    }
}

impl<T> MPSC<T> {
    pub fn new() -> MPSC<T> {

        //
        // - create the empty marker
        // - seed the queue with it
        //
        let marker = unsafe { Node::new(None) };
        MPSC {
            head: AtomicPtr::new(marker),
            tail: UnsafeCell::new(marker),
        }
    }

    pub fn push(&self, val: T) -> () {
        unsafe {
            //
            // - swap the head with our new node
            // - the swap() is the serialization point
            // - chain it to the old head
            //
            let latest = Node::new(Some(val));
            let nxt = self.head.swap(latest, Ordering::AcqRel);
            (*nxt).prv.store(latest, Ordering::Release);
        }
    }

    pub fn pop(&self) -> Result<T, Errors> {
        unsafe {
            loop {
                let tail = *self.tail.get();

                //
                // - atomic read
                // - since pop() is assumed to be called from one thread at once..
                // - .. the node to the left of the tail cannot be updated
                // - the only possibilty being a queue of size 0 being updated with a new node
                //
                let prv = (*tail).prv.load(Ordering::Acquire);
                if !prv.is_null() {

                    //
                    // - we have at least one element in the queue
                    // - shift the tail to the left by updating its pointer
                    // - consume that node value
                    //
                    *self.tail.get() = prv;
                    assert!((*tail).val.is_none());
                    assert!((*prv).val.is_some());

                    //
                    // - release the tail node via a ::from_raw() + drop checker
                    //
                    let _: Box<Node<T>> = Box::from_raw(tail);
                    return Ok((*prv).val.take().unwrap());

                //
                // - head == tail -> the queue is empty
                //
                } else if self.head.load(Ordering::Acquire) == tail {
                    return Err(Empty);
                }

                //
                // - head is != tail with prv still null
                // - this means whoever is in push() has been preempted
                //   between the swap and the store
                // - loop and retry
                //
            }
        }
    }
}
