//! A simple "auto reset event" (e.g semaphore capped to 1) as described in the excellent
//! [preshing blog](http://preshing.com/20150316/semaphores-are-surprisingly-versatile/) post.
//! This construct can typically be used to wake threads up when work is available. An additional
//! guard is also provided to allow for signaling the event when the guard drops (very handy to
//! wait for a group of threads to complete work).
//!
//! Please note each event may carry 32bits of user payload.
use self::semaphore::*;
use std::sync::Arc;
use super::*;

/// Trivial auto-reset event wrapping a semaphore whose count is capped at 1.
pub struct Event {
    sem: Arc<Semaphore>,
}

/// Shallow guard owning a clone of the event's semaphore and signaling it upon
/// dropping. This is handy to synchronize a thread based on how long the guard
/// is shared across 1+ other threads.
pub struct Guard(Arc<Semaphore>);

impl Default for Event {
    fn default() -> Self {
        Self::new()
    }
}

impl Event {
    #[inline]
    pub fn new() -> Self {
        Event::with(0)
    }

    #[inline]
    pub fn with(tag: u32) -> Self {
        Event { sem: Arc::new(Semaphore::with(tag)) }
    }

    #[inline]
    pub fn tag(&self) -> u32 {
        self.sem.tag()
    }

    #[inline]
    pub fn signal(&self) -> () {
        self.sem.signal_under(1);
    }

    #[inline]
    pub fn wait(&self) -> () {
        self.sem.wait();
    }

    #[inline]
    pub fn guard(&self) -> Arc<Guard> {
        Arc::new(Guard(self.sem.clone()))
    }
}

impl Drop for Guard {
    fn drop(&mut self) -> () {
        self.0.signal();
    }
}
