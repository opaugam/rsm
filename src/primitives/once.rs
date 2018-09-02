//! A simple "once" (e.g countdown of 1) owning a closure. This construct can typically be
//! used to run some init/de-init code precisely one time only. It will also attempt to run
//! its closure upon dropping.
use self::countdown::*;
use super::*;

/// Trivial once wrapping a countdown of 1 and a closure.
pub struct Once<F>
where
    F: Fn() -> (),
{
    once: Countdown,
    f: F,
}

impl<F> Once<F>
where
    F: Fn() -> (),
{
    #[inline]
    pub fn from(f: F) -> Self {
        let once = Countdown::new();

        //
        // - activate the countdown by calling incr() once
        //
        once.incr();
        Once { once, f }
    }

    #[inline]
    pub fn run(&self) -> () {
        self.once.run(&self.f);
    }
}

impl<F> Drop for Once<F>
where
    F: Fn() -> (),
{
    fn drop(&mut self) -> () {

        //
        // - run the once upon its own drop() in case nobody invoked it
        //
        self.run();
    }
}
