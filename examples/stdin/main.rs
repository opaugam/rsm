
//! Trivial automaton streaming its input from STDIN. The stream is interpreted as a sequence of
//! varints defining how many bytes we should then read and chunk. Chunks are then fed to the
//! automaton via a background thread.
extern crate ctrlc;
extern crate rsm;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use rsm::fsm::automaton::*;
use rsm::primitives::event::*;
use slog::{Drain, Level, LevelFilter, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::io::stderr;
use std::sync::Arc;

#[derive(Debug)]
enum Command {
    STDIN(Vec<u8>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum State {
    DEFAULT,
}

impl Default for State {
    fn default() -> State {
        State::DEFAULT
    }
}

struct FSM {
    n: usize,
    logger: Logger,
}

use self::Command::*;

impl Recv<Command, State> for FSM {
    fn recv(
        &mut self,
        _: &Arc<Automaton<Command>>,
        state: State,
        opcode: Opcode<Command, State>,
    ) -> State {
        match opcode {
            Opcode::CMD(STDIN(buf)) => {

                //
                // - we got a byte buffer from STDIN
                // - keep track of the total
                //
                self.n += buf.len();
            }
            Opcode::EXIT => {
                info!(&self.logger, "received {}B total", self.n);
            }
            _ => {}
        }
        state
    }
}

fn main() {

    let decorator = PlainSyncDecorator::new(stderr());
    let formatted = FullFormat::new(decorator).build().fuse();
    let async = Async::new(formatted).build().fuse();
    let filter = LevelFilter::new(async, Level::Trace).fuse();
    let root = Logger::root(filter, o!());
    let log = root.new(o!("sys" => "main"));
    debug!(&log, "starting (version={})", env!("CARGO_PKG_VERSION"));

    //
    // - use a termination event
    // - instantiate our automaton
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();
    let fsm = Automaton::spawn(
        guard,
        Box::new(FSM {
            n: 0,
            logger: root.new(o!("sys" => "fsm")),
        }),
    );

    //
    // - paste a streaming thread in front of the automaton
    // - each incoming byte chunk will be posted to the automaton via a STDIN message
    //
    rsm::fsm::io::pipe::stream_from_sdtin(&fsm, |bytes| Some(STDIN(bytes.to_vec())));

    //
    // - trap SIGINT/SIGTERM to properly terminate all threads
    // - each thread will signal the termination event
    //
    ctrlc::set_handler(move || { fsm.drain(); }).unwrap();

    //
    // - block on the termination event
    // - we are waiting for all our threads to gracefully drain/exit
    //
    event.wait();
    info!(&log, "exiting");
}
