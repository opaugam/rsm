
//! Basic raft automaton using STDIN/STDOUT pipes to communicate. This is meant to be coupled with
//! the python wrapper.
extern crate bincode;
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rand;
extern crate rsm;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use rsm::primitives::event::*;
use rsm::raft::io::pipe;
use rsm::raft::protocol::Payload;
use slog::{Drain, Level, LevelFilter, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::collections::HashMap;
use std::io::stderr;
use std::env;
use std::path::Path;
use std::sync::Arc;

#[derive(Default)]
struct Empty {}

impl Payload for Empty {}

fn main() {

    let decorator = PlainSyncDecorator::new(stderr());
    let formatted = FullFormat::new(decorator).build().fuse();
    let async = Async::new(formatted).build().fuse();
    let filter = LevelFilter::new(async, Level::Trace).fuse();
    let root = Logger::root(filter, o!());
    let log = root.new(o!("sys" => "main"));
    debug!(&log, "starting (version={})", env!("CARGO_PKG_VERSION"));

    //
    // - parse the CLI line
    //
    let args = clap_app!(node =>
        (version: env!("CARGO_PKG_VERSION"))
        (@arg PEERS: +takes_value +multiple +required "peers")
        (@arg ID: --id +takes_value +required "local id")
        (@arg CHDIR: -c --chdir +takes_value "chdir directory")

    ).get_matches();

    //
    // - optionally chdir if the --chdir argument is set
    //
    if let Ok(root) = value_t!(args, "CHDIR", String) {
        let path = Path::new(&root);
        assert!(env::set_current_dir(&path).is_ok(), "unable to chdir");
    }

    //
    // - convert the host sequence into a id<->host map with id starting at 0
    //
    let mut n: i8 = -1;
    let tokens: Vec<_> = args.values_of("PEERS").unwrap().collect();
    let peers: HashMap<_, _> = tokens
        .iter()
        .map(|token| {
            n += 1;
            (n as u8, *token)
        })
        .collect();

    //
    // - use a termination event
    // - the raft state machine will signal it when shutting down
    // - grab our node id
    // - start a raft automaton using STDIN/STDOUT for network I/O
    // - the automaton is passed an empty payload
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();
    let id = value_t!(args, "ID", u8).unwrap();
    let (raft, _, _) = pipe::spawn::<_, Empty, _>(
        &guard,
        id,
        peers,
        |_, _| {},
        root.new(o!("sys" => "raft", "id"=>id)),
    );

    //
    // - trap SIGINT/SIGTERM to properly terminate all threads
    // - each thread will signal the termination event
    //
    {
        ctrlc::set_handler(move || {

            //
            // - terminate the ancillary threads for raft::protocol
            // - drain the raft automaton
            //
            rsm::raft::protocol::ANCILLARY.reset();
            raft.drain();

        }).unwrap();
    }

    //
    // - block on the termination event
    // - we are waiting for all our threads to gracefully drain/exit
    //
    drop(guard);
    event.wait();
    info!(&log, "exiting");
}
