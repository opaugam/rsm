
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rand;
extern crate rsm;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use rsm::primitives::event::*;
use rsm::protocol::messages::RAW;
use rsm::protocol::replica::*;
use serde_json::Value;
use slog::{Drain, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::io::{stderr, stdin, BufRead};
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::thread;

fn main() {

    let decorator = PlainSyncDecorator::new(stderr());
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let root = Logger::root(drain, o!());
    let log = root.new(o!("sys" => "main"));
    info!(&log, "starting (version={})", env!("CARGO_PKG_VERSION"));

    let args = clap_app!(node =>
        (version: env!("CARGO_PKG_VERSION"))
        (@arg ID: --id +takes_value "local id")
        (@arg HOST: --host +takes_value "local host:port")
        (@arg SEEDS: --seeds +takes_value "comma separated host:port list")
        (@arg CHDIR: -c --chdir +takes_value "chdir directory")
    ).get_matches();

    //
    // - optionally chdir if the --chdir argument is set
    //
    match value_t!(args, "CHDIR", String) {
        Ok(root) => {
            let path = Path::new(&root);
            assert!(env::set_current_dir(&path).is_ok(), "unable to chdir");
        }
        _ => {}
    }

    //
    // - use a termination event
    // - the replication state machine will signal it when shutting down
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();

    //
    // - grab our node id and seeds
    // - start the replication state machine
    //
    let id = value_t!(args, "ID", u8).unwrap();
    let host = value_t!(args, "HOST", String).unwrap();
    let seeds = value_t!(args, "SEEDS", String).ok();
    let replica = Replica::spawn(
        guard.clone(),
        id,
        host,
        seeds,
        root.new(o!("sys" => "replica", "id" => id)),
    );

    {
        let fsm = replica.fsm.clone();
        let _ = thread::spawn(move || {

            //
            // - read stdin on a dedicated thread (required to be able to gracefully
            //   synchronize and wait for all automata to drain)
            //
            let stdin = stdin();
            for line in stdin.lock().lines() {
                match line {
                    Ok(line) => {
                        if line.len() > 0 {

                            //
                            // - parse as a RAW struct
                            // - forward to the state machine
                            //
                            let raw: RAW = serde_json::from_str(&line).unwrap();
                            let _ = fsm.post(Command::MESSAGE(raw));
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    //
    // - trap SIGINT/SIGTERM and drain the replica automaton
    // - the automaton will signal the termination event upon going down
    //
    ctrlc::set_handler(move || { replica.fsm.drain(); }).unwrap();

    //
    // - block on the termination event
    //
    drop(guard);
    event.wait();
    info!(&log, "exiting");
}
