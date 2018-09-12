
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rand;
extern crate rsm;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use rsm::primitives::event::*;
use rsm::raft::messages::RAW;
use rsm::raft::protocol::{Notification, Protocol};
use slog::{Drain, Level, LevelFilter, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::io::{stderr, stdin, BufRead};
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::thread;

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
        (@arg ID: --id +takes_value "local id")
        (@arg HOST: --host +takes_value "local host:port")
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
    // - the raft state machine will signal it when shutting down
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();

    //
    // - grab our node id and local host
    // - start the raft state-machine and use a simple println!() as our write closure
    // - the wrapping python process will pipe it from STDOUT and use it to send a RPC call
    //
    let id = value_t!(args, "ID", u8).unwrap();
    let host = value_t!(args, "HOST", String).unwrap();
    let (raft, sink) = Protocol::spawn(
        guard.clone(),
        id,
        host,
        |raw| println!("{}", raw),
        root.new(o!("sys" => "raft", "id" => id)),
    );

    {
        //
        // - the incoming RPC payload is coming from STDIN
        // - read STDIN on a dedicated thread (required to be able to gracefully
        //   synchronize and wait for all automata to drain)
        //
        let raft = raft.clone();
        let _ = thread::spawn(move || {
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
                            raft.read(raw);
                        }
                    }
                    _ => {}
                }
            }
        });
    }
    {

    }

    //
    // - trap SIGINT/SIGTERM and drain the state machine
    // - the state machine will signal the termination event upon going down
    // - start consuming from the sink
    // - as soon as next() fails on a None we can move on and wait for termination
    //
    {
        let raft = raft.clone();
        ctrlc::set_handler(move || { raft.drain(); }).unwrap();
    }
    loop {
        match sink.next() {
            None => break,            
            Some(Notification::COMMIT(blob)) => {
                info!(&log, "data -> <{}>", blob);
            }
            Some(notif) => {
                info!(&log, "notif: {:?}", notif);
                match notif {
                    Notification::FOLLOWING => {

                        //
                        // -
                        //
                        raft.store(format!("hello i am peer #{} !", id));
                    }
                    _ => {}
                }
            }
        }
    }

    //
    // - block on the termination event
    // - we are waiting for all our threads to gracefully drain/exit
    //
    info!(&log, "terminating");
    drop(guard);
    event.wait();
    info!(&log, "exiting");
}
