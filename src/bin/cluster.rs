//! Sample application running multiple raft automata and allowing them exchange commands.
extern crate bincode;
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rsm;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode::deserialize;
use rsm::primitives::event::*;
use rsm::raft::slots::LABEL;
use rsm::raft::protocol::{Notification, Protocol};
use slog::{Drain, Level, LevelFilter, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::io::stderr;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {

    //
    // - init slog to dump on stderr
    //
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
        (@arg SIZE: -s --size +takes_value "number of automata to run")
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
    // - use a termination event to synchronize our shutdown sequence
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();

    //
    // - start a few raft automata
    // - loop and consume notifications for each
    //
    let size = value_t!(args, "SIZE", u8).unwrap_or(3);
    let peers = Arc::new(Mutex::new(HashMap::<String, Protocol>::new()));
    for n in 0..size {

        let guard = guard.clone();
        let shared = peers.clone();
        let log = (
            root.new(o!("sys" => "raft", "id" => n)),
            root.new(o!("sys" => "events")),
        );
        let _ = thread::spawn(move || {

            //
            // -
            //
            let seeds : HashMap<_, _> = (0..size).map(|n| (n, format!("#{}", n))).collect();

            //
            // - start a new automata and grab its notification sink
            //
            let id = format!("#{}", n);
            let (raft, sink) = {
                let guard = guard.clone();
                let shared = shared.clone();
                Protocol::spawn(
                    guard,
                    n,
                    seeds[&n].clone(),
                    Some(seeds),
                    move |host, bytes| {
                        let peers = shared.lock().unwrap();
                        let raft = &peers[host];
                        raft.feed(bytes);
                    },
                    log.0,
                )
            };

            //
            // - lock the mutex
            // - add this automaton to the shared peer map
            //
            {
                let mut peers = shared.lock().unwrap();
                peers.insert(id, raft);
            }

            //
            // - loop as long as we get notifications from the automaton
            // - we will break automatically as soon as it shuts down
            //
            loop {
                match sink.next() {
                    None => break,
                    Some(Notification::COMMIT(off, code, bytes)) => {
                        match code {
                            LABEL::CODE => {
                                let label: LABEL = deserialize(&bytes).unwrap();
                                info!(&log.1, "commit #{} : {:?}", off, label.text);
                            }
                            _ => {}
                        }
                    }
                    Some(notif) => {
                        info!(&log.1, "> {:?}", notif);
                    }
                }
            }

            //
            // - the automaton signaled it went down
            // - the event guard will now drop
            // - once all the guards drop the final termination event will signal
            //
            info!(&log.1, "thread {} exiting", n);
        });
    }

    //
    // - trap SIGINT/SIGTERM and drain the state machine
    // - the state machine will signal the termination event upon going down
    // - start consuming from the sink
    // - as soon as next() fails on a None we can move on and wait for termination
    //
    {
        let shared = peers.clone();
        ctrlc::set_handler(move || {
            let peers = shared.lock().unwrap();
            for peer in &*peers {

                //
                // - drain() is going to gracefully shutdown the automaton thread
                // - upon termination it will signal the notification sink and drop its guard
                //
                peer.1.drain();
        }}).unwrap();
    }

    //
    // - block on the termination event
    // - we are waiting for all our threads to gracefully drain/exit
    //
    drop(guard);
    event.wait();
    info!(&log, "exiting");
}
