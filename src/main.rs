//! Test application running multiple raft automata and allowing them to exchange commands. The
//! leader append a random number of empty records on a periodic basis.
extern crate bincode;
#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rand;
extern crate rsm;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use bincode::{deserialize, serialize};
use rand::{Rng, thread_rng};
use rsm::primitives::event::*;
use rsm::raft::protocol::{Payload, Raft};
use rsm::raft::sink::*;
use slog::{Drain, Level, LevelFilter, Logger};
use slog_term::{FullFormat, PlainSyncDecorator};
use slog_async::Async;
use std::cmp;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::io::stderr;
use std::str;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

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
    if let Ok(root) = value_t!(args, "CHDIR", String) {
        let path = Path::new(&root);
        assert!(env::set_current_dir(&path).is_ok(), "unable to chdir");
    }

    //
    // - use a termination event to synchronize our shutdown sequence
    // - start a few raft automata using the --size CLI argument
    // - cap to 15 automata
    //
    let event = Arc::new(Event::new());
    let guard = event.guard();
    let size = cmp::min(value_t!(args, "SIZE", u8).unwrap_or(3), 15);
    let peers = Arc::new(Mutex::new(HashMap::<[u8; 32], Arc<Raft>>::new()));

    for id in 0..size {
        let guard = guard.clone();
        let shared = peers.clone();
        let log = root.new(o!("sys" => "raft", "id" => id));
        let _ = thread::spawn(move || {

            //
            // - prep a mock configuration with n peers
            // - this is similar to the zookeeper configuration
            // - instead of a host:port our network destination is a simple label
            //
            let tags: Vec<_> = (0..size).map(|n| format!("automaton #{}", n)).collect();
            let seeds: HashMap<_, _> = tags.iter().enumerate().map(|(n, tag)|  (n as u8, tag.as_str())).collect();
           
            //
            // - define our payload, e.g the stateful information to which each commit
            //   will be applied to by the automaton
            //
            #[derive(Debug, Default, Serialize, Deserialize)]
            struct COUNTER {
                count: u64,
            }

            impl Payload for COUNTER {
                fn flush(&self) -> Vec<u8> {
                    serialize(&self).unwrap()
                }

                fn reset(&mut self, bytes: &[u8]) -> () {
                    let msg: COUNTER = deserialize(bytes).unwrap();
                    *self = msg;
                }
            }

            //
            // - start a new automata
            // - retrieve a read lock on the payload plus a notification sink
            //
            let (raft, _, sink) = {
                let guard = guard.clone();
                let shared = shared.clone();
                Raft::spawn::<_, _, COUNTER, _>(
                    &guard,
                    id,
                    seeds,
                    move |host, bytes| {

                        //
                        // - find the destination automaton in our map
                        // - post the opaque byte buffer
                        //
                        let peers = shared.lock().unwrap();
                        let raft = &peers[host];
                        raft.feed(bytes);
                    },
                    |payload, _| {

                        //
                        // - increment the payload upon each commit
                        // - the closure is executed with a write lock being held
                        //
                        payload.count += 1;
                    },
                    log,
                )
            };

            //
            // - lock the mutex
            // - add this automaton to the shared peer map
            //
            {
                let mut peers = shared.lock().unwrap();
                peers.insert(Raft::get_host(tags[id as usize].as_str()), raft.clone());
            }

            //
            // - loop as long as we get notifications from the automaton
            // - we will break automatically as soon as it shuts down
            //
            let emit = Arc::new(AtomicBool::new(false));
            loop {
                match sink.next() {
                    None => break,
                    Some(Notification::LEADING) => {

                        //
                        // - we are leading
                        // - spawn a thread to periodically write an empty record
                        // - we use an atomic boolean as a on/off trigger
                        //
                        let raft = raft.clone();
                        let emit = emit.clone();
                        emit.store(true, Ordering::Release);
                        let _ = thread::spawn(move || loop {
                            if emit.load(Ordering::Relaxed) {

                                //
                                // - we are leading, write up to 10 empty records
                                // - pause the thread and loop back
                                //
                                for _ in 0..thread_rng().gen_range(0, 10) {
                                    raft.store(Vec::new());
                                }
                                thread::sleep(Duration::from_millis(1000));
                            } else {
                                break;
                            }
                        });

                    }
                    Some(Notification::FOLLOWING) |
                    Some(Notification::IDLE) => {

                        //
                        // - we are not leading anymore
                        // - switch to trigger off (the thread will exit if running)
                        //
                        emit.store(false, Ordering::Release);
                    }
                    _ => {}
                }
            }

            //
            // - the automaton signaled it went down
            // - the event guard will now drop
            // - once all the guards drop the final termination event will signal
            //
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
            }
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
