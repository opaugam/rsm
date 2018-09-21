pub mod messages;
pub mod protocol;
pub mod sink;
pub mod slots;

use bincode::deserialize;
use fsm::automaton::Automaton;
use fsm::timer::Timer;
use memmap::MmapMut;
use primitives::event::*;
use primitives::once::*;
use primitives::rwlock::*;
use self::protocol::{Command, FSM, Payload, Peer, Raft};
use self::sink::Sink;
use slog::Logger;
use std::cmp;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::BuildHasher;
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::sync::Arc;

macro_rules! clip_to_array {
    ($tag:expr) => {
        {
            let mut buf = [0; 32];
            let n = cmp::min($tag.len(), 32);
            buf[..n].copy_from_slice(&$tag.as_bytes()[..n]);
            buf
        }
    };
}

/// Opaque placeholder for our shared data. The goal is to share a single timer amongst all
/// raft automata.
pub struct Shared {
    timer: Arc<Timer<Command>>,
}

/// Internal data, as a once construct.
pub static ANCILLARY: Once<Shared> = Once::new();

/// Constructor method to spawn a new raft peer with a given id. It returns a triplet made of the
/// automaton wrapper, a read-only lock on the raft payload and a notification sink.
///
/// The automaton is defined by a unique u8 identifier and a network destination
/// (e.g host+port). An optional set of seed peers may be specified. Binary buffers that need
/// to be sent to a given peer are passed to the `write` closure. It is up to the user to then
/// transmit those buffers depending on the implementation (socket, pipe, etc).
///
/// The method is parameterized with the payload to use: the automaton will create and own this
/// payload. It will also update it upon each commit via the `apply` closure.
///
pub fn spawn<'a, S, T, U, V: BuildHasher>(
    guard: &Arc<Guard>,
    id: u8,
    mut peers: HashMap<u8, &'a str, V>,
    write: S,
    apply: T,
    logger: Logger,
) -> (Arc<Raft>, Arc<ROLock<U>>, Arc<Sink>)
where
    S: 'static + Send + Fn(&[u8; 32], &[u8]) -> (),
    T: 'static + Send + Fn(&mut U, &[u8]) -> (),
    U: 'static + Send + Default + Payload,
{
    //
    // - retrieve (and create upon the first invokation) a shared timer automaton used to fire
    //   timeout notifications
    //
    let shared = ANCILLARY.run(|| {
        Shared { timer: Arc::new(Timer::spawn(guard.clone())) }
    });

    //
    // - turn the specified id/host mapping into our peer map
    // - make sure to remove any entry that would be using our peer id
    //
    assert!(peers.len() < 64, "only 64 peers max are supported");
    assert!(
        id < peers.len() as u8,
        "id={} but on {} peers are specified",
        id,
        peers.len()
    );
    assert!(
        peers.contains_key(&id),
        "{} not found in the specified map",
        id
    );
    let host = clip_to_array!(peers[&id]);
    peers.retain(|&n, _| n != id);
    let peers: HashMap<_, _> = peers
        .iter()
        .map(|(n, host)| {
            (
                *n,
                Peer {
                    host: clip_to_array!(host),
                    off: 1,
                    ack: 1,
                },
            )
        })
        .collect();

    //
    // - setup the log file
    // - resize it
    //
    let path = PathBuf::from(format!("log.{}", id));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    let off = FSM::<S, T, U>::RESOLUTION * FSM::<S, T, U>::SLOT_BYTES;
    file.set_len(off as u64).unwrap();

    //
    // - create a notification sink
    // - default the payload and wrap it in a RWLock
    // - obtain a ROLock from it
    // - start the automaton proper
    //
    let sink = Arc::new(Sink::new());
    let payload = Arc::new(RWLock::from(Default::default()));
    let lock = Arc::new(payload.read_only());
    let fsm = Automaton::spawn(
        guard.clone(),
        Box::new(FSM {
            id,
            host,
            seq: 0,
            term: 0,
            tail: 1,
            head: 1,
            age: 0,
            commit: 1,
            peers,
            timer: shared.timer.clone(),
            log: unsafe { MmapMut::map_mut(&file).unwrap() },
            sink: sink.clone(),
            payload,
            snapshot: Vec::new(),
            write,
            apply,
            logger,
        }),
    );

    (Arc::new(Raft { fsm }), lock, sink)
}

/// Same as spawn() except the I/O is set to stream from/to STDIN/STDOUT. Please note Only one
/// automaton may be created like this. The stream layout for byte buffers going to STDOUT is
/// the same as the input, e.g
///
///   |<--------- chunk #1 ------------> | <---- chunk #2 ----->| ...
///   | varint |         n bytes         | varint |   m bytes   | ...
///
/// The internal streaming thread is not be guarded and left to die with the process.
pub fn spawn_piped<'a, S, T, U: BuildHasher>(
    guard: &Arc<Guard>,
    id: u8,
    peers: HashMap<u8, &'a str, U>,
    apply: S,
    logger: Logger,
) -> (Arc<Raft>, Arc<ROLock<T>>, Arc<Sink>)
where
    S: 'static + Send + Fn(&mut T, &[u8]) -> (),
    T: 'static + Send + Default + Payload,
{
    let (raft, lock, sink) = {
        spawn::<_, S, T, U>(
            guard,
            id,
            peers,
            move |host, bytes| {

                //
                // - lock STDOUT (not sure how to optimize this as the mutex guard
                //   is not Send)
                //
                let stdout = stdout();
                let mut pipe = stdout.lock();

                //
                // - encode the out buffer as the host identifier + the byte payload
                // - the buffer is preceded by its byte size as a VARINT
                // - the final layout is varint | host | bytes
                //
                let mut n = 1;
                let mut left = 32 + bytes.len();
                let mut preamble: [u8; 8] = [0; 8];
                loop {
                    let b = (left & 0x7F) as u8;
                    left >>= 7;
                    if left > 0 {
                        preamble[n - 1] = b | 0x80;
                        n += 1;
                    } else {
                        preamble[n - 1] = b;
                        break;
                    }
                }

                //
                // - the wrapped line writer in stdout() will flush upon 0x0a (which is fine)
                //
                assert!(n > 0);
                let _ = pipe.write(&preamble[..n]).unwrap();
                let _ = pipe.write(&host[..]).unwrap();
                let _ = pipe.write(&bytes[..]).unwrap();
                let _ = pipe.flush();
            },
            apply,
            logger,
        )
    };

    //
    // - paste a streaming thread on top of this automaton
    //
    super::fsm::io::pipe::stream_from_sdtin(&raft.fsm, |bytes| {

        //
        // - unpack the incoming byte stream
        // - cast to a RAW message
        // - silently discard if invalid
        //
        if let Ok(raw) = deserialize(&bytes[..]) {
            Some(Command::BYTES(raw))
        } else {
            None
        }
    });

    (raft, lock, sink)
}
