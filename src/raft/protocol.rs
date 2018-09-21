//! Raft protocol state machine backed up by a circular buffer on disk.
//!
//! The implementation incorporates a few additions (leader stickiness and pre-vote). The
//! election cycle is randomized to avoid herding as well. The underlying automaton is 100%
//! wait-free and built on atomics. Threads that need to block are synchronized on a standard
//! condition variable.
//!
//! The basic automaton state diagram is:
//!
//! ```ignore
//!                <---------------+---------------+--------------+
//!                                |               |              |
//!   START -> FOLLOWER  --->  PRE-VOTE  --->  CANDIDATE  --->  LEADER
//!                                                |
//!                                <---------------+
//! ```
//!
//!  # Payload
//!
//!  The automaton maintains some arbitrary user payload and updates it on every commit. Whenever
//!  the commit offset reaches a new checkpointing window (e.g a fixed number of commits) the
//!  current payload is serialized into a byte buffer. This buffer is transmitted to any peer
//!  that needs to be rebased (e.g lags). Please note a read-only lock is passed back to the user
//!  to access the payload at any time.
//!
//!  # Capacity
//!
//!   This implementation offers the following properties:
//!     * log backed up by a circular buffer on disk
//!     * most of the raft spec is supported
//!     * up to 64 peers in one cluster
//!     * all offsets are on 64 bits
//!     * maximum peer lag up to whatever the underlying file is
//!     * periodic checkpointing when enough commits went by
//!     * I/O between peers uses byte buffers encoded using bincode
//!     * pre-vote phase prior to triggering an election
//!
//!  # Log implementation
//!
//!   The log is append only and is backed up by a circular buffer on disk. This means the lag
//!   between the head and tail offsets is bounded (e.g peers cannot lag forever). Writes are
//!   skipped upon reaching maximum capacity and until one or more commits happen.
//!
//!   Each slot has a fixed maximum byte size and contains its term plus a free form user payload
//!   as a raw byte array. The slot is encoded using bincode.
//!
//!   The automaton maintains a variable log window tracked by a head and tail offsets. The
//!   commit offset is always located in that window. This window is checkpointed whenever the
//!   distance between the commit and tail offsets reaches a predefined boundary. The tail offset
//!   will therefore always be a multiple of this threshold.
//!
//! ```ignore
//!
//!          boundary #n                           boundary #n+1
//!
//!              |<-       checkpointing window #n       ->|
//!              +-------------------------------------------------+---------------> offsets
//!             tail                   |                          head
//!                                  commit
//! ```
//!
//!  # Impementation notes
//!
//!   * Instead of initializing the offsets to 0 we force a dummy entry at #1 to simply the code.
//!   * The original raft protocol has been slightly changed around compaction and how we handle
//!     conflicts. This implementation is simpler in the sense we always force a peer rebase at
//!     the last checkpointing boundary.
//!
//!  # Ideas
//!
//!    * force a 'commit-all' for cluster membership entries, e.g delay the commit offset index
//!      increment as long as *all* the peers are not replicated with at least that entry (and
//!      then atomically update the conf. on all peers at once).
//!
//!  # References
//!
//!   * [Original paper.](https://raft.github.io/raft.pdf)
//!   * [Optimizations.](http://openlife.cc/system/files/3-modifications-for-Raft-consensus.pdf)
use bincode::{serialize, deserialize};
use fsm::automaton::{Automaton, Opcode, Recv};
use fsm::timer::Timer;
use memmap::MmapMut;
use primitives::rwlock::*;
use raft::messages::*;
use raft::sink::*;
use raft::slots::*;
use rand::{Rng, thread_rng};
use self::Command::*;
use self::State::*;
use slog::Logger;
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::str;
use std::sync::Arc;
use std::time::Duration;

macro_rules! display {
    ($self:ident, $fmt:expr $(, $arg:expr)*) => {
        debug!(&$self.logger, $fmt, $($arg),* ;
            "term" => $self.term,
            "log" => format!("[#{} #{}] (#{})", $self.tail, $self.head, $self.commit),);
    };
}

macro_rules! disk {
    ($off:expr) => {
        (($off - 1) as usize % FSM::<S, T, U>::RESOLUTION) * FSM::<S, T, U>::SLOT_BYTES
    };
}

macro_rules! read_slot {
    ($self:ident, $off:expr) => {
        {
            let off = disk!($off);
            let slot: SLOT =
                deserialize(&$self.log[off..off + FSM::<S, T, U>::SLOT_BYTES]).unwrap();
            slot
        }
    };
}

macro_rules! write_slot {
    ($self:ident, $buf:expr, $off:expr) => {
        {
            let off = disk!($off);
            $self.log[off..off + $buf.len()].copy_from_slice(&$buf);
        }
    };
}

macro_rules! read_range {
    ($self:ident, $buf:ident, $off:expr, $n:expr) => {
        {
            let start = disk!($off);
            let end = disk!($off + $n);
            if end < start {
                $buf.extend_from_slice(&$self.log[start..]);
                $buf.extend_from_slice(&$self.log[..end]);
            } else {
                $buf.extend_from_slice(&$self.log[start..end]);
            }
        }
    };
}

macro_rules! write_range {
    ($self:ident,$buf:ident, $off:expr, $n:expr) => {
        {
            let start = disk!($off);
            let end = disk!($off + $n);
            if end < start {
                let cut = FSM::<S, T, U>::RESOLUTION * FSM::<S, T, U>::SLOT_BYTES - start;
                $self.log[start..].copy_from_slice(&$buf[..cut]);
                $self.log[..end].copy_from_slice(&$buf[cut..]);
            } else {
                $self.log[start..end].copy_from_slice(&$buf);
            }
        }
    };
}

/// Volatile information maintained while on a given state, typically who we voted
/// for, who the leader is, etc.
mod context {

    #[derive(Copy, Clone, Default, PartialEq)]
    pub struct FLWR {
        pub live: bool,
        pub leader: Option<u8>,
    }

    #[cfg_attr(rustfmt, rustfmt_skip)]
    impl super::fmt::Debug for FLWR {
        fn fmt(&self, f: &mut super::fmt::Formatter<'_>) -> super::fmt::Result {
            match self.leader {
                Some(id) => write!(f, "FOLLOWER ({:03}) |", id),
                _ =>        write!(f, "FOLLOWER (N/A) |"),
            }
        }
    }

    #[derive(Copy, Clone, Default, PartialEq)]
    pub struct CNDT {
        pub votes: u64,
        pub pick: Option<u8>,
        pub advertised: bool,
    }

    impl super::fmt::Debug for CNDT {
        fn fmt(&self, f: &mut super::fmt::Formatter<'_>) -> super::fmt::Result {
            write!(f, "CANDIDATE      |")
        }
    }

    #[derive(Copy, Clone, Default, PartialEq)]
    pub struct LEAD {}

    impl super::fmt::Debug for LEAD {
        fn fmt(&self, f: &mut super::fmt::Formatter<'_>) -> super::fmt::Result {
            write!(f, "LEADER         |")
        }
    }
}

pub(super) enum Command {
    BYTES(RAW),
    STORE(Vec<u8>),
    TIMEOUT(u64),
}

#[derive(Copy, Clone)]
pub(super) enum State {
    PREV(context::CNDT),
    CNDT(context::CNDT),
    FLWR(context::FLWR),
    LEAD(context::LEAD),
}

impl PartialEq for State {
    fn eq(&self, other: &State) -> bool {

        //
        // - since we embed a context we need to override PartialEq
        //   to do simple state comparisons
        //
        match (*self, *other) {
            (PREV(_), PREV(_)) |
            (CNDT(_), CNDT(_)) |
            (FLWR(_), FLWR(_)) |
            (LEAD(_), LEAD(_)) => true,
            _ => false,
        }
    }
}

impl Default for State {
    fn default() -> State {

        //
        // - the state machine starts as a FOLLOWER
        // - we either will receive a heartbeat or time out and
        //   initiate a new election cycle
        //
        State::FLWR(context::FLWR::default())
    }
}

/// Wrapper around the automaton. Public operations are exposed via a few methods. The actual
/// automaton implementating the protocol is not exposed.
pub struct Raft {
    pub(super) fsm: Arc<Automaton<Command>>,
}

pub(super) struct Peer {
    /// Peer network identifier padded to 32 bytes, typically xxx.xxx.xxx.xxx:yyyy
    pub(super) host: [u8; 32],
    /// Write offset on that peer, e.g offset after which new entries will be appened. Please note
    /// this may not be its current head. It is initially set to the leader's head offset.
    pub(super) off: u64,
    /// Last acknowledged offset (e.g commit offset).
    pub(super) ack: u64,
}

/// Trait defining the raft automaton payload.
pub trait Payload {
    ///
    fn flush(&self) -> Vec<u8> {
        Vec::new()
    }

    ///
    fn reset(&mut self, _bytes: &[u8]) -> () {}
}

///  todo items:
///    o) membership add()/remove()
///    o) the latest vote must be persisted (e.g persist an empty file under /tmp for a
///       given term and erase it as soon as we're not candidate anymore ?)
pub(super) struct FSM<S, T, U>
where
    S: 'static + Send + Fn(&[u8; 32], &[u8]) -> (),
    T: 'static + Send + Fn(&mut U, &[u8]) -> (),
    U: 'static + Send + Default + Payload,
{
    /// Local peer index in [0, 64].
    pub(super) id: u8,
    /// Local network identifier.
    pub(super) host: [u8; 32],
    /// Sequence counter, used to disambiguiate timeouts.
    pub(super) seq: u64,
    /// Current peer term, persisted.
    pub(super) term: u64,
    /// Last log offset we maintain, starts at #1.
    pub(super) head: u64,
    /// First log offset we maintain, starts at #1.
    pub(super) tail: u64,
    /// Term at the log tail (e.g how long ago was that entry appended).
    pub(super) age: u64,
    /// Current commit offset, as reported by a quorum, starts at #1.
    pub(super) commit: u64,
    /// Map of peer id <-> host + offsets
    pub(super) peers: HashMap<u8, Peer>,
    /// Internal timer automaton used to enforce timeouts
    pub(super) timer: Arc<Timer<Command>>,
    /// Memory mapped log file on disk, used as a circular buffer
    pub(super) log: MmapMut,
    /// Notification sink
    pub(super) sink: Arc<Sink>,
    /// Payload updated upon commit, used for checkpointing
    pub(super) payload: Arc<RWLock<U>>,
    /// Latest snapshot, e.g serialized payload at the last checkpointing boundary
    pub(super) snapshot: Vec<u8>,
    /// Network out closure
    pub(super) write: S,
    /// User payload update closure
    pub(super) apply: T,
    /// Slog logger
    pub(super) logger: Logger,
}

impl<S, T, U> FSM<S, T, U>
where
    S: 'static + Send + Fn(&[u8; 32], &[u8]) -> (),
    T: 'static + Send + Fn(&mut U, &[u8]) -> (),
    U: 'static + Send + Default + Payload,
{
    //
    // - various timeouts in milliseconds
    //
    const LIVENESS_TIMEOUT: u64 = 3000;
    const ELECTION_TIMEOUT: u64 = 750;

    //
    // - log topology (slot width, etc.)
    //
    pub(super) const SLOT_BYTES: usize = 1024;
    pub(super) const RESOLUTION: usize = 128;
    const CHECKPOINT: usize = 15;

    fn count_votes(&self, votes: &mut u64, id: u8) -> (u8, bool) {

        //
        // - the votes byte is used as a bitmask (1 bit per peer)
        // - set the bit corresponding to the specified id
        // - count the set bits
        // - compare with the quorum
        //
        let mut total = 0;
        *votes |= 1 << id;
        let mut n = *votes;
        while n > 0 {
            n &= n - 1;
            total += 1;
        }
        (total + 1, total > (1 + self.peers.len() as u8) >> 1)
    }
}

impl<S, T, U> Recv<Command, State> for FSM<S, T, U>
where
    S: 'static + Send + Fn(&[u8; 32], &[u8]) -> (),
    T: 'static + Send + Fn(&mut U, &[u8]) -> (),
    U: 'static + Send + Default + Payload,
{
    fn recv(
        &mut self,
        this: &Arc<Automaton<Command>>,
        mut state: State,
        opcode: Opcode<Command, State>,
    ) -> State {
        match opcode {
            Opcode::START => {
                display!(
                    self,
                    "               | | starting ({} peers)",
                    self.peers.len()
                );

                //
                // - by definition offset #1 is some empty marker
                // - we use this to avoid having to perform a bunch of == 0 tests
                //
                debug_assert!(self.head == 1);
                debug_assert!(self.tail == 1);
                debug_assert!(self.commit == 1);
                let slot = NULL {};
                write_slot!(self, slot.to_bytes(0), self.head);

                //
                // - we start as a FOLLOWER
                // - set the first liveness timeout
                //
                self.timer.schedule(
                    this.clone(),
                    TIMEOUT(self.seq),
                    Duration::from_millis(FSM::<S, T, U>::LIVENESS_TIMEOUT),
                );
            }
            Opcode::TRANSITION(prv) => {
                debug_assert!(state != prv);
                self.seq += 1;
                match (prv, state) {
                    (CNDT(_), PREV(ref ctx)) |
                    (FLWR(_), PREV(ref ctx)) => {

                        //
                        // - star the pre-voting cycle right away
                        //
                        self.seq += 1;
                        display!(self, "{:?} | pre-voting", ctx);
                        let _ = this.post(TIMEOUT(self.seq));
                    }
                    (PREV(_), CNDT(ref ctx)) => {

                        //
                        // - we switched to CANDIDATE
                        // - trigger an election after a random lapse of time
                        // - the goal is to avoid herding in case multiple peers transition
                        //   to CANDIDATE at around the same time
                        //
                        let ms = thread_rng().gen_range(25, 150);
                        display!(self, "{:?}*| triggering election in {} ms", ctx, ms);
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(ms),
                        );
                    }
                    (CNDT(_), LEAD(ctx)) => {

                        //
                        // - we were just elected LEADER
                        // - reset the next/replication counters for each peer
                        // - the peer offsets are set to our head offset
                        //
                        display!(self, "{:?}*| now leading", ctx);
                        for peer in &mut self.peers {
                            debug_assert!(*peer.0 != self.id);
                            peer.1.off = self.head;
                            peer.1.ack = 1;
                        }

                        //
                        // - replicate immediately
                        // - notify the sink with LEADING
                        // - increment the sink semaphore
                        //
                        self.seq += 1;
                        let _ = this.post(TIMEOUT(self.seq));
                        self.sink.push(Notification::LEADING);

                    }
                    (PREV(_), FLWR(ctx)) |
                    (CNDT(_), FLWR(ctx)) |
                    (LEAD(_), FLWR(ctx)) => {

                        //
                        // - we got a REPLICATE with a higher term
                        // - set our next timeout
                        //
                        display!(self, "{:?} | waiting for heartbeats", ctx);
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<S, T, U>::LIVENESS_TIMEOUT),
                        );
                    }
                    _ => {
                        debug_assert!(false, "invalid state transition");
                    }
                }
            }
            Opcode::CMD(TIMEOUT(seq)) if seq == self.seq => {
                match state {
                    PREV(ref mut ctx) => {

                        //
                        // - we did not get a quorum, meaning it's useless to switch
                        //   to CANDIDATE at this point
                        // - send a PROBE to all our peers, the goal being to
                        //   estimate how many would agree to vote for us as long as
                        //   they at least match our head
                        // - the term we pass is our term + 1
                        //
                        ctx.pick = None;
                        for peer in &self.peers {
                            debug_assert!(*peer.0 != self.id);
                            let msg = PROBE {
                                id: self.id,
                                term: self.term + 1,
                                head: self.head,
                                age: self.age,
                            };
                            let bytes = msg.to_raw(&self.host, &peer.1.host);
                            (self.write)(&peer.1.host, &bytes);
                            display!(self, "{:?} | probing peer #{}", ctx, peer.0);
                        }

                        //
                        // - set the next election timeout
                        // - we will cycle on PREVOTE as long as we don't get promoted
                        //   to CANDIDATE and don't get quorum
                        //
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<S, T, U>::ELECTION_TIMEOUT),
                        );
                    }
                    CNDT(ref mut ctx) => {
                        if ctx.advertised {

                            //
                            // - the election timed out
                            // - return to PREVOTE to start a new cycle
                            // - note we reset the context
                            //
                            display!(self, "{:?} | election failed, returning to pre-voting", ctx);
                            return PREV(context::CNDT::default());
                        }

                        //
                        // - we went from PREVOTE to CANDIDATE
                        // - increment our term
                        // - send a ADVERTISE to all peers
                        // - wait for VOTE RPCs to come back within the election timeout
                        //
                        self.term += 1;
                        for peer in &self.peers {
                            debug_assert!(*peer.0 != self.id);
                            let msg = ADVERTISE {
                                id: self.id,
                                term: self.term,
                                head: self.head,
                                age: self.age,
                            };
                            let bytes = msg.to_raw(&self.host, &peer.1.host);
                            (self.write)(&peer.1.host, &bytes);
                        }

                        //
                        // - set the advertised trigger
                        // - set the election timeout
                        //
                        ctx.advertised = true;
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<S, T, U>::ELECTION_TIMEOUT),
                        );
                    }
                    FLWR(ref mut ctx) => {
                        if ctx.live {

                            //
                            // - we got 1+ heartbeats from the leader, all is good
                            // - reset the liveness trigger
                            // - schedule a new timeout
                            //
                            ctx.live = false;
                            self.timer.schedule(
                                this.clone(),
                                TIMEOUT(self.seq),
                                Duration::from_millis(FSM::<S, T, U>::LIVENESS_TIMEOUT),
                            );

                        } else {

                            //
                            // - liveness timeout: the leader may be down or we are
                            //   not reachable anymore (network partition)
                            // - notify the sink with IDLE
                            // - increment the sink semaphore
                            // - switch to PREVOTE to initiate a new election cycle
                            //
                            self.sink.push(Notification::IDLE);
                            return PREV(context::CNDT::default());
                        }
                    }
                    LEAD(ref ctx) => {

                        //
                        // - assert our authority by sending a PING to all our peers
                        // - any peer receiving those will turn into a FOLLOWER if not already
                        //   the case
                        //
                        // @todo better manager idle times vs. dirty state
                        //
                        for peer in &mut self.peers {
                            debug_assert!(*peer.0 != self.id);
                            let msg = PING {
                                id: self.id,
                                term: self.term,
                                commit: self.commit,
                            };
                            let bytes = msg.to_raw(&self.host, &peer.1.host);
                            (self.write)(&peer.1.host, &bytes);
                            debug_assert!(peer.1.off <= self.head);
                            if self.head > peer.1.off {

                                //
                                // - we have entries to replicate
                                // - prep a append buffer
                                // - check if we need to replicate
                                //
                                let mut append = Vec::new();
                                debug_assert!(self.head >= peer.1.ack);
                                debug_assert!(self.head >= peer.1.off);
                                debug_assert!(
                                    peer.1.off >= peer.1.ack,
                                    format!("id {} off {} ack {}", peer.0, peer.1.off, peer.1.ack)
                                );
                                let snapshot = if peer.1.off < self.tail {

                                    //
                                    // - if the peer is behind our log window bump it
                                    // - the rebase flag will force it to align with us
                                    // - copy the whole log window to the append buffer
                                    // - please note 1+ commit notifications will thus be lost
                                    //   on that peer (at least the peer will notify it was
                                    //   rebased)
                                    //
                                    display!(
                                        self,
                                        "{:?}*| bumping peer #{} to [#{} #{}] (lag ?)",
                                        ctx,
                                        peer.0,
                                        self.tail,
                                        self.head
                                    );
                                    peer.1.off = self.tail;
                                    let n = self.head - self.tail + 1;
                                    read_range!(self, append, self.tail, n);
                                    self.snapshot.clone()

                                } else {

                                    //
                                    // - we have 1+ log entries to replicate
                                    // - copy entries from [off + 1, head] to the append buffer
                                    //
                                    display!(
                                        self,
                                        "{:?} | replicating [#{} .. #{}] to peer #{}",
                                        ctx,
                                        peer.1.off + 1,
                                        self.head,
                                        peer.0
                                    );
                                    let n = self.head - peer.1.off;
                                    read_range!(self, append, peer.1.off + 1, n);
                                    Vec::new()
                                };

                                //
                                // - specify the index+term for the write offset (e.g the offset
                                //   immediately preceding the first replicated entry)
                                // - even if we have no entries to replicate this will force the
                                //   FOLLOWER to check its log and flag any conflict
                                // - blank peers will also be able to synch-up this way
                                // - emit a REPLICATE
                                //
                                debug_assert!(!append.is_empty());
                                let slot = read_slot!(self, peer.1.off);
                                let msg = REPLICATE {
                                    id: self.id,
                                    term: self.term,
                                    commit: self.commit,
                                    off: peer.1.off,
                                    age: slot.term,
                                    append,
                                    snapshot,
                                };

                                let bytes = msg.to_raw(&self.host, &peer.1.host);
                                (self.write)(&peer.1.host, &bytes);
                                peer.1.off = self.head;
                            }
                        }

                        //
                        // - schedule a new heartbeat timeout
                        // - note we use a fraction of the liveness timeout duration
                        // - this is to be safe
                        //
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<S, T, U>::LIVENESS_TIMEOUT / 4),
                        );
                    }
                }
            }
            Opcode::CMD(STORE(bytes)) => {
                if let LEAD(ref ctx) = state {

                    //
                    // - make sure we have enough room in the log
                    //
                    if self.head - self.tail == FSM::<S, T, U>::RESOLUTION as u64 - 1 {
                        display!(self, "{:?}*| discarding record (log full)", ctx);
                    } else {

                        //
                        // - increment the head offset
                        // - update the term tracker for the head
                        // - add the entry to the log
                        //
                        self.head += 1;
                        self.age = self.term;
                        display!(self, "{:?} | appending record ({}B)", ctx, bytes.len());
                        let slot = SLOT {
                            code: 255,
                            term: self.term,
                            bytes,
                        };
                        write_slot!(self, serialize(&slot).unwrap(), self.head);
                    }
                }
            }
            Opcode::CMD(BYTES(raw)) => {
                trace!(
                    &self.logger,
                    "<- {}B from {} (code #{})",
                    raw.msg.len(),
                    str::from_utf8(&raw.src).unwrap(),
                    raw.code
                );
                #[cfg(feature = "chaos")]
                {
                    match raw.code {
                        PING::CODE | REPLICATE::CODE => {
                            if thread_rng().gen_range(0, 100) > 50 {
                                return state;
                            }
                        }
                        _ => {}
                    }
                }
                match raw.code {
                    UPGRADE::CODE => {
                        let msg: UPGRADE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term > self.term {

                            //
                            // - we are stale
                            // - upgrade our term
                            // - notify the sink with IDLE
                            // - increment the sink semaphore
                            // - always transition to FOLLOWER without a leader (yet)
                            //
                            // @note should we just nuke our log/state and wait for the leader
                            // to synch us back?
                            //
                            self.term = msg.term;
                            self.sink.push(Notification::IDLE);
                            return FLWR(context::FLWR {
                                live: false,
                                leader: None,
                            });
                        }
                    }
                    PING::CODE => {
                        let mut msg: PING = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);

                        } else {
                            match state {
                                PREV(_) | CNDT(_) => {

                                    //
                                    // - a LEADER is active
                                    // - upgrade our term if we are stale
                                    // - notify the sink with FOLLOWING
                                    // - increment the sink semaphore
                                    // - transition to FOLLOWER
                                    //
                                    self.term = msg.term;
                                    self.sink.push(Notification::FOLLOWING);
                                    return FLWR(context::FLWR {
                                        live: false,
                                        leader: Some(msg.id),
                                    });
                                }
                                FLWR(ref mut ctx) => {
                                    display!(
                                        self,
                                        "{:?} | heartbeat received from peer #{}",
                                        ctx,
                                        msg.id
                                    );
                                    if ctx.leader.is_none() {

                                        //
                                        // - notify the sink with FOLLOWING in case we went
                                        //   from no leader to one (e.g we started for instance)
                                        // - increment the sink semaphore
                                        //
                                        self.sink.push(Notification::FOLLOWING);
                                    }

                                    //
                                    // - set the live trigger
                                    // - upgrade our term if we are stale
                                    // - make sure we update our leader id if needed
                                    // - check if we need to update the commit offset
                                    //
                                    ctx.live = true;
                                    self.term = msg.term;
                                    ctx.leader = Some(msg.id);
                                    let next = cmp::min(msg.commit, self.head);
                                    if next > self.commit {
                                        debug_assert!(next >= self.tail);
                                        let mut guard = self.payload.write();
                                        for n in self.commit..next {
                                            let slot = read_slot!(self, n);
                                            (self.apply)(&mut guard, &slot.bytes);
                                        }
                                        drop(guard);
                                        self.commit = next;
                                        display!(
                                            self,
                                            "{:?} | offset #{} committed",
                                            ctx,
                                            self.commit
                                        );

                                        //
                                        // - if the commit index reached a checkpoint boundary
                                        //   reset the tail to that offset
                                        // - lock the payload and take a snapshot of it
                                        // - flush the logfile
                                        // - notify the sink with a CHECKPOINT
                                        // - signal the sink event
                                        //
                                        let boundary = self.commit -
                                            (self.commit % FSM::<S, T, U>::CHECKPOINT as u64);
                                        if boundary > self.tail {
                                            let guard = self.payload.read();
                                            let mut bytes = (*guard).flush();
                                            drop(guard);
                                            self.snapshot.clear();
                                            self.snapshot.append(&mut bytes);
                                            display!(
                                                self,
                                                "{:?} | snapshot [#{} #{}], {}B",
                                                ctx,
                                                self.tail,
                                                boundary,
                                                self.snapshot.len()
                                            );
                                            self.log.flush().unwrap();
                                            self.sink.push(Notification::CHECKPOINT(boundary));
                                            self.tail = boundary;
                                        }
                                    }
                                }
                                LEAD(ref ctx) => {

                                    //
                                    // - another peer was elected LEADER
                                    // - notify the sink with FOLLOWING
                                    // - increment the sink semaphore
                                    // - upgrade our term and transition to FOLLOWER
                                    // - this should be a rare edge case after a partition
                                    //
                                    self.term = msg.term;
                                    display!(self, "{:?}*| stepping down", ctx);
                                    self.sink.push(Notification::FOLLOWING);

                                    return FLWR(context::FLWR {
                                        live: false,
                                        leader: Some(msg.id),
                                    });
                                }
                            }
                        }
                    }
                    REPLICATE::CODE => {
                        let mut msg: REPLICATE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);

                        } else {
                            let rebase = !msg.snapshot.is_empty();
                            let n = (msg.append.len() / FSM::<S, T, U>::SLOT_BYTES) as u64;
                            debug_assert!(n > 0);
                            match state {
                                FLWR(ref mut ctx) if rebase => {

                                    //
                                    // - reset our log using the append buffer
                                    // - override our tail and head offsets
                                    //
                                    self.tail = msg.off;
                                    self.head = self.tail + n as u64 - 1;
                                    let buf = msg.append;
                                    write_range!(self, buf, msg.off, n);
                                    let slot = read_slot!(self, self.head);
                                    self.age = slot.term;
                                    display!(
                                        self,
                                        "{:?} | rebased into [#{} #{}]",
                                        ctx,
                                        self.tail,
                                        self.head
                                    );

                                    //
                                    // - lock the payload
                                    // - deserialize and use the snapshot we received
                                    //
                                    let mut guard = self.payload.write();
                                    (*guard).reset(&msg.snapshot);
                                    drop(guard);

                                    //
                                    // - emit a ACK to acknowledge our new head offset
                                    //
                                    let msg = ACK {
                                        id: self.id,
                                        term: self.term,
                                        ack: self.head,
                                    };

                                    let bytes = msg.to_raw(&self.host, &raw.src);
                                    (self.write)(&raw.src, &bytes);
                                }
                                FLWR(ref mut ctx) => {

                                    //
                                    // - consider the check mark passed down by the LEADER
                                    // - we want to make sure we a) have the specified offset in
                                    //   our log and b) that its term is the one we expect
                                    // - failure to do so mean we either are missing data (e.g
                                    //   we are catching up after, say, a reboot), or we have some
                                    //   amount of corruption
                                    // - note: a check mark with a zero value only means we have
                                    //   no log at all (e.g the node just joined the cluster)
                                    // - this is conveyed in the original paper as
                                    //
                                    //   "Reply false if log doesn’t contain an entry at
                                    //    prevLogIndex whose term matches prevLogTerm (§5.3)"
                                    //
                                    let mut conflict = true;
                                    debug_assert!(msg.off >= self.tail);
                                    if msg.off <= self.head {

                                        let slot = read_slot!(self, msg.off);
                                        if slot.term == msg.age {

                                            //
                                            // - the specified log offset matches
                                            // - now this offset may not be our head, therefore
                                            //   truncate() to drop all subsequent entries
                                            // - then transfer the entries to append at the head
                                            // - this is conveyed in the original paper as
                                            //
                                            //   "If an existing entry conflicts with a new one
                                            //    (same index but different terms), delete the
                                            //    existing entry and all that follow it (§5.3)
                                            //    Append any new entries not already in the log"
                                            //
                                            // - truncate/append at the specified offset
                                            // - udpate our head offset
                                            //
                                            let buf = msg.append;
                                            write_range!(self, buf, msg.off + 1, n);
                                            let slot = read_slot!(self, self.head);
                                            self.head = msg.off + n as u64;
                                            self.age = slot.term;
                                            display!(
                                                self,
                                                "{:?} | replicated [#{} #{}]",
                                                ctx,
                                                msg.off + 1,
                                                self.head
                                            );

                                            //
                                            // - emit a ACK to acknowledge our new head offset
                                            //
                                            let msg = ACK {
                                                id: self.id,
                                                term: self.term,
                                                ack: self.head,
                                            };

                                            let bytes = msg.to_raw(&self.host, &raw.src);
                                            (self.write)(&raw.src, &bytes);
                                            conflict = false;
                                        }
                                    }

                                    if conflict {

                                        //
                                        // - we can't validate the proposed check mark
                                        // - reply with a REBASE and propose to re-replicate
                                        //   starting at an earlier offset (e.g the previous one)
                                        // - use a simple exponential rewind strategy where we ask
                                        //   the LEADER to go back in the log with increasingly
                                        //   large steps
                                        //
                                        debug_assert!(msg.off > 0);
                                        display!(
                                            self,
                                            "{:?}*| conflict at [#{}|{}], rebasing",
                                            ctx,
                                            msg.off,
                                            msg.age
                                        );

                                        let msg = REBASE {
                                            id: self.id,
                                            term: self.term,
                                        };

                                        let bytes = msg.to_raw(&self.host, &raw.src);
                                        (self.write)(&raw.src, &bytes);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    ACK::CODE => {
                        let msg: ACK = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);

                        } else if let LEAD(ref mut ctx) = state {

                            //
                            // - a FOLLOWER just confirmed how much it now replicates
                            // - we now need to check there is enough evidence for us to
                            //   increment our commit offset (e.g do we have a quorum of
                            //   peers whose acknowledged offset is > our commit offset)
                            // - this is conveyed in the original paper as
                            //
                            //    "If there exists an N such that N > commitIndex, a
                            //     majority of matchIndex[i] ≥ N, and
                            //     log[N].term == currentTerm:
                            //     set commitIndex = N (§5.3, §5.4)."
                            //
                            // - note the quorum count is initialized at 1 since we have to
                            //   count ourselves (we are maintaining the log)
                            //
                            let mut n = 1;
                            let mut smallest = <u64>::max_value();
                            debug_assert!(
                                msg.ack <= self.head,
                                format!("ack {} head {}", msg.ack, self.head)
                            );
                            for peer in &mut self.peers {
                                debug_assert!(*peer.0 != self.id);

                                //
                                // - first, update the acknowledged offset for that peer
                                //
                                if *peer.0 == msg.id {
                                    display!(
                                        self,
                                        "{:?} | peer #{} at offset #{}",
                                        ctx,
                                        peer.0,
                                        msg.ack
                                    );
                                    peer.1.ack = msg.ack;
                                }

                                //
                                // - any peer whose confirmed replicated offset is > to
                                //   our commit is part of the quorum set
                                // - keep the smallest of those offsets
                                //
                                if peer.1.ack > self.commit {
                                    if peer.1.ack < smallest {
                                        smallest = peer.1.ack;
                                    }
                                    n += 1;
                                }
                            }

                            //
                            // - do we have quorum ?
                            //
                            if n > self.peers.len() >> 1 {

                                //
                                // - notify the sink with a COMMIT for each entry
                                // - update our commit offset to the smallest replicated
                                //   offset reported by the quorum peers
                                // - signal the sink event
                                //
                                debug_assert!(smallest >= self.tail);
                                let mut guard = self.payload.write();
                                for n in self.commit..smallest {
                                    let slot = read_slot!(self, n);
                                    (self.apply)(&mut guard, &slot.bytes);
                                    self.sink.push(Notification::COMMIT(n, slot.bytes));
                                }
                                drop(guard);
                                self.commit = smallest;
                                display!(self, "{:?} | offset #{} committed", ctx, smallest);

                                //
                                // - if the commit index reached a checkpoint boundary
                                // - reset the tail to that offset
                                // - lock the payload and take a snapshot of it
                                // - flush the log
                                // - notify the sink with a CHECKPOINT
                                // - signal the sink event
                                //
                                let boundary = self.commit -
                                    (self.commit % FSM::<S, T, U>::CHECKPOINT as u64);
                                if boundary > self.tail {
                                    display!(
                                        self,
                                        "{:?} | checkpointed [#{} #{}] ",
                                        ctx,
                                        self.tail,
                                        boundary
                                    );

                                    let guard = self.payload.read();
                                    let mut bytes = (*guard).flush();
                                    drop(guard);
                                    self.snapshot.clear();
                                    self.snapshot.append(&mut bytes);
                                    self.log.flush().unwrap();
                                    self.sink.push(Notification::CHECKPOINT(boundary));
                                    self.tail = boundary;
                                }
                            }

                        }
                    }
                    REBASE::CODE => {
                        let msg: REBASE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);

                        } else if let LEAD(_) = state {

                            //
                            // - the FOLLOWER is unable to match the check mark we
                            //   specified during replication: reset the peer offsets
                            // - we will rebase it upon the next heartbeat
                            //
                            let mut peer = self.peers.get_mut(&msg.id).unwrap();
                            peer.off = 1;
                            peer.ack = 0;

                        }
                    }
                    PROBE::CODE => {
                        let msg: PROBE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);

                        } else {
                            match state {
                                PREV(_) | CNDT(_) => {

                                    //
                                    // - always grant the pre-vote as long as we are not
                                    //   either FOLLOWER or LEADER and as long as the peer log
                                    //   is at least as up-to-date as ours
                                    //
                                    if msg.age < self.age || msg.head < self.head {
                                    } else {

                                        let msg = AVAILABLE {
                                            id: self.id,
                                            term: self.term,
                                        };
                                        let bytes = msg.to_raw(&self.host, &raw.src);
                                        (self.write)(&raw.src, &bytes);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    AVAILABLE::CODE => {
                        let msg: AVAILABLE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);
                        } else if let PREV(ref mut ctx) = state {
                            //
                            // - we are in the pre-voting phase
                            // - this vote does not count, we are just using it to
                            //   check if we can reach quorum or not
                            // - this is like testing for reachability
                            // - if we get quorum transition to CANDIDATE to trigger the
                            //   real election (and reset the votes bit array)
                            //
                            let (n, granted) = self.count_votes(&mut ctx.votes, msg.id);
                            if granted {
                                ctx.votes = 0;
                                display!(
                                    self,
                                    "{:?} | probing quorum reached ({}/{})",
                                    ctx,
                                    n,
                                    self.peers.len() + 1
                                );
                                return CNDT(*ctx);
                            }
                        }
                    }
                    ADVERTISE::CODE => {
                        let msg: ADVERTISE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);
                        } else {
                            match state {
                                CNDT(ref mut ctx) |
                                PREV(ref mut ctx) => {

                                    //
                                    // - we got a vote request from another candidate
                                    // - adjust our term if needed
                                    // - if we are indeed stale reset our pick
                                    //
                                    self.term = msg.term;
                                    if msg.term > self.term {
                                        ctx.pick = None;
                                    }

                                    //
                                    // - grant if a) we haven't picked someone yet or b) if
                                    //   we already picked this peer (duplicate message ?)
                                    // - this is also condition to the candidate having its
                                    //   log at least as up-to-date as ours (see above)
                                    //
                                    let granted = match ctx.pick {
                                        _ if msg.age < self.age || msg.head < self.head => false,
                                        Some(id) if id == msg.id => true,
                                        None => true,
                                        _ => false,
                                    };

                                    if granted {
                                        ctx.pick = Some(msg.id);
                                        display!(self, "{:?} | voted for peer #{}", ctx, msg.id);

                                        //
                                        // - the vote is granted
                                        // - reply with a VOTE RPC
                                        //
                                        let msg = VOTE {
                                            id: self.id,
                                            term: self.term,
                                        };

                                        let bytes = msg.to_raw(&self.host, &raw.src);
                                        (self.write)(&raw.src, &bytes);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    VOTE::CODE => {
                        let msg: VOTE = deserialize(&raw.msg[..]).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            let bytes = msg.to_raw(&self.host, &raw.src);
                            (self.write)(&raw.src, &bytes);
                        } else if let CNDT(ref mut ctx) = state {

                            //
                            // - same as above except this is the real election
                            // - if we reach quorum this time transition to LEADER
                            //
                            display!(self, "{:?} | vote received from peer #{}", ctx, msg.id);
                            let (n, granted) = self.count_votes(&mut ctx.votes, msg.id);
                            if granted {
                                ctx.votes = 0;
                                display!(
                                    self,
                                    "{:?} | voting quorum reached ({}/{})",
                                    ctx,
                                    n,
                                    self.peers.len() + 1
                                );
                                return LEAD(context::LEAD::default());
                            }

                        }
                    }
                    _ => {
                        display!(self, "warning, skipping invalid RPC");
                        debug_assert!(false, "invalid RPC");
                    }
                }
            }
            Opcode::DRAIN => {
                warn!(&self.logger, "               | | draining");
            }
            Opcode::EXIT => {

                //
                // - flush the log file
                // - send a last notification to our sink
                // - disable the sink semaphore which will force the consuming thread to pop
                //   all pending notifications and then move on
                //
                self.log.flush().unwrap();
                self.sink.push(Notification::EXIT);
                self.sink.sem.disable();
            }
            _ => {}
        };
        state
    }
}

impl Raft {

    #[allow(dead_code)]
    pub fn drain(&self) -> () {
        self.fsm.drain();
    }

    #[allow(dead_code)]
    pub fn store(&self, bytes: Vec<u8>) -> () {
        let _ = self.fsm.post(STORE(bytes));
    }
}

impl Clone for Raft {
    fn clone(&self) -> Self {
        Self { fsm: self.fsm.clone() }
    }
}

impl Drop for Raft {
    fn drop(&mut self) -> () {
        self.fsm.drain();
    }
}
