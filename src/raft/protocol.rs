//! Raft protocol state machine.
//!
//! The implementation incorporates a few additions (leader stickiness and pre-vote). The
//! election cycle is randomized to avoid herding as well. The underlying automaton mechanism
//! is wait-free and built on atomics.
//!
//! ```ignore
//!                <---------------+---------------+--------------+
//!                                |               |              |
//!   START -> FOLLOWER  --->  PRE-VOTE  --->  CANDIDATE  --->  LEADER
//!                                                |
//!                                <---------------+
//! ```
//!
//!  # Todo
//!      - snapshotting
//!      - membership add()/remove()
//!      - 'voted for' must be persisted (e.g persist an empty _NO_VOTE file under /tmp for a given
//!        term and erase it as soon as we're not candidate anymore)
//!
//!  # Capacity
//!
//!   This implementation offers the following upper bounds:
//!     - up to 64 peers
//!     - up to 4000G terms
//!     - log up to 65K entries
//!
//!  # Impementation notes
//!
//!   * In case of replication conflict the follower will ask the leader to rewind using a simple
//!     exponential backoff potentially down to the first offset.
//!
//!   * The term is a 42 bits quantity while the offset is on 16 bits (65K entries max). The last
//!     and previous offset/term comparisons to maintain the log up-do-date are done by using a
//!     single u64 packed as | term | offset |.
//!
//!  # Ideas
//!
//!    - force a 'commit-all' for cluster membership entries, e.g delay the commit offset index
//!      increment as long as *all* the peers are not replicated with at least that entry (and
//!      then atomically update the conf. on all peers at once).
//!
//!  # References
//!
//!   * [Original paper.](https://raft.github.io/raft.pdf)
//!   * [Optimizations.](http://openlife.cc/system/files/3-modifications-for-Raft-consensus.pdf)
use fsm::automaton::{Automaton, Opcode, Recv};
use fsm::mpsc::MPSC;
use fsm::timer::Timer;
use primitives::event::*;
use primitives::semaphore::*;
use raft::messages::*;
use rand::{Rng, thread_rng};
use serde_json;
use slog::Logger;
use std::cmp;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Volatile information maintained while on a given state, typically who we voted
/// for, who the leader is, etc.
mod context {

    #[derive(Copy, Clone, Default, PartialEq)]
    pub struct FLWR {
        pub live: bool,
        pub conflicts: u8,
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

#[derive(Copy, Clone)]
enum State {
    PREV(context::CNDT),
    CNDT(context::CNDT),
    FLWR(context::FLWR),
    LEAD(context::LEAD),
}

#[cfg_attr(rustfmt, rustfmt_skip)]
impl PartialEq for State {
    fn eq(&self, other: &State) -> bool {

        //
        // - since we embed a context we need to override PartialEq
        //   to do simple state comparisons
        //
        match (*self, *other) {
            (PREV(_),   PREV(_)) => true,
            (CNDT(_),   CNDT(_)) => true,
            (FLWR(_),   FLWR(_)) => true,
            (LEAD(_),   LEAD(_)) => true,
            _ =>                    false,
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
        State::FLWR(Default::default())
    }
}

use self::Command::*;
use self::State::*;

/// Wrapper around the automaton. Public operations are exposed via a few methods.
pub struct Protocol {
    fsm: Arc<Automaton<Command>>,
}

struct Peer {
    host: String,
    off: u16,
    ack: u16,
}

struct FSM<T>
where
    T: 'static + Send + Fn(String) -> (),
{
    /// Local peer index in [0, 64]
    id: u8,
    host: String,
    /// Sequence counter, used to disambiguiate timeouts
    seq: u64,
    /// Current term, persisted
    term: u64,
    /// Current log tail (e.g term|offset of the last log entry, 0 if empty)
    tail: u64,
    /// Commit offset
    commit: u16,
    peers: HashMap<u8, Peer>,
    timer: Timer<Command>,
    log: Vec<LogEntry>,
    sink: Arc<Sink>,
    write: T,
    logger: Logger,
}

macro_rules! pretty {
    ($self:ident, $fmt:expr $(, $arg:expr)*) => {
        debug!(&$self.logger, $fmt, $($arg),* ;
            "term" => $self.term,
            "log" => $self.log.len(),
            "commit" => $self.commit);
    };
}

macro_rules! pack {
    ($off:expr, $term:expr) => {
        ($term << 16) + $off as u64
    };
}

macro_rules! unpack {
    ($n:expr) => {
        ($n as u16, $n >> 16)
    };
}

impl<T> FSM<T>
where
    T: 'static + Send + Fn(String) -> (),
{
    const LIVENESS_TIMEOUT: u64 = 3000;
    const ELECTION_TIMEOUT: u64 = 250;

    fn add_vote(&self, votes: &mut u64, id: u8) -> bool {

        //
        // - the votes byte is used as a bitmask (1 bit per peer)
        // - set the bit corresponding to the specified id
        // - count the set bits
        // - compare with the quorum
        //
        let mut total = 1;
        *votes |= 1 << id;
        let mut n = *votes;
        while n > 0 {
            n &= n - 1;
            total += 1;
        }
        total > self.peers.len() as u8 >> 1
    }
}

impl<T> Recv<Command, State> for FSM<T>
where
    T: Send + Fn(String) -> (),
{
    fn recv(
        &mut self,
        this: &Arc<Automaton<Command>>,
        mut state: State,
        opcode: Opcode<Command, State>,
    ) -> State {
        match opcode {
            Opcode::START => {

                //
                // - fake a cluster setup with 3 nodes on localhost
                //
                for n in 0..2 {
                    self.peers.insert(
                        n,
                        Peer {
                            host: format!("127.0.0.1:900{}", n),
                            off: 0,
                            ack: 0,
                        },
                    );
                }

                //
                // - we start as a FOLLOWER
                // - set the liveness timeout
                //
                self.timer.schedule(
                    this.clone(),
                    TIMEOUT(self.seq),
                    Duration::from_millis(FSM::<T>::LIVENESS_TIMEOUT),
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
                        pretty!(self, "{:?} pre-voting", ctx);
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
                        pretty!(self, "{:?} triggering election in {} ms", ctx, ms);
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
                        //
                        pretty!(self, "{:?} now leading", ctx);
                        for peer in &mut self.peers {
                            if *peer.0 != self.id {
                                peer.1.off = self.log.len() as u16;
                                peer.1.ack = 0;
                            }
                        }

                        //
                        // - replicate immediately
                        // - notify the sink with LEADING
                        // - increment the sink semaphore
                        //
                        self.seq += 1;
                        let _ = this.post(TIMEOUT(self.seq));
                        self.sink.fifo.push(Notification::LEADING);
                        self.sink.sem.signal();
                    }
                    (PREV(_), FLWR(ctx)) |
                    (CNDT(_), FLWR(ctx)) |
                    (LEAD(_), FLWR(ctx)) => {

                        //
                        // - we got a REPLICATE with a higher term
                        // - set our next timeout
                        //
                        pretty!(self, "{:?} waiting for heartbeats", ctx);
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<T>::LIVENESS_TIMEOUT),
                        );
                    }
                    _ => {
                        debug_assert!(false, "invalid state transition");
                    }
                }
            }
            Opcode::INPUT(TIMEOUT(seq)) if seq == self.seq => {
                match state {
                    PREV(ref mut ctx) => {

                        //
                        // - we did not get a quorum, meaning it's useless to switch
                        //   to CANDIDATE at this point
                        // - send a CHECK to all our peers, the goal being to
                        //   estimate how many would agree to vote for us
                        // - the term we pass is our term + 1
                        //
                        ctx.pick = None;
                        for peer in &self.peers {
                            if *peer.0 != self.id {

                                let msg = CHECK {
                                    id: self.id,
                                    term: self.term + 1,
                                    tail: self.tail,
                                };
                                (self.write)(format!("{}", msg.to_raw(&self.host, &peer.1.host)));
                            }
                        }

                        //
                        // - set the next election timeout
                        // - we will cycle on PREVOTE as long as we don't get promoted
                        //   to FOLLOWER and don't get quorum
                        //
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<T>::ELECTION_TIMEOUT),
                        );
                    }
                    CNDT(ref mut ctx) => {
                        if ctx.advertised {

                            //
                            // - the election timed out
                            // - return to PREVOTE to start a new cycle
                            // - note we reset the context
                            //
                            return PREV(Default::default());
                        }

                        //
                        // - we went from PREVOTE to CANDIDATE
                        // - increment our term
                        // - send a ADVERTISE to all peers
                        // - wait for VOTE RPCs to come back within the election timeout
                        //
                        self.term += 1;
                        for peer in &self.peers {
                            if *peer.0 != self.id {

                                let msg = ADVERTISE {
                                    id: self.id,
                                    term: self.term,
                                    tail: self.tail,
                                };
                                (self.write)(format!("{}", msg.to_raw(&self.host, &peer.1.host)));
                            }
                        }

                        //
                        // - set the advertised trigger
                        // - set the election timeout
                        //
                        ctx.advertised = true;
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::<T>::ELECTION_TIMEOUT),
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
                                Duration::from_millis(FSM::<T>::LIVENESS_TIMEOUT),
                            );

                        } else {

                            //
                            // - liveness timeout: the leader may be down or we are
                            //   not reachable anymore (network partition)
                            // - notify the sink with IDLE
                            // - increment the sink semaphore
                            // - switch to PREVOTE to initiate a new election cycle
                            //
                            self.sink.fifo.push(Notification::IDLE);
                            self.sink.sem.signal();
                            return PREV(Default::default());
                        }
                    }
                    LEAD(ref ctx) => {

                        //
                        // - assert our authority by sending a REPLICATE to all our peers
                        // - any peer receiving those will turn into a FOLLOWER if not already
                        //   the case
                        //
                        // @todo better manager idle times vs. dirty state
                        //
                        let end = self.log.len() as u16;
                        for peer in &mut self.peers {
                            if *peer.0 != self.id {

                                //
                                // - prep a buffer
                                // - check if we need to replicate
                                //
                                let mut append = Vec::new();
                                debug_assert!(end >= peer.1.ack);
                                debug_assert!(end >= peer.1.off);
                                debug_assert!(peer.1.off >= peer.1.ack);
                                if end > peer.1.off {

                                    //
                                    // - we have 1+ log entries to replicate
                                    // - copy entries from [off + 1, end] to our buffer
                                    //
                                    let size = end - peer.1.off;
                                    pretty!(
                                        self,
                                        "{:?} replicating [#{} .. #{}] to peer #{}",
                                        ctx,
                                        peer.1.off + 1,
                                        end,
                                        peer.0
                                    );
                                    for n in 0..size {
                                        append.push(self.log[(peer.1.off + n) as usize].clone());
                                    }
                                }

                                //
                                // - pack the index+term for the write offset (e.g the offset
                                //   immediately preceding the first replicated entry)
                                // - even if we have no entries to replicate this will force the
                                //   FOLLOWER to check its log and flag any conflict
                                // - blank peers will also be able to synch-up this way
                                //
                                let check = if peer.1.off > 0 {
                                    pack!(peer.1.off, self.log[peer.1.off as usize - 1].term)
                                } else {
                                    0
                                };

                                //
                                // - update the offset for that peer
                                // - fire the REPLICATE
                                //
                                peer.1.off = end;
                                let msg = REPLICATE {
                                    id: self.id,
                                    term: self.term,
                                    commit: self.commit,
                                    check,
                                    append,
                                };

                                (self.write)(format!("{}", msg.to_raw(&self.host, &peer.1.host)));
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
                            Duration::from_millis(FSM::<T>::LIVENESS_TIMEOUT / 3),
                        );
                    }
                }
            }
            Opcode::INPUT(STORE(blob)) => {
                match state {
                    LEAD(ref ctx) => {
                        
                        //
                        // - add the entry to our log
                        //
                        pretty!(self, "{:?} log <- '{}' <- local", ctx, blob);
                        self.log.push(LogEntry {
                            term: self.term,
                            blob,
                        });
                        self.tail = pack!(self.log.len() as u16, self.term);

                        //
                        // - replicate immediately
                        //
                        self.seq += 1;
                        let _ = this.post(TIMEOUT(self.seq));
                    }
                    FLWR(ref ctx) => {
                        if let Some(id) = ctx.leader {

                            //
                            // - if we have a leader redirect the user payload to that peer via
                            //   a APPEND
                            //
                            let peer = &self.peers[&id];
                            let msg = APPEND {
                                id: self.id,
                                term: self.term,
                                blob,
                            };

                            (self.write)(format!("{}", msg.to_raw(&self.host, &peer.host)));
                        }
                    }
                    _ => {}
                }
            }
            Opcode::INPUT(MESSAGE(raw)) => {
                match raw.code {
                    APPEND::CODE => {
                        let mut msg: APPEND = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));

                        } else {
                            match state {
                                LEAD(ref ctx) => {

                                    //
                                    // - add the entry to our log
                                    //
                                    pretty!(self, "{:?} log <- '{}' <- peer #{}", ctx, msg.blob, msg.id);
                                    self.log.push(LogEntry {
                                        term: self.term,
                                        blob: msg.blob,
                                    });
                                    self.tail = pack!(self.log.len() as u16, self.term);

                                    //
                                    // - replicate immediately
                                    //
                                    self.seq += 1;
                                    let _ = this.post(TIMEOUT(self.seq));
                                }
                                _ => {}
                            }
                        }
                    }
                    UPGRADE::CODE => {
                        let msg: UPGRADE = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term > self.term {

                            //
                            // - we are stale
                            // - upgrade our term
                            // - notify the sink with IDLE
                            // - increment the sink semaphore
                            // - always transition to FOLLOWER without a leader (yet)
                            //
                            self.term = msg.term;
                            self.sink.fifo.push(Notification::IDLE);
                            self.sink.sem.signal();
                            return FLWR(context::FLWR {
                                live: false,
                                conflicts: 0,
                                leader: None,
                            });
                        }
                    }
                    REPLICATE::CODE => {
                        let mut msg: REPLICATE = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));

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
                                    self.sink.fifo.push(Notification::FOLLOWING);
                                    self.sink.sem.signal();
                                    return FLWR(context::FLWR {
                                        live: true,
                                        conflicts: 0,
                                        leader: Some(msg.id),
                                    });
                                }
                                FLWR(ref mut ctx) => {
                                    if ctx.leader.is_none() {

                                        //
                                        // - notify the sink with FOLLOWING in case we went
                                        //   from no leader to one (e.g we started for instance)
                                        // - increment the sink semaphore
                                        //
                                        self.sink.fifo.push(Notification::FOLLOWING);
                                        self.sink.sem.signal();
                                    }

                                    //
                                    // - upgrade our term if we are stale
                                    // - set the live trigger
                                    //
                                    ctx.live = true;
                                    ctx.leader = Some(msg.id);
                                    self.term = msg.term;

                                    //
                                    // - notify the sink with a COMMIT for each entry
                                    // - update our commit offset up to the leader's offset
                                    // - increment the sink semaphore
                                    //
                                    //   "If leaderCommit > commitIndex, set commitIndex =
                                    //    min(leaderCommit, index of last new entry)"
                                    //
                                    let end = self.log.len() as u16;
                                    if msg.commit > self.commit {
                                        let updated = cmp::min(msg.commit, end);
                                        pretty!(self, "{:?} commit now at #{}", ctx, updated);
                                        for n in self.commit..updated {
                                            self.sink.fifo.push(Notification::COMMIT(
                                                self.log[n as usize].blob.clone(),
                                            ));
                                            self.sink.sem.signal();
                                        }
                                        self.commit = updated;
                                    }

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
                                    let n = msg.append.len() as u16;
                                    let (off, term) = unpack!(msg.check);
                                    if off == 0 ||
                                        (off <= end && self.log[off as usize - 1].term == term)
                                    {
                                        //
                                        // - reset the conflict counter
                                        //
                                        ctx.conflicts = 0;
                                        if n > 0 {

                                            //
                                            // - the specified log offset matches
                                            // - now this offset may not be our tail, therefore
                                            //   truncate() to drop all subsequent entries
                                            // - then transfer the entries to append at the end
                                            // - this is conveyed in the original paper as
                                            //
                                            //   "If an existing entry conflicts with a new one
                                            //    (same index but different terms), delete the
                                            //    existing entry and all that follow it (§5.3)
                                            //    Append any new entries not already in the log"
                                            //
                                            self.log.truncate(off as usize);
                                            self.log.append(&mut msg.append);
                                            let ack = self.log.len() as u16;
                                            self.tail = pack!(ack, self.term);
                                            pretty!(self, "{:?} #{} now replicated", ctx, ack);
                                            let msg = CONFIRM {
                                                id: self.id,
                                                term: self.term,
                                                ack,
                                            };

                                            (self.write)(
                                                format!("{}", msg.to_raw(&self.host, &raw.src)),
                                            );
                                        }

                                    } else {

                                        //
                                        // - we can't validate the proposed check mark
                                        // - reply with a CONFLICT and propose to re-replicate
                                        //   starting at an earlier offset (e.g the previous one)
                                        // - use a simple exponential rewind strategy where we ask
                                        //   the LEADER to go back in the log with increasingly
                                        //   large steps
                                        //
                                        debug_assert!(off > 0);
                                        let rewind = cmp::min(1 << ctx.conflicts, off);
                                        ctx.conflicts += 1;
                                        pretty!(
                                            self,
                                            "{:?} conflict at #{}/T{}, asking for #{} ({}/4)",
                                            ctx,
                                            off,
                                            term,
                                            off - rewind,
                                            ctx.conflicts
                                        );
                                        let msg = CONFLICT {
                                            id: self.id,
                                            term: self.term,
                                            try: off - rewind,
                                        };

                                        (self.write)(
                                            format!("{}", msg.to_raw(&self.host, &raw.src)),
                                        );
                                    }
                                }
                                LEAD(ref ctx) => {

                                    //
                                    // - another peer was elected LEADER
                                    // - notify the sink with FOLLOWING
                                    // - increment the sink semaphore
                                    // - upgrade our term and transition to FOLLOWER
                                    // - note that usualy msg.term is > to our term except in the
                                    //   case where quorum is 2 (e.g it is possible 2 out of 3
                                    //   peers vote for each other resulting in both of them
                                    //   reaching quorum and becoming competing leaders)
                                    // - this edge case is trivially handled by forcing each LEADER
                                    //   revert to FOLLOWER
                                    //
                                    self.term = msg.term;
                                    debug_assert!(
                                        msg.term > self.term || self.peers.len() >> 1 == 1
                                    );
                                    pretty!(self, "{:?} stepping down", ctx);
                                    self.sink.fifo.push(Notification::FOLLOWING);
                                    self.sink.sem.signal();
                                    return FLWR(context::FLWR {
                                        live: true,
                                        conflicts: 0,
                                        leader: Some(msg.id),
                                    });
                                }
                            }
                        }
                    }
                    CONFIRM::CODE => {
                        let msg: CONFIRM = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));

                        } else {
                            match state {
                                LEAD(ref mut ctx) => {

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
                                    let mut smallest = <u16>::max_value();
                                    for peer in &mut self.peers {
                                        if *peer.0 != self.id {

                                            //
                                            // - first, update the acknowledged offset for that peer
                                            //
                                            if *peer.0 == msg.id {
                                                pretty!(
                                                    self,
                                                    "{:?} peer #{} at offset #{}",
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
                                    }

                                    //
                                    // - do we have quorum ?
                                    //
                                    if n > self.peers.len() >> 1 {
                                        pretty!(self, "{:?} commit now at #{}", ctx, smallest);

                                        //
                                        // - notify the sink with a COMMIT for each entry
                                        // - update our commit offset to the smallest replicated
                                        //   offset reported by the quorum peers
                                        // - signal the sink event
                                        //
                                        debug_assert!(smallest > 0);
                                        for n in self.commit..smallest {
                                            self.sink.fifo.push(Notification::COMMIT(
                                                self.log[n as usize].blob.clone(),
                                            ));
                                            self.sink.sem.signal();
                                        }
                                        self.commit = smallest;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    CONFLICT::CODE => {
                        let msg: CONFLICT = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));

                        } else {
                            match state {
                                LEAD(mut ctx) => {

                                    //
                                    // - the FOLLOWER is unable to match the check mark we
                                    //   specified during replication: update its write offset
                                    //   (usually decrementing it)
                                    // - make sure to potentially reset the acknowledged offset
                                    //   as well
                                    //
                                    let mut peer = self.peers.get_mut(&msg.id).unwrap();
                                    pretty!(
                                        self,
                                        "{:?} peer #{} offset reset to #{} (conflict)",
                                        ctx,
                                        msg.id,
                                        msg.try
                                    );
                                    debug_assert!(msg.try < peer.off);
                                    peer.ack = cmp::min(peer.ack, msg.try);
                                    peer.off = msg.try;

                                    //
                                    // - replicate immediately
                                    //
                                    self.seq += 1;
                                    let _ = this.post(TIMEOUT(self.seq));
                                }
                                _ => {}
                            }
                        }
                    }
                    CHECK::CODE => {
                        let msg: CHECK = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));

                        } else {
                            match state {
                                PREV(_) | CNDT(_) => {

                                    //
                                    // - always grant the pre-vote as long as we are not
                                    //   either FOLLOWER or LEADER and as long as the peer log
                                    //   is at least as up-to-date as ours
                                    // - we pack the tail composite offset in such way we can
                                    //   directly compare in one shot
                                    //
                                    if msg.tail >= self.tail {

                                        let msg = VOTE {
                                            id: self.id,
                                            term: self.term,
                                        };
                                        (self.write)(
                                            format!("{}", msg.to_raw(&self.host, &raw.src)),
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    ADVERTISE::CODE => {
                        let msg: ADVERTISE = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));
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
                                        _ if msg.tail < self.tail => false,
                                        Some(id) if id == msg.id => true,
                                        None => true,
                                        _ => false,
                                    };

                                    if granted {
                                        ctx.pick = Some(msg.id);
                                        pretty!(self, "{:?} voted for peer #{}", ctx, msg.id);

                                        //
                                        // - the vote is granted
                                        // - reply with a VOTE RPC
                                        //
                                        let msg = VOTE {
                                            id: self.id,
                                            term: self.term,
                                        };

                                        (self.write)(
                                            format!("{}", msg.to_raw(&self.host, &raw.src)),
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    VOTE::CODE => {
                        let msg: VOTE = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            (self.write)(format!("{}", msg.to_raw(&self.host, &raw.src)));
                        } else {
                            match state {
                                PREV(ref mut ctx) => {

                                    //
                                    // - we are in the pre-voting phase
                                    // - this vote does not count, we are just using it to
                                    //   check if we can reach quorum or not
                                    // - this is like testing for reachability
                                    // - if we get quorum transition to CANDIDATE to trigger the
                                    //   real election (and reset the votes bit array)
                                    //
                                    if self.add_vote(&mut ctx.votes, msg.id) {
                                        ctx.votes = 0;
                                        return CNDT(*ctx);
                                    }
                                }
                                CNDT(ref mut ctx) => {

                                    //
                                    // - same as above except this is the real election
                                    // - if we reach quorum this time transition to LEADER
                                    //
                                    if self.add_vote(&mut ctx.votes, msg.id) {
                                        return LEAD(Default::default());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {
                        pretty!(self, "warning, skipping invalid RPC");
                        debug_assert!(false, "invalid RPC");
                    }
                }
            }
            Opcode::DRAIN => {
                warn!(&self.logger, "draining");
            }
            Opcode::EXIT => {

                //
                // - send a last notification to our sink
                // - disable the sink semaphore which will force the consuming thread to pop
                //   all pending notifications and then move on
                //
                self.sink.fifo.push(Notification::EXIT);
                self.sink.sem.disable();
            }
            _ => {}
        };
        state
    }
}

impl Protocol {
    pub fn spawn<T>(
        guard: Arc<Guard>,
        id: u8,
        host: String,
        write: T,
        logger: Logger,
    ) -> (Self, Arc<Sink>)
    where
        T: 'static + Send + Fn(String) -> (),
    {

        let sink = Arc::new(Sink::new());
        let fsm = Automaton::spawn(
            guard.clone(),
            Box::new(FSM {
                id,
                host,
                seq: 0,
                term: 0,
                tail: 0,
                commit: 0,
                peers: HashMap::new(),
                timer: Timer::spawn(guard.clone()),
                log: Vec::new(),
                sink: sink.clone(),
                write,
                logger,
            }),
        );

        (Protocol { fsm }, sink)
    }

    #[allow(dead_code)]
    pub fn drain(&self) -> () {
        self.fsm.drain();
    }

    #[allow(dead_code)]
    pub fn read(&self, raw: RAW) -> () {
        let _ = self.fsm.post(MESSAGE(raw));
    }

    #[allow(dead_code)]
    pub fn store(&self, blob: String) -> () {
        let _ = self.fsm.post(STORE(blob));
    }
}

impl Clone for Protocol {
    fn clone(&self) -> Self {
        Protocol { fsm: self.fsm.clone() }
    }
}

impl Drop for Protocol {
    fn drop(&mut self) -> () {
        self.fsm.drain();
    }
}

/// Events emitted by the state machine. Those are available for the user to react
/// to membership changes, commits, etc.
#[derive(Debug)]
pub enum Notification {
    FOLLOWING,
    LEADING,
    IDLE,
    COMMIT(String),
    EXIT,
}

/// Simple blocking notification sink consuming from a MPSC. Once signaled with no
/// content the sink will disable itself and always fail.
pub struct Sink {
    sem: Semaphore,
    fifo: MPSC<Notification>,
}

impl Sink {
    #[allow(dead_code)]
    pub fn next(&self) -> Option<Notification> {

        //
        // - wait/pop, this will fast-fail on None as soon as we disable the semaphore, e.g
        //   when the state-machine exits
        //
        self.sem.wait();
        self.fifo.pop().ok()
    }

    fn new() -> Self {

        Sink {
            sem: Semaphore::new(),
            fifo: MPSC::new(),
        }
    }
}
