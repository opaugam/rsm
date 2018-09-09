//! Cluster membership state machine managing elections, LEAD vs. FLWR, etc.
//!
//! The implementation incorporates a few additions (leader stickiness and pre-vote). The
//! election cycle is randomized to avoid herding as well.
//!
//!                <---------------+---------------+--------------+
//!                                |               |              |
//!   START -> FOLLOWER  --->  PRE-VOTE  --->  CANDIDATE  --->  LEADER
//!                                                |
//!                                <---------------+
//!
//! references:
//! * http://openlife.cc/system/files/3-modifications-for-Raft-consensus.pdf
//! * https://raft.github.io/raft.pdf
use fsm::automaton::{Automaton, Opcode, Recv};
use fsm::timer::Timer;
use primitives::event::*;
use raft::messages::*;
use rand::{Rng, thread_rng};
use serde_json;
use slog::Logger;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

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

pub struct Topology {
    pub fsm: Arc<Automaton<Command>>,
}

struct Peer {
    host: String,
}

struct FSM {
    id: u8,
    host: String,
    cid: Uuid,
    seq: u64,
    term: u64,
    peers: HashMap<u8, Peer>,
    timer: Timer<Command>,
    log: Logger,
}

macro_rules! pretty {
    ($self:ident, $fmt:expr $(, $arg:expr)*) => {
        info!(&$self.log, $fmt, $($arg),* ;
            "term" => $self.term,
            "peers" => $self.peers.len());
    };
}

impl FSM {
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

impl Recv<Command, State> for FSM {
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
                for n in 0..4 {
                    self.peers.insert(
                        n,
                        Peer { host: format!("127.0.0.1:900{}", n) },
                    );
                }

                //
                // - we start as a FOLLOWER
                // - set the liveness timeout
                //
                self.timer.schedule(
                    this.clone(),
                    TIMEOUT(self.seq),
                    Duration::from_millis(FSM::LIVENESS_TIMEOUT),
                );
            }
            Opcode::TRANSITION(prv) => {
                assert!(state != prv);
                self.seq += 1;
                match (prv, state) {
                    (CNDT(_), PREV(ref ctx)) |
                    (FLWR(_), PREV(ref ctx)) => {

                        //
                        // - force a immediate timeout to start the prevote cycle
                        //
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
                        // - force a immediate timeout to send PING RPCs immediately
                        //
                        pretty!(self, "{:?} now leading", ctx);
                        let _ = this.post(TIMEOUT(self.seq));
                    }
                    (PREV(_), FLWR(ctx)) |
                    (CNDT(_), FLWR(ctx)) |
                    (LEAD(_), FLWR(ctx)) => {

                        //
                        // - we got a PING with a higher term
                        // - set our next timeout
                        //
                        pretty!(self, "{:?} waiting for heartbeats", ctx);
                        self.timer.schedule(
                            this.clone(),
                            TIMEOUT(self.seq),
                            Duration::from_millis(FSM::LIVENESS_TIMEOUT),
                        );
                    }
                    _ => {
                        assert!(false, "invalid state transition");
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
                        //
                        ctx.pick = None;
                        for peer in &self.peers {
                            if *peer.0 != self.id {

                                let msg = CHECK {
                                    id: self.id,
                                    term: self.term + 1,
                                };
                                println!("{}", msg.to_raw(&self.host, &peer.1.host));
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
                            Duration::from_millis(FSM::ELECTION_TIMEOUT),
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
                                };
                                println!("{}", msg.to_raw(&self.host, &peer.1.host));
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
                            Duration::from_millis(FSM::ELECTION_TIMEOUT),
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
                                Duration::from_millis(FSM::LIVENESS_TIMEOUT),
                            );

                        } else {

                            //
                            // - liveness timeout: the leader may be down or we are
                            //   not reachable anymore (network partition)
                            // - switch to PREVOTE to initiate a new election cycle
                            //
                            return PREV(Default::default());
                        }
                    }
                    LEAD(_) => {

                        //
                        // - assert our authority by sending a PING
                        // - any peer receiving those will turn into a FOLLOWER
                        //
                        for peer in &self.peers {
                            if *peer.0 != self.id {

                                let msg = PING {
                                    id: self.id,
                                    term: self.term,
                                };
                                println!("{}", msg.to_raw(&self.host, &peer.1.host));
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
                            Duration::from_millis(FSM::LIVENESS_TIMEOUT / 3),
                        );
                    }
                    _ => {}
                }
            }
            Opcode::INPUT(MESSAGE(raw)) => {
                match raw.code {
                    UPGRADE::CODE => {
                        let msg: UPGRADE = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term > self.term {

                            //
                            // - we are stale
                            // - upgrade our term
                            // - always transition to FOLLOWER
                            //
                            self.term = msg.term;
                            return FLWR(context::FLWR {
                                live: false,
                                leader: None,
                            });
                        }
                    }
                    PING::CODE => {
                        let msg: PING = serde_json::from_value(raw.user).unwrap();
                        debug_assert!(msg.id != self.id);
                        if msg.term < self.term {

                            //
                            // - stale peer: send back a UPGRADE
                            //
                            let msg = UPGRADE {
                                id: self.id,
                                term: self.term,
                            };
                            println!("{}", msg.to_raw(&self.host, &raw.src));

                        } else {
                            match state {
                                PREV(_) | CNDT(_) => {

                                    //
                                    // - a LEADER is active
                                    // - upgrade our term if we are stale
                                    // - transition to FOLLOWER
                                    //
                                    self.term = msg.term;
                                    return FLWR(context::FLWR {
                                        live: true,
                                        leader: Some(msg.id),
                                    });
                                }
                                FLWR(ref mut ctx) => {

                                    //
                                    // - upgrade our term if we are stale
                                    // - set the live trigger
                                    //
                                    ctx.live = true;
                                    ctx.leader = Some(msg.id);
                                    self.term = msg.term;
                                }
                                LEAD(ref ctx) => {

                                    //
                                    // - another LEADER was elected
                                    // - upgrade our term (we *must* be stale by now otherwise bug)
                                    // - transition to FOLLOWER
                                    //
                                    assert!(self.term < msg.term);
                                    self.term = msg.term;
                                    pretty!(self, "{:?} stepping down", ctx);
                                    return FLWR(context::FLWR {
                                        live: true,
                                        leader: Some(msg.id),
                                    });
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
                            println!("{}", msg.to_raw(&self.host, &raw.src));

                        } else {
                            match state {
                                PREV(_) | CNDT(_) => {

                                    //
                                    // - always grant the pre-vote as long as we are not
                                    //   either FOLLOWER or LEADER
                                    //
                                    let msg = VOTE {
                                        id: self.id,
                                        term: self.term,
                                    };
                                    println!("{}", msg.to_raw(&self.host, &raw.src));
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
                            println!("{}", msg.to_raw(&self.host, &raw.src));
                        } else {
                            match state {
                                CNDT(ref mut ctx) | PREV(ref mut ctx) => {

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
                                    //
                                    let granted = match ctx.pick {
                                        Some(id) if id == msg.id => true,
                                        None => true,
                                        _ => false,
                                    };

                                    if granted {

                                        ctx.pick = Some(msg.id);
                                        pretty!(self, "{:?} voted for #{}", ctx, msg.id);

                                        //
                                        // - the vote is granted
                                        // - reply with a VOTE RPC
                                        //
                                        let msg = VOTE {
                                            id: self.id,
                                            term: self.term,
                                        };

                                        println!("{}", msg.to_raw(&self.host, &raw.src));
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
                            println!("{}", msg.to_raw(&self.host, &raw.src));
                        } else {
                            match state {
                                PREV(ref mut ctx) => {

                                    //
                                    // - we are in the pre-voting phase
                                    // - this vote does not count, we are just using it to
                                    //   check if we can reach quorum or not
                                    // - this is like testing for reachability
                                    // - ref: http://openlife.cc/system/files/3-modifications-for-Raft-consensus.pdf
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
                warn!(&self.log, "draining");
            }
            _ => {}
        };
        state
    }
}

impl Topology {
    pub fn spawn(guard: Arc<Guard>, id: u8, host: String, log: Logger) -> Topology {

        let fsm = Automaton::spawn(
            guard.clone(),
            Box::new(FSM {
                id,
                host,
                cid: Uuid::new_v4(),
                seq: 0,
                term: 0,
                peers: HashMap::new(),
                timer: Timer::spawn(guard.clone()),
                log,
            }),
        );

        Topology { fsm }
    }
}

impl Drop for Topology {
    fn drop(&mut self) -> () {
        self.fsm.drain();
    }
}
