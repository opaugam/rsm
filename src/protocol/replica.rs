//! Log replication protocol.

use fsm::automaton::{Automaton, Opcode, Recv};
use primitives::event::*;
use protocol::messages::*;
use serde_json;
use slog::Logger;
use std::sync::Arc;

pub enum Command {
    MESSAGE(RAW),
}

#[derive(Debug, Copy, Clone)]
enum State {
    FOLLOWER,
   // CANDIDATE,
   // LEADER,
}

impl Default for State {
    fn default() -> State {
        State::FOLLOWER
    }
}

use self::Command::*;
use self::State::*;

pub struct Replica {
    pub fsm: Arc<Automaton<Command>>,
}

struct FSM {
    id: u8,
    host: String,
    term: u32,
    seeds: Option<String>,
    log: Logger,
}

impl Recv<Command, State> for FSM {
    fn recv(
        &mut self,
        _this: &Arc<Automaton<Command>>,
        state: State,
        opcode: Opcode<Command>,
    ) -> State {
        match (state, opcode) {
            (_, Opcode::START) => {
                match self.seeds {
                    Some(ref seeds) => {

                        //
                        // - we just started and have 1+ seeds
                        // - emit a heartbeat for each
                        //
                        let msg = PING { id: self.id, term: self.term };
                        for seed in seeds.split(",") {
                            println!("{}", msg.to_raw(&self.host, seed));
                        }
                    }
                    _ => {}
                }
            }
            (FOLLOWER, Opcode::INPUT(MESSAGE(raw))) => {
                match raw.code {
                    PING::CODE => {

                        //
                        // -
                        //
                        let msg: PING = serde_json::from_value(raw.user).unwrap();
                        info!(&self.log, "{:?} <- {:?}", state, msg; "src" => &raw.src);
                    }
                    _ => {}
                }
            }
            (_, Opcode::DRAIN) => {

                warn!(&self.log, "draining");
            }
            _ => {}
        }
        state
    }
}

impl Replica {
    pub fn spawn(
        guard: Arc<Guard>,
        id: u8,
        host: String,
        seeds: Option<String>,
        log: Logger,
    ) -> Replica {

        let fsm = Automaton::spawn(
            guard.clone(),
            Box::new(FSM {
                id,
                host,
                term: 0,
                seeds,
                log,
            }),
        );

        Replica { fsm }
    }
}

impl Drop for Replica {
    fn drop(&mut self) -> () {
        self.fsm.drain();
    }
}
