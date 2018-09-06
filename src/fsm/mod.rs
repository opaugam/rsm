pub mod automaton;
pub mod mpsc;
pub mod timer;

#[cfg(test)]
mod tests {

    use primitives::event::*;
    use fsm::automaton::*;
    use fsm::timer::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    enum Command {
        TERMINATE,
    }

    #[derive(Debug, Copy, Clone)]
    enum State {
        DEFAULT,
    }

    use self::Command::*;

    impl Default for State {
        fn default() -> State {
            State::DEFAULT
        }
    }

    #[test]
    fn basic_lifecycle() {
        struct FSM {
            cnt: usize,
        }

        impl Recv<Command, State> for FSM {
            fn recv(
                &mut self,
                _this: &Arc<Automaton<Command>>,
                state: State,
                _opcode: Opcode<Command>,
            ) -> State {
                self.cnt += 1;
                state
            }
        }

        impl Drop for FSM {
            fn drop(&mut self) -> () {
                assert!(self.cnt == 3);
            }
        }

        let event = Event::new();
        let guard = event.guard();
        let fsm = Automaton::spawn(guard.clone(), Box::new(FSM { cnt: 0 }));
        drop(guard);
        fsm.drain();
        event.wait();
    }

    #[test]
    fn terminate_in_250ms() {

        struct FSM {
            timer: Timer<Command>,
        }

        impl Recv<Command, State> for FSM {
            fn recv(
                &mut self,
                this: &Arc<Automaton<Command>>,
                state: State,
                opcode: Opcode<Command>,
            ) -> State {
                match (state, opcode) {
                    (_, Opcode::START) => {
                        self.timer.schedule(
                            this.clone(),
                            Command::TERMINATE,
                            Duration::from_millis(250),
                        );
                    }
                    (_, Opcode::INPUT(TERMINATE)) => {
                        this.drain();
                    }
                    _ => {}
                }
                state
            }
        }

        let event = Event::new();
        let guard = event.guard();
        let timer = Timer::<Command>::spawn(guard.clone());
        let _ = Automaton::spawn(guard.clone(), Box::new(FSM { timer }));
        drop(guard);
        event.wait();
    }
}
