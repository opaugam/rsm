
pub mod automaton;
pub mod mpsc;
pub mod timer;

#[cfg(test)]
mod tests {

    use fsm::automaton::*;

    #[derive(Debug)]
    enum CMD {
        FOO,
    }

    #[derive(Debug, Copy, Clone)]
    enum State {
        RESET,
    }

    impl Default for State {
        fn default() -> State {
            State::RESET
        }
    }

    #[test]
    fn basic() {

        struct A {}

        impl Recv<CMD, State> for A {
            fn recv(&mut self, state: State, opcode: Opcode<CMD>) -> State {
                println!("> {:?} {:?}", state, opcode);
                state
            }
        }

        let fsm = Automaton::with(Box::new(A {}));
        /*
        {
            let fsm = fsm.clone();
            let 
            thread::spawn(move || {
                while fsm.post(CMD::PING) {}
            });

        }
        */
        fsm.start();
        fsm.post(CMD::FOO);
        fsm.drain();
    }
}
