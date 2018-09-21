//! Decorator spawning a background streaming thread on top of one automaton. The function will
//! only bind to the automaton on the first invokation (subsequent invokations have no effect).
//! The incoming byte stream is interpreted as a sequence of varints encoding the number of bytes
//! following them:
//!
//!   |<--------- chunk #1 ------------> | <---- chunk #2 ----->| ...
//!   | varint |         n bytes         | varint |   m bytes   | ...
//!
use fsm::automaton::*;
use primitives::once::*;
use std::io::{stdin, BufRead};
use std::sync::Arc;
use std::thread;

/// Internal once synchronizing the start of the streaming thread. There is no need to explicitely
/// reset it and the thread can be left running til proces exit.
static ANCILLARY: Once<()> = Once::new();

#[inline]
pub fn stream_from_sdtin<F, T>(fsm: &Arc<Automaton<T>>, deserialize: F) -> ()
where
    F: Fn(&[u8]) -> Option<T> + Sync + Send + 'static,
    T: Send,
{
    //
    // - create upon the first invokation a thread to stream byte chunks from STDIN
    // - the once does not hold onto any data nor do we use a termination event guard for this
    //   thread as it is stateless and can be nuked with no side effect
    //
    let wrapped = Arc::new(deserialize);
    let _ = ANCILLARY.run(move || {

        //
        // - we need to clone the arcs to avoid a captured outer variable error
        //
        let fsm = fsm.clone();
        let wrapped = wrapped.clone();
        let _ = thread::spawn(move || {

            let mut off = 0;
            let mut chunk = 0;
            let stdin = stdin();
            let mut buf = Vec::new();
            let mut pipe = stdin.lock();
            loop {

                //
                // - read from stdin and append to our buffer
                //
                let n = {
                    let read = pipe.fill_buf().unwrap();
                    let n = read.len();
                    if n > 0 {
                        buf.extend_from_slice(read);
                    }
                    n
                };

                //
                // - if we have nothing pause
                // - otherwise check if we have 1+ chunks to forward
                //
                if n > 0 {

                    //
                    // - consume the bytes we accumulated
                    // - loop and try to get as many chunks as possible
                    // - each chunk is preceded by a varint holding its byte size
                    //
                    pipe.consume(n);
                    loop {
                        let total = buf.len();
                        if total == 0 {
                            break;
                        }

                        if off > 0 {
                            if total >= off + chunk {

                                //
                                // - we have a new chunk ready
                                // - use the closure to either parse the bytes into a T or reject
                                // - truncate our buffer and reset the offsets
                                //
                                if let Some(msg) = wrapped(&buf[off..off + chunk]) {
                                    let _ = fsm.post(msg);
                                }
                                buf = buf.split_off(off + chunk);
                                chunk = 0;
                                off = 0;
                            } else {

                                //
                                // - we miss 1+ bytes, read more
                                //
                                break;
                            }
                        } else {

                            //
                            // - decode the first few bytes as a varint
                            // - use it to track how many bytes the actual payload is
                            //
                            let mut m = 0;
                            let mut shift = 0;
                            let mut bytes = 0;
                            loop {
                                if m >= total {
                                    break;
                                }
                                let val = buf[m] as usize;
                                bytes |= (val & 0x7F) << shift;
                                shift += 7;
                                m += 1;
                                if (val & 0x80) == 0 {
                                    off = m;
                                    chunk = bytes;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    });
}
