//!
use primitives::event::*;
use primitives::rwlock::*;
use raft::protocol::{Payload, Raft};
use raft::sink::*;
use slog::Logger;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::io::{stdin, stdout, BufRead, Write};
use std::sync::Arc;
use std::thread;

///
///
///
pub fn spawn<'a, S, T, U: BuildHasher>(
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
        Raft::spawn::<_, S, T, U>(
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
    // - spawn an I/O thread to parse STDOUT
    // - we stream in chunks of payload to forward them to the automaton
    // - each chunk is preceded by its size in bytes encoded as a VARINT
    // - please note this thread is not guarded, e.g will run til process exit (which is okay)
    //
    {
        let raft = raft.clone();
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

                            //
                            // - we have a new chunk ready
                            // - truncate our buffer
                            //
                            if total >= off + chunk {
                                raft.feed(&buf[off..off + chunk]);
                                buf = buf.split_off(off + chunk);
                                chunk = 0;
                                off = 0;
                            } else {
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
    }

    (raft, lock, sink)
}
