#![deny(warnings)]
#![deny(bad_style)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]
#![deny(unused)]
#![deny(unused_imports)]
#[cfg_attr(feature = "cargo-clippy", allow(mutex_atomic, too_many_arguments))]
extern crate bincode;
extern crate rand;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;

pub mod fsm;
pub mod primitives;
pub mod raft;
