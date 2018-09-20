#![deny(warnings)]
#![deny(bad_style)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]
#![deny(unused)]
#![deny(unused_imports)]
#![feature(const_fn)]
#![cfg_attr(feature = "cargo-clippy", feature(tool_lints))]
#![cfg_attr(feature = "cargo-clippy", warn(clippy, clippy_pedantic))]
#![cfg_attr(feature = "cargo-clippy", allow(
    cast_possible_truncation,
    cyclomatic_complexity,
    enum_glob_use,
    mutex_atomic,
    too_many_arguments,
    use_self))]
extern crate bincode;
extern crate memmap;
extern crate rand;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;

pub mod fsm;
pub mod primitives;
pub mod raft;
