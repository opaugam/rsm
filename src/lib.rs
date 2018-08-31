#![deny(warnings)]
#![deny(bad_style)]
#![deny(future_incompatible)]
#![deny(nonstandard_style)]
#![deny(rust_2018_compatibility)]
#![deny(rust_2018_idioms)]
#![deny(unused)]
#![deny(unused_imports)]
#[cfg_attr(feature = "cargo-clippy", allow(mutex_atomic))]

pub mod lock;
