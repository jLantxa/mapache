#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::unreachable,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::undocumented_unsafe_blocks,
    )
)]

pub mod archiver;
pub mod backend;
pub mod bundle;
pub mod commands;
pub mod common;
pub mod fs;
pub mod repository;
pub mod restorer;
pub mod ui;
pub mod utils;

#[cfg(all(feature = "mount", unix))]
pub(crate) mod mount;
