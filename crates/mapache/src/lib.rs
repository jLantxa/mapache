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

//! # mapache
//!
//! A fast, encrypted, deduplicating backup tool inspired by [restic](https://restic.net/).
//!
//! ## Architecture
//!
//! mapache organizes data into **repositories** containing encrypted, compressed **pack files**.
//! Files are split into content-defined chunks using [FastCDC](https://github.com/jlantxa/mapache/tree/main/crates/chunker),
//! deduplicated via BLAKE3 content IDs, and stored in packs encrypted with AES-256-GCM-SIV.

pub mod archiver;
pub mod backend;
pub mod bundle;
pub mod commands;
pub mod common;
mod ecc;
pub mod fs;
pub mod repository;
pub mod restorer;
pub mod ui;
pub mod utils;

#[cfg(all(feature = "mount", unix))]
pub(crate) mod mount;

#[cfg(test)]
mod tests;
