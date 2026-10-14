//! Compression policy.
//!
//! Compression is useful for most data, but already-compressed content (media,
//! archives, encrypted files) only wastes CPU when zstd attempts it and can even
//! grow slightly due to frame overhead. This module decides whether a blob
//! should be compressed.
//!
//! The decision combines three sources, in order of precedence:
//!
//! 1. [`CompressionPolicy::force_compression`] patterns — always compress.
//! 2. [`CompressionPolicy::skip_compression`] patterns — never compress.
//! 3. [`CompressionPolicy::probe`] — sample the blob's content and skip
//!    compression when the sample is already incompressible.
//!
//! The content probe is a compression primitive and lives with the zstd codec
//! (`SecureStorage::is_compressible`); this module only holds the user-facing
//! policy and the decision vocabulary. The probe is passed in as a callback so
//! this module stays independent of zstd.
//!
//! When nothing matches definitively, [`CompressionHint::Auto`] lets the
//! underlying storage decide (it can still store raw data when compression does
//! not shrink a blob).

use std::path::Path;

use crate::fs::filter::wildmatch_bytes;

/// What should be done with a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionHint {
    /// Compress using the repository's compression level.
    #[default]
    Compress,
    /// Store without compressing (the data is still encoded/encrypted).
    Store,
    /// No path-based decision; consult the content probe.
    Auto,
}

/// Compression policy: `skip-compression`/`force-compression` patterns plus a
/// flag for the content probe.
///
/// Empty default: no patterns, probing enabled (so the heuristic is on by
/// default). Users can add patterns and disable probing via the config file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompressionPolicy {
    /// Glob patterns for files that should never be compressed.
    pub skip_compression: Vec<String>,
    /// Glob patterns for files that should always be compressed, overriding
    /// both `skip_compression` and the content probe.
    pub force_compression: Vec<String>,
    /// Whether to sample blob contents to detect incompressible data.
    pub probe: bool,
}

impl CompressionPolicy {
    /// A policy with probing enabled (the default behavior).
    pub const fn with_probe() -> Self {
        Self {
            skip_compression: Vec::new(),
            force_compression: Vec::new(),
            probe: true,
        }
    }

    /// Decides purely from the file path and configured patterns, ignoring file
    /// contents. Returns `None` when the content probe should be consulted.
    pub fn decide_by_path(&self, path: &Path) -> Option<CompressionHint> {
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
        let name = name?;
        let name = name.as_bytes();

        if pattern_list_matches(&self.force_compression, name) {
            return Some(CompressionHint::Compress);
        }
        if pattern_list_matches(&self.skip_compression, name) {
            return Some(CompressionHint::Store);
        }
        None
    }

    /// Resolves a [`CompressionHint`] against the leading `sample` bytes into a
    /// concrete "run zstd?" answer.
    ///
    /// `Compress`/`Store` are taken literally; `Auto` (no path-based decision)
    /// defers to `probe` when probing is enabled.
    pub fn should_compress(
        &self,
        hint: CompressionHint,
        sample: &[u8],
        probe: impl Fn(&[u8]) -> bool,
    ) -> bool {
        match hint {
            CompressionHint::Compress => true,
            CompressionHint::Store => false,
            CompressionHint::Auto => !self.probe || sample.is_empty() || probe(sample),
        }
    }
}

fn pattern_list_matches(patterns: &[String], name: &[u8]) -> bool {
    patterns.iter().any(|p| wildmatch_bytes(p.as_bytes(), name))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn policy(skip: &[&str], force: &[&str], probe: bool) -> CompressionPolicy {
        CompressionPolicy {
            skip_compression: skip.iter().map(|s| s.to_string()).collect(),
            force_compression: force.iter().map(|s| s.to_string()).collect(),
            probe,
        }
    }

    #[test]
    fn force_overrides_skip() {
        let p = policy(&["*.jpg"], &["*.jpg"], false);
        assert_eq!(
            p.decide_by_path(Path::new("a.jpg")),
            Some(CompressionHint::Compress)
        );
    }

    #[test]
    fn patterns_match_filename_only() {
        let p = policy(&["*.mp4"], &[], false);
        assert_eq!(
            p.decide_by_path(Path::new("/x/y/movie.mp4")),
            Some(CompressionHint::Store)
        );
        assert_eq!(p.decide_by_path(Path::new("/x/y/movie.mkv")), None);
    }

    #[test]
    fn auto_defers_to_probe() {
        let p = policy(&[], &[], true);
        assert!(!p.should_compress(CompressionHint::Auto, b"data", |_| false));
        assert!(p.should_compress(CompressionHint::Auto, b"data", |_| true));
    }

    #[test]
    fn probe_disabled_never_stores() {
        let p = policy(&[], &[], false);
        assert!(p.should_compress(CompressionHint::Auto, b"data", |_| false));
    }

    #[test]
    fn literal_hints_ignore_probe() {
        let p = policy(&[], &[], true);
        assert!(p.should_compress(CompressionHint::Compress, b"data", |_| false));
        assert!(!p.should_compress(CompressionHint::Store, b"data", |_| true));
    }

    #[test]
    fn empty_sample_is_compressible() {
        let p = policy(&[], &[], true);
        assert!(p.should_compress(CompressionHint::Auto, &[], |_| false));
    }
}
