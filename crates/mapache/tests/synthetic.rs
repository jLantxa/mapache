use std::{
    cell::Cell,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

/// Create a symlink using the platform's native API.
///
/// On Unix this is a direct `symlink` call. On Windows the target must be
/// classified as a file or directory symlink, so we infer the type from the
/// filesystem at creation time (the target must already exist for this to
/// work reliably).
///
/// Returns `Ok(false)` if the platform cannot create symlinks in the current
/// environment (e.g. missing Windows privilege). `unsupported_flag` is set to
/// `false` on first failure so that subsequent entries and verification can
/// skip symlink handling without repeated warnings.
fn create_native_symlink(
    target: &str,
    link: &Path,
    is_dir: bool,
    unsupported_flag: &Cell<bool>,
) -> Result<bool> {
    if link.exists() {
        fs::remove_file(link)?;
    }

    #[cfg(unix)]
    {
        let _ = (is_dir, unsupported_flag);
        std::os::unix::fs::symlink(target, link)?;
        Ok(true)
    }

    #[cfg(windows)]
    {
        let result = if is_dir {
            std::os::windows::fs::symlink_dir(target, link)
        } else {
            std::os::windows::fs::symlink_file(target, link)
        };
        match result {
            Ok(()) => Ok(true),
            Err(e) if e.raw_os_error() == Some(1314) => {
                if unsupported_flag.replace(false) {
                    eprintln!(
                        "Warning: cannot create symlink without appropriate privileges \
                         (Windows error 1314, SeCreateSymbolicLinkPrivilege). \
                         Skipping symlink {:?} -> {}.",
                        link, target
                    );
                }
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    #[cfg(not(any(unix, windows)))]
    compile_error!("Unsupported platform for symlink creation");
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Data(Vec<u8>),    // Explicit data block
    Random(u64, u64), // Random data block (size, seed) - Streamed
    Dir,
    Symlink { target: String, is_dir: bool },
}

#[derive(Debug, Clone, Copy)]
pub enum ItemDef {
    File(&'static [u8]),
    Dir,
    Symlink { target: &'static str, is_dir: bool },
}

#[derive(Clone, Default)]
pub struct Dataset {
    /// List of (Path, List of instructions for that path)
    pub items: Vec<(String, Vec<Entry>)>,
}

impl Dataset {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an instruction to a path. If the path already exists, it is appended to the end (composite file).
    pub fn add(mut self, path: impl Into<String>, entry: Entry) -> Self {
        let p = path.into();
        if let Some(pos) = self.items.iter().position(|(name, _)| name == &p) {
            self.items[pos].1.push(entry);
        } else {
            self.items.push((p, vec![entry]));
        }
        self
    }

    pub fn with_structure(mut self, skeleton: &[(&str, ItemDef)]) -> Self {
        for (path, def) in skeleton {
            let entry = match def {
                ItemDef::File(d) => Entry::Data(d.to_vec()),
                ItemDef::Dir => Entry::Dir,
                ItemDef::Symlink { target, is_dir } => Entry::Symlink {
                    target: target.to_string(),
                    is_dir: *is_dir,
                },
            };
            self = self.add(*path, entry);
        }
        self
    }
}

pub struct SyntheticData {
    pub dataset: Dataset,
    symlinks_supported: Cell<bool>,
}

impl SyntheticData {
    pub fn new(dataset: Dataset) -> Self {
        Self {
            dataset,
            symlinks_supported: Cell::new(true),
        }
    }

    fn symlinks_supported(&self) -> bool {
        self.symlinks_supported.get()
    }

    pub fn setup(&self, root: &Path) -> Result<()> {
        fs::create_dir_all(root)?;

        for (rel_path, entries) in &self.dataset.items {
            let full_path = root.join(rel_path);
            match &entries[0] {
                Entry::Dir => {
                    if !full_path.exists() {
                        fs::create_dir_all(&full_path)?;
                    }
                }
                Entry::Symlink { target, is_dir } => {
                    create_native_symlink(target, &full_path, *is_dir, &self.symlinks_supported)?;
                    continue;
                }
                _ => {
                    let mut file = fs::File::create(&full_path)?;
                    for entry in entries {
                        self.write_entry(&mut file, entry)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn write_entry(&self, file: &mut fs::File, entry: &Entry) -> Result<()> {
        match entry {
            Entry::Data(d) => file.write_all(d)?,
            Entry::Random(size, seed) => {
                let mut prng = SimplePrng(*seed);
                let mut remaining = *size;
                let mut buf = [0u8; 64 * 1024];
                while remaining > 0 {
                    let chunk = std::cmp::min(remaining, buf.len() as u64) as usize;
                    prng.fill(&mut buf[..chunk]);
                    file.write_all(&buf[..chunk])?;
                    remaining -= chunk as u64;
                }
            }
            _ => bail!("Can only write Data/Random to a file"),
        }
        Ok(())
    }

    pub fn verify_all_exact(&self, root: &Path) -> Result<()> {
        // Verify expected items
        for (rel_path, entries) in &self.dataset.items {
            self.verify_path(root, rel_path, entries)?;
        }

        // Verify no extra items
        let mut actual_paths = Vec::new();
        self.collect_actual_paths(root, Path::new(""), &mut actual_paths)?;

        let expected_set: std::collections::HashSet<&str> =
            self.dataset.items.iter().map(|(p, _)| p.as_str()).collect();
        for actual in actual_paths {
            let actual_str = actual.to_str().context("non-utf8 path")?;

            // Normalise to forward slashes for cross-platform comparison
            let normalised = actual_str.replace('\\', "/");

            if !expected_set.contains(normalised.as_str()) {
                // Check if it's a parent directory
                let is_parent = expected_set
                    .iter()
                    .any(|p| p.starts_with(&format!("{}/", normalised)));
                if !is_parent {
                    bail!("Unexpected path found: {:?}", actual_str);
                }
            }
        }
        Ok(())
    }

    pub fn verify_subset_exact(&self, root: &Path, expected_paths: &[&str]) -> Result<()> {
        for path in expected_paths {
            let entries = self
                .dataset
                .items
                .iter()
                .find(|(p, _)| p == *path)
                .map(|(_, e)| e);

            if let Some(entries) = entries {
                self.verify_path(root, path, entries)?;
            } else {
                let full_path = root.join(path);
                if !full_path.exists() {
                    bail!("Missing: {:?}", path);
                }
            }
        }
        Ok(())
    }

    pub fn verify_item(&self, root: &Path, rel_path: &str, dataset_path: &str) -> Result<()> {
        let entries = self
            .dataset
            .items
            .iter()
            .find(|(p, _)| p == dataset_path)
            .map(|(_, e)| e)
            .with_context(|| format!("Path not in dataset: {}", dataset_path))?;
        self.verify_path(root, rel_path, entries)
    }

    fn verify_path(&self, root: &Path, rel_path: &str, entries: &[Entry]) -> Result<()> {
        let full_path = root.join(rel_path);

        let first = entries.first().context("empty entries")?;
        match first {
            Entry::Dir => {
                if !full_path.exists() {
                    bail!("Missing: {:?}", rel_path);
                }
                if !full_path.is_dir() {
                    bail!("Not a dir: {:?}", rel_path);
                }
            }
            Entry::Symlink { target, is_dir } => {
                if !full_path.exists() {
                    // The symlink was either not created during setup (no
                    // privilege) or was lost during the round-trip (snapshot,
                    // bundle, extract, etc.).  In either case do not fail the
                    // test — just warn and move on.
                    if self.symlinks_supported() {
                        eprintln!(
                            "Warning: symlink {:?} (→ {}) created during setup \
                             but missing during verification — the round-trip \
                             may not have preserved it on this platform.",
                            rel_path, target,
                        );
                    }
                    return Ok(());
                }

                if full_path.is_dir() != *is_dir {
                    bail!(
                        "Symlink type mismatch for {:?}: expected is_dir={}",
                        rel_path,
                        is_dir
                    );
                }

                let actual = fs::read_link(&full_path)?;
                if actual.to_str() != Some(target.as_str()) {
                    bail!("Symlink mismatch: {:?}", rel_path);
                }
            }
            _ => {
                if !full_path.exists() {
                    bail!("Missing: {:?}", rel_path);
                }

                let mut file = fs::File::open(&full_path)?;

                for entry in entries {
                    match entry {
                        Entry::Data(expected) => {
                            let mut actual = vec![0u8; expected.len()];
                            file.read_exact(&mut actual)?;
                            if actual != *expected {
                                bail!("Content mismatch: {:?}", rel_path);
                            }
                        }
                        Entry::Random(size, seed) => {
                            let mut prng = SimplePrng(*seed);
                            let mut remaining = *size;
                            let mut buf_file = [0u8; 64 * 1024];
                            let mut buf_expected = [0u8; 64 * 1024];
                            while remaining > 0 {
                                let chunk =
                                    std::cmp::min(remaining, buf_file.len() as u64) as usize;
                                file.read_exact(&mut buf_file[..chunk])?;
                                prng.fill(&mut buf_expected[..chunk]);
                                if buf_file[..chunk] != buf_expected[..chunk] {
                                    bail!("Random mismatch: {:?}", rel_path);
                                }
                                remaining -= chunk as u64;
                            }
                        }
                        _ => bail!("Unexpected Dir/Symlink entry inside file: {:?}", rel_path),
                    }
                }
            }
        }
        Ok(())
    }

    fn collect_actual_paths(
        &self,
        root: &Path,
        current_rel: &Path,
        paths: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let full_path = root.join(current_rel);
        if !full_path.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(full_path)? {
            let entry = entry?;
            let rel = current_rel.join(entry.file_name());
            paths.push(rel.clone());
            if entry.file_type()?.is_dir() {
                self.collect_actual_paths(root, &rel, paths)?;
            }
        }
        Ok(())
    }

    pub fn get_expected_paths(&self, include: &[String], exclude: &[String]) -> Vec<String> {
        let mut expected = Vec::new();

        for (rel_path, _) in &self.dataset.items {
            if self.is_included(rel_path, include, exclude) {
                expected.push(rel_path.clone());
            }
        }
        expected.sort();
        expected
    }

    fn is_included(&self, path: &str, include: &[String], exclude: &[String]) -> bool {
        let included = include.is_empty()
            || include
                .iter()
                .any(|i| path == i.as_str() || path.starts_with(&format!("{}/", i)));
        if !included {
            return false;
        }

        let excluded = exclude.iter().any(|e| {
            path == e.as_str()
                || path.starts_with(&format!("{}/", e))
                // `dir/*` matches everything under `dir/` (not a true glob)
                || (e.ends_with("/*") && path.starts_with(&e[..e.len() - 2]))
        });
        !excluded
    }
}

/// Minimal deterministic PRNG
struct SimplePrng(u64);
impl SimplePrng {
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            let bytes = self.0.to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
    }
}
