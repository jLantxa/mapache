#![cfg(test)]

mod tests {
    use std::{collections::HashMap, fs, path::Path, process::Output};

    use anyhow::{Context, Result};
    use flate2::read::GzDecoder;
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::integration_tests::run_bin;

    const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/golden_repo");
    const USER: &str = "golden";
    const PASSWORD: &str = "golden-password";

    fn hash_file(path: &Path) -> Result<(u64, String)> {
        let data = fs::read(path)?;
        let size = data.len() as u64;
        Ok((size, blake3::hash(&data).to_hex().to_string()))
    }

    fn collect_hashes(
        dir: &Path,
        root: &Path,
        out: &mut HashMap<String, (u64, String)>,
    ) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            let rel = path
                .strip_prefix(root)
                .context("path under root")?
                .to_string_lossy()
                .replace('\\', "/");
            if path.is_dir() {
                collect_hashes(&path, root, out)?;
            } else if path.is_file() {
                out.insert(rel, hash_file(&path)?);
            }
        }
        Ok(())
    }

    fn hash_tree(root: &Path) -> Result<HashMap<String, (u64, String)>> {
        let mut out = HashMap::new();
        collect_hashes(root, root, &mut out)?;
        Ok(out)
    }

    fn extract_tar_gz(archive: &Path, dest: &Path) -> Result<()> {
        let file = fs::File::open(archive).context("open fixture archive")?;
        let mut ar = tar::Archive::new(GzDecoder::new(file));
        ar.unpack(dest).context("extract fixture archive")?;
        Ok(())
    }

    fn run_mapache(repo: &Path, auth: &Path, args: &[&str]) -> Result<Output> {
        let mut full: Vec<String> = Vec::with_capacity(args.len() + 4);
        full.push(args[0].to_string());
        full.push("--repo".into());
        full.push(repo.to_string_lossy().into_owned());
        full.push("--auth-file".into());
        full.push(auth.to_string_lossy().into_owned());
        full.extend(args[1..].iter().map(|s| s.to_string()));
        let refs: Vec<&str> = full.iter().map(|s| s.as_str()).collect();
        run_bin(&refs)
    }

    fn run_ok(repo: &Path, auth: &Path, args: &[&str]) -> Result<String> {
        let out = run_mapache(repo, auth, args)?;
        assert!(
            out.status.success(),
            "mapache {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    fn assert_matches(
        actual: &HashMap<String, (u64, String)>,
        expected: &Value,
        label: &str,
    ) -> Result<()> {
        let obj = expected
            .as_object()
            .context("expected manifest must be an object")?;
        let mut diffs = Vec::new();
        for (key, val) in obj {
            let arr = val.as_array().context("expected entry must be an array")?;
            let size = arr[0].as_u64().context("expected entry size")?;
            let sha = arr[1].as_str().context("expected entry sha256")?;
            match actual.get(key) {
                None => diffs.push(format!("{label}: missing in restore: {key}")),
                Some((sz, h)) if *sz != size || h != sha => {
                    diffs.push(format!("{label}: content differs: {key}"));
                }
                _ => {}
            }
        }
        for key in actual.keys() {
            if !obj.contains_key(key) {
                diffs.push(format!("{label}: unexpected in restore: {key}"));
            }
        }
        assert!(diffs.is_empty(), "{}", diffs.join("\n"));
        Ok(())
    }

    fn roundtrip(format: &str) -> Result<()> {
        let tmp = tempdir().context("tempdir")?;
        let fixtures = Path::new(FIXTURES_DIR);

        let repo = tmp.path().join("repo");
        extract_tar_gz(&fixtures.join(format!("golden_{format}.tar.gz")), &repo)?;

        let provenance: Value = serde_json::from_str(&fs::read_to_string(
            fixtures.join(format!("provenance_{format}.json")),
        )?)?;
        let expected: Value = serde_json::from_str(&fs::read_to_string(
            fixtures.join(format!("expected_{format}.json")),
        )?)?;

        let auth = tmp.path().join("auth");
        fs::write(&auth, format!("{USER}\n{PASSWORD}\n"))?;

        run_ok(&repo, &auth, &["verify"])?;

        let snapshots = provenance["snapshots"]
            .as_object()
            .context("snapshots object")?;
        for label in ["a", "b", "c"] {
            let id = snapshots[label].as_str().context("snapshot id")?;
            let target = tmp.path().join(format!("restore_{label}"));
            let target_str = target.to_string_lossy().into_owned();
            run_ok(
                &repo,
                &auth,
                &["restore", "--target", target_str.as_str(), id],
            )?;
            let actual = hash_tree(&target)?;
            assert_matches(&actual, &expected[label], label)?;
        }

        // Write a brand-new snapshot into the frozen repo and read it back:
        // the frozen format must not only be readable, it must also accept new
        // data written by the current code.
        let data = tmp.path().join("golden-write");
        fs::create_dir(&data)?;
        fs::write(data.join("new.txt"), b"golden fixture writeback\n")?;
        fs::write(data.join("sub.txt"), b"second file\n")?;
        run_ok(&repo, &auth, &["snapshot", data.to_string_lossy().as_ref()])?;

        let target_new = tmp.path().join("restore_new");
        let target_new_str = target_new.to_string_lossy().into_owned();
        run_ok(
            &repo,
            &auth,
            &["restore", "--target", target_new_str.as_str(), "latest"],
        )?;

        let prefix = data
            .file_name()
            .context("data dir name")?
            .to_str()
            .context("data dir name utf8")?
            .to_string();
        let prefixed: HashMap<String, (u64, String)> = hash_tree(&data)?
            .into_iter()
            .map(|(key, val)| (format!("{prefix}/{key}"), val))
            .collect();
        assert_eq!(
            hash_tree(&target_new)?,
            prefixed,
            "write-back snapshot mismatch"
        );
        Ok(())
    }

    #[test]
    fn test_golden_v1_compat() -> Result<()> {
        roundtrip("v1")
    }

    #[test]
    fn test_golden_v2_compat() -> Result<()> {
        roundtrip("v2")
    }
}
