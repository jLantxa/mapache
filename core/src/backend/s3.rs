use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use tokio::runtime::Runtime;

use crate::backend::{Handle, NodeAttr, StorageBackend};
use crate::ui;

pub struct S3Backend {
    runtime: ManuallyDrop<Runtime>,
    bucket: Bucket,
    prefix: PathBuf,
}

impl S3Backend {
    pub fn new(
        region_str: String,
        bucket_name: String,
        prefix: PathBuf,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to create Tokio runtime for S3 backend")?;

        let credentials = Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)?;

        let region = Region::Custom {
            region: region_str,
            endpoint,
        };

        let bucket = *Bucket::new(&bucket_name, region, credentials)?;

        let mut backend = Self {
            runtime: ManuallyDrop::new(runtime),
            bucket,
            prefix,
        };

        backend.bucket.set_path_style();

        Ok(backend)
    }

    #[inline]
    fn key_from_path(&self, path: &Path) -> String {
        let prefix = self.prefix.to_string_lossy().trim_matches('/').to_string();
        let sub_path = path
            .to_string_lossy()
            .trim_matches(|c| c == '/' || c == '\\')
            .replace('\\', "/");

        if prefix.is_empty() {
            sub_path
        } else {
            format!("{}/{}", prefix, sub_path)
        }
    }
    fn path_from_key(&self, key: &str) -> Result<PathBuf> {
        let path = PathBuf::from(key);
        path.strip_prefix(&self.prefix)
            .map(|p| p.to_path_buf())
            .map_err(|_| {
                anyhow!(
                    "S3 backend: key '{}' is not under prefix '{}'",
                    key,
                    self.prefix.display()
                )
            })
    }

    async fn retry<T, F, Fut>(&self, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        const MAX_ATTEMPTS: u32 = 4;
        const BASE_DELAY_MS: u64 = 100;

        let mut attempts = 0;
        loop {
            match op().await {
                Ok(val) => return Ok(val),
                Err(e) if attempts < MAX_ATTEMPTS => {
                    attempts += 1;
                    let wait_ms = BASE_DELAY_MS * (2_u64.pow(attempts - 1));
                    ui::cli::warning!("S3 operation failed: {}. Retrying in {}ms...", e, wait_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                }
                Err(e) => return Err(e.context("S3 operation failed after multiple retries")),
            }
        }
    }
}

impl Drop for S3Backend {
    fn drop(&mut self) {
        let rt = unsafe { ManuallyDrop::take(&mut self.runtime) };
        rt.shutdown_background();
    }
}

impl StorageBackend for S3Backend {
    fn create(&self) -> Result<()> {
        let (_head, status_code) = self
            .runtime
            .block_on(self.bucket.head_object(""))
            .context("S3 backend: connectivity check failed")?;

        if status_code == 403 {
            return Err(anyhow!("S3 backend: Access denied to bucket"));
        }
        Ok(())
    }

    fn path_exists(&self, path: &Path) -> bool {
        self.is_file(path) || self.is_dir(path)
    }

    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let key = self.key_from_path(handle.path);

        self.runtime.block_on(self.retry(|| async {
            let response = if offset == 0 && length == 0 {
                self.bucket.get_object(&key).await?
            } else {
                let start = if offset < 0 {
                    let (head, _) = self.bucket.head_object(&key).await?;
                    let size = head.content_length.unwrap_or(0) as u64;
                    size.saturating_sub(offset.unsigned_abs() as u64)
                } else {
                    offset as u64
                };
                let end = if length > 0 {
                    Some(start + length as u64 - 1)
                } else {
                    None
                };
                self.bucket.get_object_range(&key, start, end).await?
            };

            if response.status_code() >= 400 {
                return Err(anyhow!("S3 read failed: HTTP {}", response.status_code()));
            }
            Ok(response.bytes().to_vec())
        }))
    }

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let key = self.key_from_path(handle.path);

        self.runtime.block_on(self.retry(|| async {
            let response = self.bucket.put_object(&key, contents).await?;
            if response.status_code() >= 400 {
                return Err(anyhow!("S3 write failed: HTTP {}", response.status_code()));
            }
            Ok(())
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let src_key = self.key_from_path(from);
        let dest_key = self.key_from_path(to);

        let code = self
            .runtime
            .block_on(self.bucket.copy_object_internal(&src_key, &dest_key))?;
        if code >= 400 {
            return Err(anyhow!("S3 rename (copy phase) failed: HTTP {}", code));
        }

        self.runtime.block_on(self.bucket.delete_object(&src_key))?;
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut prefix = self.key_from_path(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut paths = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let (result, status_code) = self.runtime.block_on(self.bucket.list_page(
                prefix.clone(),
                Some("/".to_string()),
                continuation_token.clone(),
                Some(1000.to_string()),
                None,
            ))?;

            if status_code >= 400 {
                bail!("S3 list failed with status {}", status_code);
            }

            // Process "Directories" (Common Prefixes)
            if let Some(prefixes) = result.common_prefixes {
                for p in prefixes {
                    if let Ok(rel) = self.path_from_key(p.prefix.trim_end_matches('/')) {
                        paths.push(rel);
                    }
                }
            }

            // Process Files
            for obj in result.contents {
                if let Ok(rel) = self.path_from_key(&obj.key)
                    && !obj.key.ends_with('/')
                {
                    paths.push(rel);
                }
            }

            // Check if we need to loop again for the next page
            if result.is_truncated && result.next_continuation_token.is_some() {
                continuation_token = result.next_continuation_token;
            } else {
                break;
            }
        }

        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn create_dir(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let key = self.key_from_path(path);
        self.runtime.block_on(self.bucket.delete_object(&key)).ok();

        let mut prefix = key;
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let list = self.runtime.block_on(self.bucket.list(prefix, None))?;
        for result in list {
            for obj in result.contents {
                self.runtime.block_on(self.bucket.delete_object(&obj.key))?;
            }
        }
        Ok(())
    }

    fn is_file(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);
        let res = self.runtime.block_on(self.bucket.head_object(&key));
        matches!(res, Ok((_, 200)))
    }

    fn is_dir(&self, path: &Path) -> bool {
        let mut prefix = self.key_from_path(path);
        if prefix.is_empty() {
            return true;
        }
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let res = self
            .runtime
            .block_on(self.bucket.list(prefix, Some(1.to_string())));
        match res {
            Ok(list) => list
                .iter()
                .any(|r| !r.contents.is_empty() || r.common_prefixes.is_some()),
            _ => false,
        }
    }

    fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let key = self.key_from_path(path);
        let res = self.runtime.block_on(self.bucket.head_object(&key));

        match res {
            Ok((head, 200)) => {
                let mtime = head
                    .last_modified
                    .as_deref()
                    .and_then(|ts| httpdate::parse_http_date(ts).ok());

                Ok(NodeAttr {
                    size: Some(head.content_length.unwrap_or(0) as u64),
                    uid: None,
                    gid: None,
                    perm: None,
                    atime: None,
                    mtime,
                })
            }
            _ => {
                if self.is_dir(path) {
                    Ok(NodeAttr {
                        size: Some(0),
                        uid: None,
                        gid: None,
                        perm: Some(0o755),
                        atime: None,
                        mtime: None,
                    })
                } else {
                    Err(anyhow!("S3 lstat: path not found: {}", path.display()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn setup_test_backend() -> S3Backend {
        S3Backend::new(
            "us-east-1".to_string(),
            "test-bucket".to_string(),
            PathBuf::from("backup_root"),
            "http://localhost:9000".to_string(),
            "mapacheaccesskey".to_string(),
            "mapachesecretkey".to_string(),
        )
        .expect("Failed to create backend")
    }

    #[test]
    fn test_key_mapping() {
        let backend = setup_test_backend();

        // Test basic path
        let path = Path::new("file.txt");
        assert_eq!(backend.key_from_path(path), "backup_root/file.txt");

        // Test nested path
        let nested = Path::new("dir/subdir/data.bin");
        assert_eq!(
            backend.key_from_path(nested),
            "backup_root/dir/subdir/data.bin"
        );

        // Test absolute-looking path (should still be relative to prefix)
        let absolute = Path::new("/root_file.txt");
        assert_eq!(backend.key_from_path(absolute), "backup_root/root_file.txt");
    }

    #[test]
    fn test_path_reconstruction() {
        let backend = setup_test_backend();
        let key = "backup_root/some/key/here.txt";
        let path = backend.path_from_key(key).unwrap();

        assert_eq!(path, PathBuf::from("some/key/here.txt"));
    }
}
