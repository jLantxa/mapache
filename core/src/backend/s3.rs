use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use s3::creds::Credentials;
use s3::{Bucket, Region};

use crate::backend::{Handle, NodeAttr, RetryOptions, StorageBackend, WriteContents, retry};

/// A storage backend that interacts with S3-compatible APIs.
pub struct S3Backend {
    bucket: Box<Bucket>,
    prefix: PathBuf,
    retry_opts: RetryOptions,
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
        let credentials = Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)?;

        let region = Region::Custom {
            region: region_str,
            endpoint,
        };

        let mut bucket = Bucket::new(&bucket_name, region, credentials)
            .context("Failed to create S3 bucket client")?;

        bucket.set_path_style();

        let retry_opts = RetryOptions {
            max_attempts: 4,
            base_delay: Duration::from_millis(100),
            request_timeout: Duration::from_secs(30),
        };

        Ok(Self {
            bucket,
            prefix,
            retry_opts,
        })
    }

    #[inline]
    fn normalize_key(s: &str) -> String {
        s.trim_matches('/').replace('\\', "/")
    }

    /// Converts a repository-relative path into an S3 object key (prefix + path).
    fn key_from_path(&self, path: &Path) -> String {
        let prefix = Self::normalize_key(&self.prefix.to_string_lossy());
        let sub = Self::normalize_key(&path.to_string_lossy());

        if prefix.is_empty() {
            sub
        } else if sub.is_empty() {
            prefix
        } else {
            format!("{}/{}", prefix, sub)
        }
    }

    /// Converts an S3 key back into a repository-relative path by stripping the configured prefix.
    ///
    /// Done in string space for cross-platform correctness (S3 keys are always '/').
    fn path_from_key(&self, key: &str) -> Result<PathBuf> {
        let prefix = Self::normalize_key(&self.prefix.to_string_lossy());
        let key_norm = key.replace('\\', "/").trim_matches('/').to_string();

        let rel = if prefix.is_empty() {
            key_norm
        } else if key_norm == prefix {
            // key points exactly to the prefix "directory marker"
            String::new()
        } else {
            let prefix_slash = format!("{}/", prefix);
            key_norm
                .strip_prefix(&prefix_slash)
                .ok_or_else(|| {
                    anyhow!("S3 backend: key '{}' is not under prefix '{}'", key, prefix)
                })?
                .to_string()
        };

        Ok(PathBuf::from(rel))
    }

    /// Wraps an async operation with exponential backoff retries and timeouts.
    async fn retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        retry("S3", &self.retry_opts, op).await
    }

    /// Lightweight "connectivity + permission" check using a tiny listing.
    async fn connectivity_check(&self) -> Result<()> {
        // List at most 1 entry under the configured prefix (or bucket root).
        let mut prefix = Self::normalize_key(&self.key_from_path(Path::new("")));
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let (_result, status) = self
            .retry(|| async {
                let (res, code) = self
                    .bucket
                    .list_page(
                        prefix.clone(),
                        Some("/".to_string()),
                        None,
                        Some("1".to_string()),
                        None,
                    )
                    .await?;
                Ok((res, code))
            })
            .await
            .context("S3 backend: connectivity check failed")?;

        if status == 403 {
            bail!("S3 backend: Access denied to bucket");
        }
        if status >= 400 {
            bail!("S3 backend: connectivity check failed (HTTP {})", status);
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn create(&self) -> Result<()> {
        self.connectivity_check().await
    }

    async fn path_exists(&self, path: &Path) -> bool {
        self.is_file(path).await || self.is_dir(path).await
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let key = self.key_from_path(handle.path);

        self.retry(|| async {
            let response = if offset == 0 && length == 0 {
                self.bucket.get_object(&key).await?
            } else {
                let start = if offset < 0 {
                    let (head, code) = self.bucket.head_object(&key).await?;
                    if code >= 400 {
                        bail!("S3 head failed for range read: HTTP {}", code);
                    }
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
                bail!("S3 read failed: HTTP {}", response.status_code());
            }
            Ok(response.into_bytes().to_vec())
        })
        .await
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let key = self.key_from_path(handle.path);

        self.retry(|| async {
            let response = self.bucket.put_object(&key, &contents).await?;
            if response.status_code() >= 400 {
                bail!("S3 write failed: HTTP {}", response.status_code());
            }
            Ok(())
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let src_key = self.key_from_path(from);
        let dest_key = self.key_from_path(to);

        // S3 rename is COPY + DELETE (not atomic).
        self.retry(|| async {
            let code = self
                .bucket
                .copy_object_internal(&src_key, &dest_key)
                .await?;
            if code >= 400 {
                bail!("S3 rename (copy phase) failed: HTTP {}", code);
            }
            Ok(())
        })
        .await?;

        self.retry(|| async {
            let resp = self.bucket.delete_object(&src_key).await?;
            if resp.status_code() >= 400 {
                bail!(
                    "S3 rename (delete phase) failed: HTTP {}",
                    resp.status_code()
                );
            }
            Ok(())
        })
        .await?;

        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut prefix = self.key_from_path(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut paths = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let (result, status_code) = self
                .retry(|| async {
                    let (res, code) = self
                        .bucket
                        .list_page(
                            prefix.clone(),
                            Some("/".to_string()),
                            continuation_token.clone(),
                            Some("1000".to_string()),
                            None,
                        )
                        .await?;
                    Ok((res, code))
                })
                .await?;

            if status_code >= 400 {
                bail!("S3 list failed with status {}", status_code);
            }

            // "Directories" (Common Prefixes)
            if let Some(prefixes) = result.common_prefixes {
                for p in prefixes {
                    let dir_key = p.prefix.trim_end_matches('/');
                    if let Ok(rel) = self.path_from_key(dir_key) {
                        paths.push(rel);
                    }
                }
            }

            // Files
            for obj in result.contents {
                if obj.key.ends_with('/') {
                    continue;
                }
                if let Ok(rel) = self.path_from_key(&obj.key) {
                    paths.push(rel);
                }
            }

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

    async fn create_dir(&self, _path: &Path) -> Result<()> {
        // S3 is flat; directory creation is a no-op.
        Ok(())
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        let key = self.key_from_path(path);

        // Try deleting the exact key (file case). Ignore errors.
        let _ = self
            .retry(|| async { Ok(self.bucket.delete_object(&key).await) })
            .await;

        // Then delete anything under it as a prefix (dir case).
        let mut prefix = key;
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut continuation_token: Option<String> = None;

        loop {
            let (result, status) = self
                .retry(|| async {
                    let (res, code) = self
                        .bucket
                        .list_page(
                            prefix.clone(),
                            None,
                            continuation_token.clone(),
                            Some("1000".to_string()),
                            None,
                        )
                        .await?;
                    Ok((res, code))
                })
                .await?;

            if status >= 400 {
                break;
            }

            for obj in result.contents {
                let obj_key = obj.key.clone();
                self.retry(|| async {
                    let resp = self.bucket.delete_object(&obj_key).await?;
                    if resp.status_code() >= 400 {
                        bail!(
                            "S3 delete failed for '{}': HTTP {}",
                            obj_key,
                            resp.status_code()
                        );
                    }
                    Ok(())
                })
                .await?;
            }

            if result.is_truncated && result.next_continuation_token.is_some() {
                continuation_token = result.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn is_file(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);
        let res = self
            .retry(|| async { Ok(self.bucket.head_object(&key).await) })
            .await;

        matches!(res, Ok(Ok((_, 200))))
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let mut prefix = self.key_from_path(path);

        // Repo root always exists as a "directory"
        if prefix.is_empty() {
            return true;
        }
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        // List a single entry under the prefix; if anything exists, treat as dir.
        let res = self
            .retry(|| async {
                let (list, code) = self
                    .bucket
                    .list_page(
                        prefix.clone(),
                        Some("/".to_string()),
                        None,
                        Some("1".to_string()),
                        None,
                    )
                    .await?;
                Ok((list, code))
            })
            .await;

        match res {
            Ok((list, code)) if code < 400 => {
                !list.contents.is_empty()
                    || list.common_prefixes.as_ref().is_some_and(|p| !p.is_empty())
            }
            _ => false,
        }
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let key = self.key_from_path(path);

        let res = self
            .retry(|| async { Ok(self.bucket.head_object(&key).await) })
            .await?;

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
                if self.is_dir(path).await {
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
