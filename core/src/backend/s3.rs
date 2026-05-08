use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use s3::{Bucket, Region, creds::Credentials};
use zeroize::Zeroizing;

use crate::{
    backend::{BackendNode, Handle, NodeAttr, RetryOptions, StorageBackend, WriteContents, retry},
    mapache::defaults::{S3_MULTIPART_PART_SIZE, S3_MULTIPART_THRESHOLD},
};

/// A storage backend that interacts with S3-compatible APIs.
pub struct S3Backend {
    bucket: Box<Bucket>,
    prefix: PathBuf,
    retry_opts: RetryOptions,
}

impl S3Backend {
    pub fn new(
        region: String,
        bucket_name: String,
        prefix: PathBuf,
        endpoint: String,
        access_key: Zeroizing<String>,
        secret_key: Zeroizing<String>,
    ) -> Result<Self> {
        let region = if endpoint == "amazonaws.com" {
            region.parse::<Region>().map_err(|e| anyhow::anyhow!(e))?
        } else {
            Region::Custom { region, endpoint }
        };

        let credentials =
            Credentials::new(Some(&*access_key), Some(&*secret_key), None, None, None)
                .map_err(|e| anyhow::anyhow!(e))?;
        let bucket = Bucket::new(&bucket_name, region, credentials)
            .map_err(|e| anyhow::anyhow!(e))?
            .with_path_style();

        Ok(Self {
            bucket,
            prefix,
            retry_opts: RetryOptions::default(),
        })
    }

    fn key_from_path(&self, path: &Path) -> String {
        let full_path = self.prefix.join(path);
        let key = full_path.to_string_lossy().replace('\\', "/");
        key.trim_start_matches('/').to_string()
    }

    fn path_from_key(&self, key: &str) -> Result<PathBuf> {
        let key_path = PathBuf::from(key);
        key_path
            .strip_prefix(&self.prefix)
            .map(PathBuf::from)
            .context("S3 key does not match backend prefix")
    }

    async fn connectivity_check(&self) -> Result<()> {
        let prefix = self.prefix.to_string_lossy().replace('\\', "/");
        let prefix = prefix.trim_start_matches('/').to_string();

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

    /// Internal retry helper.
    async fn retry<T, F, Fut>(&self, f: F) -> Result<T>
    where
        F: FnMut() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        retry("S3", &self.retry_opts, f).await
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
        let content_len = contents.len();

        if (content_len as u64) < S3_MULTIPART_THRESHOLD {
            self.retry(|| async {
                let response = self.bucket.put_object(&key, &contents).await?;
                if response.status_code() >= 400 {
                    bail!("S3 write failed: HTTP {}", response.status_code());
                }
                Ok(())
            })
            .await
        } else {
            // Multipart Upload
            let content_type = "application/octet-stream";
            let upload_id = self
                .retry(|| async {
                    let init_res = self
                        .bucket
                        .initiate_multipart_upload(&key, content_type)
                        .await?;
                    Ok(init_res.upload_id)
                })
                .await?;

            let mut completed_parts = Vec::new();
            let mut part_number: u32 = 1;
            let mut current_offset: usize = 0;

            while current_offset < content_len {
                let end = (current_offset + S3_MULTIPART_PART_SIZE as usize).min(content_len);
                let part_data = &contents[current_offset..end];

                let etag_res = self
                    .retry(|| async {
                        let part = self
                            .bucket
                            .put_multipart_chunk(
                                part_data.to_vec(),
                                &key,
                                part_number,
                                &upload_id,
                                content_type,
                            )
                            .await?;
                        Ok(part)
                    })
                    .await;

                match etag_res {
                    Ok(part) => {
                        completed_parts.push(part);
                        current_offset = end;
                        part_number += 1;
                    }
                    Err(e) => {
                        // Abort upload on failure
                        let _ = self.bucket.abort_upload(&key, &upload_id).await;
                        return Err(e).context(format!("Failed to upload part {}", part_number));
                    }
                }
            }

            self.retry(|| async {
                let complete_res = self
                    .bucket
                    .complete_multipart_upload(&key, &upload_id, completed_parts.clone())
                    .await?;
                if complete_res.status_code() >= 400 {
                    bail!(
                        "S3 multipart complete failed: HTTP {}",
                        complete_res.status_code()
                    );
                }
                Ok(())
            })
            .await
            .inspect_err(|_e| {
                // Try to abort on completion failure too
                tokio::spawn({
                    let bucket = self.bucket.clone();
                    let key = key.clone();
                    let upload_id = upload_id.clone();
                    async move { bucket.abort_upload(&key, &upload_id).await }
                });
            })
        }
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
        .await
    }

    async fn create_dir(&self, _path: &Path) -> Result<()> {
        // S3 is flat; directory creation is a no-op.
        Ok(())
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        let key = self.key_from_path(path);

        self.retry(|| async {
            let resp = self.bucket.delete_object(&key).await?;
            if resp.status_code() >= 400 && resp.status_code() != 404 {
                bail!("S3 remove failed: HTTP {}", resp.status_code());
            }
            Ok(())
        })
        .await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>> {
        let mut prefix = self.key_from_path(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let mut nodes = Vec::new();
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
                        nodes.push(BackendNode::Dir(rel));
                    }
                }
            }

            // Files
            for obj in result.contents {
                if obj.key.ends_with('/') {
                    continue;
                }
                if let Ok(rel) = self.path_from_key(&obj.key) {
                    nodes.push(BackendNode::File(rel, obj.size));
                }
            }

            if result.is_truncated && result.next_continuation_token.is_some() {
                continuation_token = result.next_continuation_token;
            } else {
                break;
            }
        }

        nodes.sort();
        nodes.dedup();
        Ok(nodes)
    }

    async fn is_file(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);
        self.retry(|| async {
            let (_head, code) = self.bucket.head_object(&key).await?;
            Ok(code == 200)
        })
        .await
        .unwrap_or(false)
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let mut prefix = self.key_from_path(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        self.retry(|| async {
            let (result, code) = self
                .bucket
                .list_page(
                    prefix.clone(),
                    Some("/".to_string()),
                    None,
                    Some("1".to_string()),
                    None,
                )
                .await?;
            Ok(code == 200 && (!result.contents.is_empty() || result.common_prefixes.is_some()))
        })
        .await
        .unwrap_or(false)
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let key = self.key_from_path(path);
        self.retry(|| async {
            let (head, code) = self.bucket.head_object(&key).await?;
            if code >= 400 {
                bail!("S3 lstat failed: HTTP {}", code);
            }

            Ok(NodeAttr {
                size: head.content_length.map(|s| s as u64),
                atime: None,
                mtime: head.last_modified.map(|s| {
                    use chrono::DateTime;
                    DateTime::parse_from_rfc2822(&s)
                        .map(|dt| dt.into())
                        .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                }),
                uid: None,
                gid: None,
                perm: None,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BackendUrl;

    #[test]
    fn test_s3_backend_url() -> Result<()> {
        assert_eq!(
            BackendUrl::from("s3://bucket/prefix")?,
            BackendUrl::S3("bucket".to_string(), PathBuf::from("prefix"))
        );

        assert_eq!(
            BackendUrl::from("s3://bucket")?,
            BackendUrl::S3("bucket".to_string(), PathBuf::from(""))
        );

        assert_eq!(
            BackendUrl::from("s3://bucket/with%20spaces")?,
            BackendUrl::S3("bucket".to_string(), PathBuf::from("with spaces"))
        );

        Ok(())
    }
}
