use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client,
    config::{Credentials, Region},
};
use tokio::runtime::Runtime;

use crate::backend::{Handle, NodeAttr, StorageBackend};

const AMAZON_AWS_ENDPOINT: &str = "amazonaws.com";

pub struct S3Backend {
    runtime: Runtime,
    client: Client,
    bucket: String,
    prefix: PathBuf,
}

impl S3Backend {
    pub fn new(
        region: String,
        bucket: String,
        prefix: PathBuf,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> Result<Self> {
        let runtime = Runtime::new().context("Failed to create Tokio runtime")?;

        let credentials = Credentials::new(access_key, secret_key, None, None, "static");

        let force_path_style = !endpoint.ends_with(AMAZON_AWS_ENDPOINT);

        let config_loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .credentials_provider(credentials)
            .endpoint_url(&endpoint);
        let config = runtime.block_on(config_loader.load());

        let client_builder =
            aws_sdk_s3::config::Builder::from(&config).force_path_style(force_path_style);

        let client = Client::from_conf(client_builder.build());

        Ok(Self {
            runtime,
            client,
            bucket,
            prefix,
        })
    }

    #[inline]
    fn key_from_path(&self, path: &Path) -> String {
        make_key(&self.prefix, path)
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
}

impl StorageBackend for S3Backend {
    fn create(&self) -> Result<()> {
        // Verify bucket exists and is accessible
        self.runtime
            .block_on(self.client.head_bucket().bucket(&self.bucket).send())
            .context(format!(
                "S3 backend: bucket '{}' does not exist or is not accessible",
                self.bucket
            ))?;
        Ok(())
    }

    fn path_exists(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);

        // Check if it's a file
        let file_exists = self
            .runtime
            .block_on(
                self.client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send(),
            )
            .is_ok();

        if file_exists {
            return true;
        }

        // Check if it's a directory (prefix)
        let dir_key = if key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };
        let list_res = self.runtime.block_on(
            self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&dir_key)
                .max_keys(1)
                .send(),
        );

        match list_res {
            Ok(output) => output.key_count.unwrap_or(0) > 0,
            Err(_) => false,
        }
    }

    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let key = self.key_from_path(handle.path);
        let mut req = self.client.get_object().bucket(&self.bucket).key(&key);

        if offset != 0 || length != 0 {
            let range = if offset < 0 {
                // S3 doesn't easily support "last N bytes" without knowing size.
                // We'd need to HEAD first.
                let head = self
                    .runtime
                    .block_on(
                        self.client
                            .head_object()
                            .bucket(&self.bucket)
                            .key(&key)
                            .send(),
                    )
                    .context("S3 backend: failed to head object for relative seek")?;
                let size = head.content_length.unwrap_or(0) as u64;
                let start = size.saturating_sub(offset.unsigned_abs() as u64);

                let end = if length > 0 {
                    format!("{}", start + length as u64 - 1)
                } else {
                    "".to_string()
                };
                format!("bytes={}-{}", start, end)
            } else {
                let end = if length > 0 {
                    format!("{}", offset as usize + length - 1)
                } else {
                    "".to_string()
                };
                format!("bytes={}-{}", offset, end)
            };
            req = req.range(range);
        }

        let output = self
            .runtime
            .block_on(req.send())
            .with_context(|| format!("S3 backend: failed to read object '{}'", key))?;

        let data = self
            .runtime
            .block_on(output.body.collect())
            .context("S3 backend: failed to collect body data")?
            .into_bytes();

        Ok(data.to_vec())
    }

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let key = self.key_from_path(handle.path);
        let body = aws_sdk_s3::primitives::ByteStream::from(contents.to_vec());

        self.runtime
            .block_on(
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(body)
                    .send(),
            )
            .context("S3 backend: failed to write object")?;

        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let src_key = self.key_from_path(from);
        let dest_key = self.key_from_path(to);

        // Copy
        self.runtime
            .block_on(
                self.client
                    .copy_object()
                    .bucket(&self.bucket)
                    .copy_source(format!("{}/{}", self.bucket, src_key))
                    .key(&dest_key)
                    .send(),
            )
            .with_context(|| {
                format!("S3 rename: failed to copy '{}' to '{}'", src_key, dest_key)
            })?;

        // Delete original
        self.runtime
            .block_on(
                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(&src_key)
                    .send(),
            )
            .with_context(|| {
                format!(
                    "S3 rename: failed to delete source '{}' after copy",
                    src_key
                )
            })?;

        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let prefix = self.key_from_path(path);
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", prefix)
        };

        let mut paths = Vec::new();
        let mut continuation_token = None;

        loop {
            let resp = self
                .runtime
                .block_on(
                    self.client
                        .list_objects_v2()
                        .bucket(&self.bucket)
                        .prefix(&prefix)
                        .delimiter("/") // Use delimiter to emulate directories
                        .set_continuation_token(continuation_token)
                        .send(),
                )
                .context("S3 backend: failed to list objects")?;

            // Common Prefixes (directories)
            if let Some(common_prefixes) = resp.common_prefixes {
                for cp in common_prefixes {
                    if let Some(p) = cp.prefix {
                        // Remove trailing slash
                        let p = p.trim_end_matches('/');
                        if let Ok(rel) = self.path_from_key(p) {
                            paths.push(rel);
                        }
                    }
                }
            }

            // Objects (files)
            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(k) = obj.key {
                        // Skip the directory itself if listed
                        if k == prefix {
                            continue;
                        }
                        if let Ok(rel) = self.path_from_key(&k) {
                            paths.push(rel);
                        }
                    }
                }
            }

            if resp.is_truncated == Some(true) {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(paths)
    }

    fn create_dir(&self, _path: &Path) -> Result<()> {
        // S3 is a flat structure, directories don't physically exist.
        // We don't need to do anything here.
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let key = self.key_from_path(path);

        if !key.is_empty() {
            self.runtime
                .block_on(
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(&key)
                        .send(),
                )
                .ok();
        }

        // Also try to delete as a directory (all objects with this prefix)
        let dir_prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{}/", key)
        };
        let mut continuation_token = None;

        loop {
            let list_resp = self.runtime.block_on(
                self.client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&dir_prefix)
                    .set_continuation_token(continuation_token)
                    .send(),
            )?;

            if let Some(objects) = list_resp.contents
                && !objects.is_empty()
            {
                let mut delete_ids = Vec::new();
                for obj in objects {
                    if let Some(k) = obj.key {
                        delete_ids.push(
                            aws_sdk_s3::types::ObjectIdentifier::builder()
                                .key(k)
                                .build()?,
                        );
                    }
                }

                if !delete_ids.is_empty() {
                    let delete = aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(delete_ids))
                        .build()?;

                    self.runtime.block_on(
                        self.client
                            .delete_objects()
                            .bucket(&self.bucket)
                            .delete(delete)
                            .send(),
                    )?;
                }
            }

            if list_resp.is_truncated == Some(true) {
                continuation_token = list_resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    fn is_file(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);
        if key.is_empty() {
            return false;
        }
        self.runtime
            .block_on(
                self.client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send(),
            )
            .is_ok()
    }

    fn is_dir(&self, path: &Path) -> bool {
        let key = self.key_from_path(path);

        // The root of our prefix is always considered a directory.
        if key.is_empty() {
            return true;
        }

        // Ensure the prefix ends with a slash to avoid "partial string" matches.
        // Without this, a search for "snapshots" might match "snapshots_index.json".
        let prefix = if key.ends_with('/') {
            key
        } else {
            format!("{}/", key)
        };

        let res = self.runtime.block_on(
            self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .max_keys(1) // We only need to know if at least one object exists
                .send(),
        );

        match res {
            Ok(output) => output.key_count.unwrap_or(0) > 0,
            Err(_) => false,
        }
    }

    fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let key = self.key_from_path(path);

        match self.runtime.block_on(
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send(),
        ) {
            Ok(head) => {
                Ok(NodeAttr {
                    size: Some(head.content_length.unwrap_or(0) as u64),
                    uid: None,
                    gid: None,
                    perm: None, // S3 doesn't have permissions in the same way
                    atime: None,
                    mtime: head.last_modified.map(|d| {
                        // Convert aws_smithy_types::DateTime to SystemTime
                        let secs = d.secs();
                        let nanos = d.subsec_nanos();
                        std::time::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos)
                    }),
                })
            }
            Err(_) => {
                // Could be a directory?
                if self.is_dir(path) {
                    Ok(NodeAttr {
                        size: Some(0),
                        uid: None,
                        gid: None,
                        perm: Some(0o755), // Fake directory permissions
                        atime: None,
                        mtime: None,
                    })
                } else {
                    Err(anyhow!("S3 backend: path not found: {}", path.display()))
                }
            }
        }
    }
}

fn make_key(prefix: &Path, path: &Path) -> String {
    let mut parts = Vec::new();

    for component in prefix.components().chain(path.components()) {
        if let std::path::Component::Normal(c) = component {
            parts.push(c.to_string_lossy())
        }
        // Skip root, current dir, etc.
    }

    parts.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_key() {
        let prefix = Path::new("backups");
        let path = Path::new("data/file.txt");
        assert_eq!(make_key(prefix, path), "backups/data/file.txt");

        let prefix = Path::new("");
        let path = Path::new("file.txt");
        assert_eq!(make_key(prefix, path), "file.txt");

        let prefix = Path::new("/");
        let path = Path::new("file.txt");
        assert_eq!(make_key(prefix, path), "file.txt");
    }
}
