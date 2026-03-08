use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf},
    time::{Duration, Instant, Sleep, sleep},
};

use crate::{
    backend::{Handle, NodeAttr, StorageBackend, WriteContents},
    utils::size,
};

/// Standard chunk size used for rate limiting IO operations.
/// Breaking large transfers into smaller chunks allows for smoother
/// traffic shaping and prevents pipeline stalls.
pub const THROTTLED_CHUNK_SIZE: usize = (64 * size::KiB) as usize;

/// A simple rate limiter using a virtual time based algorithm.
pub struct RateLimiter {
    limit: u64,
    next_free: Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(limit: u64) -> Self {
        let now = Instant::now();
        Self {
            limit,
            // Start with a full burst allowance (1 second).
            next_free: Mutex::new(now.checked_sub(Duration::from_secs(1)).unwrap_or(now)),
        }
    }

    /// Asynchronously wait until the requested amount of bytes can be consumed.
    pub async fn wait(&self, amount: u64) {
        if self.limit == 0 {
            return;
        }

        let wait_time = self.consume(amount);
        if !wait_time.is_zero() {
            sleep(wait_time).await;
        }
    }

    /// Synchronously attempt to consume tokens.
    /// Returns the Duration to wait before the amount can be fully consumed.
    /// If it returns ZERO, the tokens were consumed successfully.
    pub fn consume(&self, amount: u64) -> Duration {
        if self.limit == 0 {
            return Duration::ZERO;
        }

        let mut next_free = self.next_free.lock();
        let now = Instant::now();

        // We allow a burst of up to 1 second.
        let burst_capacity = Duration::from_secs(1);
        let earliest_next_free = now.checked_sub(burst_capacity).unwrap_or(now);

        if *next_free < earliest_next_free {
            *next_free = earliest_next_free;
        }

        // Update next_free for the next request.
        let duration = Duration::from_secs_f64(amount as f64 / self.limit as f64);
        *next_free += duration;

        next_free.saturating_duration_since(now)
    }
}

/// A wrapper around an AsyncRead that applies rate limiting.
pub struct ThrottledReader<R> {
    inner: R,
    limiter: Arc<RateLimiter>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<R: AsyncRead + Unpin> ThrottledReader<R> {
    pub fn new(inner: R, limiter: Arc<RateLimiter>) -> Self {
        Self {
            inner,
            limiter,
            sleep: None,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ThrottledReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Check if we are currently sleeping from a previous poll
        if let Some(ref mut sleep_fut) = self.sleep {
            match Pin::new(sleep_fut).poll(cx) {
                Poll::Ready(_) => self.sleep = None,
                Poll::Pending => return Poll::Pending,
            }
        }

        // Read from inner
        let prev_len = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = (buf.filled().len() - prev_len) as u64;
                if n > 0 {
                    // Calculate wait time
                    let wait_time = self.limiter.consume(n);
                    if !wait_time.is_zero() {
                        // We set the sleep future. The current data IS returned to the caller,
                        // but the NEXT call to poll_read will return Pending until wait_time expires.
                        // This achieves smooth "interleaved" throttling.
                        self.sleep = Some(Box::pin(tokio::time::sleep(wait_time)));
                    }
                }
                Poll::Ready(Ok(()))
            }
            res => res,
        }
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for ThrottledReader<R> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.inner).poll_complete(cx)
    }
}

/// A wrapper backend that applies rate limiting to read and write operations.
/// This uses a "wait then burst" approach for writes because the StorageBackend
/// trait passes the full buffer at once.
pub struct ThrottledBackend {
    inner: Arc<dyn StorageBackend>,
    upload_limiter: Option<Arc<RateLimiter>>,
    download_limiter: Option<Arc<RateLimiter>>,
}

impl ThrottledBackend {
    pub fn new(
        inner: Arc<dyn StorageBackend>,
        upload_limit: Option<u64>,
        download_limit: Option<u64>,
    ) -> Self {
        Self {
            inner,
            upload_limiter: upload_limit.map(|l| Arc::new(RateLimiter::new(l))),
            download_limiter: download_limit.map(|l| Arc::new(RateLimiter::new(l))),
        }
    }
}

#[async_trait]
impl StorageBackend for ThrottledBackend {
    async fn create(&self) -> Result<()> {
        self.inner.create().await
    }

    async fn path_exists(&self, path: &Path) -> bool {
        self.inner.path_exists(path).await
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let contents = self.inner.read(handle, offset, length).await?;
        if let Some(limiter) = &self.download_limiter {
            for chunk in contents.chunks(THROTTLED_CHUNK_SIZE) {
                limiter.wait(chunk.len() as u64).await;
            }
        }
        Ok(contents)
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        if let Some(limiter) = &self.upload_limiter {
            for chunk in contents.chunks(THROTTLED_CHUNK_SIZE) {
                limiter.wait(chunk.len() as u64).await;
            }
        }
        self.inner.write(handle, contents).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.inner.list_dir(path).await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        self.inner.create_dir(path).await
    }

    async fn remove(&self, file_path: &Path) -> Result<()> {
        self.inner.remove(file_path).await
    }

    async fn is_file(&self, path: &Path) -> bool {
        self.inner.is_file(path).await
    }

    async fn is_dir(&self, path: &Path) -> bool {
        self.inner.is_dir(path).await
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        self.inner.lstat(path).await
    }

    fn is_dry_run(&self) -> bool {
        self.inner.is_dry_run()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limit = 1000; // 1000 bytes/sec
        let limiter = RateLimiter::new(limit);

        // First 1000 bytes should be instant due to initial burst allowance
        let start = Instant::now();
        limiter.wait(1000).await;
        assert!(start.elapsed().as_millis() < 50);

        // Next 500 bytes should wait ~0.5s
        let start = Instant::now();
        limiter.wait(500).await;
        let elapsed = start.elapsed();
        assert!(elapsed.as_secs_f64() >= 0.4);
    }

    #[tokio::test]
    async fn test_throttled_reader() {
        let limit = 1000; // 1000 bytes/sec
        let limiter = Arc::new(RateLimiter::new(limit));

        // Initial burst consumed
        limiter.wait(1000).await;

        let data = vec![0u8; 500];
        let mut reader = ThrottledReader::new(Cursor::new(data), limiter);

        let start = Instant::now();
        let mut buf = vec![0u8; 500];
        reader.read_exact(&mut buf).await.unwrap();

        // Should have returned immediately but set the sleep for next call
        assert!(start.elapsed().as_millis() < 50);

        // Second read should wait
        let start = Instant::now();
        // Since we reached EOF or just read again
        let _ = reader.read(&mut buf).await.unwrap();
        assert!(start.elapsed().as_secs_f64() >= 0.4);
    }
}
