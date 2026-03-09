use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf},
    time::{Duration, Instant, Sleep, sleep},
};

use crate::{
    backend::{Handle, NodeAttr, StorageBackend, WriteContents},
    utils::size,
};

/// Standard chunk size for discrete IO operations in the throttled wrapper.
const DISCRETE_CHUNK_SIZE: usize = size::MiB as usize;

/// Minimum sleep duration to avoid wasteful context switches for tiny delays.
const MIN_SLEEP: Duration = Duration::from_millis(10);

/// A high-performance rate limiter using atomic virtual time and integer math.
/// This implementation is lock-free and supports highly concurrent access.
pub struct RateLimiter {
    limit_bps: u64,
    // Nanoseconds since base_instant when the limiter will be "free".
    next_free_ns: AtomicU64,
    base_instant: Instant,
}

impl RateLimiter {
    pub fn new(limit: u64) -> Self {
        let now = Instant::now();
        Self {
            limit_bps: limit,
            // Initialize to 1s so it matches now_ns at startup (since base_instant is now - 1s).
            next_free_ns: AtomicU64::new(1_000_000_000),
            base_instant: now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
        }
    }

    pub fn limit_bps(&self) -> u64 {
        self.limit_bps
    }

    /// Asynchronously wait until the requested amount of bytes can be consumed.
    pub async fn wait(&self, amount: u64) {
        if self.limit_bps == 0 || amount == 0 {
            return;
        }

        let wait_time = self.consume(amount);
        if !wait_time.is_zero() {
            sleep(wait_time).await;
        }
    }

    /// Synchronously attempt to consume tokens using an atomic virtual-time CAS loop.
    pub fn consume(&self, amount: u64) -> Duration {
        if self.limit_bps == 0 || amount == 0 {
            return Duration::ZERO;
        }

        let now_ns = self.base_instant.elapsed().as_nanos() as u64;
        let burst_ns = 1_000_000_000; // 1s burst capacity
        let max_debt_ns = 30_000_000_000; // 30s max debt to prevent infinite "freezes"

        // Use u128 for intermediate calculation to avoid overflow.
        let nanos_to_add = ((amount as u128 * 1_000_000_000) / self.limit_bps as u128)
            .min(u64::MAX as u128) as u64;

        loop {
            let current_free = self.next_free_ns.load(Ordering::Acquire);

            // The virtual time when this request is allowed to start.
            // We can start at current_free, but no earlier than now_ns - burst_ns.
            let min_start = now_ns.saturating_sub(burst_ns);
            let start_time = current_free.max(min_start);

            // Cap the future debt to prevent infinite "freezes" if the limit is very low.
            let max_free = now_ns.saturating_add(max_debt_ns);
            let start_time = start_time.min(max_free);

            let new_free = start_time.saturating_add(nanos_to_add);

            if self
                .next_free_ns
                .compare_exchange_weak(current_free, new_free, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                if start_time <= now_ns {
                    return Duration::ZERO;
                }
                let wait = Duration::from_nanos(start_time - now_ns);
                // Return zero for very small waits to avoid tiny context switches
                return if wait < MIN_SLEEP {
                    Duration::ZERO
                } else {
                    wait
                };
            }
        }
    }
}

/// Helper trait to handle common sleep logic in throttled wrappers.
trait ThrottledIO {
    fn limiter(&self) -> &RateLimiter;
    fn sleep(&mut self) -> &mut Option<Pin<Box<Sleep>>>;

    fn poll_sleep(&mut self, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if let Some(sleep_fut) = self.sleep() {
            match Pin::new(sleep_fut).poll(cx) {
                Poll::Ready(_) => {
                    *self.sleep() = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn register_data_consumed(&mut self, cx: &mut Context<'_>, n: u64) {
        if n > 0 {
            let wait_time = self.limiter().consume(n);
            if !wait_time.is_zero() {
                let mut sleep_fut = Box::pin(tokio::time::sleep(wait_time));
                // Poll once to register with the waker
                if Pin::new(&mut sleep_fut).poll(cx).is_pending() {
                    *self.sleep() = Some(sleep_fut);
                }
            }
        }
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

impl<R: AsyncRead + Unpin> ThrottledIO for ThrottledReader<R> {
    fn limiter(&self) -> &RateLimiter {
        &self.limiter
    }
    fn sleep(&mut self) -> &mut Option<Pin<Box<Sleep>>> {
        &mut self.sleep
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for ThrottledReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.poll_sleep(cx).is_pending() {
            return Poll::Pending;
        }

        let prev_len = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let n = (buf.filled().len() - prev_len) as u64;
                self.register_data_consumed(cx, n);
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

/// A wrapper around an AsyncWrite that applies rate limiting.
pub struct ThrottledWriter<W> {
    inner: W,
    limiter: Arc<RateLimiter>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl<W: AsyncWrite + Unpin> ThrottledWriter<W> {
    pub fn new(inner: W, limiter: Arc<RateLimiter>) -> Self {
        Self {
            inner,
            limiter,
            sleep: None,
        }
    }
}

impl<W: AsyncWrite + Unpin> ThrottledIO for ThrottledWriter<W> {
    fn limiter(&self) -> &RateLimiter {
        &self.limiter
    }
    fn sleep(&mut self) -> &mut Option<Pin<Box<Sleep>>> {
        &mut self.sleep
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for ThrottledWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.poll_sleep(cx).is_pending() {
            return Poll::Pending;
        }

        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.register_data_consumed(cx, n as u64);
                Poll::Ready(Ok(n))
            }
            res => res,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.poll_sleep(cx).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.poll_sleep(cx).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A wrapper backend that applies rate limiting to read and write operations.
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
        if self.inner.is_dry_run() {
            return Ok(contents);
        }

        if let Some(limiter) = &self.download_limiter {
            for chunk in contents.chunks(DISCRETE_CHUNK_SIZE) {
                limiter.wait(chunk.len() as u64).await;
            }
        }
        Ok(contents)
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        if self.inner.is_dry_run() {
            return self.inner.write(handle, contents).await;
        }

        if let Some(limiter) = &self.upload_limiter {
            // Wait for the capacity to write this buffer.
            // For very large buffers, we wait in chunks to allow for smoother
            // interleaving with other tasks if they are also using the limiter.
            const THROTTLE_CHUNK: usize = 2 * size::MiB as usize;

            for chunk in contents.chunks(THROTTLE_CHUNK) {
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

        // Second read should wait (using a second read call to trigger the sleep)
        let start = Instant::now();
        let mut dummy = [0u8; 1];
        let _ = reader.read(&mut dummy).await.unwrap();
        assert!(start.elapsed().as_secs_f64() >= 0.4);
    }

    #[tokio::test]
    async fn test_throttled_writer() {
        let limit = 1000; // 1000 bytes/sec
        let limiter = Arc::new(RateLimiter::new(limit));

        // Initial burst consumed
        limiter.wait(1000).await;

        let mut output = Vec::new();
        let mut writer = ThrottledWriter::new(Cursor::new(&mut output), limiter);

        let start = Instant::now();
        let data = vec![0u8; 500];
        writer.write_all(&data).await.unwrap();

        // First write should be instant
        assert!(start.elapsed().as_millis() < 50);

        // Second write should wait
        let start = Instant::now();
        writer.write_all(&[0u8]).await.unwrap();
        assert!(start.elapsed().as_secs_f64() >= 0.4);
    }
}
