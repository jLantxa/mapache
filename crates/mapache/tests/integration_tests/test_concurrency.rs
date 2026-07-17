#![cfg(test)]

mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use mapache::{
        backend::{StorageBackend, localfs::LocalFS},
        common::defaults::TEST_REPO_CONFIG,
        repository::repo::{Auth, LOCKS_DIR, Repository},
    };
    use tempfile::tempdir;
    use zeroize::Zeroizing;

    fn make_auth() -> Auth {
        Auth {
            username: "test".to_string(),
            password: Zeroizing::new("password".to_string()),
        }
    }

    /// Exclusive lock blocks another exclusive lock.
    #[tokio::test]
    async fn test_exclusive_lock_blocks_exclusive() -> Result<()> {
        let tmp = tempdir()?;
        let repo_path = tmp.path().join("repo");
        let auth = make_auth();

        let backend: Arc<dyn StorageBackend> = Arc::new(LocalFS::new(repo_path.clone()));
        Repository::init(&auth, None, backend.clone()).await?;

        // First exclusive lock
        let (_repo1, _ss1, _lock1) = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;

        // Second exclusive lock should fail with short timeout
        let result = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            Some(chrono::Duration::milliseconds(200)),
        )
        .await;

        assert!(result.is_err(), "Second exclusive lock should fail");

        Ok(())
    }

    /// Verify that lock files are properly cleaned up after release.
    #[tokio::test]
    async fn test_lock_cleanup_after_sequential_ops() -> Result<()> {
        let tmp = tempdir()?;
        let repo_path = tmp.path().join("repo");
        let auth = make_auth();

        let backend: Arc<dyn StorageBackend> = Arc::new(LocalFS::new(repo_path.clone()));
        Repository::init(&auth, None, backend.clone()).await?;

        let locks_dir = repo_path.join(LOCKS_DIR);

        // Run 3 sequential lock acquire/release cycles
        for _ in 0..3 {
            {
                let (_repo, _ss, _lock) = Repository::try_open_with_lock(
                    &auth,
                    None,
                    backend.clone(),
                    TEST_REPO_CONFIG,
                    true,
                    None,
                )
                .await?;
                // Lock is held until end of block, then dropped
            }

            // Wait for lock file to be cleaned up
            let mut cleaned = false;
            for _ in 0..100 {
                if mapache::utils::count_files(&locks_dir)? == 0 {
                    cleaned = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(cleaned, "Lock file should be cleaned up after drop");
        }

        Ok(())
    }

    /// Acquire lock, drop it, re-acquire successfully.
    #[tokio::test]
    async fn test_lock_reacquire_after_release() -> Result<()> {
        let tmp = tempdir()?;
        let repo_path = tmp.path().join("repo");
        let auth = make_auth();

        let backend: Arc<dyn StorageBackend> = Arc::new(LocalFS::new(repo_path.clone()));
        Repository::init(&auth, None, backend.clone()).await?;

        // Acquire and release
        {
            let (_repo, _ss, _lock) = Repository::try_open_with_lock(
                &auth,
                None,
                backend.clone(),
                TEST_REPO_CONFIG,
                true,
                None,
            )
            .await?;
        }

        // Wait for cleanup
        let locks_dir = repo_path.join(LOCKS_DIR);
        for _ in 0..100 {
            if mapache::utils::count_files(&locks_dir)? == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Re-acquire should succeed
        let result = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            Some(chrono::Duration::seconds(5)),
        )
        .await;
        assert!(result.is_ok(), "Should re-acquire lock after release");

        Ok(())
    }

    /// Lock with retry eventually succeeds after first lock is released.
    #[tokio::test]
    async fn test_lock_retry_succeeds_after_release() -> Result<()> {
        let tmp = tempdir()?;
        let repo_path = tmp.path().join("repo");
        let auth = make_auth();

        let backend: Arc<dyn StorageBackend> = Arc::new(LocalFS::new(repo_path.clone()));
        Repository::init(&auth, None, backend.clone()).await?;

        // First lock
        let (_repo1, _ss1, lock1) = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;

        // Spawn a task that will release the lock after 300ms
        let backend_clone = backend.clone();
        let auth_clone = make_auth();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            drop(_repo1);
            drop(_ss1);
            drop(lock1);

            // Wait for lock file cleanup
            let locks_dir = repo_path.join(LOCKS_DIR);
            for _ in 0..100 {
                if mapache::utils::count_files(&locks_dir).unwrap_or(0) == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        // Second lock with retry should eventually succeed
        let result = Repository::try_open_with_lock(
            &auth_clone,
            None,
            backend_clone,
            TEST_REPO_CONFIG,
            true,
            Some(chrono::Duration::seconds(5)),
        )
        .await;

        assert!(
            result.is_ok(),
            "Lock retry should succeed after first lock is released"
        );

        handle.await?;
        Ok(())
    }
}
