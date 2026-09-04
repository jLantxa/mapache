#![cfg(test)]

mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::{Duration, Instant},
    };

    use mapache::{
        backend::{RetryOptions, retry_with},
        common::error::MapacheError,
    };

    #[derive(Clone)]
    struct RetryTracker {
        attempt_count: Arc<AtomicU32>,
        attempt_times: Arc<parking_lot::Mutex<Vec<Instant>>>,
    }

    impl RetryTracker {
        fn new() -> Self {
            Self {
                attempt_count: Arc::new(AtomicU32::new(0)),
                attempt_times: Arc::new(parking_lot::Mutex::new(Vec::new())),
            }
        }

        fn increment(&self) {
            self.attempt_count.fetch_add(1, Ordering::SeqCst);
            self.attempt_times.lock().push(Instant::now());
        }

        fn count(&self) -> u32 {
            self.attempt_count.load(Ordering::SeqCst)
        }

        fn times(&self) -> Vec<Instant> {
            self.attempt_times.lock().clone()
        }

        fn delays(&self) -> Vec<Duration> {
            let times = self.times();
            if times.len() < 2 {
                return vec![];
            }

            times
                .windows(2)
                .map(|window| window[1].duration_since(window[0]))
                .collect()
        }
    }

    #[tokio::test]
    async fn exponential_backoff_timing() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 4,
            base_delay: Duration::from_millis(10),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    if tc.count() < 3 {
                        Err::<i32, MapacheError>(MapacheError::Backend("retryable".to_string()))
                    } else {
                        Ok(42)
                    }
                }
            },
            |_| true, // All errors are retryable
        )
        .await;

        // Should succeed on attempt 3
        assert!(result.is_ok());
        assert_eq!(tracker.count(), 3);

        // Verify retries never occur before their configured backoff expires.
        let delays = tracker.delays();
        assert_eq!(delays.len(), 2);

        // The first retry must wait base_delay * 2^0.
        let expected_first = Duration::from_millis(10);
        assert!(delays[0] >= expected_first, "first delay: {:?}", delays[0]);

        // The second retry must wait base_delay * 2^1.
        let expected_second = Duration::from_millis(20);
        assert!(
            delays[1] >= expected_second,
            "second delay: {:?}",
            delays[1]
        );
    }

    #[tokio::test]
    async fn respects_max_attempts() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 5,
            base_delay: Duration::from_millis(5),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    Err::<i32, MapacheError>(MapacheError::Backend("always fails".to_string()))
                }
            },
            |_| true, // All errors are retryable
        )
        .await;

        // Should fail after exactly max_attempts attempts
        assert!(result.is_err());
        assert_eq!(tracker.count(), 5);
    }

    #[tokio::test]
    async fn non_retryable_fails_fast() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    Err::<i32, MapacheError>(MapacheError::Backend("permanent error".to_string()))
                }
            },
            |_| false, // No errors are retryable (all permanent)
        )
        .await;

        // Should fail immediately without retries
        assert!(result.is_err());
        assert_eq!(tracker.count(), 1);
    }

    #[tokio::test]
    async fn success_on_first_attempt() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    Ok::<i32, MapacheError>(42)
                }
            },
            |_| true,
        )
        .await;

        // Should succeed immediately
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(tracker.count(), 1);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 5,
            base_delay: Duration::from_millis(10),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    if tc.count() <= 3 {
                        Err::<i32, MapacheError>(MapacheError::Backend("transient".to_string()))
                    } else {
                        Ok(99)
                    }
                }
            },
            |_| true,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 99);
        assert_eq!(tracker.count(), 4);
    }

    #[tokio::test]
    async fn retry_predicate_conditional() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 10,
            base_delay: Duration::from_millis(10),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    if tc.count() == 1 {
                        Err::<i32, MapacheError>(MapacheError::Backend(
                            "transient error".to_string(),
                        ))
                    } else {
                        Err::<i32, MapacheError>(MapacheError::Backend(
                            "permanent error".to_string(),
                        ))
                    }
                }
            },
            |err| {
                // Only retry if error contains "transient"
                err.inner().contains("transient")
            },
        )
        .await;

        assert!(result.is_err());
        // Should attempt once for transient, then once for permanent (fail fast)
        assert_eq!(tracker.count(), 2);
    }

    #[tokio::test]
    async fn timeout_is_respected() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 3,
            base_delay: Duration::from_millis(10),
            request_timeout: Duration::from_millis(50),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    // Never completes, so the request timeout is the only thing
                    // that can fire. Using a real sleep races against the timer:
                    // under load-driven timer drift the sleep could win and make
                    // the operation succeed spuriously.
                    std::future::pending::<Result<i32, MapacheError>>().await
                }
            },
            |_| true,
        )
        .await;

        // Must time out on every attempt and eventually fail with the
        // retry-exhausted timeout error.
        let err = result.expect_err("operation should timeout after max_attempts");
        assert!(
            matches!(&err, MapacheError::Backend(msg) if msg.contains("timed out after multiple retries")),
            "expected retry-exhausted timeout error, got: {err}"
        );
        // Should attempt max_attempts times
        assert_eq!(tracker.count(), 3);
    }

    #[tokio::test]
    async fn zero_delay_works() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 3,
            base_delay: Duration::ZERO,
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    if tc.count() < 3 {
                        Err::<i32, MapacheError>(MapacheError::Backend("fail".to_string()))
                    } else {
                        Ok(7)
                    }
                }
            },
            |_| true,
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 7);
        assert_eq!(tracker.count(), 3);
    }

    #[tokio::test]
    async fn max_attempts_one() {
        let tracker = RetryTracker::new();
        let opts = RetryOptions {
            max_attempts: 1,
            base_delay: Duration::from_millis(10),
            request_timeout: Duration::from_secs(10),
        };

        let tracker_clone = tracker.clone();
        let result = retry_with(
            "test",
            &opts,
            || {
                let tc = tracker_clone.clone();
                async move {
                    tc.increment();
                    Err::<i32, MapacheError>(MapacheError::Backend("fail".to_string()))
                }
            },
            |_| true,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            tracker.count(),
            1,
            "Should attempt exactly once with max_attempts=1"
        );
    }
}
