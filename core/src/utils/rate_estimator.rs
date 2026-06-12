use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// A sliding-window rate estimator for measuring throughput (bytes, items, etc.)
/// and estimating remaining time (ETA) based on recent activity.
///
/// Generic: the caller defines the unit — bytes, items, or any scalar.
/// Rate is always reported in `units/second` and ETA as `Duration`.
///
/// # Concurrency
///
/// [`observe`](Self::observe) and [`reset`](Self::reset) require `&mut self`
/// (or a `Mutex` lock).  [`rate`](Self::rate) and [`eta`](Self::eta) read a
/// pre-computed value from an internal `AtomicU64` and are cheap — they do
/// not iterate the sample window.
pub struct RateEstimator {
    window: Duration,
    samples: VecDeque<(Instant, f64)>,
    cached: AtomicU64,
}

impl RateEstimator {
    /// Creates a new estimator with the given sliding window duration.
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
            cached: AtomicU64::new(f64::to_bits(0.0)),
        }
    }

    /// Records the current cumulative value at this instant.
    /// Call this whenever progress advances (e.g., bytes processed so far).
    pub fn observe(&mut self, value: f64) {
        self.observe_at(value, Instant::now());
    }

    fn observe_at(&mut self, value: f64, now: Instant) {
        self.samples.push_back((now, value));

        while let Some(front) = self.samples.front() {
            if now.duration_since(front.0) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }

        self.cached
            .store(f64::to_bits(self.compute_rate()), Ordering::Relaxed);
    }

    /// Returns the rate in units/second, computed from the first and last
    /// sample in the sliding window. Returns 0.0 if insufficient data.
    ///
    /// Reads a pre-computed value from an internal `AtomicU64` — does not
    /// iterate the sample window.
    pub fn rate(&self) -> f64 {
        f64::from_bits(self.cached.load(Ordering::Relaxed))
    }

    /// Estimates remaining time (ETA) to reach `total` from `current`,
    /// based on the recent rate. Returns `None` if rate is zero or
    /// there's insufficient data.
    pub fn eta(&self, current: f64, total: f64) -> Option<Duration> {
        let rate = self.rate();
        if rate <= 0.0 {
            return None;
        }
        let remaining = total - current;
        if remaining <= 0.0 {
            return Some(Duration::ZERO);
        }
        Some(Duration::from_secs_f64(remaining / rate))
    }

    /// Resets all samples (e.g., after a phase change).
    pub fn reset(&mut self) {
        self.samples.clear();
        self.cached.store(f64::to_bits(0.0), Ordering::Relaxed);
    }

    fn compute_rate(&self) -> f64 {
        let (earliest, earliest_val) = match self.samples.front() {
            Some(s) => *s,
            None => return 0.0,
        };
        let (latest, latest_val) = match self.samples.back() {
            Some(s) => *s,
            None => return 0.0,
        };
        let dt = latest.duration_since(earliest).as_secs_f64();
        if dt <= 0.0 {
            return 0.0;
        }
        (latest_val - earliest_val) / dt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64, tolerance: f64) -> bool {
        (a - b).abs() <= tolerance
    }

    #[test]
    fn test_rate_and_eta() {
        let mut re = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();

        assert_eq!(re.rate(), 0.0);
        assert!(re.eta(0.0, 100.0).is_none());

        re.observe_at(0.0, t0);
        assert_eq!(re.rate(), 0.0);

        let t1 = t0 + Duration::from_millis(200);
        re.observe_at(100.0, t1);

        let rate = re.rate();
        assert!(approx_eq(rate, 500.0, 100.0), "rate={}", rate);

        let eta = re.eta(100.0, 200.0).unwrap();
        assert!(approx_eq(eta.as_secs_f64(), 0.2, 0.1), "eta={:?}", eta);

        assert_eq!(re.eta(200.0, 200.0), Some(Duration::ZERO));
    }

    #[test]
    fn test_reset() {
        let mut re = RateEstimator::new(Duration::from_secs(30));
        let t0 = Instant::now();

        re.observe_at(0.0, t0);
        re.observe_at(100.0, t0 + Duration::from_millis(10));
        assert!(re.rate() > 0.0);

        re.reset();
        assert_eq!(re.rate(), 0.0);
        assert!(re.eta(0.0, 100.0).is_none());
    }

    #[test]
    fn test_window_eviction() {
        let mut re = RateEstimator::new(Duration::from_millis(50));
        let t0 = Instant::now();

        re.observe_at(0.0, t0);
        re.observe_at(10.0, t0 + Duration::from_millis(10));
        assert!(re.rate() > 0.0);

        re.observe_at(20.0, t0 + Duration::from_millis(100));
        assert_eq!(re.rate(), 0.0);
    }
}
