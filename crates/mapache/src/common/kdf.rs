use std::time::{Duration, Instant};

use argon2::Params;

use crate::{repository::storage::SecureStorage, ui::cli, utils::size};

/// Target wall-clock time for a single Argon2id derivation.
pub const CALIBRATE_TARGET: Duration = Duration::from_millis(500);

/// Upper bound multiplier: calibration result must not exceed target * this.
pub const CALIBRATE_UPPER: f64 = 1.33;

/// Lower and upper memory bounds for calibration (MiB).
pub const CALIBRATE_MEMORY_BOUNDS: (u32, u32) = (32, 64);

/// Factor applied to memory when reducing after overshoot.
const REDUCTION_FACTOR: f64 = 0.95;

/// Dummy password used for calibration benchmarks.
const DUMMY_PASSWORD: &str = "mapachito";

/// Return default Argon2id parameters (fast, no benchmarking).
pub fn default_params() -> Params {
    Params::default()
}

/// Return total physical memory in MiB.
fn total_memory_mib() -> u64 {
    // SAFETY: sysconf(2) is always safe to call; returns -1 on error.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGE_SIZE),
        )
    };
    (pages * page_size) as u64 / size::MiB
}

/// Benchmark and return Argon2id parameters tuned for the current hardware.
///
/// If `max_memory_mib` is `None`, uses up to 10% of total system memory (clamped to
/// [`CALIBRATE_MEMORY_BOUNDS`]). If `Some(n)`, uses `n` MiB as the upper bound.
pub fn calibrate_params(target: Duration, max_memory_mib: Option<u32>) -> Params {
    cli::warning!(
        "calibrating Argon2id parameters. \
         Run this only when the system is idle for accurate results."
    );

    const MEMORY_USAGE_RATIO: f32 = 0.10;
    let max_memory_mib = max_memory_mib.unwrap_or_else(|| {
        ((total_memory_mib() as f32 * MEMORY_USAGE_RATIO) as u32)
            .clamp(CALIBRATE_MEMORY_BOUNDS.0, CALIBRATE_MEMORY_BOUNDS.1)
    });

    calibrate(target, max_memory_mib)
}

/// Determine Argon2id parameters for the current hardware.
///
/// Target is a minimum: calibration always overshoots (slower = stronger).
/// The result must not exceed `CALIBRATE_UPPER × target`.
///
/// 1. Fix `p = 1` for deterministic calibration.
/// 2. Start with the maximum memory that fits under `max_memory_mib`.
/// 3. Increase `t` until duration >= target.
/// 4. If too slow (> upper bound), reduce memory and repeat.
fn calibrate(target: Duration, max_memory_mib: u32) -> Params {
    fn find_t(p: u32, m: u32, target: Duration, salt: &[u8]) -> (u32, Duration) {
        let mut t = 1;
        let mut duration = bench(DUMMY_PASSWORD, salt, m, t, p, 1);

        while duration < target {
            // Argon2 cost is ~linear in t for fixed m: estimate the t that
            // hits the target from the last measurement instead of t += 1.
            let per_iter = duration.as_secs_f64() / f64::from(t);
            let estimated = (target.as_secs_f64() / per_iter).ceil() as u32;
            t = estimated.max(t + 1);
            duration = bench(DUMMY_PASSWORD, salt, m, t, p, 1);
        }
        (t, duration)
    }

    let p = optimal_parallelism();

    let max_memory_kib = max_memory_mib.saturating_mul(size::KiB as u32);
    let min_memory_kib = CALIBRATE_MEMORY_BOUNDS.0.saturating_mul(size::KiB as u32);
    let mut m = find_max_memory(p, max_memory_kib);

    let salt = SecureStorage::generate_salt::<32>();

    let (mut t, mut duration) = find_t(p, m, target, &salt);

    let upper = Duration::from_secs_f64(target.as_secs_f64() * CALIBRATE_UPPER);
    while duration > upper && m > min_memory_kib {
        let fraction = target.as_secs_f64() / duration.as_secs_f64();
        m = find_max_memory(p, (m as f64 * fraction * REDUCTION_FACTOR) as u32);
        (t, duration) = find_t(p, m, target, &salt);
    }

    argon2::ParamsBuilder::new()
        .m_cost(m)
        .t_cost(t)
        .p_cost(p)
        .build()
        .expect("calibrated argon2 params are always valid")
}

/// Determine parallelism: use available cores, capped to 4.
fn optimal_parallelism() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
        .clamp(1, 4)
}

/// Find the largest multiple of `128 * p` KiB that fits under `max_kib`.
/// Returns at least `128 * p` to ensure a minimum allocation.
fn find_max_memory(p: u32, max_kib: u32) -> u32 {
    let block = 128 * p;
    let aligned = (max_kib / block) * block;
    aligned.max(block)
}

/// Run Argon2id derivation and return the average elapsed time over `runs` iterations.
fn bench(password: &str, salt: &[u8], m: u32, t: u32, p: u32, runs: u32) -> Duration {
    let params = argon2::ParamsBuilder::new()
        .m_cost(m)
        .t_cost(t)
        .p_cost(p)
        .build()
        .expect("valid argon2 params in bench");

    let argon2 = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut key = [0u8; 32];

    let mut total = Duration::ZERO;
    for _ in 0..runs {
        let start = Instant::now();
        argon2
            .hash_password_into(password.as_bytes(), salt, &mut key)
            .expect("argon2 bench should not fail");
        total += start.elapsed();
    }
    total / runs
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serial_test::serial;

    use crate::repository::storage::SecureStorage;

    use super::*;

    #[rstest]
    #[case(4, 512, 512)]
    #[case(4, 1000, 512)]
    #[case(1, 1024, 1024)]
    #[case(1, 1500, 1408)]
    #[case(4, 256 * 1024, 256 * 1024)]
    #[case(2, 4096, 4096)]
    fn find_max_memory_alignment(#[case] p: u32, #[case] max_kib: u32, #[case] expected: u32) {
        let m = find_max_memory(p, max_kib);
        let block = 128 * p;
        assert_eq!(m, expected);
        assert!(m <= max_kib);
        assert_eq!(m % block, 0);
        assert!(m >= block);
    }

    #[test]
    #[serial]
    #[ignore] // slow — run with `cargo test -- --ignored`
    fn calibrate_hits_target_duration() {
        let target = CALIBRATE_TARGET;
        let params = calibrate(target, CALIBRATE_MEMORY_BOUNDS.1);

        // Independently measure the calibrated parameters (median of N runs).
        const N: usize = 5;
        let salt = SecureStorage::generate_salt::<32>();
        let mut key = [0u8; 32];
        let argon2 =
            argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = Instant::now();
            argon2
                .hash_password_into(b"test-password", &salt, &mut key)
                .unwrap();
            samples.push(start.elapsed());
        }
        samples.sort();
        let measured = samples[N / 2]; // median of N

        assert!(
            measured >= target,
            "calibration too fast: {measured:?} (expected >= {target:?})"
        );
    }
}
