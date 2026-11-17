//! Detection of the resources available to this process.
//!
//! Parallelism is derived from the number of usable CPUs and the memory the
//! process can realistically use. Both are detected once and cached, taking
//! cgroup limits into account so containers do not overcommit the host.
//!
//! Two environment variables override the detection, which is useful in CI and
//! in environments where the container limits are not visible:
//!
//! - `MAPACHE_CPU_COUNT`: number of CPUs to assume.
//! - `MAPACHE_MEMORY_BUDGET`: memory budget (e.g. `4GiB`), applied by the
//!   parallelism policy rather than here.

use std::sync::OnceLock;

use crate::{common::vars::get_envvar, utils::size};

/// Environment override for the detected CPU count.
pub(crate) const CPU_COUNT_ENVVAR: &str = "MAPACHE_CPU_COUNT";

/// Detected resources available to the process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemProfile {
    /// Usable logical CPUs (OS affinity, cgroup quota and environment override).
    pub logical_cpus: usize,
    /// Total physical memory visible to the process, in bytes.
    pub total_memory: u64,
    /// Memory the process may allocate before hitting pressure or a cgroup
    /// limit, in bytes.
    pub available_memory: u64,
    /// Whether a cgroup CPU quota was detected.
    pub cgroup_cpu_limit: bool,
    /// Whether a cgroup memory limit was detected.
    pub cgroup_memory_limit: bool,
}

impl SystemProfile {
    /// Detects the current profile. Exposed for tests and `config info`.
    pub fn detect() -> Self {
        let (cgroup_cpus, cgroup_cpu_limit) = cgroup_cpu_limit();
        let os_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let mut logical_cpus = cgroup_cpus
            .map_or(os_cpus, |quota| os_cpus.min(quota))
            .max(1);
        if let Some(override_cpus) = env_usize(CPU_COUNT_ENVVAR) {
            logical_cpus = override_cpus.max(1);
        }

        let memory = detect_memory();

        Self {
            logical_cpus,
            total_memory: memory.total,
            available_memory: memory.available,
            cgroup_cpu_limit,
            cgroup_memory_limit: memory.cgroup_limited,
        }
    }
}

/// Returns the process-wide detected profile, detecting it on first use.
pub fn profile() -> &'static SystemProfile {
    static PROFILE: OnceLock<SystemProfile> = OnceLock::new();
    PROFILE.get_or_init(SystemProfile::detect)
}

/// Total physical memory visible to the process, in MiB.
pub(crate) fn total_memory_mib() -> u64 {
    profile().total_memory / size::MiB
}

fn env_usize(name: &str) -> Option<usize> {
    get_envvar(name).and_then(|v| v.trim().parse::<usize>().ok())
}

#[derive(Debug, Clone, Copy)]
struct DetectedMemory {
    total: u64,
    available: u64,
    cgroup_limited: bool,
}

#[cfg(target_os = "linux")]
fn detect_memory() -> DetectedMemory {
    let host = linux_meminfo();
    let (total, available) = match host {
        Some((total, available)) => (total, available),
        None => {
            let fallback = sysconf_memory();
            (fallback.0, fallback.1)
        }
    };

    match cgroup_memory_limit() {
        Some((limit, available_limit)) => DetectedMemory {
            total: total.min(limit),
            available: available.min(available_limit),
            cgroup_limited: true,
        },
        None => DetectedMemory {
            total,
            available,
            cgroup_limited: false,
        },
    }
}

#[cfg(windows)]
fn detect_memory() -> DetectedMemory {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    // SAFETY: `GlobalMemoryStatusEx` is a documented Win32 API that fills in a
    // `MEMORYSTATUSEX`; the struct is zeroed and its length field is set as
    // required by the API.
    let mut mem = unsafe { std::mem::zeroed::<MEMORYSTATUSEX>() };
    mem.dwLength = size_of::<MEMORYSTATUSEX>() as u32;
    // SAFETY: see above; `mem` is a valid, properly sized `MEMORYSTATUSEX`.
    let ok = unsafe { GlobalMemoryStatusEx(&mut mem) };
    if ok == 0 {
        return DetectedMemory {
            total: size::GiB,
            available: size::GiB,
            cgroup_limited: false,
        };
    }

    DetectedMemory {
        total: mem.ullTotalPhys,
        available: mem.ullAvailPhys.max(size::MiB),
        cgroup_limited: false,
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn detect_memory() -> DetectedMemory {
    let mut system = ::sysinfo::System::new();
    system.refresh_memory();
    let total = system.total_memory().max(size::GiB);
    let available = system.available_memory().max(1).min(total);
    DetectedMemory {
        total,
        available,
        cgroup_limited: false,
    }
}

#[cfg(not(any(unix, windows)))]
fn detect_memory() -> DetectedMemory {
    DetectedMemory {
        total: size::GiB,
        available: size::GiB,
        cgroup_limited: false,
    }
}

#[cfg(target_os = "linux")]
fn sysconf_memory() -> (u64, u64) {
    // SAFETY: sysconf(2) is always safe to call and returns -1 on error.
    let (pages, available_pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_AVPHYS_PAGES),
            libc::sysconf(libc::_SC_PAGE_SIZE),
        )
    };
    if pages <= 0 || page_size <= 0 {
        return (size::GiB, size::GiB);
    }
    let total = (pages as u64).saturating_mul(page_size as u64);
    let available = if available_pages > 0 {
        (available_pages as u64).saturating_mul(page_size as u64)
    } else {
        total
    };
    (total, available.min(total))
}

#[cfg(target_os = "linux")]
fn linux_meminfo() -> Option<(u64, u64)> {
    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut total = None;
    let mut available = None;

    for line in content.lines() {
        let (key, value) = line.split_once(':')?;
        let bytes = value
            .trim()
            .trim_end_matches(" kB")
            .trim()
            .parse::<u64>()
            .ok()?
            .saturating_mul(1024);
        match key {
            "MemTotal" => total = Some(bytes),
            "MemAvailable" => available = Some(bytes),
            _ => {}
        }
    }

    let total = total?;
    Some((total, available.unwrap_or(total).min(total)))
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit() -> Option<(u64, u64)> {
    let base = cgroup_v2_path().or_else(|| cgroup_v1_path("memory"))?;

    if let Some(limit) = read_u64(&base.join("memory.max")) {
        let used = read_u64(&base.join("memory.current")).unwrap_or(0);
        let available = limit.saturating_sub(used).max(1);
        return Some((limit, available));
    }

    // cgroup v1: huge values are the "unlimited" sentinel.
    let limit = read_u64(&base.join("memory.limit_in_bytes"))?;
    if limit >= (1u64 << 60) {
        return None;
    }
    let used = read_u64(&base.join("memory.usage_in_bytes")).unwrap_or(0);
    let available = limit.saturating_sub(used).max(1);
    Some((limit, available))
}

/// Detects the CPU quota from cgroup v2 (`cpu.max`) or v1 (`cpu.cfs_quota_us`).
#[cfg(target_os = "linux")]
fn cgroup_cpu_limit() -> (Option<usize>, bool) {
    let Some(base) = cgroup_v2_path().or_else(|| cgroup_v1_path("cpu")) else {
        return (None, false);
    };

    if let Some(content) = read_trimmed(&base.join("cpu.max")) {
        let mut parts = content.split_whitespace();
        let quota = parts.next().unwrap_or("max");
        let period = parts.next().and_then(|p| p.parse::<u64>().ok());
        if let (Ok(quota), Some(period)) = (quota.parse::<u64>(), period)
            && period > 0
        {
            let cpus = quota.div_ceil(period) as usize;
            return (Some(cpus.max(1)), true);
        }
        return (None, false);
    }

    let Some(quota) = read_i64(&base.join("cpu.cfs_quota_us")) else {
        return (None, false);
    };
    let period = read_i64(&base.join("cpu.cfs_period_us")).unwrap_or(100_000);
    if quota > 0 && period > 0 {
        let cpus = (quota as u64).div_ceil(period as u64) as usize;
        return (Some(cpus.max(1)), true);
    }
    (None, false)
}

#[cfg(not(target_os = "linux"))]
fn cgroup_cpu_limit() -> (Option<usize>, bool) {
    (None, false)
}

#[cfg(target_os = "linux")]
fn cgroup_v2_path() -> Option<std::path::PathBuf> {
    let content = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim();
    Some(std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

#[cfg(target_os = "linux")]
fn cgroup_v1_path(controller: &str) -> Option<std::path::PathBuf> {
    let content = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = content
        .lines()
        .find_map(|line| {
            let (id, rest) = line.split_once(':')?;
            let (controllers, path) = rest.split_once(':')?;
            if id.is_empty() || !controllers.split(',').any(|c| c == controller) {
                return None;
            }
            Some(path)
        })?
        .trim();
    Some(
        std::path::Path::new("/sys/fs/cgroup")
            .join(controller)
            .join(path.trim_start_matches('/')),
    )
}

#[cfg(target_os = "linux")]
fn read_trimmed(path: &std::path::Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

#[cfg(target_os = "linux")]
fn read_u64(path: &std::path::Path) -> Option<u64> {
    let s = read_trimmed(path)?;
    if s == "max" {
        return None;
    }
    s.parse().ok()
}

#[cfg(target_os = "linux")]
fn read_i64(path: &std::path::Path) -> Option<i64> {
    read_trimmed(path)?.parse().ok()
}
