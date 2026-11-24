//! Adaptive parallelism model.
//!
//! Every concurrency site in mapache is described by a [`SiteSpec`]: how many
//! CPUs one worker needs, how many bytes it can hold, and how much of the
//! memory budget that pipeline may spend. A [`Plan`] resolves all sites from the
//! detected [`SystemProfile`] and the user's [`Policy`], so the same binary uses
//! a handful of workers on a small container and many more on a large server.
//!
//! Explicit user values always win for the outer stage counts; runtime memory
//! limiters still provide a hard ceiling. See [`resolve_group`] for the formula.

use std::{
    collections::BTreeMap,
    fmt,
    str::FromStr,
    sync::{Arc, OnceLock},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    common::{
        config::RuntimeConfig,
        defaults::{
            DEFAULT_PACK_SIZE, DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE, MAX_CHUNK_SIZE,
            MIN_CHUNK_SIZE,
        },
        sysinfo::{self, SystemProfile},
        vars::{MAX_BLOCKING_THREADS_ENVVAR, get_envvar},
    },
    utils::{self, size},
};

/// Environment override for the memory budget (e.g. `4GiB` or `auto`).
pub(crate) const MEMORY_BUDGET_ENVVAR: &str = "MAPACHE_MEMORY_BUDGET";
/// Environment override for the CPU scale factor.
pub(crate) const CPU_SCALE_ENVVAR: &str = "MAPACHE_CPU_SCALE";

/// Default multiplier applied to the detected CPU count.
pub const DEFAULT_CPU_SCALE: f32 = 1.0;
/// Smallest accepted CPU scale factor.
pub const MIN_CPU_SCALE: f32 = 0.25;
/// Largest accepted CPU scale factor.
pub const MAX_CPU_SCALE: f32 = 4.0;
/// Fraction of the available memory used by the automatic budget.
pub const AUTO_MEMORY_FRACTION: f64 = 0.70;
/// Avoid a zero-sized budget when detection reports no available memory.
pub const MIN_MEMORY_BUDGET: u64 = 1;

/// Slack added to the pack buffer by the packer.
const PACK_BUFFER_SLACK: u64 = 256 * size::KiB;
const MIN_VERIFY_CHUNK_SIZE: u64 = 256 * size::KiB;
/// Largest per-stream decoded budget; keeps a single stream from consuming the
/// whole Decode group allowance.
const MAX_RESTORE_DECODED_BUDGET: u64 = 128 * size::MiB;

/// How the memory budget is chosen.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MemoryBudget {
    /// Derive the budget from the memory available to the process.
    #[default]
    Auto,
    /// Use this many bytes.
    Fixed(u64),
}

impl FromStr for MemoryBudget {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.eq_ignore_ascii_case("auto") {
            return Ok(Self::Auto);
        }
        let bytes = utils::parse_size(trimmed).map_err(|e| e.to_string())?;
        if bytes == 0 {
            return Err("memory budget must be greater than 0".to_string());
        }
        Ok(Self::Fixed(bytes))
    }
}

impl fmt::Display for MemoryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Fixed(bytes) => write!(f, "{}", utils::format_size_binary(*bytes, 1)),
        }
    }
}

impl Serialize for MemoryBudget {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MemoryBudget {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// User policy controlling how the plan is derived.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Policy {
    pub memory_budget: MemoryBudget,
    pub cpu_scale: f32,
    pub shares: GroupShares,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            memory_budget: MemoryBudget::Auto,
            cpu_scale: DEFAULT_CPU_SCALE,
            shares: GroupShares::default(),
        }
    }
}

impl Policy {
    /// Resolves the policy from CLI flags, environment and the `[runtime]`
    /// section. CLI beats environment, which beats the config file.
    pub fn from_sources(
        config: Option<&RuntimeConfig>,
        cli_budget: Option<MemoryBudget>,
        cli_cpu_scale: Option<f32>,
    ) -> Self {
        let env_budget = get_envvar(MEMORY_BUDGET_ENVVAR).and_then(|raw| match raw.parse() {
            Ok(budget) => Some(budget),
            Err(e) => {
                tracing::warn!(
                    target: "parallelism",
                    "ignoring invalid {MEMORY_BUDGET_ENVVAR}: {e}"
                );
                None
            }
        });
        let config_budget = config.and_then(|c| c.memory_budget);

        let env_scale = get_envvar(CPU_SCALE_ENVVAR).and_then(|raw| match raw.trim().parse::<f32>() {
            Ok(scale) if is_valid_cpu_scale(scale) => Some(scale),
            _ => {
                tracing::warn!(
                    target: "parallelism",
                    "ignoring invalid {CPU_SCALE_ENVVAR}: expected a number between {MIN_CPU_SCALE} and {MAX_CPU_SCALE}"
                );
                None
            }
        });
        let config_scale = config.and_then(|c| c.cpu_scale);

        let shares = config
            .and_then(|c| c.group_shares.as_ref())
            .map(|overrides| match GroupShares::from_overrides(overrides) {
                Ok(shares) => shares,
                Err(e) => {
                    tracing::warn!(target: "parallelism", "ignoring invalid group shares: {e}");
                    GroupShares::default()
                }
            })
            .unwrap_or_default();

        Self {
            memory_budget: cli_budget
                .or(env_budget)
                .or(config_budget)
                .unwrap_or(MemoryBudget::Auto),
            cpu_scale: cli_cpu_scale
                .or(env_scale)
                .or(config_scale)
                .unwrap_or(DEFAULT_CPU_SCALE)
                .clamp(MIN_CPU_SCALE, MAX_CPU_SCALE),
            shares,
        }
    }

    /// Resolves the byte budget for a profile.
    pub fn budget_bytes(&self, profile: &SystemProfile) -> u64 {
        match self.memory_budget {
            MemoryBudget::Auto => (((profile.available_memory as f64) * AUTO_MEMORY_FRACTION)
                as u64)
                .max(MIN_MEMORY_BUDGET),
            MemoryBudget::Fixed(bytes) => bytes.max(MIN_MEMORY_BUDGET),
        }
    }
}

/// Returns `true` when the CPU scale is inside the supported range.
pub fn is_valid_cpu_scale(scale: f32) -> bool {
    scale.is_finite() && (MIN_CPU_SCALE..=MAX_CPU_SCALE).contains(&scale)
}

/// Command pipeline a concurrency site belongs to.
///
/// Pipelines are mutually exclusive at command level, so every pipeline may use
/// the whole machine. Sites *within* one pipeline share it through their
/// [`BudgetGroup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pipeline {
    Snapshot,
    Restore,
    Verify,
    Gc,
    /// Shared layers and auxiliary commands, which keep their historical
    /// whole-machine limits.
    Other,
}

/// A set of sites that can be active at the same time within a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetGroup {
    /// Filesystem scanning / reading.
    Scan,
    /// Encoding and packing.
    Pack,
    /// Decoding and verification.
    Decode,
    /// Writing decoded data or deleting backend objects.
    Write,
    /// Backend I/O, listing and prefetching.
    Io,
    /// Shared services not attributed to a specific pipeline.
    Other,
}

impl Pipeline {
    const ALL: [Self; 5] = [
        Self::Snapshot,
        Self::Restore,
        Self::Verify,
        Self::Gc,
        Self::Other,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Snapshot => 0,
            Self::Restore => 1,
            Self::Verify => 2,
            Self::Gc => 3,
            Self::Other => 4,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Restore => "restore",
            Self::Verify => "verify",
            Self::Gc => "gc",
            Self::Other => "shared",
        }
    }

    const fn config_label(self) -> &'static str {
        match self {
            Self::Other => "other",
            _ => self.label(),
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|value| value.config_label() == label)
    }
}

impl BudgetGroup {
    const ALL: [Self; 6] = [
        Self::Scan,
        Self::Pack,
        Self::Decode,
        Self::Write,
        Self::Io,
        Self::Other,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Scan => 0,
            Self::Pack => 1,
            Self::Decode => 2,
            Self::Write => 3,
            Self::Io => 4,
            Self::Other => 5,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Pack => "pack",
            Self::Decode => "decode",
            Self::Write => "write",
            Self::Io => "io",
            Self::Other => "shared",
        }
    }

    const fn config_label(self) -> &'static str {
        match self {
            Self::Other => "other",
            _ => self.label(),
        }
    }

    fn from_label(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|value| value.config_label() == label)
    }
}

/// CPU and memory available to one budget group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupLimits {
    /// CPU cores assigned to the group.
    pub cpu_cores: usize,
    /// Bytes assigned to the group.
    pub bytes: u64,
}

/// Built-in CPU and memory shares for one pipeline group.
fn default_group_share(pipeline: Pipeline, group: BudgetGroup) -> (f32, f32) {
    match (pipeline, group) {
        (Pipeline::Snapshot, BudgetGroup::Scan) => (0.5, 0.35),
        (Pipeline::Snapshot, BudgetGroup::Pack) => (0.5, 0.65),

        (Pipeline::Restore, BudgetGroup::Io) => (0.25, 0.30),
        (Pipeline::Restore, BudgetGroup::Decode) => (0.50, 0.40),
        (Pipeline::Restore, BudgetGroup::Write) => (0.25, 0.30),

        (Pipeline::Verify, BudgetGroup::Io) => (0.20, 0.20),
        (Pipeline::Verify, BudgetGroup::Decode) => (0.80, 0.80),

        (Pipeline::Gc, BudgetGroup::Io) => (0.20, 0.25),
        (Pipeline::Gc, BudgetGroup::Pack) => (0.55, 0.55),
        (Pipeline::Gc, BudgetGroup::Write) => (0.25, 0.20),

        (Pipeline::Other, BudgetGroup::Other) => (1.0, 1.0),
        _ => (0.0, 0.0),
    }
}

/// Configurable CPU and memory shares for every pipeline group.
///
/// Shares within a pipeline sum to at most 1.0, so concurrently active stages
/// cannot claim more than the whole machine. Users can override individual
/// entries with `[runtime.group-shares]` keys such as `snapshot-scan-cpu`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GroupShares {
    cpu: [[f32; 6]; 5],
    memory: [[f32; 6]; 5],
}

impl Default for GroupShares {
    fn default() -> Self {
        let mut cpu = [[0.0; 6]; 5];
        let mut memory = [[0.0; 6]; 5];
        for pipeline in Pipeline::ALL {
            for group in BudgetGroup::ALL {
                let (cpu_share, memory_share) = default_group_share(pipeline, group);
                cpu[pipeline.index()][group.index()] = cpu_share;
                memory[pipeline.index()][group.index()] = memory_share;
            }
        }
        Self { cpu, memory }
    }
}

impl GroupShares {
    /// Returns the CPU and memory share for one group.
    pub fn get(self, pipeline: Pipeline, group: BudgetGroup) -> (f32, f32) {
        (
            self.cpu[pipeline.index()][group.index()],
            self.memory[pipeline.index()][group.index()],
        )
    }

    /// Applies overrides to the built-in shares.
    ///
    /// Keys have the form `<pipeline>-<group>-<cpu|memory>`, for example
    /// `restore-decode-memory = 0.5`.
    pub fn from_overrides(overrides: &BTreeMap<String, f32>) -> std::result::Result<Self, String> {
        let mut shares = Self::default();
        for (key, value) in overrides {
            let (pipeline, group, is_cpu) = Self::parse_key(key)?;
            if !value.is_finite() || !(0.0..=1.0).contains(value) {
                return Err(format!(
                    "runtime.group-shares.{key} must be a finite number between 0 and 1"
                ));
            }
            if is_cpu {
                shares.cpu[pipeline.index()][group.index()] = *value;
            } else {
                shares.memory[pipeline.index()][group.index()] = *value;
            }
        }
        shares.validate()?;
        Ok(shares)
    }

    fn parse_key(key: &str) -> std::result::Result<(Pipeline, BudgetGroup, bool), String> {
        let mut parts = key.rsplitn(3, '-');
        let kind = parts.next().unwrap_or_default();
        let group = parts.next().unwrap_or_default();
        let pipeline = parts.next().unwrap_or_default();
        let is_cpu = match kind {
            "cpu" => true,
            "memory" => false,
            _ => {
                return Err(format!(
                    "unknown runtime.group-shares key '{key}': expected ...-cpu or ...-memory"
                ));
            }
        };
        let Some(pipeline) = Pipeline::from_label(pipeline) else {
            return Err(format!(
                "unknown pipeline '{pipeline}' in group-share key '{key}'"
            ));
        };
        let Some(group) = BudgetGroup::from_label(group) else {
            return Err(format!(
                "unknown group '{group}' in group-share key '{key}'"
            ));
        };
        Ok((pipeline, group, is_cpu))
    }

    /// Rejects share tables that would let a pipeline claim more than one
    /// machine's worth of CPU or memory.
    pub fn validate(&self) -> std::result::Result<(), String> {
        for pipeline in Pipeline::ALL {
            let mut cpu = 0.0f32;
            let mut memory = 0.0f32;
            for group in BudgetGroup::ALL {
                let (c, m) = self.get(pipeline, group);
                cpu += c;
                memory += m;
            }
            if cpu > 1.0 + f32::EPSILON || memory > 1.0 + f32::EPSILON {
                return Err(format!(
                    "group shares for {} exceed one machine (cpu={cpu:.2}, memory={memory:.2})",
                    pipeline.label()
                ));
            }
        }
        Ok(())
    }
}

/// Resolves the concrete limits for a pipeline group.
fn group_limits(
    pipeline: Pipeline,
    group: BudgetGroup,
    cpu_cap: usize,
    budget: u64,
    shares: &GroupShares,
) -> GroupLimits {
    let (cpu_share, memory_share) = shares.get(pipeline, group);
    GroupLimits {
        cpu_cores: ((cpu_cap as f64 * cpu_share as f64).floor() as usize).max(1),
        bytes: ((budget as f64 * memory_share as f64) as u64).max(MIN_MEMORY_BUDGET),
    }
}

/// Number of concurrently active sites sharing a group's allowance.
///
/// Dividing the group allowance by this count is what keeps the *sum* of its
/// sites within the group budget; applying the whole allowance to every site
/// would let several stages each claim the full share.
fn group_site_count(pipeline: Pipeline, group: BudgetGroup) -> usize {
    match (pipeline, group) {
        (Pipeline::Snapshot, BudgetGroup::Scan) | (Pipeline::Snapshot, BudgetGroup::Pack) => 1,
        (Pipeline::Restore, BudgetGroup::Io) => 2,
        (Pipeline::Restore, BudgetGroup::Decode) | (Pipeline::Restore, BudgetGroup::Write) => 1,
        (Pipeline::Verify, BudgetGroup::Io) => 2,
        (Pipeline::Verify, BudgetGroup::Decode) => 1,
        (Pipeline::Gc, BudgetGroup::Io) => 6,
        (Pipeline::Gc, BudgetGroup::Pack) | (Pipeline::Gc, BudgetGroup::Write) => 2,
        _ => 1,
    }
}

/// Resolves the allowance of one site within a group.
fn site_limits(spec: &SiteSpec, cpu_cap: usize, budget: u64, shares: &GroupShares) -> GroupLimits {
    let group = group_limits(spec.pipeline, spec.group, cpu_cap, budget, shares);
    let count = group_site_count(spec.pipeline, spec.group).max(1);
    GroupLimits {
        cpu_cores: (group.cpu_cores / count).max(1),
        bytes: (group.bytes / count as u64).max(MIN_MEMORY_BUDGET),
    }
}

/// Declarative description of one concurrency site.
#[derive(Debug, Clone, Copy)]
pub struct SiteSpec {
    /// Human-readable site name, used in warnings.
    pub name: &'static str,
    /// Pipeline the site belongs to.
    pub pipeline: Pipeline,
    /// Group sharing the pipeline's CPU and memory allowance.
    pub group: BudgetGroup,
    /// CPUs one worker consumes. Values below 1 oversubscribe (I/O-bound work).
    pub cores_per_worker: f32,
    /// CPUs attributed to one worker in the demand model. This can be zero for
    /// sites that only bound handles or in-flight slots, not CPU work.
    pub demand_cores: f32,
    /// Estimated peak bytes held by one worker, used to size concurrency
    /// (0 = not memory bound).
    pub bytes_per_unit: u64,
    /// Bytes attributed to one worker in the demand model. Defaults to
    /// [`Self::bytes_per_unit`]; differs when sizing must include bytes that
    /// are reported under another group.
    pub demand_bytes_per_unit: u64,
    /// Lower bound for the resolved value.
    pub min: usize,
    /// Hard workload ceiling, independent of available CPUs and memory.
    pub max: usize,
}

impl SiteSpec {
    const fn new(
        pipeline: Pipeline,
        group: BudgetGroup,
        name: &'static str,
        cores_per_worker: f32,
        bytes_per_unit: u64,
        min: usize,
        max: usize,
    ) -> Self {
        Self {
            name,
            pipeline,
            group,
            cores_per_worker,
            demand_cores: cores_per_worker,
            bytes_per_unit,
            demand_bytes_per_unit: bytes_per_unit,
            min,
            max,
        }
    }

    /// Reports a different byte amount in the demand model than the one used
    /// to size the site. The decoded restore buffers, for example, constrain
    /// prefetch but are accounted for under the Decode group.
    const fn with_demand_bytes(mut self, bytes: u64) -> Self {
        self.demand_bytes_per_unit = bytes;
        self
    }

    /// Marks a site as a handle/slot cap rather than a CPU consumer.
    const fn without_cpu_demand(mut self) -> Self {
        self.demand_cores = 0.0;
        self
    }
}

/// Resolves one site against explicit CPU and byte limits.
///
/// ```text
/// cpu_limit = floor(limits.cpu_cores / cores_per_worker)
/// mem_limit = floor(limits.bytes / bytes_per_unit)
/// auto      = clamp(min(cpu_limit, mem_limit, max), min)
/// value     = explicit ?? auto
/// ```
///
/// Flooring the CPU term is deliberate: it guarantees that the workers a site
/// actually launches never claim more cores than its group was assigned.
pub fn resolve_group(spec: &SiteSpec, limits: GroupLimits, explicit: Option<usize>) -> usize {
    if let Some(value) = explicit {
        return value.max(1);
    }

    let cores_per_worker = spec.cores_per_worker.max(f32::EPSILON);
    let cpu_limit = ((limits.cpu_cores as f32 / cores_per_worker).floor() as usize).max(1);

    let mem_limit = limits
        .bytes
        .checked_div(spec.bytes_per_unit)
        .map(|bytes| bytes as usize)
        .unwrap_or(usize::MAX)
        .max(1);

    cpu_limit
        .min(mem_limit)
        .min(spec.max.max(1))
        .max(spec.min)
        .max(1)
}

/// CPU and byte usage of one pipeline group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupDemand {
    /// CPU cores assigned to the group.
    pub allocated_cores: usize,
    /// Bytes assigned to the group.
    pub allocated_bytes: u64,
    /// CPU cores implied by the resolved workers in the group.
    pub used_cores: usize,
    /// Peak bytes implied by the resolved workers in the group.
    pub used_bytes: u64,
    /// Number of sites contributing to this group.
    pub sites: usize,
}

/// Aggregate demand of the resolved plan.
///
/// Pipelines never run at the same time, so totals are computed per pipeline;
/// the reported usage is the largest pipeline rather than the sum of all of
/// them. Group allocations are derived from [`GroupShares`] and therefore sum
/// to at most one whole machine per pipeline.
#[derive(Debug, Clone, Copy)]
pub struct PlanDemand {
    pub cpu_cap: usize,
    pub memory_budget: u64,
    cells: [GroupDemand; 30],
}

impl PlanDemand {
    fn new(cpu_cap: usize, memory_budget: u64, shares: &GroupShares) -> Self {
        let mut cells = [GroupDemand::default(); 30];
        for pipeline in Pipeline::ALL {
            for group in BudgetGroup::ALL {
                let limits = group_limits(pipeline, group, cpu_cap, memory_budget, shares);
                let index = Self::cell_index(pipeline, group);
                cells[index] = GroupDemand {
                    allocated_cores: limits.cpu_cores,
                    allocated_bytes: limits.bytes,
                    ..GroupDemand::default()
                };
            }
        }
        Self {
            cpu_cap,
            memory_budget,
            cells,
        }
    }

    const fn cell_index(pipeline: Pipeline, group: BudgetGroup) -> usize {
        pipeline.index() * BudgetGroup::ALL.len() + group.index()
    }

    fn cell_mut(&mut self, pipeline: Pipeline, group: BudgetGroup) -> &mut GroupDemand {
        let index = Self::cell_index(pipeline, group);
        &mut self.cells[index]
    }

    fn add_site(&mut self, spec: &SiteSpec, value: usize) {
        let cell = self.cell_mut(spec.pipeline, spec.group);
        cell.sites += 1;
        cell.used_cores = cell
            .used_cores
            .saturating_add((value as f64 * spec.demand_cores as f64).ceil() as usize);
        cell.used_bytes = cell
            .used_bytes
            .saturating_add((value as u64).saturating_mul(spec.demand_bytes_per_unit));
    }

    fn add_bytes(&mut self, pipeline: Pipeline, group: BudgetGroup, bytes: u64) {
        let cell = self.cell_mut(pipeline, group);
        cell.used_bytes = cell.used_bytes.saturating_add(bytes);
    }

    /// Returns the resolved demand for one pipeline group.
    pub fn group(&self, pipeline: Pipeline, group: BudgetGroup) -> GroupDemand {
        self.cells[Self::cell_index(pipeline, group)]
    }

    /// Returns the implied CPU and byte usage of one pipeline.
    pub fn pipeline_totals(&self, pipeline: Pipeline) -> (usize, u64) {
        let mut cores: usize = 0;
        let mut bytes: u64 = 0;
        for group in BudgetGroup::ALL {
            let cell = self.group(pipeline, group);
            cores = cores.saturating_add(cell.used_cores);
            bytes = bytes.saturating_add(cell.used_bytes);
        }
        (cores, bytes)
    }

    /// Returns the busiest main pipeline's CPU and byte usage.
    ///
    /// `Other` is a catch-all for mutually exclusive auxiliary commands and
    /// shared layers, so it is not a real concurrently active pipeline.
    pub fn worst(&self) -> (usize, u64) {
        Pipeline::ALL
            .iter()
            .filter(|pipeline| **pipeline != Pipeline::Other)
            .fold((0usize, 0u64), |(cores, bytes), pipeline| {
                let (p_cores, p_bytes) = self.pipeline_totals(*pipeline);
                (cores.max(p_cores), bytes.max(p_bytes))
            })
    }

    /// Returns `true` when no pipeline exceeds the detected memory cap and no
    /// pipeline's CPU demand exceeds the cap by more than one worker per site.
    ///
    /// The CPU allowance for a single worker per site is unavoidable: even on
    /// a two-core container every pipeline stage must keep at least one worker.
    pub fn is_within_budget(&self) -> bool {
        for pipeline in Pipeline::ALL {
            if pipeline == Pipeline::Other {
                continue;
            }
            let (cores, bytes) = self.pipeline_totals(pipeline);
            let sites: usize = BudgetGroup::ALL
                .iter()
                .map(|group| self.group(pipeline, *group).sites)
                .sum();
            if bytes > self.memory_budget || cores > self.cpu_cap.saturating_add(sites) {
                return false;
            }
        }
        true
    }

    /// Renders the demand for `mapache config info`.
    pub fn summary(&self) -> String {
        let (cores, bytes) = self.worst();
        let mut parts = Vec::with_capacity(Pipeline::ALL.len());
        for pipeline in Pipeline::ALL {
            if pipeline == Pipeline::Other {
                continue;
            }
            let (p_cores, p_bytes) = self.pipeline_totals(pipeline);
            if p_cores == 0 && p_bytes == 0 {
                continue;
            }
            let mut groups = Vec::new();
            for group in BudgetGroup::ALL {
                let cell = self.group(pipeline, group);
                if cell.used_cores == 0 && cell.used_bytes == 0 {
                    continue;
                }
                groups.push(format!(
                    "{}={}c/{}",
                    group.label(),
                    cell.used_cores,
                    utils::format_size_binary(cell.used_bytes, 1),
                ));
            }
            parts.push(format!(
                "{}={}/{} cores {}/{} [{}]",
                pipeline.label(),
                p_cores,
                self.cpu_cap,
                utils::format_size_binary(p_bytes, 1),
                utils::format_size_binary(self.memory_budget, 1),
                groups.join(" "),
            ));
        }
        format!(
            "worst cpu={}/{} bytes={}/{} ({})",
            cores,
            self.cpu_cap,
            utils::format_size_binary(bytes, 1),
            utils::format_size_binary(self.memory_budget, 1),
            parts.join(", "),
        )
    }

    /// Renders a multi-line demand breakdown for `mapache config info`.
    pub fn detailed_summary(&self) -> String {
        let (cores, bytes) = self.worst();
        let mut out = String::new();
        out.push_str(&format!(
            "  Worst:     {}/{} cores, {}/{}\n",
            cores,
            self.cpu_cap,
            utils::format_size_binary(bytes, 1),
            utils::format_size_binary(self.memory_budget, 1),
        ));

        for pipeline in Pipeline::ALL {
            if pipeline == Pipeline::Other {
                continue;
            }
            let (p_cores, p_bytes) = self.pipeline_totals(pipeline);
            if p_cores == 0 && p_bytes == 0 {
                continue;
            }
            out.push_str(&format!(
                "  {:<10} {}/{} cores, {}/{}\n",
                format!("{}:", pipeline.label()),
                p_cores,
                self.cpu_cap,
                utils::format_size_binary(p_bytes, 1),
                utils::format_size_binary(self.memory_budget, 1),
            ));
            for group in BudgetGroup::ALL {
                let cell = self.group(pipeline, group);
                if cell.sites == 0 && cell.used_cores == 0 && cell.used_bytes == 0 {
                    continue;
                }
                out.push_str(&format!(
                    "    {:<8} {} cores, {}\n",
                    format!("{}:", group.label()),
                    cell.used_cores,
                    utils::format_size_binary(cell.used_bytes, 1),
                ));
            }
        }

        if out.ends_with('\n') {
            out.pop();
        }
        out
    }
}

/// The fully resolved plan, computed once per process.
#[derive(Debug, Clone)]
pub struct Plan {
    pub memory_budget: u64,
    pub cpu_scale: f32,
    pub shares: GroupShares,
    pub pack_size: u64,
    /// Modelled CPU and memory demand of the resolved workers.
    pub demand: PlanDemand,

    // Snapshot / bundle create
    pub snapshot_readers: usize,
    pub snapshot_packers: usize,

    // Restore
    pub restore_pack_prefetch: usize,
    pub restore_blob_concurrency: usize,
    pub restore_decoded_budget: u64,
    pub restore_max_open_files: usize,
    /// Largest encoded segment held per prefetch stream.
    pub restore_pack_segment_max_size: u64,

    // GC / clean / repack
    pub gc_repack_workers: usize,
    pub gc_repack_packers: usize,
    pub gc_segment_window: usize,
    pub gc_tree_fetch_concurrency: usize,
    pub gc_snapshot_concurrency: usize,
    pub gc_tree_budget: usize,
    pub gc_delete_concurrency: usize,
    pub gc_delete_small_concurrency: usize,
    pub gc_dropped_scan_concurrency: usize,
    pub gc_dup_scan_concurrency: usize,

    // Verify
    pub verify_parallel: usize,
    pub verify_chunk_size: u64,
    pub verify_batch_size: u64,
    pub verify_batch_concurrency: usize,
    pub verify_footer_concurrency: usize,
    pub verify_aux_concurrency: usize,

    // Auxiliary commands
    pub copy_concurrency: usize,
    pub copy_packers: usize,
    pub sync_concurrency: usize,
    pub rebuild_scan_concurrency: usize,
    pub rebuild_delete_concurrency: usize,
    pub stats_footer_concurrency: usize,
    pub stats_analysis_concurrency: usize,
    pub bundle_stream_concurrency: usize,
    pub cache_threads: usize,

    // Shared layers
    pub list_dir_concurrency: usize,
    pub list_all_files_concurrency: usize,
    pub loader_segment_concurrency: usize,
    pub rayon_threads: usize,
}

impl Plan {
    fn build(
        profile: &SystemProfile,
        policy: &Policy,
        pack_size: u64,
        config: Option<&RuntimeConfig>,
    ) -> Self {
        let budget = policy.budget_bytes(profile);
        let scale = policy.cpu_scale;
        let cpu_cap = ((profile.logical_cpus.max(1) as f32 * scale).ceil() as usize).max(1);
        let mut resolver = Resolver {
            cpu_cap,
            budget,
            shares: policy.shares,
            sites: Vec::new(),
        };

        // Restore buffers are the largest per-worker allocation. Derive both the
        // encoded segment and the decoded budget from the memory budget so a
        // small machine does not reserve an excessive share of RAM, while large
        // machines keep their previous buffer sizes.
        let segment_max = config
            .and_then(|c| c.restore_pack_segment_max_size)
            .unwrap_or_else(|| DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE.min((budget / 64).max(1)))
            .max(1);
        let restore_decoded_budget = config
            .and_then(|c| c.restore_decoded_budget)
            .unwrap_or_else(|| (budget / 32).clamp(MIN_MEMORY_BUDGET, MAX_RESTORE_DECODED_BUDGET));
        // Encoded bytes are attributed to the I/O group; decoded buffers are
        // attributed to Decode below. Prefetch sizing, however, must see both
        // because they are live at the same time per stream.
        let io_stream_bytes = segment_max + MAX_CHUNK_SIZE;
        let prefetch_sizing_bytes = io_stream_bytes + restore_decoded_budget;

        let reader_bytes = 2 * MAX_CHUNK_SIZE + MIN_CHUNK_SIZE;
        let packer_bytes = packer_bytes(pack_size);

        // Snapshot / bundle create.
        let snapshot_readers = resolver.resolve(SiteSpec::new(
            Pipeline::Snapshot,
            BudgetGroup::Scan,
            "snapshot.readers",
            1.0,
            reader_bytes,
            1,
            32,
        ));
        let snapshot_packers = resolver.resolve(SiteSpec::new(
            Pipeline::Snapshot,
            BudgetGroup::Pack,
            "snapshot.packers",
            1.0,
            packer_bytes,
            1,
            16,
        ));

        // Restore. Prefetch is I/O bound (well under one core per stream).
        let restore_explicit_prefetch = config.and_then(|c| c.restore_pack_prefetch);
        let restore_pack_prefetch = resolver.resolve_explicit(
            SiteSpec::new(
                Pipeline::Restore,
                BudgetGroup::Io,
                "runtime.restore-pack-prefetch",
                0.25,
                prefetch_sizing_bytes,
                1,
                8,
            )
            .with_demand_bytes(io_stream_bytes),
            restore_explicit_prefetch,
        );
        let restore_blob_concurrency = resolver.resolve_explicit(
            SiteSpec::new(
                Pipeline::Restore,
                BudgetGroup::Write,
                "runtime.restore-blob-concurrency",
                0.25,
                0,
                1,
                32,
            ),
            config.and_then(|c| c.restore_blob_concurrency),
        );
        let restore_max_open_files = resolver.resolve_explicit(
            SiteSpec::new(
                Pipeline::Restore,
                BudgetGroup::Io,
                "runtime.restore-max-open-files",
                1.0 / 16.0,
                0,
                64,
                512,
            )
            .without_cpu_demand(),
            config.and_then(|c| c.restore_max_open_files),
        );
        // GC / clean / repack.
        let gc_repack_workers = resolver.resolve_explicit(
            SiteSpec::new(
                Pipeline::Gc,
                BudgetGroup::Pack,
                "runtime.gc-repack-concurrency",
                1.0,
                2 * MAX_CHUNK_SIZE,
                1,
                16,
            ),
            config.and_then(|c| c.gc_repack_concurrency),
        );
        let gc_repack_packers = resolver
            .resolve(SiteSpec::new(
                Pipeline::Gc,
                BudgetGroup::Pack,
                "gc.packers",
                2.0,
                packer_bytes,
                1,
                8,
            ))
            .min(gc_repack_workers);
        let gc_segment_window = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Io,
            "gc.segment-window",
            0.25,
            segment_max,
            1,
            8,
        ));
        let gc_tree_fetch_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Io,
            "gc.tree-fetch",
            0.125,
            0,
            1,
            16,
        ));
        let gc_snapshot_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Io,
            "gc.snapshots",
            0.25,
            0,
            1,
            8,
        ));
        let gc_tree_budget = resolver.resolve(
            SiteSpec::new(
                Pipeline::Gc,
                BudgetGroup::Io,
                "gc.tree-budget",
                16.0,
                size::MiB,
                16,
                64,
            )
            .without_cpu_demand(),
        );
        let gc_delete_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Write,
            "gc.delete",
            0.0625,
            0,
            1,
            32,
        ));
        let gc_delete_small_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Write,
            "gc.delete-small",
            0.125,
            0,
            1,
            16,
        ));
        let gc_dropped_scan_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Io,
            "gc.dropped-scan",
            0.25,
            0,
            1,
            16,
        ));
        let gc_dup_scan_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Gc,
            BudgetGroup::Io,
            "gc.dup-scan",
            0.125,
            0,
            1,
            16,
        ));

        // Verify. The Decode group supplies both the pack count and the
        // per-pack byte allowance used to size chunks and batches.
        let verify_parallel = resolver.resolve(SiteSpec::new(
            Pipeline::Verify,
            BudgetGroup::Decode,
            "verify.packs",
            2.0,
            0,
            1,
            16,
        ));
        let verify_limits = group_limits(
            Pipeline::Verify,
            BudgetGroup::Decode,
            cpu_cap,
            budget,
            &policy.shares,
        );
        let per_pack_budget = verify_limits.bytes / verify_parallel.max(1) as u64;
        let cpu_batch_limit = (verify_limits.cpu_cores / verify_parallel.max(1)).clamp(1, 4);
        let memory_batch_limit =
            (per_pack_budget / (3 * MIN_VERIFY_CHUNK_SIZE)).clamp(1, 4) as usize;
        let verify_batch_concurrency = cpu_batch_limit.min(memory_batch_limit);
        let verify_chunk_size = (per_pack_budget / (3 * verify_batch_concurrency as u64))
            .clamp(MIN_VERIFY_CHUNK_SIZE, 64 * size::MiB);
        let verify_batch_size = verify_chunk_size;
        let verify_footer_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Verify,
            BudgetGroup::Io,
            "verify.footer-dump",
            0.25,
            0,
            1,
            16,
        ));
        let verify_aux_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Verify,
            BudgetGroup::Io,
            "verify.aux",
            0.25,
            0,
            1,
            8,
        ));

        // Auxiliary commands. These keep their historical whole-machine
        // limits: they never run concurrently with the main pipelines.
        let copy_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "copy.stream",
            0.25,
            32 * size::MiB,
            1,
            16,
        ));
        let copy_packers = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "copy.packers",
            2.0,
            packer_bytes,
            1,
            16,
        ));
        let sync_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "sync.stream",
            0.25,
            32 * size::MiB,
            1,
            16,
        ));
        let rebuild_scan_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "rebuild-index.scan",
            0.5,
            0,
            1,
            16,
        ));
        let rebuild_delete_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "rebuild-index.delete",
            1.0,
            0,
            1,
            8,
        ));
        let stats_footer_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "stats.footer",
            0.5,
            0,
            1,
            16,
        ));
        let stats_analysis_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "stats.analysis",
            1.0,
            0,
            1,
            8,
        ));
        let bundle_stream_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "bundle.stream",
            0.5,
            0,
            1,
            16,
        ));
        let cache_threads = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "cache.threads",
            1.0,
            0,
            1,
            8,
        ));

        // Shared layers.
        let list_dir_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "backend.list-dir",
            0.5,
            0,
            1,
            32,
        ));
        let list_all_files_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "repo.list-files",
            1.0,
            0,
            1,
            8,
        ));
        let loader_segment_concurrency = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "loader.segments",
            1.0,
            segment_max,
            1,
            8,
        ));
        let rayon_threads = resolver.resolve(SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            "rayon.threads",
            1.0,
            0,
            1,
            64,
        ));

        let mut demand = PlanDemand::new(cpu_cap, budget, &policy.shares);
        for (spec, value) in &resolver.sites {
            demand.add_site(spec, *value);
        }
        demand.add_bytes(
            Pipeline::Restore,
            BudgetGroup::Decode,
            (restore_pack_prefetch as u64).saturating_mul(restore_decoded_budget),
        );
        demand.add_bytes(
            Pipeline::Verify,
            BudgetGroup::Decode,
            (verify_parallel as u64)
                .saturating_mul(verify_chunk_size)
                .saturating_mul(verify_batch_concurrency as u64)
                .saturating_mul(3),
        );

        Self {
            memory_budget: budget,
            cpu_scale: scale,
            shares: policy.shares,
            pack_size,
            demand,
            snapshot_readers,
            snapshot_packers,
            restore_pack_prefetch,
            restore_blob_concurrency,
            restore_decoded_budget,
            restore_max_open_files,
            restore_pack_segment_max_size: segment_max,
            gc_repack_workers,
            gc_repack_packers,
            gc_segment_window,
            gc_tree_fetch_concurrency,
            gc_snapshot_concurrency,
            gc_tree_budget,
            gc_delete_concurrency,
            gc_delete_small_concurrency,
            gc_dropped_scan_concurrency,
            gc_dup_scan_concurrency,
            verify_parallel,
            verify_chunk_size,
            verify_batch_size,
            verify_batch_concurrency,
            verify_footer_concurrency,
            verify_aux_concurrency,
            copy_concurrency,
            copy_packers,
            sync_concurrency,
            rebuild_scan_concurrency,
            rebuild_delete_concurrency,
            stats_footer_concurrency,
            stats_analysis_concurrency,
            bundle_stream_concurrency,
            cache_threads,
            list_dir_concurrency,
            list_all_files_concurrency,
            loader_segment_concurrency,
            rayon_threads,
        }
    }

    /// Renders the plan for `mapache config info` and the startup log.
    pub fn summary(&self) -> String {
        format!(
            "snapshot(readers={}, packers={}) restore(prefetch={}, blobs={}, decoded={}, open-files={}) \
             gc(workers={}, packers={}, window={}, tree-fetch={}, snapshots={}, tree-budget={}, delete={}/{}, scans={}/{}) \
             verify(packs={}, chunk={}, batch={}x{}) copy(stream={}, packers={}) sync={} \
             rebuild-index(scan={}, delete={}) stats(footer={}, analysis={}) bundle={} cache={} \
             shared(list-dir={}, list-files={}, loader-segments={}, rayon={})",
            self.snapshot_readers,
            self.snapshot_packers,
            self.restore_pack_prefetch,
            self.restore_blob_concurrency,
            utils::format_size_binary(self.restore_decoded_budget, 1),
            self.restore_max_open_files,
            self.gc_repack_workers,
            self.gc_repack_packers,
            self.gc_segment_window,
            self.gc_tree_fetch_concurrency,
            self.gc_snapshot_concurrency,
            self.gc_tree_budget,
            self.gc_delete_concurrency,
            self.gc_delete_small_concurrency,
            self.gc_dropped_scan_concurrency,
            self.gc_dup_scan_concurrency,
            self.verify_parallel,
            utils::format_size_binary(self.verify_chunk_size, 1),
            self.verify_batch_size,
            self.verify_batch_concurrency,
            self.copy_concurrency,
            self.copy_packers,
            self.sync_concurrency,
            self.rebuild_scan_concurrency,
            self.rebuild_delete_concurrency,
            self.stats_footer_concurrency,
            self.stats_analysis_concurrency,
            self.bundle_stream_concurrency,
            self.cache_threads,
            self.list_dir_concurrency,
            self.list_all_files_concurrency,
            self.loader_segment_concurrency,
            self.rayon_threads,
        )
    }

    /// Renders a multi-line plan breakdown for `mapache config info`.
    pub fn detailed_summary(&self) -> String {
        format!(
            "  Snapshot   readers={}, packers={}\n\
             \x20 Restore    prefetch={}, blobs={}, decoded={}, open-files={}\n\
             \x20 GC         workers={}, packers={}, window={}, tree-fetch={}, snapshots={}, tree-budget={}, delete={}/{}, scans={}/{}\n\
             \x20 Verify     packs={}, chunk={}, batch-size={}, batch-concurrency={}\n\
             \x20 Copy       stream={}, packers={}\n\
             \x20 Sync       concurrency={}\n\
             \x20 Rebuild    scan={}, delete={}\n\
             \x20 Stats      footer={}, analysis={}\n\
             \x20 Bundle     stream={}\n\
             \x20 Cache      threads={}\n\
             \x20 Shared     list-dir={}, list-files={}, loader-segments={}, rayon={}",
            self.snapshot_readers,
            self.snapshot_packers,
            self.restore_pack_prefetch,
            self.restore_blob_concurrency,
            utils::format_size_binary(self.restore_decoded_budget, 1),
            self.restore_max_open_files,
            self.gc_repack_workers,
            self.gc_repack_packers,
            self.gc_segment_window,
            self.gc_tree_fetch_concurrency,
            self.gc_snapshot_concurrency,
            self.gc_tree_budget,
            self.gc_delete_concurrency,
            self.gc_delete_small_concurrency,
            self.gc_dropped_scan_concurrency,
            self.gc_dup_scan_concurrency,
            self.verify_parallel,
            utils::format_size_binary(self.verify_chunk_size, 1),
            utils::format_size_binary(self.verify_batch_size, 1),
            self.verify_batch_concurrency,
            self.copy_concurrency,
            self.copy_packers,
            self.sync_concurrency,
            self.rebuild_scan_concurrency,
            self.rebuild_delete_concurrency,
            self.stats_footer_concurrency,
            self.stats_analysis_concurrency,
            self.bundle_stream_concurrency,
            self.cache_threads,
            self.list_dir_concurrency,
            self.list_all_files_concurrency,
            self.loader_segment_concurrency,
            self.rayon_threads,
        )
    }
}

/// Estimated peak bytes held by one packer worker.
fn packer_bytes(pack_size: u64) -> u64 {
    pack_size
        .saturating_add(PACK_BUFFER_SLACK)
        .saturating_add(2 * MAX_CHUNK_SIZE)
}

struct Resolver {
    cpu_cap: usize,
    budget: u64,
    shares: GroupShares,
    sites: Vec<(SiteSpec, usize)>,
}

impl Resolver {
    fn limits(&self, spec: &SiteSpec) -> GroupLimits {
        site_limits(spec, self.cpu_cap, self.budget, &self.shares)
    }

    fn resolve(&mut self, spec: SiteSpec) -> usize {
        let value = resolve_group(&spec, self.limits(&spec), None);
        self.sites.push((spec, value));
        value
    }

    fn resolve_explicit(&mut self, spec: SiteSpec, explicit: Option<usize>) -> usize {
        let limits = self.limits(&spec);
        let value = resolve_group(&spec, limits, explicit);
        if explicit.is_some() {
            warn_if_over_budget_value(&spec, value, limits.bytes);
        }
        self.sites.push((spec, value));
        value
    }
}

/// Warns when an explicit value's modelled footprint exceeds the budget.
fn warn_if_over_budget_value(spec: &SiteSpec, value: usize, budget: u64) {
    if spec.bytes_per_unit == 0 {
        return;
    }
    let footprint = (value as u64).saturating_mul(spec.bytes_per_unit);
    if footprint > budget {
        tracing::warn!(
            target: "parallelism",
            "{}: explicit value {} needs about {} but the memory budget is {}; \
             reduce it or raise memory-budget",
            spec.name,
            value,
            utils::format_size_binary(footprint, 1),
            utils::format_size_binary(budget, 1),
        );
    }
}

/// Warns when an explicit value exceeds the allowance of its budget group.
pub fn warn_if_over_group_budget(
    pipeline: Pipeline,
    group: BudgetGroup,
    name: &str,
    value: usize,
    bytes_per_unit: u64,
) {
    let plan = plan();
    let limits = group_limits(
        pipeline,
        group,
        plan.demand.cpu_cap,
        plan.memory_budget,
        &plan.shares,
    );
    let count = group_site_count(pipeline, group).max(1) as u64;
    warn_if_over(name, value, bytes_per_unit, limits.bytes / count);
}

fn warn_if_over(name: &str, value: usize, bytes_per_unit: u64, available: u64) {
    if bytes_per_unit == 0 {
        return;
    }
    let footprint = (value as u64).saturating_mul(bytes_per_unit);
    if footprint > available {
        tracing::warn!(
            target: "parallelism",
            "{}: explicit value {} needs about {} but the available budget is {}; \
             reduce it or raise memory-budget",
            name,
            value,
            utils::format_size_binary(footprint, 1),
            utils::format_size_binary(available, 1),
        );
    }
}

/// Estimated peak bytes for one snapshot reader.
pub fn snapshot_reader_bytes() -> u64 {
    2 * MAX_CHUNK_SIZE + MIN_CHUNK_SIZE
}

/// Estimated peak bytes for one snapshot or GC packer.
pub fn packer_worker_bytes(pack_size: u64) -> u64 {
    packer_bytes(pack_size)
}

static PLAN: OnceLock<Plan> = OnceLock::new();

/// Initializes the process-wide plan. Must be called before the first command
/// runs; later calls are ignored.
pub fn init(policy: Policy, pack_size: u64, config: Option<&RuntimeConfig>) {
    let profile = sysinfo::profile();
    let budget = policy.budget_bytes(profile);
    let plan = Plan::build(profile, &policy, pack_size, config);
    let summary = plan.summary();
    let demand_summary = plan.demand.summary();
    let demand_within_budget = plan.demand.is_within_budget();
    let rayon_threads = plan.rayon_threads;
    if PLAN.set(plan).is_err() {
        tracing::warn!(
            target: "parallelism",
            "parallelism plan already initialized; ignoring re-initialization"
        );
        return;
    }
    tracing::info!(
        target: "parallelism",
        "cpus={} memory={} budget={} cpu-scale={:.2}; {}; demand {}",
        profile.logical_cpus,
        utils::format_size_binary(profile.available_memory, 1),
        utils::format_size_binary(budget, 1),
        policy.cpu_scale,
        summary,
        demand_summary,
    );
    if !demand_within_budget {
        tracing::warn!(
            target: "parallelism",
            "resolved demand exceeds the detected CPU or memory budget: {demand_summary}"
        );
    }
    if let Err(error) = rayon::ThreadPoolBuilder::new()
        .num_threads(rayon_threads)
        .build_global()
    {
        tracing::warn!(
            target: "parallelism",
            "could not configure the global Rayon pool: {error}"
        );
    }
}

/// Returns the process-wide plan, initializing it with defaults if necessary.
pub fn plan() -> &'static Plan {
    PLAN.get_or_init(|| {
        Plan::build(
            sysinfo::profile(),
            &Policy::default(),
            DEFAULT_PACK_SIZE,
            None,
        )
    })
}

const MIN_BLOCKING_THREADS: usize = 4;
const MAX_BLOCKING_THREADS: usize = 128;

fn blocking_thread_cap(cpus: usize) -> usize {
    cpus.saturating_mul(2)
        .clamp(MIN_BLOCKING_THREADS, MAX_BLOCKING_THREADS)
}

/// Number of Tokio async worker threads for the main runtime.
///
/// This intentionally does not read the parallelism plan: the runtime must be
/// built before argument parsing can initialize it.
pub fn runtime_worker_threads() -> usize {
    sysinfo::profile().logical_cpus.max(1)
}

/// Upper bound for Tokio's blocking pool.
///
/// Tokio's default maximum of 512 threads is far above what mapache needs and
/// made long `verify --read-packs` runs spawn hundreds of mostly idle threads.
/// Twice the usable CPU count leaves room for file I/O and nested work while
/// keeping the total thread count predictable.
pub fn runtime_max_blocking_threads() -> usize {
    if let Some(value) = get_envvar(MAX_BLOCKING_THREADS_ENVVAR)
        && let Ok(value) = value.trim().parse::<usize>()
        && value > 0
    {
        return value;
    }
    blocking_thread_cap(runtime_worker_threads())
}

/// Granularity of the byte limiters. One permit represents one MiB, which
/// keeps permit counts small even for large memory budgets.
const MEMORY_UNIT: u64 = size::MiB;

#[derive(Debug, Clone)]
struct SharedLimiter {
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl SharedLimiter {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    fn used(&self) -> usize {
        self.capacity
            .saturating_sub(self.semaphore.available_permits())
    }
}

/// A hard cap on the bytes of large buffers held by concurrently active work.
///
/// The permit is acquired before the buffer is allocated and must be kept
/// alive until the buffer is dropped. This turns the memory model from a
/// planning estimate into an actual ceiling for restore and verify buffers.
#[derive(Debug, Clone)]
pub struct MemoryLimiter {
    semaphore: Arc<Semaphore>,
    capacity_bytes: u64,
}

impl MemoryLimiter {
    fn new(capacity_bytes: u64) -> Self {
        let units = Self::units(capacity_bytes).max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(units)),
            capacity_bytes,
        }
    }

    fn units(bytes: u64) -> usize {
        let units = bytes.div_ceil(MEMORY_UNIT).max(1);
        u32::try_from(units).unwrap_or(u32::MAX) as usize
    }

    /// Acquires enough permits for `bytes`, capped at the limiter's capacity.
    /// A request larger than the whole group still proceeds, but occupies every
    /// permit until it finishes rather than deadlocking.
    pub async fn acquire(&self, bytes: u64) -> Option<OwnedSemaphorePermit> {
        let units = Self::units(bytes).min(Self::units(self.capacity_bytes));
        self.semaphore
            .clone()
            .acquire_many_owned(units as u32)
            .await
            .ok()
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn used_bytes(&self) -> u64 {
        let used = Self::units(self.capacity_bytes)
            .saturating_sub(self.semaphore.available_permits()) as u64;
        used.saturating_mul(MEMORY_UNIT).min(self.capacity_bytes)
    }
}

static CPU_LIMITER: OnceLock<SharedLimiter> = OnceLock::new();
static WRITE_LIMITER: OnceLock<SharedLimiter> = OnceLock::new();
static RESTORE_DECODE_MEMORY: OnceLock<MemoryLimiter> = OnceLock::new();
static VERIFY_DECODE_MEMORY: OnceLock<MemoryLimiter> = OnceLock::new();

fn cpu_shared() -> &'static SharedLimiter {
    CPU_LIMITER.get_or_init(|| SharedLimiter::new(plan().demand.cpu_cap))
}

fn write_shared() -> &'static SharedLimiter {
    WRITE_LIMITER.get_or_init(|| {
        let plan = plan();
        let natural = plan
            .restore_pack_prefetch
            .saturating_mul(plan.restore_blob_concurrency)
            .max(8);
        SharedLimiter::new(plan.restore_max_open_files.min(natural).max(8))
    })
}

/// Shared cap on concurrently running CPU-bound blocking work.
///
/// Each permit represents one active CPU-bound task. Verify and restore keep
/// the work inside a permitted task on that task's own thread, so stage
/// concurrency cannot fan out into extra Rayon workers and multiply the cap.
pub fn cpu_limiter() -> Arc<Semaphore> {
    cpu_shared().semaphore.clone()
}

/// Shared cap on concurrently running file writes.
///
/// Sized from the restore pipeline so `prefetch × blob-concurrency` cannot
/// turn into hundreds of simultaneous blocking writes.
pub fn write_limiter() -> Arc<Semaphore> {
    write_shared().semaphore.clone()
}

/// Hard cap on decoded restore buffers, sized from the Restore Decode group.
pub fn restore_decode_memory() -> &'static MemoryLimiter {
    RESTORE_DECODE_MEMORY.get_or_init(|| {
        MemoryLimiter::new(
            plan()
                .demand
                .group(Pipeline::Restore, BudgetGroup::Decode)
                .allocated_bytes,
        )
    })
}

/// Hard cap on verify buffers, sized from the Verify Decode group.
pub fn verify_decode_memory() -> &'static MemoryLimiter {
    VERIFY_DECODE_MEMORY.get_or_init(|| {
        MemoryLimiter::new(
            plan()
                .demand
                .group(Pipeline::Verify, BudgetGroup::Decode)
                .allocated_bytes,
        )
    })
}

/// Acquires one CPU-bound work slot, or `None` if the limiter was closed.
pub async fn acquire_cpu() -> Option<OwnedSemaphorePermit> {
    cpu_limiter().acquire_owned().await.ok()
}

/// Acquires one file-write slot, or `None` if the limiter was closed.
pub async fn acquire_write() -> Option<OwnedSemaphorePermit> {
    write_limiter().acquire_owned().await.ok()
}

/// Acquires restore decode buffer space, or `None` if the limiter was closed.
pub async fn acquire_restore_decode_memory(bytes: u64) -> Option<OwnedSemaphorePermit> {
    restore_decode_memory().acquire(bytes).await
}

/// Acquires verify buffer space, or `None` if the limiter was closed.
pub async fn acquire_verify_decode_memory(bytes: u64) -> Option<OwnedSemaphorePermit> {
    verify_decode_memory().acquire(bytes).await
}

/// Current limiter utilization, for diagnostics and `config info`.
#[derive(Debug, Clone, Copy)]
pub struct LimiterStatus {
    pub cpu_used: usize,
    pub cpu_capacity: usize,
    pub writes_used: usize,
    pub writes_capacity: usize,
    pub restore_decode_used_bytes: u64,
    pub restore_decode_capacity_bytes: u64,
    pub verify_decode_used_bytes: u64,
    pub verify_decode_capacity_bytes: u64,
}

pub fn limiter_status() -> LimiterStatus {
    let restore = restore_decode_memory();
    let verify = verify_decode_memory();
    LimiterStatus {
        cpu_used: cpu_shared().used(),
        cpu_capacity: cpu_shared().capacity,
        writes_used: write_shared().used(),
        writes_capacity: write_shared().capacity,
        restore_decode_used_bytes: restore.used_bytes(),
        restore_decode_capacity_bytes: restore.capacity_bytes(),
        verify_decode_used_bytes: verify.used_bytes(),
        verify_decode_capacity_bytes: verify.capacity_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(cpus: usize, available_gib: u64) -> SystemProfile {
        SystemProfile {
            logical_cpus: cpus,
            total_memory: available_gib * size::GiB,
            available_memory: available_gib * size::GiB,
            cgroup_cpu_limit: false,
            cgroup_memory_limit: false,
        }
    }

    fn build(cpus: usize, available_gib: u64, pack_size_mib: u64) -> Plan {
        let profile = profile(cpus, available_gib);
        let policy = Policy::default();
        Plan::build(&profile, &policy, pack_size_mib * size::MiB, None)
    }

    fn test_spec(name: &'static str, cores: f32, bytes: u64, min: usize, max: usize) -> SiteSpec {
        SiteSpec::new(
            Pipeline::Other,
            BudgetGroup::Other,
            name,
            cores,
            bytes,
            min,
            max,
        )
    }

    #[test]
    fn blocking_thread_cap_is_bounded() {
        assert_eq!(blocking_thread_cap(1), MIN_BLOCKING_THREADS);
        assert_eq!(blocking_thread_cap(8), 16);
        assert_eq!(blocking_thread_cap(24), 48);
        assert_eq!(blocking_thread_cap(64), MAX_BLOCKING_THREADS);
        assert_eq!(blocking_thread_cap(256), MAX_BLOCKING_THREADS);
    }

    #[test]
    fn resolve_formula_is_cpu_bound_for_small_machines() {
        let spec = test_spec("test", 2.0, 16 * size::MiB, 1, 32);
        let limits = GroupLimits {
            cpu_cores: 2,
            bytes: 2 * size::GiB,
        };
        assert_eq!(resolve_group(&spec, limits, None), 1);
    }

    #[test]
    fn resolve_formula_is_memory_bound_for_small_budget() {
        let spec = test_spec("test", 2.0, 256 * size::MiB, 1, 16);
        let limits = GroupLimits {
            cpu_cores: 8,
            bytes: 7 * size::GiB,
        };
        // Memory allows ~28 workers but the CPU group allows 4.
        assert_eq!(resolve_group(&spec, limits, None), 4);
    }

    #[test]
    fn explicit_values_win() {
        let spec = test_spec("test", 2.0, 256 * size::MiB, 1, 16);
        let limits = GroupLimits {
            cpu_cores: 2,
            bytes: 2 * size::GiB,
        };
        assert_eq!(resolve_group(&spec, limits, Some(9)), 9);
    }

    #[test]
    fn group_shares_never_exceed_one_machine() {
        assert!(GroupShares::default().validate().is_ok());
    }

    #[test]
    fn group_share_overrides_are_validated() {
        let mut overrides = BTreeMap::new();
        overrides.insert("snapshot-scan-cpu".to_string(), 0.5);
        overrides.insert("snapshot-pack-cpu".to_string(), 0.6);
        assert!(GroupShares::from_overrides(&overrides).is_err());

        let mut valid = BTreeMap::new();
        valid.insert("restore-decode-memory".to_string(), 0.35);
        assert!(GroupShares::from_overrides(&valid).is_ok());

        let mut bad_key = BTreeMap::new();
        bad_key.insert("restore-decode-bogus".to_string(), 0.5);
        assert!(GroupShares::from_overrides(&bad_key).is_err());
    }

    #[test]
    fn site_counts_match_group_sharing_assumptions() {
        let plan = build(8, 16, 16);
        for pipeline in Pipeline::ALL {
            if pipeline == Pipeline::Other {
                continue;
            }
            for group in BudgetGroup::ALL {
                let cell = plan.demand.group(pipeline, group);
                if cell.sites == 0 {
                    continue;
                }
                assert_eq!(
                    cell.sites,
                    group_site_count(pipeline, group),
                    "{pipeline:?}/{group:?}"
                );
            }
        }
    }

    #[test]
    fn demand_stays_within_budget_across_profiles() {
        for (cpus, gib) in [(2, 2), (8, 16), (64, 256)] {
            let plan = build(cpus, gib, 16);
            assert!(
                plan.demand.is_within_budget(),
                "{} CPUs / {gib} GiB: {}",
                cpus,
                plan.demand.summary()
            );
        }
    }

    #[test]
    fn derived_buffers_fit_their_group_allocation() {
        for (cpus, gib) in [(2, 2), (8, 16), (64, 256)] {
            let plan = build(cpus, gib, 16);
            let restore = plan.demand.group(Pipeline::Restore, BudgetGroup::Decode);
            assert!(
                restore.used_bytes <= restore.allocated_bytes,
                "restore decode {}/{}",
                restore.used_bytes,
                restore.allocated_bytes
            );
            let verify = plan.demand.group(Pipeline::Verify, BudgetGroup::Decode);
            assert!(
                verify.used_bytes <= verify.allocated_bytes,
                "verify decode {}/{}",
                verify.used_bytes,
                verify.allocated_bytes
            );
        }
    }

    #[test]
    fn tiny_container_stays_within_two_workers() {
        let plan = build(2, 2, 16);
        assert!(plan.snapshot_readers <= 2);
        assert!(plan.snapshot_packers <= 2);
        // Sizing prefetch against the decoded buffer as well as the encoded
        // segment keeps small containers at a low stream count.
        assert!(plan.restore_pack_prefetch <= 2);
        assert!(plan.gc_repack_workers <= 2);
        assert!(plan.verify_parallel <= 2);
    }

    #[test]
    fn small_machine_restore_buffers_stay_proportional() {
        for (cpus, gib) in [(2, 1), (4, 2), (4, 4), (8, 4)] {
            let plan = build(cpus, gib, 16);
            let live = (plan.restore_pack_prefetch as u64).saturating_mul(
                plan.restore_decoded_budget + plan.restore_pack_segment_max_size + MAX_CHUNK_SIZE,
            );
            assert!(
                live <= plan.memory_budget / 6,
                "{cpus}c/{gib}GiB: restore buffers {live} exceed 1/6 of budget {}",
                plan.memory_budget
            );
            assert!(
                plan.restore_decoded_budget < MAX_RESTORE_DECODED_BUDGET,
                "{cpus}c/{gib}GiB decoded budget must scale below the large-machine cap"
            );
            if gib <= 2 {
                assert!(
                    plan.restore_pack_segment_max_size < DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE,
                    "{cpus}c/{gib}GiB segment must shrink with the budget"
                );
            }
        }
    }

    #[test]
    fn restore_demand_splits_encoded_and_decoded_bytes() {
        let plan = build(8, 16, 16);
        let streams = plan.restore_pack_prefetch as u64;
        let io = plan.demand.group(Pipeline::Restore, BudgetGroup::Io);
        let decode = plan.demand.group(Pipeline::Restore, BudgetGroup::Decode);
        assert_eq!(
            io.used_bytes,
            streams * (plan.restore_pack_segment_max_size + MAX_CHUNK_SIZE)
        );
        assert_eq!(decode.used_bytes, streams * plan.restore_decoded_budget);
    }

    #[test]
    fn budgets_are_clamped_to_minimum() {
        let p = profile(1, 0);
        assert_eq!(Policy::default().budget_bytes(&p), MIN_MEMORY_BUDGET);
        assert_eq!(
            Policy {
                memory_budget: MemoryBudget::Fixed(0),
                cpu_scale: 1.0,
                shares: GroupShares::default(),
            }
            .budget_bytes(&p),
            MIN_MEMORY_BUDGET
        );
    }

    #[test]
    fn auto_budget_uses_seventy_percent_of_available_memory() {
        let p = profile(4, 10);
        assert_eq!(Policy::default().budget_bytes(&p), 7 * size::GiB);
    }

    #[test]
    fn explicit_memory_budget_is_not_raised_to_a_minimum() {
        let p = profile(4, 16);
        let policy = Policy {
            memory_budget: MemoryBudget::Fixed(64 * size::MiB),
            cpu_scale: 1.0,
            shares: GroupShares::default(),
        };
        assert_eq!(policy.budget_bytes(&p), 64 * size::MiB);
    }

    #[test]
    fn memory_budget_parses_units() {
        assert_eq!("auto".parse::<MemoryBudget>(), Ok(MemoryBudget::Auto));
        assert_eq!(
            "4GiB".parse::<MemoryBudget>(),
            Ok(MemoryBudget::Fixed(4 * size::GiB))
        );
        assert!("0".parse::<MemoryBudget>().is_err());
        assert!("not-a-size".parse::<MemoryBudget>().is_err());
    }

    #[test]
    fn cpu_scale_validation() {
        assert!(is_valid_cpu_scale(1.0));
        assert!(is_valid_cpu_scale(0.25));
        assert!(is_valid_cpu_scale(4.0));
        assert!(!is_valid_cpu_scale(0.0));
        assert!(!is_valid_cpu_scale(4.1));
        assert!(!is_valid_cpu_scale(f32::NAN));
    }
}
