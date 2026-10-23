// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-disk background I/O budget for the data scanner.
//!
//! Each local disk owns two token buckets, one for operations per second and
//! one for bytes per second. Directory opens, enumerated entries, scanner
//! metadata/stat operations, and metadata bytes charge the relevant buckets;
//! an exhausted bucket delays the walk. This paces scanner work on top of the
//! local-disk foreground-priority admission gate: admission decides *when*
//! background I/O may start while foreground requests are active, while the
//! budget slows scanner work even when the disk is idle. Bounded directory
//! prefetch and the maximum wait mean this is not a strict OS-level IOPS or
//! bandwidth limit. A charge can never bypass the admission gate.
//!
//! A zero or tiny budget still admits one charge per
//! [`SCANNER_DISK_IO_BUDGET_MAX_WAIT`], so a walk keeps making progress
//! instead of starving.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use rustfs_config::{
    DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_BYTES_PER_SEC, DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_IOPS,
    DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_BYTES_PER_SEC, DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_IOPS, ScannerSpeed,
};
use rustfs_scanner_metrics::metrics::global_metrics;
use serde::Serialize;
use tokio::time::{Duration, Instant};
use tracing::info;

const LOG_COMPONENT_SCANNER: &str = "scanner";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
const EVENT_SCANNER_DISK_IO_BUDGET: &str = "scanner_disk_io_budget";

const OTEL_SCANNER_DISK_IO_BUDGET_WAIT_SECONDS: &str = "rustfs_scanner_disk_io_budget_wait_seconds";
const OTEL_SCANNER_DISK_IO_BUDGET_WAITS_TOTAL: &str = "rustfs_scanner_disk_io_budget_waits_total";

/// Longest wait one budget charge may impose.
///
/// This is the minimum service guarantee: whatever the configured rate, a
/// walk is admitted at least once per window.
pub const SCANNER_DISK_IO_BUDGET_MAX_WAIT: Duration = Duration::from_secs(1);

/// Burst allowance expressed in seconds of the configured rate. One second
/// lets a short directory finish without pacing while a long walk converges
/// on the configured rate.
const BURST_WINDOW_SECS: f64 = 1.0;

const BUDGET_LIMIT_AUTO: &str = "auto";
const BUDGET_LIMIT_OFF: &str = "off";

/// Media class of a local disk as far as the scanner can tell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScannerDiskMedia {
    Rotational,
    Flash,
    /// Media could not be detected; budgets fall back to the rotational
    /// defaults because a spindle is the case that needs protection.
    Unknown,
}

impl ScannerDiskMedia {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rotational => "rotational",
            Self::Flash => "flash",
            Self::Unknown => "unknown",
        }
    }
}

/// One configured budget axis: media default, no cap, or an explicit rate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScannerIoBudgetLimit {
    Auto,
    Unlimited,
    Explicit(u64),
}

impl ScannerIoBudgetLimit {
    /// Parse the `auto` | `off` | `<non-negative integer>` spelling shared by
    /// the persisted keys and the environment variables.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            BUDGET_LIMIT_AUTO => Some(Self::Auto),
            BUDGET_LIMIT_OFF | "disabled" | "unlimited" | "none" => Some(Self::Unlimited),
            number => number.parse::<u64>().ok().map(Self::Explicit),
        }
    }
}

impl std::fmt::Display for ScannerIoBudgetLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => f.write_str(BUDGET_LIMIT_AUTO),
            Self::Unlimited => f.write_str(BUDGET_LIMIT_OFF),
            Self::Explicit(value) => write!(f, "{value}"),
        }
    }
}

/// Configured budget inputs, resolved per disk once its media is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScannerDiskIoBudgetConfig {
    pub iops: ScannerIoBudgetLimit,
    pub bytes_per_sec: ScannerIoBudgetLimit,
    /// The `fastest` preset means "do not pace"; `auto` budgets resolve to
    /// unlimited under it while explicit values still apply.
    pub speed: ScannerSpeed,
}

impl ScannerDiskIoBudgetConfig {
    pub const DEFAULT: Self = Self {
        iops: ScannerIoBudgetLimit::Auto,
        bytes_per_sec: ScannerIoBudgetLimit::Auto,
        speed: ScannerSpeed::Default,
    };
}

impl Default for ScannerDiskIoBudgetConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Effective per-disk rates; `None` means that axis is not capped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScannerDiskIoBudgetLimits {
    pub iops: Option<u64>,
    pub bytes_per_sec: Option<u64>,
}

fn resolve_budget_axis(limit: ScannerIoBudgetLimit, speed: ScannerSpeed, media_default: u64) -> Option<u64> {
    match limit {
        ScannerIoBudgetLimit::Explicit(value) => Some(value),
        ScannerIoBudgetLimit::Unlimited => None,
        ScannerIoBudgetLimit::Auto if speed == ScannerSpeed::Fastest => None,
        ScannerIoBudgetLimit::Auto => Some(media_default),
    }
}

/// Resolve the configured budget for one disk's media. Unknown media uses
/// the rotational defaults.
pub fn resolve_disk_io_budget(config: ScannerDiskIoBudgetConfig, media: ScannerDiskMedia) -> ScannerDiskIoBudgetLimits {
    let (default_iops, default_bytes) = match media {
        ScannerDiskMedia::Flash => (
            DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_IOPS,
            DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_BYTES_PER_SEC,
        ),
        ScannerDiskMedia::Rotational | ScannerDiskMedia::Unknown => (
            DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_IOPS,
            DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_BYTES_PER_SEC,
        ),
    };
    ScannerDiskIoBudgetLimits {
        iops: resolve_budget_axis(config.iops, config.speed, default_iops),
        bytes_per_sec: resolve_budget_axis(config.bytes_per_sec, config.speed, default_bytes),
    }
}

/// Rates, charges, and wait totals are counts; precision loss above 2^53
/// is irrelevant for a per-second budget.
fn count_as_f64(value: u64) -> f64 {
    value as f64
}

#[derive(Debug)]
struct TokenBucket {
    rate_per_sec: f64,
    capacity: f64,
    tokens: f64,
    refilled_at: Instant,
}

impl TokenBucket {
    fn new(rate_per_sec: u64, now: Instant) -> Self {
        let rate_per_sec = count_as_f64(rate_per_sec);
        let capacity = Self::capacity_for(rate_per_sec);
        Self {
            rate_per_sec,
            capacity,
            tokens: capacity,
            refilled_at: now,
        }
    }

    fn capacity_for(rate_per_sec: f64) -> f64 {
        (rate_per_sec * BURST_WINDOW_SECS).max(1.0)
    }

    fn reconfigure(&mut self, rate_per_sec: u64, now: Instant) {
        self.refill(now);
        self.rate_per_sec = count_as_f64(rate_per_sec);
        self.capacity = Self::capacity_for(self.rate_per_sec);
        self.tokens = self.tokens.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        self.refilled_at = now;
    }

    /// Charge `amount` and return how long the caller must wait before the
    /// bucket is non-negative again, capped at the max wait. Debt is bounded
    /// to one burst window so an oversized charge costs at most the max wait
    /// plus one window, never a permanent stall.
    fn charge(&mut self, amount: f64, now: Instant) -> Duration {
        self.refill(now);
        self.tokens -= amount;
        if self.tokens >= 0.0 {
            return Duration::ZERO;
        }
        let max_wait_secs = SCANNER_DISK_IO_BUDGET_MAX_WAIT.as_secs_f64();
        let wait_secs = if self.rate_per_sec > 0.0 {
            (-self.tokens / self.rate_per_sec).min(max_wait_secs)
        } else {
            max_wait_secs
        };
        self.tokens = self.tokens.max(-self.capacity);
        // `wait_secs` is finite and within `[0, max_wait_secs]`.
        Duration::from_secs_f64(wait_secs)
    }
}

#[derive(Debug)]
struct BudgetState {
    limits: ScannerDiskIoBudgetLimits,
    iops: Option<TokenBucket>,
    bytes: Option<TokenBucket>,
}

impl BudgetState {
    fn new(limits: ScannerDiskIoBudgetLimits, now: Instant) -> Self {
        Self {
            limits,
            iops: limits.iops.map(|rate| TokenBucket::new(rate, now)),
            bytes: limits.bytes_per_sec.map(|rate| TokenBucket::new(rate, now)),
        }
    }

    fn reconfigure(&mut self, limits: ScannerDiskIoBudgetLimits, now: Instant) {
        Self::reconfigure_axis(&mut self.iops, limits.iops, now);
        Self::reconfigure_axis(&mut self.bytes, limits.bytes_per_sec, now);
        self.limits = limits;
    }

    fn reconfigure_axis(bucket: &mut Option<TokenBucket>, rate: Option<u64>, now: Instant) {
        match (bucket.as_mut(), rate) {
            (Some(bucket), Some(rate)) => bucket.reconfigure(rate, now),
            (None, Some(rate)) => *bucket = Some(TokenBucket::new(rate, now)),
            (_, None) => *bucket = None,
        }
    }
}

/// The budget of one local disk, shared by every walker on that disk.
#[derive(Debug)]
pub struct ScannerDiskIoBudget {
    disk: String,
    media: ScannerDiskMedia,
    state: Mutex<BudgetState>,
    wait_events: AtomicU64,
    wait_millis: AtomicU64,
    charged_ops: AtomicU64,
    charged_bytes: AtomicU64,
}

impl ScannerDiskIoBudget {
    fn new(disk: String, media: ScannerDiskMedia, limits: ScannerDiskIoBudgetLimits, now: Instant) -> Self {
        Self {
            disk,
            media,
            state: Mutex::new(BudgetState::new(limits, now)),
            wait_events: AtomicU64::new(0),
            wait_millis: AtomicU64::new(0),
            charged_ops: AtomicU64::new(0),
            charged_bytes: AtomicU64::new(0),
        }
    }

    pub fn media(&self) -> ScannerDiskMedia {
        self.media
    }

    pub fn limits(&self) -> ScannerDiskIoBudgetLimits {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).limits
    }

    /// Whether byte charges can change pacing; callers skip measuring sizes
    /// when they cannot.
    pub fn budgets_bytes(&self) -> bool {
        self.limits().bytes_per_sec.is_some()
    }

    fn reconfigure(&self, limits: ScannerDiskIoBudgetLimits) {
        let now = Instant::now();
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .reconfigure(limits, now);
    }

    /// Charge `ops` operations and `bytes` bytes, returning the wait the
    /// budget imposes before the caller may proceed.
    pub fn charge(&self, ops: u64, bytes: u64) -> Duration {
        self.charged_ops.fetch_add(ops, Ordering::Relaxed);
        self.charged_bytes.fetch_add(bytes, Ordering::Relaxed);
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut wait = Duration::ZERO;
        if ops > 0
            && let Some(bucket) = state.iops.as_mut()
        {
            wait = wait.max(bucket.charge(count_as_f64(ops), now));
        }
        if bytes > 0
            && let Some(bucket) = state.bytes.as_mut()
        {
            wait = wait.max(bucket.charge(count_as_f64(bytes), now));
        }
        wait.min(SCANNER_DISK_IO_BUDGET_MAX_WAIT)
    }

    /// Charge and wait out the budget. Runs regardless of idle mode or
    /// speed preset: the budget is a ceiling, not a preset sleep.
    pub async fn admit(&self, ops: u64, bytes: u64) {
        let wait = self.charge(ops, bytes);
        if wait.is_zero() {
            return;
        }
        tokio::time::sleep(wait).await;
        self.record_wait(wait);
    }

    fn record_wait(&self, wait: Duration) {
        self.wait_events.fetch_add(1, Ordering::Relaxed);
        self.wait_millis
            .fetch_add(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
        metrics::histogram!(OTEL_SCANNER_DISK_IO_BUDGET_WAIT_SECONDS, "disk" => self.disk.clone()).record(wait.as_secs_f64());
        metrics::counter!(OTEL_SCANNER_DISK_IO_BUDGET_WAITS_TOTAL, "disk" => self.disk.clone()).increment(1);
        global_metrics().record_scanner_disk_io_budget_wait(wait);
    }

    pub fn status(&self) -> ScannerDiskIoBudgetStatus {
        let limits = self.limits();
        ScannerDiskIoBudgetStatus {
            disk: self.disk.clone(),
            media: self.media,
            iops: limits.iops,
            bytes_per_sec: limits.bytes_per_sec,
            charged_ops: self.charged_ops.load(Ordering::Relaxed),
            charged_bytes: self.charged_bytes.load(Ordering::Relaxed),
            wait_events: self.wait_events.load(Ordering::Relaxed),
            wait_seconds: count_as_f64(self.wait_millis.load(Ordering::Relaxed)) / 1000.0,
        }
    }
}

/// Status of one disk budget as reported by the scanner status endpoint.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScannerDiskIoBudgetStatus {
    pub disk: String,
    pub media: ScannerDiskMedia,
    /// `null` when that axis is uncapped.
    pub iops: Option<u64>,
    pub bytes_per_sec: Option<u64>,
    pub charged_ops: u64,
    pub charged_bytes: u64,
    pub wait_events: u64,
    pub wait_seconds: f64,
}

static DISK_IO_BUDGET_CONFIG: RwLock<ScannerDiskIoBudgetConfig> = RwLock::new(ScannerDiskIoBudgetConfig::DEFAULT);

static DISK_IO_BUDGETS: LazyLock<Mutex<BTreeMap<String, Arc<ScannerDiskIoBudget>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(crate) fn current_disk_io_budget_config() -> ScannerDiskIoBudgetConfig {
    *DISK_IO_BUDGET_CONFIG.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Look up or create the budget for one local disk path. Walkers that share
/// a disk share its buckets; the media is detected once per path.
pub(crate) fn disk_io_budget_for(disk_path: &Path) -> Arc<ScannerDiskIoBudget> {
    let key = disk_path.to_string_lossy().into_owned();
    let mut registry = DISK_IO_BUDGETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(budget) = registry.get(&key) {
        return budget.clone();
    }
    let media = detect_disk_media(disk_path);
    let config = current_disk_io_budget_config();
    let limits = resolve_disk_io_budget(config, media);
    let budget = Arc::new(ScannerDiskIoBudget::new(key.clone(), media, limits, Instant::now()));
    info!(
        target: "rustfs::scanner",
        event = EVENT_SCANNER_DISK_IO_BUDGET,
        component = LOG_COMPONENT_SCANNER,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "resolved",
        disk = %key,
        media = media.as_str(),
        iops = ?limits.iops,
        bytes_per_sec = ?limits.bytes_per_sec,
        "Scanner disk I/O budget resolved"
    );
    registry.insert(key, budget.clone());
    budget
}

/// Apply a new configuration to every known disk budget.
pub(crate) fn update_disk_io_budget_config(config: ScannerDiskIoBudgetConfig) {
    *DISK_IO_BUDGET_CONFIG.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    let registry = DISK_IO_BUDGETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    for budget in registry.values() {
        budget.reconfigure(resolve_disk_io_budget(config, budget.media));
    }
}

/// Per-disk budget status, ordered by disk path.
pub fn scanner_disk_io_budget_status() -> Vec<ScannerDiskIoBudgetStatus> {
    let registry = DISK_IO_BUDGETS.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    registry.values().map(|budget| budget.status()).collect()
}

#[cfg(target_os = "linux")]
fn detect_disk_media(disk_path: &Path) -> ScannerDiskMedia {
    let Ok(devices) = rustfs_utils::os::get_physical_device_ids(disk_path.to_string_lossy().as_ref()) else {
        return ScannerDiskMedia::Unknown;
    };
    let mut detected = ScannerDiskMedia::Unknown;
    for device in devices {
        let candidates = [
            format!("/sys/block/{device}/queue/rotational"),
            format!("/sys/dev/block/{device}/queue/rotational"),
        ];
        let Some(content) = candidates
            .iter()
            .find_map(|candidate| std::fs::read_to_string(candidate).ok())
        else {
            continue;
        };
        match content.trim() {
            // Any spindle in the backing stack makes the path rotational.
            "1" => return ScannerDiskMedia::Rotational,
            "0" => detected = ScannerDiskMedia::Flash,
            _ => {}
        }
    }
    detected
}

#[cfg(not(target_os = "linux"))]
fn detect_disk_media(_disk_path: &Path) -> ScannerDiskMedia {
    ScannerDiskMedia::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn config(iops: ScannerIoBudgetLimit, bytes_per_sec: ScannerIoBudgetLimit, speed: ScannerSpeed) -> ScannerDiskIoBudgetConfig {
        ScannerDiskIoBudgetConfig {
            iops,
            bytes_per_sec,
            speed,
        }
    }

    #[test]
    fn disk_io_budget_limit_parses_auto_off_and_numbers() {
        assert_eq!(ScannerIoBudgetLimit::parse("auto"), Some(ScannerIoBudgetLimit::Auto));
        assert_eq!(ScannerIoBudgetLimit::parse(" AUTO "), Some(ScannerIoBudgetLimit::Auto));
        assert_eq!(ScannerIoBudgetLimit::parse("off"), Some(ScannerIoBudgetLimit::Unlimited));
        assert_eq!(ScannerIoBudgetLimit::parse("unlimited"), Some(ScannerIoBudgetLimit::Unlimited));
        assert_eq!(ScannerIoBudgetLimit::parse("0"), Some(ScannerIoBudgetLimit::Explicit(0)));
        assert_eq!(ScannerIoBudgetLimit::parse("4096"), Some(ScannerIoBudgetLimit::Explicit(4096)));
        assert_eq!(ScannerIoBudgetLimit::parse("-1"), None);
        assert_eq!(ScannerIoBudgetLimit::parse("fast"), None);
        assert_eq!(ScannerIoBudgetLimit::Auto.to_string(), "auto");
        assert_eq!(ScannerIoBudgetLimit::Unlimited.to_string(), "off");
        assert_eq!(ScannerIoBudgetLimit::Explicit(7).to_string(), "7");
    }

    #[test]
    fn disk_io_budget_resolves_media_defaults_and_overrides() {
        let auto = config(ScannerIoBudgetLimit::Auto, ScannerIoBudgetLimit::Auto, ScannerSpeed::Default);
        assert_eq!(
            resolve_disk_io_budget(auto, ScannerDiskMedia::Rotational),
            ScannerDiskIoBudgetLimits {
                iops: Some(DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_IOPS),
                bytes_per_sec: Some(DEFAULT_SCANNER_DISK_IO_BUDGET_ROTATIONAL_BYTES_PER_SEC),
            }
        );
        // Unknown media is treated like a spindle.
        assert_eq!(
            resolve_disk_io_budget(auto, ScannerDiskMedia::Unknown),
            resolve_disk_io_budget(auto, ScannerDiskMedia::Rotational)
        );
        assert_eq!(
            resolve_disk_io_budget(auto, ScannerDiskMedia::Flash),
            ScannerDiskIoBudgetLimits {
                iops: Some(DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_IOPS),
                bytes_per_sec: Some(DEFAULT_SCANNER_DISK_IO_BUDGET_FLASH_BYTES_PER_SEC),
            }
        );
        // Rotational media is paced tighter than flash on both axes.
        let rotational = resolve_disk_io_budget(auto, ScannerDiskMedia::Rotational);
        let flash = resolve_disk_io_budget(auto, ScannerDiskMedia::Flash);
        assert!(rotational.iops < flash.iops, "{rotational:?} vs {flash:?}");
        assert!(rotational.bytes_per_sec < flash.bytes_per_sec, "{rotational:?} vs {flash:?}");

        // The fastest preset never paces unless a value is explicit.
        let fastest = config(ScannerIoBudgetLimit::Auto, ScannerIoBudgetLimit::Explicit(5), ScannerSpeed::Fastest);
        assert_eq!(
            resolve_disk_io_budget(fastest, ScannerDiskMedia::Rotational),
            ScannerDiskIoBudgetLimits {
                iops: None,
                bytes_per_sec: Some(5),
            }
        );

        let explicit = config(ScannerIoBudgetLimit::Explicit(0), ScannerIoBudgetLimit::Unlimited, ScannerSpeed::Slow);
        assert_eq!(
            resolve_disk_io_budget(explicit, ScannerDiskMedia::Flash),
            ScannerDiskIoBudgetLimits {
                iops: Some(0),
                bytes_per_sec: None,
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_waits_once_the_burst_is_spent() {
        let budget = ScannerDiskIoBudget::new(
            "disk-a".to_string(),
            ScannerDiskMedia::Rotational,
            ScannerDiskIoBudgetLimits {
                iops: Some(10),
                bytes_per_sec: None,
            },
            Instant::now(),
        );

        // The burst window admits one second of rate without waiting.
        for _ in 0..10 {
            assert_eq!(budget.charge(1, 0), Duration::ZERO);
        }
        // The eleventh operation owes one token at 10/s.
        assert_eq!(budget.charge(1, 0), Duration::from_millis(100));

        // After the rate has refilled the deficit, the next charge is free again.
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(budget.charge(1, 0), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_bytes_axis_caps_independently_of_iops() {
        let budget = ScannerDiskIoBudget::new(
            "disk-b".to_string(),
            ScannerDiskMedia::Flash,
            ScannerDiskIoBudgetLimits {
                iops: Some(1_000),
                bytes_per_sec: Some(1_000),
            },
            Instant::now(),
        );

        assert_eq!(budget.charge(1, 1_000), Duration::ZERO);
        assert_eq!(budget.charge(1, 500), Duration::from_millis(500));
        assert!(budget.budgets_bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_zero_budget_still_admits_one_charge_per_window() {
        let budget = ScannerDiskIoBudget::new(
            "disk-c".to_string(),
            ScannerDiskMedia::Unknown,
            ScannerDiskIoBudgetLimits {
                iops: Some(0),
                bytes_per_sec: Some(0),
            },
            Instant::now(),
        );

        // A zero budget starts with one token of burst, then degrades to the
        // floor instead of waiting forever.
        assert_eq!(budget.charge(1, 1), Duration::ZERO);
        for _ in 0..3 {
            assert_eq!(budget.charge(1, 4096), SCANNER_DISK_IO_BUDGET_MAX_WAIT);
        }

        let started = Instant::now();
        budget.admit(1, 4096).await;
        assert_eq!(started.elapsed(), SCANNER_DISK_IO_BUDGET_MAX_WAIT);
        let status = budget.status();
        assert_eq!(status.wait_events, 1);
        assert_eq!(status.wait_seconds, 1.0);
        assert_eq!(status.iops, Some(0));
        assert_eq!(status.bytes_per_sec, Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_oversized_charge_owes_at_most_one_extra_window() {
        let budget = ScannerDiskIoBudget::new(
            "disk-d".to_string(),
            ScannerDiskMedia::Rotational,
            ScannerDiskIoBudgetLimits {
                iops: None,
                bytes_per_sec: Some(1_000),
            },
            Instant::now(),
        );

        // A charge far above the rate is capped at the max wait ...
        assert_eq!(budget.charge(0, 1_000_000), SCANNER_DISK_IO_BUDGET_MAX_WAIT);
        // ... and leaves at most one burst window of debt behind.
        assert_eq!(budget.charge(0, 0), Duration::ZERO);
        assert_eq!(budget.charge(0, 1), SCANNER_DISK_IO_BUDGET_MAX_WAIT);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(budget.charge(0, 1), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_unlimited_axis_never_waits() {
        let budget = ScannerDiskIoBudget::new(
            "disk-e".to_string(),
            ScannerDiskMedia::Flash,
            ScannerDiskIoBudgetLimits::default(),
            Instant::now(),
        );

        for _ in 0..1_000 {
            assert_eq!(budget.charge(1, 1 << 20), Duration::ZERO);
        }
        assert!(!budget.budgets_bytes());
        let started = Instant::now();
        budget.admit(1, 1 << 20).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert_eq!(budget.status().wait_events, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn disk_io_budget_reconfigure_applies_the_new_rate_to_live_buckets() {
        let budget = ScannerDiskIoBudget::new(
            "disk-f".to_string(),
            ScannerDiskMedia::Rotational,
            ScannerDiskIoBudgetLimits {
                iops: Some(1),
                bytes_per_sec: None,
            },
            Instant::now(),
        );
        assert_eq!(budget.charge(1, 0), Duration::ZERO);
        assert_eq!(budget.charge(1, 0), SCANNER_DISK_IO_BUDGET_MAX_WAIT);

        budget.reconfigure(ScannerDiskIoBudgetLimits {
            iops: Some(1_000),
            bytes_per_sec: Some(10),
        });
        // Debt carried over is repaid at the new rate.
        assert_eq!(budget.charge(1, 0), Duration::from_millis(2));
        assert!(budget.budgets_bytes());

        budget.reconfigure(ScannerDiskIoBudgetLimits::default());
        assert_eq!(budget.charge(1, 1 << 30), Duration::ZERO);
        assert_eq!(budget.limits(), ScannerDiskIoBudgetLimits::default());
    }

    #[tokio::test(start_paused = true)]
    #[serial]
    async fn disk_io_budget_registry_shares_one_budget_per_disk_and_follows_config_updates() {
        // A path that does not exist resolves to unknown media on every platform.
        let path = std::path::PathBuf::from(format!("/rustfs-scanner-budget-test-{}", std::process::id()));

        let first = disk_io_budget_for(&path);
        let second = disk_io_budget_for(&path);
        assert!(Arc::ptr_eq(&first, &second));

        let status = scanner_disk_io_budget_status();
        assert!(status.iter().any(|entry| entry.disk == path.to_string_lossy()));

        update_disk_io_budget_config(config(
            ScannerIoBudgetLimit::Explicit(3),
            ScannerIoBudgetLimit::Unlimited,
            ScannerSpeed::Default,
        ));
        assert_eq!(
            first.limits(),
            ScannerDiskIoBudgetLimits {
                iops: Some(3),
                bytes_per_sec: None,
            }
        );

        update_disk_io_budget_config(ScannerDiskIoBudgetConfig::DEFAULT);
        assert_eq!(first.limits(), resolve_disk_io_budget(ScannerDiskIoBudgetConfig::DEFAULT, first.media()));
    }
}
