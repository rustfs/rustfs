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

//! Per-bucket on-demand migration counters (rustfs/backlog#2152).
//!
//! `OdmStats` is lock-free and survives config rebuilds; `snapshot()` turns
//! it into the serializable `OdmStatsSnapshot` that the metrics collector
//! and the admin status route (ODM-10/14/15) consume. Field names and label
//! values are a wire contract: the golden JSON test below pins them.

use super::breaker::BreakerState;
use super::source_client::SourceError;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use time::OffsetDateTime;

/// Request operations that can enter ODM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OdmOp {
    Get,
    Head,
}

impl OdmOp {
    pub const ALL: [OdmOp; 2] = [OdmOp::Get, OdmOp::Head];

    pub fn as_str(self) -> &'static str {
        match self {
            OdmOp::Get => "get",
            OdmOp::Head => "head",
        }
    }
}

/// How a request that entered ODM ended. `local_hit` is deliberately absent:
/// requests served locally never reach the runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OdmOutcome {
    SourceHit,
    SourceMiss,
    SourceError,
    BreakerOpen,
    NegativeCached,
    Filtered,
    Unsupported,
}

impl OdmOutcome {
    pub const ALL: [OdmOutcome; 7] = [
        OdmOutcome::SourceHit,
        OdmOutcome::SourceMiss,
        OdmOutcome::SourceError,
        OdmOutcome::BreakerOpen,
        OdmOutcome::NegativeCached,
        OdmOutcome::Filtered,
        OdmOutcome::Unsupported,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            OdmOutcome::SourceHit => "source_hit",
            OdmOutcome::SourceMiss => "source_miss",
            OdmOutcome::SourceError => "source_error",
            OdmOutcome::BreakerOpen => "breaker_open",
            OdmOutcome::NegativeCached => "negative_cached",
            OdmOutcome::Filtered => "filtered",
            OdmOutcome::Unsupported => "unsupported",
        }
    }
}

/// Which pipeline stored a pulled object locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PullPath {
    /// Streamed to the client and written locally in one pass.
    Inline,
    /// Pulled by a background task after a partial/large read.
    Background,
    /// Pulled by the backfill job.
    Backfill,
}

impl PullPath {
    pub const ALL: [PullPath; 3] = [PullPath::Inline, PullPath::Background, PullPath::Backfill];

    pub fn as_str(self) -> &'static str {
        match self {
            PullPath::Inline => "inline",
            PullPath::Background => "background",
            PullPath::Backfill => "backfill",
        }
    }
}

/// Why a pull did not produce a local object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullFailureReason {
    SourceNotFound,
    SourceAccessDenied,
    SourceThrottled,
    SourceTimeout,
    SourceConnect,
    SourceServerError,
    SourceUnsupported,
    SourceOther,
    /// Source bytes did not match the ETag advertised by HEAD/GET.
    EtagMismatch,
    /// The local write (internal PUT) failed.
    LocalWrite,
    /// The bucket state was removed or the process is shutting down.
    Canceled,
    /// The background pull queue was full.
    QueueFull,
}

impl PullFailureReason {
    pub const ALL: [PullFailureReason; 12] = [
        PullFailureReason::SourceNotFound,
        PullFailureReason::SourceAccessDenied,
        PullFailureReason::SourceThrottled,
        PullFailureReason::SourceTimeout,
        PullFailureReason::SourceConnect,
        PullFailureReason::SourceServerError,
        PullFailureReason::SourceUnsupported,
        PullFailureReason::SourceOther,
        PullFailureReason::EtagMismatch,
        PullFailureReason::LocalWrite,
        PullFailureReason::Canceled,
        PullFailureReason::QueueFull,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PullFailureReason::SourceNotFound => "source_not_found",
            PullFailureReason::SourceAccessDenied => "source_access_denied",
            PullFailureReason::SourceThrottled => "source_throttled",
            PullFailureReason::SourceTimeout => "source_timeout",
            PullFailureReason::SourceConnect => "source_connect",
            PullFailureReason::SourceServerError => "source_server_error",
            PullFailureReason::SourceUnsupported => "source_unsupported",
            PullFailureReason::SourceOther => "source_other",
            PullFailureReason::EtagMismatch => "etag_mismatch",
            PullFailureReason::LocalWrite => "local_write",
            PullFailureReason::Canceled => "canceled",
            PullFailureReason::QueueFull => "queue_full",
        }
    }
}

impl From<&SourceError> for PullFailureReason {
    fn from(err: &SourceError) -> Self {
        match err {
            SourceError::NotFound => PullFailureReason::SourceNotFound,
            SourceError::AccessDenied => PullFailureReason::SourceAccessDenied,
            SourceError::Throttled => PullFailureReason::SourceThrottled,
            SourceError::Timeout => PullFailureReason::SourceTimeout,
            SourceError::Connect(_) => PullFailureReason::SourceConnect,
            SourceError::ServerError(_) => PullFailureReason::SourceServerError,
            SourceError::Unsupported(_) => PullFailureReason::SourceUnsupported,
            SourceError::Other(_) => PullFailureReason::SourceOther,
        }
    }
}

/// Upper bounds (milliseconds) of the source latency histogram buckets; the
/// implicit last bucket is unbounded. Roughly logarithmic from 5 ms to 60 s.
pub const SOURCE_LATENCY_BUCKET_BOUNDS_MS: [u64; 14] = [
    5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 60_000,
];

#[derive(Debug, Default)]
struct LatencyHistogram {
    /// One counter per bound plus one for the overflow bucket.
    buckets: [AtomicU64; SOURCE_LATENCY_BUCKET_BOUNDS_MS.len() + 1],
    count: AtomicU64,
    sum_ms: AtomicU64,
}

impl LatencyHistogram {
    fn observe(&self, latency: Duration) {
        let ms = u64::try_from(latency.as_millis()).unwrap_or(u64::MAX);
        let index = SOURCE_LATENCY_BUCKET_BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(SOURCE_LATENCY_BUCKET_BOUNDS_MS.len());
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);
    }

    fn snapshot(&self) -> SourceLatencySnapshot {
        let mut cumulative = 0;
        let buckets = SOURCE_LATENCY_BUCKET_BOUNDS_MS
            .iter()
            .zip(self.buckets.iter())
            .map(|(bound, counter)| {
                cumulative += counter.load(Ordering::Relaxed);
                LatencyBucketSnapshot {
                    le_ms: *bound,
                    count: cumulative,
                }
            })
            .collect();
        SourceLatencySnapshot {
            buckets,
            count: self.count.load(Ordering::Relaxed),
            sum_ms: self.sum_ms.load(Ordering::Relaxed),
        }
    }
}

/// The most recent source failure, kept for operators: class only, never the
/// key or the message (which may echo attacker-controlled input).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastSourceError {
    pub class: String,
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
}

#[derive(Debug, Default)]
pub struct OdmStats {
    requests_total: [[AtomicU64; OdmOutcome::ALL.len()]; OdmOp::ALL.len()],
    pulled_bytes_total: AtomicU64,
    pulled_objects_total: [AtomicU64; PullPath::ALL.len()],
    pull_failures_total: [AtomicU64; PullFailureReason::ALL.len()],
    inflight_pulls: AtomicU64,
    queue_depth: AtomicU64,
    source_latency: LatencyHistogram,
    last_source_error: Mutex<Option<LastSourceError>>,
}

impl OdmStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&self, op: OdmOp, outcome: OdmOutcome) {
        self.requests_total[op as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pulled_bytes(&self, bytes: u64) {
        self.pulled_bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_pulled_object(&self, path: PullPath) {
        self.pulled_objects_total[path as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_pull_failure(&self, reason: PullFailureReason) {
        self.pull_failures_total[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_source_latency(&self, latency: Duration) {
        self.source_latency.observe(latency);
    }

    pub fn record_source_error(&self, err: &SourceError) {
        self.record_source_error_at(err, OffsetDateTime::now_utc());
    }

    pub fn record_source_error_at(&self, err: &SourceError, at: OffsetDateTime) {
        *self.last_source_error.lock() = Some(LastSourceError {
            class: err.class_label().to_string(),
            at,
        });
    }

    pub fn last_source_error(&self) -> Option<LastSourceError> {
        self.last_source_error.lock().clone()
    }

    pub fn inflight_pulls(&self) -> u64 {
        self.inflight_pulls.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// RAII increment of `inflight_pulls`.
    pub fn inflight_guard(self: &Arc<Self>) -> GaugeGuard {
        GaugeGuard::new(Arc::clone(self), OdmGauge::InflightPulls)
    }

    /// RAII increment of `queue_depth`.
    pub fn queue_guard(self: &Arc<Self>) -> GaugeGuard {
        GaugeGuard::new(Arc::clone(self), OdmGauge::QueueDepth)
    }

    fn gauge(&self, gauge: OdmGauge) -> &AtomicU64 {
        match gauge {
            OdmGauge::InflightPulls => &self.inflight_pulls,
            OdmGauge::QueueDepth => &self.queue_depth,
        }
    }

    /// Read-only, side-effect-free copy of every counter. The breaker lives
    /// next to the stats in the bucket state; its state is passed in so the
    /// snapshot stays a single document.
    pub fn snapshot(&self, breaker_state: BreakerState) -> OdmStatsSnapshot {
        let mut requests_total = BTreeMap::new();
        for op in OdmOp::ALL {
            let mut by_outcome = BTreeMap::new();
            for outcome in OdmOutcome::ALL {
                by_outcome.insert(
                    outcome.as_str().to_string(),
                    self.requests_total[op as usize][outcome as usize].load(Ordering::Relaxed),
                );
            }
            requests_total.insert(op.as_str().to_string(), by_outcome);
        }
        let pulled_objects_total = PullPath::ALL
            .iter()
            .map(|path| {
                (
                    path.as_str().to_string(),
                    self.pulled_objects_total[*path as usize].load(Ordering::Relaxed),
                )
            })
            .collect();
        let pull_failures_total = PullFailureReason::ALL
            .iter()
            .map(|reason| {
                (
                    reason.as_str().to_string(),
                    self.pull_failures_total[*reason as usize].load(Ordering::Relaxed),
                )
            })
            .collect();
        OdmStatsSnapshot {
            requests_total,
            pulled_bytes_total: self.pulled_bytes_total.load(Ordering::Relaxed),
            pulled_objects_total,
            pull_failures_total,
            inflight_pulls: self.inflight_pulls(),
            queue_depth: self.queue_depth(),
            source_latency: self.source_latency.snapshot(),
            last_source_error: self.last_source_error(),
            breaker_state,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OdmGauge {
    InflightPulls,
    QueueDepth,
}

/// Increments a gauge on creation and decrements it on drop. Owns its
/// `OdmStats` so it can live inside the pull slot handed to callers.
#[derive(Debug)]
pub struct GaugeGuard {
    stats: Arc<OdmStats>,
    gauge: OdmGauge,
}

impl GaugeGuard {
    fn new(stats: Arc<OdmStats>, gauge: OdmGauge) -> Self {
        stats.gauge(gauge).fetch_add(1, Ordering::Relaxed);
        Self { stats, gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.stats.gauge(self.gauge).fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyBucketSnapshot {
    /// Upper bound of the bucket in milliseconds.
    pub le_ms: u64,
    /// Cumulative observations at or below `le_ms`.
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLatencySnapshot {
    pub buckets: Vec<LatencyBucketSnapshot>,
    /// Total observations, including those above the last bound.
    pub count: u64,
    pub sum_ms: u64,
}

/// Serializable copy of [`OdmStats`]. Every key is snake_case and every
/// label set is fixed, so consumers can rely on the document shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OdmStatsSnapshot {
    /// `op -> outcome -> count`.
    pub requests_total: BTreeMap<String, BTreeMap<String, u64>>,
    pub pulled_bytes_total: u64,
    /// `path -> count`.
    pub pulled_objects_total: BTreeMap<String, u64>,
    /// `reason -> count`.
    pub pull_failures_total: BTreeMap<String, u64>,
    pub inflight_pulls: u64,
    pub queue_depth: u64,
    pub source_latency: SourceLatencySnapshot,
    pub last_source_error: Option<LastSourceError>,
    pub breaker_state: BreakerState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::macros::datetime;

    #[test]
    fn snapshot_matches_golden_json() {
        let stats = Arc::new(OdmStats::new());
        stats.record_request(OdmOp::Get, OdmOutcome::SourceHit);
        stats.record_request(OdmOp::Get, OdmOutcome::SourceHit);
        stats.record_request(OdmOp::Head, OdmOutcome::NegativeCached);
        stats.record_pulled_bytes(4096);
        stats.record_pulled_object(PullPath::Inline);
        stats.record_pull_failure(PullFailureReason::from(&SourceError::Timeout));
        stats.record_source_latency(Duration::from_millis(3));
        stats.record_source_latency(Duration::from_millis(750));
        stats.record_source_latency(Duration::from_secs(90));
        stats.record_source_error_at(&SourceError::ServerError(502), datetime!(2026-09-02 10:00:00 UTC));
        let _inflight = stats.inflight_guard();
        let _queued = stats.queue_guard();

        let snapshot = stats.snapshot(BreakerState::HalfOpen);
        let actual = serde_json::to_value(&snapshot).unwrap();
        let expected = json!({
            "requests_total": {
                "get": {
                    "breaker_open": 0, "filtered": 0, "negative_cached": 0, "source_error": 0,
                    "source_hit": 2, "source_miss": 0, "unsupported": 0
                },
                "head": {
                    "breaker_open": 0, "filtered": 0, "negative_cached": 1, "source_error": 0,
                    "source_hit": 0, "source_miss": 0, "unsupported": 0
                }
            },
            "pulled_bytes_total": 4096,
            "pulled_objects_total": { "backfill": 0, "background": 0, "inline": 1 },
            "pull_failures_total": {
                "canceled": 0, "etag_mismatch": 0, "local_write": 0, "queue_full": 0,
                "source_access_denied": 0, "source_connect": 0, "source_not_found": 0, "source_other": 0,
                "source_server_error": 0, "source_throttled": 0, "source_timeout": 1, "source_unsupported": 0
            },
            "inflight_pulls": 1,
            "queue_depth": 1,
            "source_latency": {
                "buckets": [
                    { "le_ms": 5, "count": 1 }, { "le_ms": 10, "count": 1 }, { "le_ms": 20, "count": 1 },
                    { "le_ms": 50, "count": 1 }, { "le_ms": 100, "count": 1 }, { "le_ms": 200, "count": 1 },
                    { "le_ms": 500, "count": 1 }, { "le_ms": 1000, "count": 2 }, { "le_ms": 2000, "count": 2 },
                    { "le_ms": 5000, "count": 2 }, { "le_ms": 10000, "count": 2 }, { "le_ms": 20000, "count": 2 },
                    { "le_ms": 30000, "count": 2 }, { "le_ms": 60000, "count": 2 }
                ],
                "count": 3,
                "sum_ms": 90753
            },
            "last_source_error": { "class": "server_error", "at": "2026-09-02T10:00:00Z" },
            "breaker_state": "half_open"
        });
        assert_eq!(actual, expected);

        let round_trip: OdmStatsSnapshot = serde_json::from_value(actual).unwrap();
        assert_eq!(round_trip, snapshot);
    }

    #[test]
    fn gauges_return_to_zero_when_guards_drop() {
        let stats = Arc::new(OdmStats::new());
        {
            let _a = stats.inflight_guard();
            let _b = stats.inflight_guard();
            let _c = stats.queue_guard();
            assert_eq!(stats.inflight_pulls(), 2);
            assert_eq!(stats.queue_depth(), 1);
        }
        assert_eq!(stats.inflight_pulls(), 0);
        assert_eq!(stats.queue_depth(), 0);
    }

    #[test]
    fn pull_failure_reason_covers_every_source_error_class() {
        let cases = [
            (SourceError::NotFound, PullFailureReason::SourceNotFound),
            (SourceError::AccessDenied, PullFailureReason::SourceAccessDenied),
            (SourceError::Throttled, PullFailureReason::SourceThrottled),
            (SourceError::Timeout, PullFailureReason::SourceTimeout),
            (SourceError::Connect("x".into()), PullFailureReason::SourceConnect),
            (SourceError::ServerError(500), PullFailureReason::SourceServerError),
            (SourceError::Unsupported("x".into()), PullFailureReason::SourceUnsupported),
            (SourceError::Other("x".into()), PullFailureReason::SourceOther),
        ];
        for (err, reason) in cases {
            assert_eq!(PullFailureReason::from(&err), reason, "{err:?}");
            assert_eq!(serde_json::to_string(&reason).unwrap(), format!("\"{}\"", reason.as_str()));
        }
    }

    #[test]
    fn label_lists_are_exhaustive_and_unique() {
        let outcomes: std::collections::BTreeSet<_> = OdmOutcome::ALL.iter().map(|o| o.as_str()).collect();
        assert_eq!(outcomes.len(), OdmOutcome::ALL.len());
        let reasons: std::collections::BTreeSet<_> = PullFailureReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(reasons.len(), PullFailureReason::ALL.len());
        let paths: std::collections::BTreeSet<_> = PullPath::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(paths.len(), PullPath::ALL.len());
    }
}
