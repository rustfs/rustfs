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

//! Analysis orchestration (rustfs/backlog#1287): one pass over the event
//! stream does rule matching, timeline bucketing, and unmatched-pattern
//! clustering at once. All aggregates stay order-independent.

use crate::ingest::{IngestReport, SkipReason};
use crate::model::{EventKind, LogEvent, LogLevel, ParseStats};
use crate::redact;
use crate::rules::{Finding, FindingsCollector, RuleEngine};
use chrono::{DateTime, FixedOffset, Utc};
use regex::Regex;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::LazyLock;

const UNMATCHED_DISTINCT_CAP: usize = 5000;
const OVERFLOW_TEMPLATE: &str = "<overflow>";
const SAMPLE_TEXT_CAP: usize = 500;
/// Bucket widths (minutes) tried in order until <= 60 buckets span the range.
const BUCKET_WIDTHS_MIN: [i64; 6] = [1, 5, 15, 60, 360, 1440];

#[derive(Debug, Clone)]
pub struct AnalyzeOptions {
    /// Keep events at/after this time. Timestamp-less events always pass.
    pub since: Option<DateTime<FixedOffset>>,
    /// Keep events at/before this time. Timestamp-less events always pass.
    pub until: Option<DateTime<FixedOffset>>,
    /// Drop events below this level; level-less events always pass.
    pub min_level: Option<LogLevel>,
    pub max_samples: usize,
    pub top_unmatched: usize,
    pub redact: bool,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            since: None,
            until: None,
            min_level: None,
            max_samples: 3,
            top_unmatched: 20,
            redact: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub events_total: u64,
    pub events_after_filter: u64,
    /// "ERROR" -> n, …, "no_level" -> n.
    pub level_counts: BTreeMap<String, u64>,
    pub time_range: Option<(DateTime<FixedOffset>, DateTime<FixedOffset>)>,
    pub nodes: Vec<String>,
    pub parse: ParseStats,
    pub files_parsed: u64,
    pub bytes_fed: u64,
    /// Distinct UTC offsets observed (e.g. ["+08:00", "Z"]); more than one
    /// means cross-node clock/timezone comparison needs care.
    pub distinct_offsets: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnmatchedCluster {
    /// Normalized message template (numbers/uuids/paths/... placeholders).
    pub template: String,
    pub count: u64,
    pub level: String,
    /// One raw sample message (lexicographic min, capped at 500 chars).
    pub sample: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeBucket {
    /// Bucket start, normalized to UTC.
    pub start: DateTime<Utc>,
    pub error: u64,
    pub warn: u64,
    pub other: u64,
}

#[derive(Debug, Serialize)]
pub struct AnalysisReport {
    /// Stability contract for JSON consumers.
    pub schema_version: u32,
    pub summary: Summary,
    /// Sorted findings at/above their rule's min_count.
    pub findings: Vec<Finding>,
    /// Findings below their rule's min_count (low-confidence hints).
    pub low_confidence: Vec<Finding>,
    pub unmatched_top: Vec<UnmatchedCluster>,
    pub timeline: Vec<TimeBucket>,
    /// Pass-through of every skipped input (no silent caps).
    pub skipped_inputs: Vec<(String, SkipReason)>,
}

#[derive(Default)]
struct BucketCounts {
    error: u64,
    warn: u64,
    other: u64,
}

struct ClusterAcc {
    count: u64,
    /// Highest level seen (order-independent max).
    level: LogLevel,
    /// Lexicographic-min raw sample (order-independent).
    sample: String,
    target: Option<String>,
}

pub struct Analyzer {
    engine: RuleEngine,
    opts: AnalyzeOptions,
    collector: FindingsCollector,
    events_total: u64,
    events_after_filter: u64,
    level_counts: BTreeMap<String, u64>,
    time_range: Option<(DateTime<FixedOffset>, DateTime<FixedOffset>)>,
    nodes: BTreeSet<String>,
    offsets: BTreeSet<String>,
    /// unix-minute -> counts.
    minute_buckets: BTreeMap<i64, BucketCounts>,
    unmatched: HashMap<String, ClusterAcc>,
    unmatched_overflow: u64,
}

impl Analyzer {
    pub fn new(engine: RuleEngine, opts: AnalyzeOptions) -> Self {
        Self {
            engine,
            collector: FindingsCollector::new(opts.max_samples),
            opts,
            events_total: 0,
            events_after_filter: 0,
            level_counts: BTreeMap::new(),
            time_range: None,
            nodes: BTreeSet::new(),
            offsets: BTreeSet::new(),
            minute_buckets: BTreeMap::new(),
            unmatched: HashMap::new(),
            unmatched_overflow: 0,
        }
    }

    /// Wire this directly as the ingest `on_event` callback.
    pub fn observe(&mut self, event: LogEvent) {
        self.events_total += 1;

        if let Some(ts) = event.timestamp
            && (self.opts.since.is_some_and(|since| ts < since) || self.opts.until.is_some_and(|until| ts > until))
        {
            return;
        }
        if let (Some(min), Some(level)) = (self.opts.min_level, event.level)
            && level < min
        {
            return;
        }
        self.events_after_filter += 1;

        let level_key = event.level.map_or_else(|| "no_level".to_string(), |l| l.to_string());
        *self.level_counts.entry(level_key).or_insert(0) += 1;

        if let Some(node) = event.node.as_deref() {
            self.nodes.insert(node.to_string());
        }

        if let Some(ts) = event.timestamp {
            self.time_range = Some(match self.time_range {
                None => (ts, ts),
                Some((first, last)) => (first.min(ts), last.max(ts)),
            });
            self.offsets.insert(format_offset(&ts));
            let bucket = self.minute_buckets.entry(ts.timestamp().div_euclid(60)).or_default();
            match event.level {
                Some(LogLevel::Error) => bucket.error += 1,
                Some(LogLevel::Warn) => bucket.warn += 1,
                _ => bucket.other += 1,
            }
        }

        let hits = self.engine.matches(&event);
        if hits.is_empty() {
            // Unmatched WARN/ERROR feeds the "unknown patterns" section —
            // the input for rule-library iteration. Panics have their own rule.
            if event.kind != EventKind::Panic && matches!(event.level, Some(LogLevel::Warn) | Some(LogLevel::Error)) {
                self.cluster_unmatched(&event);
            }
            return;
        }
        for idx in hits {
            let rule = &self.engine.rules()[idx];
            self.collector.observe(rule, idx, &event);
        }
    }

    pub fn finalize(self, ingest: IngestReport) -> AnalysisReport {
        let mut findings = Vec::new();
        let mut low_confidence = Vec::new();
        for finding in self.collector.into_findings() {
            if finding.below_min_count {
                low_confidence.push(finding);
            } else {
                findings.push(finding);
            }
        }

        let mut unmatched_top: Vec<UnmatchedCluster> = self
            .unmatched
            .into_iter()
            .map(|(template, acc)| UnmatchedCluster {
                template,
                count: acc.count,
                level: acc.level.to_string(),
                sample: acc.sample,
                target: acc.target,
            })
            .collect();
        if self.unmatched_overflow > 0 {
            unmatched_top.push(UnmatchedCluster {
                template: OVERFLOW_TEMPLATE.to_string(),
                count: self.unmatched_overflow,
                level: LogLevel::Warn.to_string(),
                sample: format!("(+{} events beyond the {UNMATCHED_DISTINCT_CAP}-template cap)", self.unmatched_overflow),
                target: None,
            });
        }
        unmatched_top.sort_by(|a, b| b.count.cmp(&a.count).then(a.template.cmp(&b.template)));
        unmatched_top.truncate(self.opts.top_unmatched);

        let timeline = merge_timeline(&self.minute_buckets);

        let mut report = AnalysisReport {
            schema_version: 1,
            summary: Summary {
                events_total: self.events_total,
                events_after_filter: self.events_after_filter,
                level_counts: self.level_counts,
                time_range: self.time_range,
                nodes: self.nodes.into_iter().collect(),
                parse: ingest.stats,
                files_parsed: ingest.files_parsed,
                bytes_fed: ingest.bytes_fed,
                distinct_offsets: self.offsets.into_iter().collect(),
            },
            findings,
            low_confidence,
            unmatched_top,
            timeline,
            skipped_inputs: ingest.skipped,
        };

        if self.opts.redact {
            redact_report(&mut report);
        }
        report
    }

    fn cluster_unmatched(&mut self, event: &LogEvent) {
        let template = normalize_template(&event.message);
        if !self.unmatched.contains_key(&template) && self.unmatched.len() >= UNMATCHED_DISTINCT_CAP {
            self.unmatched_overflow += 1;
            return;
        }
        let level = event.level.unwrap_or(LogLevel::Warn);
        let mut sample: String = event.message.chars().take(SAMPLE_TEXT_CAP).collect();
        if event.message.chars().count() > SAMPLE_TEXT_CAP {
            sample.push('…');
        }
        let entry = self.unmatched.entry(template).or_insert_with(|| ClusterAcc {
            count: 0,
            level,
            sample: sample.clone(),
            target: event.target.clone(),
        });
        entry.count += 1;
        entry.level = entry.level.max(level);
        // Lexicographic min keeps the representative sample order-independent.
        if sample < entry.sample {
            entry.sample = sample;
        }
    }
}

fn format_offset(ts: &DateTime<FixedOffset>) -> String {
    let seconds = ts.offset().local_minus_utc();
    if seconds == 0 {
        return "Z".to_string();
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let abs = seconds.abs();
    format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60)
}

fn merge_timeline(minute_buckets: &BTreeMap<i64, BucketCounts>) -> Vec<TimeBucket> {
    let (Some((&first, _)), Some((&last, _))) = (minute_buckets.first_key_value(), minute_buckets.last_key_value()) else {
        return Vec::new();
    };
    let span = last - first + 1;
    let width = BUCKET_WIDTHS_MIN
        .into_iter()
        .find(|w| (span as u64).div_ceil(*w as u64) <= 60)
        .unwrap_or(*BUCKET_WIDTHS_MIN.last().expect("non-empty"));

    let start_slot = first.div_euclid(width);
    let end_slot = last.div_euclid(width);
    let mut merged: BTreeMap<i64, BucketCounts> = (start_slot..=end_slot).map(|slot| (slot, BucketCounts::default())).collect();
    for (&minute, counts) in minute_buckets {
        let slot = merged.get_mut(&minute.div_euclid(width)).expect("slot within range");
        slot.error += counts.error;
        slot.warn += counts.warn;
        slot.other += counts.other;
    }
    merged
        .into_iter()
        .map(|(slot, counts)| TimeBucket {
            start: DateTime::<Utc>::from_timestamp(slot * width * 60, 0).expect("valid timestamp"),
            error: counts.error,
            warn: counts.warn,
            other: counts.other,
        })
        .collect()
}

fn redact_report(report: &mut AnalysisReport) {
    for finding in report.findings.iter_mut().chain(report.low_confidence.iter_mut()) {
        for sample in &mut finding.samples {
            redact::redact_event(sample);
        }
        for (field, values) in &mut finding.evidence {
            if redact::is_sensitive_field(field) {
                let hashed: BTreeSet<String> = values.values.iter().map(|v| redact::hash_value(v)).collect();
                values.values = hashed;
            }
        }
    }
    for cluster in &mut report.unmatched_top {
        cluster.sample = redact::redact_text(&cluster.sample);
    }
}

// Template normalization, applied in order (rustfs/backlog#1287 §2).
static RE_UUID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}").expect("static regex")
});
static RE_HEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b[0-9a-fA-F]{16,}\b").expect("static regex"));
static RE_TS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\d{4}-\d{2}-\d{2}T[0-9:.]+(?:Z|[+-]\d{2}:\d{2})?").expect("static regex"));
static RE_ADDR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?\b").expect("static regex"));
static RE_PATH: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(^|\s)/\S+").expect("static regex"));
static RE_NUM: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\d+").expect("static regex"));
static RE_BACKTICK: LazyLock<Regex> = LazyLock::new(|| Regex::new("`[^`]*`").expect("static regex"));
static RE_SINGLE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"'[^']*'").expect("static regex"));
static RE_DOUBLE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""[^"]*""#).expect("static regex"));

fn normalize_template(message: &str) -> String {
    let s = RE_UUID.replace_all(message, "<uuid>");
    let s = RE_HEX.replace_all(&s, "<hex>");
    let s = RE_TS.replace_all(&s, "<ts>");
    let s = RE_ADDR.replace_all(&s, "<addr>");
    let s = RE_PATH.replace_all(&s, "$1<path>");
    let s = RE_NUM.replace_all(&s, "<n>");
    let s = RE_BACKTICK.replace_all(&s, "`<q>`");
    let s = RE_SINGLE.replace_all(&s, "'<q>'");
    let s = RE_DOUBLE.replace_all(&s, "\"<q>\"");
    s.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceRef;
    use crate::rules::seed_rule_set;
    use std::sync::Arc;

    fn analyzer(opts: AnalyzeOptions) -> Analyzer {
        Analyzer::new(RuleEngine::new(seed_rule_set()), opts)
    }

    fn event(message: &str, level: Option<LogLevel>, ts: Option<&str>) -> LogEvent {
        LogEvent {
            timestamp: ts.map(|t| DateTime::parse_from_rfc3339(t).expect("ts")),
            level,
            target: Some("rustfs::server::http".to_string()),
            message: message.to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("rustfs.log"),
                line: 1,
            },
            node: None,
            kind: EventKind::Json,
        }
    }

    #[test]
    fn normalize_template_collapses_variable_parts() {
        assert_eq!(
            normalize_template("upload 12/34 failed for /data/x9 after 5 retries"),
            normalize_template("upload 7/9 failed for /mnt/disk1/obj after 2 retries"),
        );
        assert_eq!(
            normalize_template("upload 12/34 failed for /data/x9 after 5 retries"),
            "upload <n>/<n> failed for <path> after <n> retries"
        );
        assert_eq!(normalize_template("lock 'media/a.bin' timed out"), "lock '<q>' timed out");
        assert_eq!(normalize_template("peer 10.0.0.2:9000 down"), "peer <addr> down");
    }

    #[test]
    fn end_to_end_pipeline_produces_findings_and_clusters() {
        let mut a = analyzer(AnalyzeOptions::default());
        for i in 0..3 {
            a.observe(event(
                "erasure write quorum (required=8, achieved=5)",
                Some(LogLevel::Error),
                Some(&format!("2026-07-15T03:0{i}:00+08:00")),
            ));
        }
        for suffix in ["a", "b"] {
            a.observe(event(
                &format!("failed to sync /data/{suffix} after 5 retries"),
                Some(LogLevel::Error),
                Some("2026-07-15T03:05:00+08:00"),
            ));
        }
        a.observe(event("request ok", Some(LogLevel::Info), Some("2026-07-15T03:06:00+08:00")));

        let report = a.finalize(IngestReport::default());
        assert_eq!(report.summary.events_total, 6);
        assert_eq!(report.summary.events_after_filter, 6);
        assert_eq!(report.summary.level_counts["ERROR"], 5);
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].rule_id, "ec-write-quorum");
        assert_eq!(report.findings[0].count, 3);
        assert_eq!(report.unmatched_top.len(), 1);
        assert_eq!(report.unmatched_top[0].count, 2);
        assert_eq!(report.unmatched_top[0].template, "failed to sync <path> after <n> retries");
        assert_eq!(report.summary.distinct_offsets, vec!["+08:00"]);
    }

    #[test]
    fn since_until_filters_timestamped_events_only() {
        let mut a = analyzer(AnalyzeOptions {
            since: Some(DateTime::parse_from_rfc3339("2026-07-15T03:00:00+08:00").expect("ts")),
            ..Default::default()
        });
        a.observe(event("Disk full", Some(LogLevel::Error), Some("2026-07-15T02:00:00+08:00")));
        a.observe(event("Disk full", Some(LogLevel::Error), Some("2026-07-15T04:00:00+08:00")));
        let mut panic_ev = event("thread 'main' panicked at src/main.rs:1:1: boom", Some(LogLevel::Error), None);
        panic_ev.kind = EventKind::Panic;
        a.observe(panic_ev);

        let report = a.finalize(IngestReport::default());
        assert_eq!(report.summary.events_total, 3);
        assert_eq!(report.summary.events_after_filter, 2);
        let ids: Vec<_> = report.findings.iter().map(|f| f.rule_id.as_str()).collect();
        assert!(ids.contains(&"disk-full"));
        assert!(ids.contains(&"process-panic"), "timestamp-less panic must survive --since");
        assert_eq!(report.findings.iter().find(|f| f.rule_id == "disk-full").expect("f").count, 1);
    }

    #[test]
    fn timeline_buckets_are_gap_filled_and_merged() {
        let mut a = analyzer(AnalyzeOptions::default());
        // 10h span -> 15m buckets (600/15 = 40 <= 60).
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T00:00:30Z")));
        a.observe(event("x", Some(LogLevel::Warn), Some("2026-07-15T10:00:30Z")));
        let report = a.finalize(IngestReport::default());
        assert_eq!(report.timeline.len(), 41);
        assert_eq!(report.timeline[0].error, 1);
        assert_eq!(report.timeline.last().expect("last").warn, 1);
        assert!(report.timeline[1..40].iter().all(|b| b.error + b.warn + b.other == 0));

        // Single event -> a single 1m bucket.
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T00:00:30Z")));
        assert_eq!(a.finalize(IngestReport::default()).timeline.len(), 1);
    }

    #[test]
    fn redaction_hashes_identifiers_consistently() {
        let make = |redact: bool| {
            let mut a = analyzer(AnalyzeOptions {
                redact,
                ..Default::default()
            });
            let mut ev = event(
                "erasure write quorum (required=8) peer 10.0.0.2:9000",
                Some(LogLevel::Error),
                Some("2026-07-15T03:00:00+08:00"),
            );
            ev.fields
                .insert("bucket".to_string(), serde_json::Value::String("media".into()));
            a.observe(ev.clone());
            a.observe(ev);
            a.finalize(IngestReport::default())
        };

        let plain = make(false);
        let sample = &plain.findings[0].samples[0];
        assert!(sample.message.contains("10.0.0.2"));
        assert_eq!(sample.field_str("bucket"), Some("media"));

        let redacted = make(true);
        let sample = &redacted.findings[0].samples[0];
        assert!(!sample.message.contains("10.0.0.2"));
        let hashed = sample.field_str("bucket").expect("bucket").to_string();
        assert!(hashed.starts_with("h:"));
        // Same value -> same hash across samples.
        assert_eq!(redacted.findings[0].samples[1].field_str("bucket"), Some(hashed.as_str()));
    }

    #[test]
    fn mixed_offsets_are_reported() {
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:00:00+08:00")));
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:00:00Z")));
        let report = a.finalize(IngestReport::default());
        assert_eq!(report.summary.distinct_offsets, vec!["+08:00", "Z"]);
    }

    #[test]
    fn empty_input_produces_a_renderable_report() {
        let report = analyzer(AnalyzeOptions::default()).finalize(IngestReport::default());
        assert_eq!(report.summary.events_total, 0);
        assert!(report.findings.is_empty());
        assert!(report.timeline.is_empty());
        assert!(serde_json::to_string(&report).is_ok());
    }

    #[test]
    fn low_confidence_findings_are_split_out() {
        let mut a = analyzer(AnalyzeOptions::default());
        // client-signature-mismatch has min_count 10; feed 3.
        for _ in 0..3 {
            a.observe(event("SignatureDoesNotMatch", Some(LogLevel::Error), None));
        }
        let report = a.finalize(IngestReport::default());
        assert!(report.findings.is_empty());
        assert_eq!(report.low_confidence.len(), 1);
        assert_eq!(report.low_confidence[0].rule_id, "client-signature-mismatch");
    }
}
