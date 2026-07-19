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
use crate::rules::{Finding, FindingsCollector, RuleEngine, Severity};
use chrono::{DateTime, Duration, FixedOffset, Utc};
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

/// One deterministic timeline heuristic hit (rustfs/backlog#1290 sub-item
/// B). All three kinds are hints, not verdicts.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineAnomaly {
    /// "mixed_offsets" | "node_ranges_disjoint" | "log_gap".
    pub kind: String,
    /// Operator-facing description.
    pub message: String,
    /// Involved node labels (node_ranges_disjoint); empty otherwise.
    pub nodes: Vec<String>,
    /// Gap boundaries in UTC (log_gap); null otherwise.
    pub gap_start: Option<DateTime<Utc>>,
    pub gap_end: Option<DateTime<Utc>>,
    /// log_gap only: a startup-class finding begins within 5min after the gap.
    pub restart_evidence: bool,
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
    /// Timeline/clock heuristics (schema v2, rustfs/backlog#1290).
    pub timeline_anomalies: Vec<TimelineAnomaly>,
    /// Pass-through of every skipped input (no silent caps).
    pub skipped_inputs: Vec<(String, SkipReason)>,
}

/// (min ts, max ts, timestamped event count) for one node label.
type NodeRange = (DateTime<FixedOffset>, DateTime<FixedOffset>, u64);

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
    /// node label -> (min ts, max ts, timestamped event count); feeds the
    /// disjoint-node-ranges heuristic.
    node_ranges: BTreeMap<String, NodeRange>,
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
            node_ranges: BTreeMap::new(),
            minute_buckets: BTreeMap::new(),
            unmatched: HashMap::new(),
            unmatched_overflow: 0,
        }
    }

    /// Wire this directly as the ingest `on_event` callback.
    pub fn observe(&mut self, mut event: LogEvent) {
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

        // Under --redact, hash the node label once here so every downstream
        // surface (summary.nodes, per-node timeline ranges, samples, timeline
        // anomalies) shows the same stable hash rather than the raw hostname.
        if self.opts.redact
            && let Some(node) = event.node.as_deref()
        {
            event.node = Some(std::sync::Arc::from(redact::hash_value(node).as_str()));
        }

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
            if let Some(node) = event.node.as_deref() {
                self.node_ranges
                    .entry(node.to_string())
                    .and_modify(|(min, max, count)| {
                        *min = (*min).min(ts);
                        *max = (*max).max(ts);
                        *count += 1;
                    })
                    .or_insert((ts, ts, 1));
            }
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
        collapse_findings(&mut findings);

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

        let (timeline, bucket_width_min) = merge_timeline(&self.minute_buckets);
        let timeline_anomalies = detect_timeline_anomalies(
            &self.offsets,
            &self.node_ranges,
            &self.minute_buckets,
            bucket_width_min,
            &findings,
            &low_confidence,
        );

        let mut report = AnalysisReport {
            schema_version: 2,
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
            timeline_anomalies,
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
        // Lexicographic min on (sample, target) keeps the representative
        // order-independent and the shown target from that same event.
        if (sample.as_str(), &event.target) < (entry.sample.as_str(), &entry.target) {
            entry.sample = sample;
            entry.target = event.target.clone();
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

/// Returns the merged display buckets and the chosen bucket width (minutes);
/// the width also parameterizes the log-gap heuristic threshold.
fn merge_timeline(minute_buckets: &BTreeMap<i64, BucketCounts>) -> (Vec<TimeBucket>, i64) {
    let (Some((&first, _)), Some((&last, _))) = (minute_buckets.first_key_value(), minute_buckets.last_key_value()) else {
        return (Vec::new(), 1);
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
    let buckets = merged
        .into_iter()
        .map(|(slot, counts)| TimeBucket {
            start: DateTime::<Utc>::from_timestamp(slot * width * 60, 0).expect("valid timestamp"),
            error: counts.error,
            warn: counts.warn,
            other: counts.other,
        })
        .collect();
    (buckets, width)
}

/// Finding ids whose presence right after a log gap upgrades it to restart
/// evidence, and ids that make mixed offsets a likely signature root cause.
const STARTUP_RULE_IDS: [&str; 3] = ["startup-fatal", "runtime-failed", "endpoint-resolve-failed"];
const SIGNATURE_RULE_IDS: [&str; 2] = ["client-signature-mismatch", "internode-signature-mismatch"];

/// Timeline/clock heuristics (rustfs/backlog#1290 sub-item B). Three
/// deterministic hints — mixed UTC offsets, disjoint per-node time ranges,
/// and gaps in an otherwise continuous timeline — each phrased as a hint,
/// never a verdict.
fn detect_timeline_anomalies(
    offsets: &BTreeSet<String>,
    node_ranges: &BTreeMap<String, NodeRange>,
    minute_buckets: &BTreeMap<i64, BucketCounts>,
    bucket_width_min: i64,
    findings: &[Finding],
    low_confidence: &[Finding],
) -> Vec<TimelineAnomaly> {
    let mut anomalies = Vec::new();
    let has_finding = |ids: &[&str]| {
        findings
            .iter()
            .chain(low_confidence)
            .any(|f| ids.contains(&f.rule_id.as_str()))
    };

    // 1. Mixed UTC offsets: nodes disagree on timezone or clock source.
    if offsets.len() > 1 {
        let mut message = format!(
            "日志包含多个 UTC 偏移({}):节点时区/时钟源可能不一致,跨节点排序前先归一到 UTC。",
            offsets.iter().cloned().collect::<Vec<_>>().join(" / ")
        );
        if has_finding(&SIGNATURE_RULE_IDS) {
            message.push_str("同时检测到签名不匹配类 finding,时钟偏移是其高概率根因。");
        }
        anomalies.push(TimelineAnomaly {
            kind: "mixed_offsets".to_string(),
            message,
            nodes: Vec::new(),
            gap_start: None,
            gap_end: None,
            restart_evidence: false,
        });
    }

    // 2. Per-node time ranges that do not overlap at all (both sides with a
    // meaningful sample size): clocks are wrong or collection windows differ.
    let sizable: Vec<(&String, &NodeRange)> = node_ranges.iter().filter(|(_, (_, _, count))| *count > 100).collect();
    for (i, (node_a, (min_a, max_a, _))) in sizable.iter().enumerate() {
        for (node_b, (min_b, max_b, _)) in sizable.iter().skip(i + 1) {
            if max_a < min_b || max_b < min_a {
                anomalies.push(TimelineAnomaly {
                    kind: "node_ranges_disjoint".to_string(),
                    message: format!(
                        "节点 {node_a} 与 {node_b} 的日志时间范围完全不重叠({node_a}: {} → {},{node_b}: {} → {}):时钟错乱或采集不同期。",
                        min_a.to_rfc3339(),
                        max_a.to_rfc3339(),
                        min_b.to_rfc3339(),
                        max_b.to_rfc3339(),
                    ),
                    nodes: vec![(*node_a).clone(), (*node_b).clone()],
                    gap_start: None,
                    gap_end: None,
                    restart_evidence: false,
                });
            }
        }
    }

    // 3. Log gaps: >= 3 consecutive non-empty minutes, then a zero-event
    // span of at least max(15min, 3x display bucket width), then activity
    // again — the shape of a process restart or hang.
    let threshold = (3 * bucket_width_min).max(15);
    let startup_after_gap = |gap_end: DateTime<Utc>| {
        findings.iter().chain(low_confidence).find_map(|f| {
            if !STARTUP_RULE_IDS.contains(&f.rule_id.as_str()) {
                return None;
            }
            let first = f.first_seen?.with_timezone(&Utc);
            (first >= gap_end && first <= gap_end + Duration::minutes(5)).then(|| f.rule_id.clone())
        })
    };
    let minutes: Vec<i64> = minute_buckets.keys().copied().collect();
    let mut run_len = 1i64;
    for window in minutes.windows(2) {
        let (prev, next) = (window[0], window[1]);
        let delta = next - prev;
        if delta == 1 {
            run_len += 1;
            continue;
        }
        if run_len >= 3 && delta > threshold {
            let gap_start = DateTime::<Utc>::from_timestamp((prev + 1) * 60, 0).expect("valid timestamp");
            let gap_end = DateTime::<Utc>::from_timestamp(next * 60, 0).expect("valid timestamp");
            let restart = startup_after_gap(gap_end);
            let span = human_duration_minutes(delta - 1);
            let message = match &restart {
                Some(rule_id) => format!(
                    "时间线断档 {} → {}(约 {span}),断档后 5 分钟内出现 {rule_id}:重启证据。",
                    gap_start.to_rfc3339(),
                    gap_end.to_rfc3339(),
                ),
                None => format!(
                    "时间线断档 {} → {}(约 {span}),此前有连续活动:疑似进程重启或挂起区间。",
                    gap_start.to_rfc3339(),
                    gap_end.to_rfc3339(),
                ),
            };
            anomalies.push(TimelineAnomaly {
                kind: "log_gap".to_string(),
                message,
                nodes: Vec::new(),
                gap_start: Some(gap_start),
                gap_end: Some(gap_end),
                restart_evidence: restart.is_some(),
            });
        }
        run_len = 1;
    }
    anomalies
}

fn human_duration_minutes(minutes: i64) -> String {
    if minutes >= 60 {
        format!("{:.1}h", minutes as f64 / 60.0)
    } else {
        format!("{minutes}min")
    }
}

/// Causal folding (rustfs/backlog#1290 sub-item A): a symptom finding
/// collapses under a root-cause finding named by its rule's
/// `implies_root_cause` edges when the root plausibly precedes it
/// (`root.first_seen <= symptom.first_seen + 5min`) and is not a
/// long-finished historical episode (`root.last_seen >= symptom.first_seen -
/// 30min`). A timestamp-less side (pure stderr panics) associates by
/// existence. Only confirmed findings participate — folding real symptoms
/// under a low-confidence root would hide them behind shaky evidence.
///
/// Findings are then re-sorted so a root block is promoted to the most
/// severe position among itself and its collapsed symptoms, weighted by the
/// block's combined count: the report top keeps answering "the most likely
/// cause" even when the root itself is a lower-severity finding.
fn collapse_findings(findings: &mut [Finding]) {
    let index_of: HashMap<String, usize> = findings.iter().enumerate().map(|(i, f)| (f.rule_id.clone(), i)).collect();
    // Among several qualifying roots pick the earliest first_seen;
    // timestamp-less roots come last, rule id breaks remaining ties.
    let root_key = |f: &Finding| (f.first_seen.is_none(), f.first_seen, f.rule_id.clone());

    let mut chosen: Vec<Option<usize>> = vec![None; findings.len()];
    for (i, symptom) in findings.iter().enumerate() {
        for root_id in &symptom.implies_root_cause {
            let Some(&r) = index_of.get(root_id) else { continue };
            if r == i {
                continue;
            }
            let root = &findings[r];
            let qualifies = match (root.first_seen, symptom.first_seen) {
                (Some(root_first), Some(symptom_first)) => {
                    root_first <= symptom_first + Duration::minutes(5)
                        && root.last_seen.unwrap_or(root_first) >= symptom_first - Duration::minutes(30)
                }
                _ => true,
            };
            if qualifies && chosen[i].is_none_or(|cur| root_key(&findings[r]) < root_key(&findings[cur])) {
                chosen[i] = Some(r);
            }
        }
    }

    // Follow chains so a symptom never points at a root that is itself
    // collapsed (external rules may build A -> B -> C edges); the visited
    // guard stops on adversarial cycles.
    let resolve = |mut r: usize| {
        let mut seen = vec![false; chosen.len()];
        while let Some(next) = chosen[r] {
            if seen[r] {
                break;
            }
            seen[r] = true;
            r = next;
        }
        r
    };

    let mut caused: Vec<Vec<String>> = vec![Vec::new(); findings.len()];
    for i in 0..findings.len() {
        let Some(first) = chosen[i] else { continue };
        let root = resolve(first);
        if root == i {
            continue;
        }
        let root_id = findings[root].rule_id.clone();
        findings[i].collapsed_into = Some(root_id);
        caused[root].push(findings[i].rule_id.clone());
    }
    for (i, mut list) in caused.into_iter().enumerate() {
        list.sort();
        findings[i].caused = list;
    }
    // A cycle can leave a "root" both collapsed and carrying symptoms; keep
    // it visible rather than folding every participant out of the report.
    for finding in findings.iter_mut() {
        if !finding.caused.is_empty() {
            finding.collapsed_into = None;
        }
    }

    let mut effective: HashMap<String, (Severity, u64)> =
        findings.iter().map(|f| (f.rule_id.clone(), (f.severity, f.count))).collect();
    for finding in findings.iter() {
        if let Some(root) = &finding.collapsed_into {
            let entry = effective.get_mut(root).expect("collapse target exists");
            entry.0 = entry.0.min(finding.severity);
            entry.1 += finding.count;
        }
    }
    findings.sort_by(|a, b| {
        let (severity_a, count_a) = effective[&a.rule_id];
        let (severity_b, count_b) = effective[&b.rule_id];
        severity_a
            .cmp(&severity_b)
            .then(count_b.cmp(&count_a))
            .then(a.rule_id.cmp(&b.rule_id))
    });
}

fn redact_report(report: &mut AnalysisReport) {
    for finding in report.findings.iter_mut().chain(report.low_confidence.iter_mut()) {
        // Samples carry the full event (message + every field + provenance);
        // node labels are already hashed at ingestion time.
        for sample in &mut finding.samples {
            redact::redact_event(sample);
        }
        // Evidence values: sensitive-named fields hash whole, everything else
        // is scrubbed for embedded IPs / key=value identifiers.
        for (field, values) in &mut finding.evidence {
            let redacted: BTreeSet<String> = values
                .values
                .iter()
                .map(|v| redact::redact_evidence_value(field, v))
                .collect();
            values.values = redacted;
        }
    }
    for cluster in &mut report.unmatched_top {
        cluster.template = redact::redact_text(&cluster.template);
        cluster.sample = redact::redact_text(&cluster.sample);
        if let Some(target) = cluster.target.take() {
            cluster.target = Some(redact::redact_text(&target));
        }
    }
    // Skipped-input provenance (customer archive names / absolute paths).
    for (path, _) in &mut report.skipped_inputs {
        *path = redact::redact_provenance(path);
    }
    // summary.nodes and every node label inside timeline_anomalies derive from
    // event.node, hashed at ingestion, so no separate pass is needed here.
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
    fn disjoint_node_ranges_are_flagged() {
        let mut a = analyzer(AnalyzeOptions::default());
        // node1: 101 events across 00:00-01:40; node2: 101 events across
        // 03:00-04:40 — completely disjoint windows, both sizable.
        for (node, base_hour) in [("node1", 0), ("node2", 3)] {
            for i in 0..101 {
                let mut ev = event(
                    "request ok",
                    Some(LogLevel::Info),
                    Some(&format!("2026-07-15T{:02}:{:02}:00Z", base_hour + i / 60, i % 60)),
                );
                ev.node = Some(Arc::from(node));
                a.observe(ev);
            }
        }
        let report = a.finalize(IngestReport::default());
        let disjoint: Vec<_> = report
            .timeline_anomalies
            .iter()
            .filter(|a| a.kind == "node_ranges_disjoint")
            .collect();
        assert_eq!(disjoint.len(), 1, "{:?}", report.timeline_anomalies);
        assert_eq!(disjoint[0].nodes, vec!["node1".to_string(), "node2".to_string()]);
        assert!(disjoint[0].message.contains("完全不重叠"));

        // Below the >100-events bar the heuristic stays quiet.
        let mut a = analyzer(AnalyzeOptions::default());
        for (node, hour) in [("node1", 0), ("node2", 3)] {
            for i in 0..5 {
                let mut ev = event("request ok", Some(LogLevel::Info), Some(&format!("2026-07-15T0{hour}:0{i}:00Z")));
                ev.node = Some(Arc::from(node));
                a.observe(ev);
            }
        }
        let report = a.finalize(IngestReport::default());
        assert!(report.timeline_anomalies.iter().all(|a| a.kind != "node_ranges_disjoint"));
    }

    #[test]
    fn log_gap_is_flagged_and_upgrades_on_startup_finding() {
        let mut a = analyzer(AnalyzeOptions::default());
        // 4 consecutive active minutes, a 40min hole, then activity again.
        for minute in 0..4 {
            a.observe(event("x", Some(LogLevel::Error), Some(&format!("2026-07-15T03:0{minute}:00Z"))));
        }
        a.observe(event(
            "[FATAL] Observability initialization failed: collector unavailable",
            Some(LogLevel::Error),
            Some("2026-07-15T03:44:30Z"),
        ));
        let report = a.finalize(IngestReport::default());
        let gaps: Vec<_> = report.timeline_anomalies.iter().filter(|a| a.kind == "log_gap").collect();
        assert_eq!(gaps.len(), 1, "{:?}", report.timeline_anomalies);
        assert!(gaps[0].restart_evidence, "startup-fatal right after the gap: {:?}", gaps[0]);
        assert!(gaps[0].message.contains("重启证据"));
        assert_eq!(gaps[0].gap_start.expect("start").to_rfc3339(), "2026-07-15T03:04:00+00:00");
        assert_eq!(gaps[0].gap_end.expect("end").to_rfc3339(), "2026-07-15T03:44:00+00:00");

        // Same hole but no startup finding afterwards: still a gap, no upgrade.
        let mut a = analyzer(AnalyzeOptions::default());
        for minute in 0..4 {
            a.observe(event("x", Some(LogLevel::Error), Some(&format!("2026-07-15T03:0{minute}:00Z"))));
        }
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:44:30Z")));
        let report = a.finalize(IngestReport::default());
        let gaps: Vec<_> = report.timeline_anomalies.iter().filter(|a| a.kind == "log_gap").collect();
        assert_eq!(gaps.len(), 1);
        assert!(!gaps[0].restart_evidence);
        assert!(gaps[0].message.contains("疑似进程重启或挂起"));

        // A short 3-minute lull is not a gap.
        let mut a = analyzer(AnalyzeOptions::default());
        for minute in 0..4 {
            a.observe(event("x", Some(LogLevel::Error), Some(&format!("2026-07-15T03:0{minute}:00Z"))));
        }
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:07:00Z")));
        let report = a.finalize(IngestReport::default());
        assert!(report.timeline_anomalies.iter().all(|a| a.kind != "log_gap"));
    }

    #[test]
    fn mixed_offsets_anomaly_requires_multiple_offsets() {
        // Single offset: quiet.
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:00:00+08:00")));
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T04:00:00+08:00")));
        let report = a.finalize(IngestReport::default());
        assert!(report.timeline_anomalies.iter().all(|a| a.kind != "mixed_offsets"));

        // Mixed offsets plus a signature finding: flagged with the clock note.
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event("x", Some(LogLevel::Error), Some("2026-07-15T03:00:00+08:00")));
        a.observe(event("SignatureDoesNotMatch", Some(LogLevel::Error), Some("2026-07-15T03:00:00Z")));
        let report = a.finalize(IngestReport::default());
        let mixed: Vec<_> = report
            .timeline_anomalies
            .iter()
            .filter(|a| a.kind == "mixed_offsets")
            .collect();
        assert_eq!(mixed.len(), 1);
        assert!(mixed[0].message.contains("+08:00"));
        assert!(mixed[0].message.contains("签名不匹配"), "signature note missing: {}", mixed[0].message);
    }

    #[test]
    fn causal_collapse_folds_quorum_under_disk_faulty() {
        let mut a = analyzer(AnalyzeOptions::default());
        // Root: disk faulty at t0 (P2). Symptom: write quorum at t0+2min (P1).
        for minute in [0, 1] {
            a.observe(event(
                "Disk health check marked disk faulty",
                Some(LogLevel::Error),
                Some(&format!("2026-07-15T03:0{minute}:00+08:00")),
            ));
        }
        for second in 0..3 {
            a.observe(event(
                "erasure write quorum (required=8, achieved=5)",
                Some(LogLevel::Error),
                Some(&format!("2026-07-15T03:02:0{second}+08:00")),
            ));
        }
        let report = a.finalize(IngestReport::default());

        let quorum = report
            .findings
            .iter()
            .find(|f| f.rule_id == "ec-write-quorum")
            .expect("quorum");
        assert_eq!(quorum.collapsed_into.as_deref(), Some("disk-marked-faulty"));
        let disk = report
            .findings
            .iter()
            .find(|f| f.rule_id == "disk-marked-faulty")
            .expect("disk");
        assert_eq!(disk.caused, vec!["ec-write-quorum".to_string()]);
        // The P2 root is promoted to the collapsed P1 symptom's position.
        assert_eq!(report.findings[0].rule_id, "disk-marked-faulty");

        let mut out = Vec::new();
        crate::report::render(&report, crate::report::ReportFormat::Text, &mut out).expect("render");
        let text = String::from_utf8(out).expect("utf8");
        assert!(text.contains("级联症状: ec-write-quorum ×3"), "missing cascade line:\n{text}");
        assert!(
            !text.contains("[P1 服务不可用] ec-write-quorum"),
            "collapsed symptom must not render flat:\n{text}"
        );
    }

    #[test]
    fn causal_collapse_respects_the_time_window() {
        let mut a = analyzer(AnalyzeOptions::default());
        // Root appears 2h AFTER the symptom: outside the 5min precedence window.
        a.observe(event(
            "erasure write quorum (required=8, achieved=5)",
            Some(LogLevel::Error),
            Some("2026-07-15T03:00:00+08:00"),
        ));
        a.observe(event(
            "Disk health check marked disk faulty",
            Some(LogLevel::Error),
            Some("2026-07-15T05:00:00+08:00"),
        ));
        let report = a.finalize(IngestReport::default());
        let quorum = report
            .findings
            .iter()
            .find(|f| f.rule_id == "ec-write-quorum")
            .expect("quorum");
        assert_eq!(quorum.collapsed_into, None);
        assert!(report.findings.iter().all(|f| f.caused.is_empty()));
    }

    #[test]
    fn causal_collapse_uses_existence_for_timestamp_less_roots() {
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event(
            "rwlock IAM cache poisoned, recovering",
            Some(LogLevel::Error),
            Some("2026-07-15T03:00:00+08:00"),
        ));
        let mut panic_ev = event("thread 'main' panicked at src/main.rs:1:1: boom", Some(LogLevel::Error), None);
        panic_ev.kind = EventKind::Panic;
        a.observe(panic_ev);

        let report = a.finalize(IngestReport::default());
        let poisoned = report
            .findings
            .iter()
            .find(|f| f.rule_id == "rwlock-poisoned")
            .expect("poisoned");
        assert_eq!(poisoned.collapsed_into.as_deref(), Some("process-panic"));
        let panic = report.findings.iter().find(|f| f.rule_id == "process-panic").expect("panic");
        assert_eq!(panic.caused, vec!["rwlock-poisoned".to_string()]);
    }

    #[test]
    fn json_keeps_collapsed_findings_with_relation_fields() {
        let mut a = analyzer(AnalyzeOptions::default());
        a.observe(event(
            "Disk health check marked disk faulty",
            Some(LogLevel::Error),
            Some("2026-07-15T03:00:00+08:00"),
        ));
        a.observe(event(
            "erasure write quorum (required=8, achieved=5)",
            Some(LogLevel::Error),
            Some("2026-07-15T03:02:00+08:00"),
        ));
        let report = a.finalize(IngestReport::default());
        let value = serde_json::to_value(&report).expect("json");
        let findings = value["findings"].as_array().expect("array");
        let ids: Vec<&str> = findings.iter().map(|f| f["rule_id"].as_str().expect("id")).collect();
        assert!(ids.contains(&"ec-write-quorum"), "collapsed finding stays in JSON: {ids:?}");
        let quorum = findings.iter().find(|f| f["rule_id"] == "ec-write-quorum").expect("quorum");
        assert_eq!(quorum["collapsed_into"], "disk-marked-faulty");
        let disk = findings.iter().find(|f| f["rule_id"] == "disk-marked-faulty").expect("disk");
        assert_eq!(disk["caused"][0], "ec-write-quorum");
    }

    #[test]
    fn unmatched_cluster_representative_is_order_independent() {
        let mk = |msg: &str, target: &str| {
            let mut ev = event(msg, Some(LogLevel::Error), None);
            ev.target = Some(target.to_string());
            ev
        };
        // Same template, different messages and targets.
        let a = mk("failed to sync /data/a after 5 retries", "rustfs::sync::a");
        let b = mk("failed to sync /data/b after 2 retries", "rustfs::sync::b");

        let representative = |events: &[&LogEvent]| {
            let mut an = analyzer(AnalyzeOptions::default());
            for ev in events {
                an.observe((*ev).clone());
            }
            let report = an.finalize(IngestReport::default());
            let cluster = &report.unmatched_top[0];
            (cluster.sample.clone(), cluster.target.clone())
        };
        let forward = representative(&[&a, &b]);
        let reverse = representative(&[&b, &a]);
        assert_eq!(forward, reverse);
        // Sample and target must come from the same event.
        assert_eq!(forward.0, "failed to sync /data/a after 5 retries");
        assert_eq!(forward.1.as_deref(), Some("rustfs::sync::a"));
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
    fn redaction_covers_evidence_provenance_node_and_template() {
        let mut a = analyzer(AnalyzeOptions {
            redact: true,
            ..Default::default()
        });

        // Matched rule that captures a `peer` evidence field (not a whitelisted
        // name before the fix), on a customer node label + provenance path,
        // with an IPv6 peer in the message.
        let mut ev = event(
            "peer_connection_marked_offline dial fe80::dead:9 failed",
            Some(LogLevel::Error),
            Some("2026-07-15T03:00:00+08:00"),
        );
        ev.fields
            .insert("peer".into(), serde_json::Value::String("10.0.0.99:9000".into()));
        ev.node = Some(Arc::from("prod-node-01"));
        ev.source = SourceRef {
            file: Arc::from("/home/acme-corp/bundle/prod-node-01/rustfs.log"),
            line: 42,
        };
        a.observe(ev);

        // Unmatched ERROR whose message carries a field-shaped bucket name.
        let mut un = event("GetObject failed bucket: topsecret down", Some(LogLevel::Error), None);
        un.node = Some(Arc::from("prod-node-01"));
        a.observe(un);

        let report = a.finalize(IngestReport::default());
        let mut out = Vec::new();
        crate::report::render(&report, crate::report::ReportFormat::Text, &mut out).expect("render");
        let text = String::from_utf8(out).expect("utf8");

        assert!(!text.contains("10.0.0.99"), "peer evidence leaked:\n{text}");
        assert!(!text.contains("fe80::dead"), "IPv6 in sample leaked:\n{text}");
        assert!(!text.contains("prod-node-01"), "node label leaked:\n{text}");
        assert!(!text.contains("acme-corp"), "provenance prefix leaked:\n{text}");
        assert!(!text.contains("topsecret"), "unmatched template bucket leaked:\n{text}");
        // Node stays correlatable via a stable hash.
        assert_eq!(report.summary.nodes.len(), 1);
        assert!(report.summary.nodes[0].starts_with("h:"), "node not hashed: {:?}", report.summary.nodes);
        // The JSON renderer dumps the whole sample event — it must not leak the
        // raw peer field either.
        let mut jout = Vec::new();
        crate::report::render(&report, crate::report::ReportFormat::Json, &mut jout).expect("json");
        let json = String::from_utf8(jout).expect("utf8");
        assert!(!json.contains("10.0.0.99"), "JSON fields dump leaked peer:\n{json}");
        assert!(!json.contains("prod-node-01"), "JSON leaked node:\n{json}");
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
