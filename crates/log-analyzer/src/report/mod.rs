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

//! Report rendering (rustfs/backlog#1287): terminal text (no ANSI,
//! pipe-friendly), stable JSON (`schema_version`), and Markdown that pastes
//! straight into a support ticket.

mod json;
mod markdown;
mod text;

use crate::analyze::AnalysisReport;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportFormat {
    Text,
    Json,
    Markdown,
}

pub fn render(report: &AnalysisReport, format: ReportFormat, writer: &mut dyn io::Write) -> io::Result<()> {
    match format {
        ReportFormat::Text => text::render(report, writer),
        ReportFormat::Json => json::render(report, writer),
        ReportFormat::Markdown => markdown::render(report, writer),
    }
}

// -- shared formatting helpers --

pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 4] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10), ("B", 1)];
    for (unit, size) in UNITS {
        if bytes >= size {
            return format!("{:.1} {unit}", bytes as f64 / size as f64);
        }
    }
    "0 B".to_string()
}

pub(crate) fn human_duration(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds >= 86_400 {
        format!("{:.1}d", seconds as f64 / 86_400.0)
    } else if seconds >= 3_600 {
        format!("{:.1}h", seconds as f64 / 3_600.0)
    } else if seconds >= 60 {
        format!("{:.1}m", seconds as f64 / 60.0)
    } else {
        format!("{seconds}s")
    }
}

pub(crate) fn parse_percentage(report: &AnalysisReport) -> f64 {
    report.summary.parse.json_ratio() * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::{AnalyzeOptions, Analyzer};
    use crate::ingest::IngestReport;
    use crate::model::{EventKind, LogEvent, LogLevel, SourceRef};
    use crate::rules::{RuleEngine, seed_rule_set};
    use chrono::DateTime;
    use std::sync::Arc;

    fn fixed_report() -> AnalysisReport {
        let mut analyzer = Analyzer::new(RuleEngine::new(seed_rule_set()), AnalyzeOptions::default());
        let mut ev = LogEvent {
            timestamp: Some(DateTime::parse_from_rfc3339("2026-07-15T03:12:44+08:00").expect("ts")),
            level: Some(LogLevel::Error),
            target: Some("rustfs_ecstore".to_string() + "::erasure"),
            message: "erasure write quorum (required=8, achieved=5, failed=3)".to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("customer.zip!node2/rustfs.log"),
                line: 88231,
            },
            node: Some(Arc::from("node2")),
            kind: EventKind::Json,
        };
        ev.fields
            .insert("required".to_string(), serde_json::Value::String("8".into()));
        analyzer.observe(ev.clone());
        analyzer.observe(LogEvent {
            timestamp: Some(DateTime::parse_from_rfc3339("2026-07-15T03:20:44+08:00").expect("ts")),
            ..ev
        });
        analyzer.observe(LogEvent {
            timestamp: Some(DateTime::parse_from_rfc3339("2026-07-15T03:25:00+08:00").expect("ts")),
            level: Some(LogLevel::Error),
            target: None,
            message: "failed to sync /data/x9 after 5 retries".to_string(),
            fields: serde_json::Map::new(),
            source: SourceRef {
                file: Arc::from("customer.zip!node1/rustfs.log"),
                line: 12,
            },
            node: Some(Arc::from("node1")),
            kind: EventKind::Json,
        });

        let ingest = IngestReport {
            files_parsed: 2,
            bytes_fed: 1_234_567,
            skipped: vec![("customer.zip!node3/core.bin".to_string(), crate::ingest::SkipReason::Binary)],
            stats: crate::model::ParseStats {
                total_lines: 3,
                json_ok: 3,
                ..Default::default()
            },
        };
        analyzer.finalize(ingest)
    }

    #[test]
    fn text_report_contains_every_section() {
        let report = fixed_report();
        let mut out = Vec::new();
        render(&report, ReportFormat::Text, &mut out).expect("render");
        let text = String::from_utf8(out).expect("utf8");

        assert!(text.starts_with("RustFS 日志诊断报告\n"));
        assert!(text.ends_with('\n'));
        for needle in [
            "时间范围",
            "解析率 100.0%",
            "node1, node2",
            "[!] 跳过   customer.zip!node3/core.bin (binary)",
            "[P1 服务不可用] ec-write-quorum",
            "× 2",
            "诊断: 在线盘数低于写仲裁阈值,写入被拒。",
            "证据: required = 8",
            "customer.zip!node2/rustfs.log:88231",
            "未识别的高频错误",
            "failed to sync <path> after <n> retries",
            "时间线",
        ] {
            assert!(text.contains(needle), "text report missing {needle:?}\n---\n{text}");
        }
    }

    #[test]
    fn json_report_is_stable_and_parseable() {
        let report = fixed_report();
        let mut out = Vec::new();
        render(&report, ReportFormat::Json, &mut out).expect("render");
        let value: serde_json::Value = serde_json::from_slice(&out).expect("parse");
        assert_eq!(value["schema_version"], 2);
        assert!(value["timeline_anomalies"].is_array());
        assert_eq!(value["findings"][0]["rule_id"], "ec-write-quorum");
        assert_eq!(value["findings"][0]["count"], 2);
        assert_eq!(value["summary"]["files_parsed"], 2);
        assert_eq!(value["unmatched_top"][0]["template"], "failed to sync <path> after <n> retries");
        assert_eq!(value["skipped_inputs"][0][1], "binary");
        assert!(out.ends_with(b"\n"));
    }

    #[test]
    fn markdown_report_is_ticket_pasteable() {
        let report = fixed_report();
        let mut out = Vec::new();
        render(&report, ReportFormat::Markdown, &mut out).expect("render");
        let md = String::from_utf8(out).expect("utf8");
        assert!(md.starts_with("# RustFS 日志诊断报告\n"));
        assert!(md.ends_with('\n'));
        for needle in [
            "## 概览",
            "| 解析率 |",
            "### [P1 服务不可用] ec-write-quorum",
            "```json",
            "## 未识别的高频错误",
            "| 1 | ERROR | `failed to sync <path> after <n> retries` |",
            "## 时间线",
        ] {
            assert!(md.contains(needle), "markdown report missing {needle:?}\n---\n{md}");
        }
    }

    #[test]
    fn empty_report_renders_the_no_findings_notice() {
        let analyzer = Analyzer::new(RuleEngine::new(seed_rule_set()), AnalyzeOptions::default());
        let report = analyzer.finalize(IngestReport::default());
        for format in [ReportFormat::Text, ReportFormat::Markdown, ReportFormat::Json] {
            let mut out = Vec::new();
            render(&report, format, &mut out).expect("render");
            assert!(!out.is_empty());
        }
        let mut out = Vec::new();
        render(&report, ReportFormat::Text, &mut out).expect("render");
        assert!(String::from_utf8(out).expect("utf8").contains("未发现已知故障模式"));
    }

    #[test]
    fn helpers_format_reasonably() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1_234_567), "1.2 MiB");
        assert_eq!(human_bytes(5 << 30), "5.0 GiB");
        assert_eq!(human_duration(42), "42s");
        assert_eq!(human_duration(42_480), "11.8h");
        assert_eq!(human_duration(200_000), "2.3d");
    }
}
