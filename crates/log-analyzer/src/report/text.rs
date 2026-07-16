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

//! Terminal text renderer: no ANSI colors (pipe-friendly by design).

use super::{human_bytes, human_duration, parse_percentage};
use crate::analyze::AnalysisReport;
use crate::rules::Finding;
use std::io;

const HEAVY_RULE: &str = "════════════════════════════════════════";
const LIGHT_RULE: &str = "────────────────────────────────────────";

pub(super) fn render(report: &AnalysisReport, w: &mut dyn io::Write) -> io::Result<()> {
    writeln!(w, "RustFS 日志诊断报告")?;
    writeln!(w, "{HEAVY_RULE}")?;

    match report.summary.time_range {
        Some((first, last)) => {
            let span = human_duration(last.signed_duration_since(first).num_seconds());
            writeln!(w, "时间范围   {} → {}   ({span})", first.to_rfc3339(), last.to_rfc3339())?;
        }
        None => writeln!(w, "时间范围   (无带时间戳的事件)")?,
    }
    let stats = &report.summary.parse;
    writeln!(
        w,
        "输入       {} 个文件, {} 解压后, 解析率 {:.1}% (JSON {} / panic {} / 文本 {})",
        report.summary.files_parsed,
        human_bytes(report.summary.bytes_fed),
        parse_percentage(report),
        stats.json_ok + stats.prefixed_json,
        stats.panic_blocks,
        stats.text_lines,
    )?;
    let nodes = if report.summary.nodes.is_empty() {
        "-".to_string()
    } else {
        report.summary.nodes.join(", ")
    };
    writeln!(w, "节点       {nodes}")?;
    let levels: Vec<String> = report
        .summary
        .level_counts
        .iter()
        .map(|(level, count)| format!("{level} {count}"))
        .collect();
    writeln!(
        w,
        "级别       {}",
        if levels.is_empty() {
            "-".to_string()
        } else {
            levels.join(" · ")
        }
    )?;
    writeln!(
        w,
        "事件       {} 条参与分析 / {} 条读入",
        report.summary.events_after_filter, report.summary.events_total
    )?;
    for (path, reason) in &report.skipped_inputs {
        writeln!(w, "[!] 跳过   {path} ({})", reason_slug(*reason))?;
    }
    if report.summary.distinct_offsets.len() > 1 {
        writeln!(
            w,
            "[!] 时区   混合偏移 {} —— 跨节点时间对比需注意",
            report.summary.distinct_offsets.join(" / ")
        )?;
    }
    writeln!(w)?;

    if !report.timeline_anomalies.is_empty() {
        writeln!(w, "时间线异常(提示)")?;
        writeln!(w, "{LIGHT_RULE}")?;
        for anomaly in &report.timeline_anomalies {
            writeln!(w, "[!] {}", anomaly.message)?;
        }
        writeln!(w)?;
    }

    writeln!(w, "发现(按严重度)")?;
    writeln!(w, "{LIGHT_RULE}")?;
    if report.findings.is_empty() {
        writeln!(w, "未发现已知故障模式;请查看下方「未识别的高频错误」是否有线索。")?;
    }
    // Collapsed symptoms render inside their root's block, not flat.
    let counts: std::collections::HashMap<&str, u64> = report.findings.iter().map(|f| (f.rule_id.as_str(), f.count)).collect();
    for finding in report.findings.iter().filter(|f| f.collapsed_into.is_none()) {
        render_finding(finding, &counts, w)?;
    }

    if !report.low_confidence.is_empty() {
        writeln!(w)?;
        writeln!(w, "低频提示(命中数低于规则阈值,仅供参考)")?;
        writeln!(w, "{LIGHT_RULE}")?;
        for finding in &report.low_confidence {
            writeln!(w, "    {} ×{}  {}", finding.rule_id, finding.count, finding.title)?;
        }
    }

    writeln!(w)?;
    writeln!(w, "未识别的高频错误(规则库未覆盖,Top {})", report.unmatched_top.len())?;
    writeln!(w, "{LIGHT_RULE}")?;
    if report.unmatched_top.is_empty() {
        writeln!(w, "    (无)")?;
    }
    for cluster in &report.unmatched_top {
        writeln!(w, "    ×{:<7} {:5}  {}", cluster.count, cluster.level, cluster.template)?;
    }

    if !report.timeline.is_empty() {
        writeln!(w)?;
        writeln!(w, "时间线(UTC,ERROR/WARN,{} 桶)", report.timeline.len())?;
        writeln!(w, "{LIGHT_RULE}")?;
        let max_errors = report.timeline.iter().map(|b| b.error).max().unwrap_or(0).max(1);
        for chunk in report.timeline.chunks(4) {
            let cells: Vec<String> = chunk
                .iter()
                .map(|bucket| {
                    let bar_len = (bucket.error * 6).div_ceil(max_errors) as usize;
                    format!(
                        "{} {:<6} {}/{}",
                        bucket.start.format("%m-%d %H:%M"),
                        "█".repeat(if bucket.error > 0 { bar_len.max(1) } else { 0 }),
                        bucket.error,
                        bucket.warn
                    )
                })
                .collect();
            writeln!(w, "    {}", cells.join("   "))?;
        }
    }
    Ok(())
}

fn render_finding(finding: &Finding, counts: &std::collections::HashMap<&str, u64>, w: &mut dyn io::Write) -> io::Result<()> {
    let nodes = finding.nodes.iter().cloned().collect::<Vec<_>>().join(", ");
    writeln!(
        w,
        "[{}] {}  {}  × {}   {}",
        finding.severity.label(),
        finding.rule_id,
        finding.title,
        finding.count,
        nodes
    )?;
    if let (Some(first), Some(last)) = (finding.first_seen, finding.last_seen) {
        writeln!(w, "    首次 {}   末次 {}", first.to_rfc3339(), last.to_rfc3339())?;
    }
    if !finding.caused.is_empty() {
        let items: Vec<String> = finding
            .caused
            .iter()
            .map(|id| format!("{id} ×{}", counts.get(id.as_str()).copied().unwrap_or(0)))
            .collect();
        writeln!(w, "    级联症状: {}", items.join("、"))?;
    }
    writeln!(w, "    诊断: {}", finding.diagnosis)?;
    writeln!(w, "    建议: {}", finding.suggestion)?;
    for (field, values) in &finding.evidence {
        let shown: Vec<&str> = values.values.iter().map(String::as_str).collect();
        let overflow = if values.overflow > 0 {
            format!(" (+{} 更多)", values.overflow)
        } else {
            String::new()
        };
        writeln!(w, "    证据: {field} = {}{overflow}", shown.join(", "))?;
    }
    for sample in &finding.samples {
        let ts = sample.timestamp.map_or_else(|| "(无时间戳)".to_string(), |t| t.to_rfc3339());
        let node = sample.node.as_deref().unwrap_or("-");
        writeln!(w, "    样本: {ts} {node} {}:{}", sample.source.file, sample.source.line)?;
        writeln!(w, "          {}", sample.message)?;
    }
    Ok(())
}

fn reason_slug(reason: crate::ingest::SkipReason) -> &'static str {
    use crate::ingest::SkipReason::*;
    match reason {
        Binary => "binary",
        TooDeep => "too_deep",
        EntryCap => "entry_cap",
        ByteCap => "byte_cap",
        LineTooLong => "line_too_long",
        ArchiveTooLargeForMemory => "archive_too_large_for_memory",
        Unreadable => "unreadable",
    }
}
