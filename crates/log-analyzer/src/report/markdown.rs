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

//! Markdown renderer — same information structure as text, formatted so it
//! pastes straight into a support ticket.

use super::{human_bytes, parse_percentage};
use crate::analyze::AnalysisReport;
use std::io;

pub(super) fn render(report: &AnalysisReport, w: &mut dyn io::Write) -> io::Result<()> {
    writeln!(w, "# RustFS 日志诊断报告")?;
    writeln!(w)?;
    writeln!(w, "## 概览")?;
    writeln!(w)?;
    writeln!(w, "| 项 | 值 |")?;
    writeln!(w, "|---|---|")?;
    match report.summary.time_range {
        Some((first, last)) => writeln!(w, "| 时间范围 | {} → {} |", first.to_rfc3339(), last.to_rfc3339())?,
        None => writeln!(w, "| 时间范围 | (无带时间戳的事件) |")?,
    }
    writeln!(w, "| 文件数 | {} |", report.summary.files_parsed)?;
    writeln!(w, "| 解压后字节 | {} |", human_bytes(report.summary.bytes_fed))?;
    writeln!(w, "| 解析率 | {:.1}% |", parse_percentage(report))?;
    writeln!(
        w,
        "| 节点 | {} |",
        if report.summary.nodes.is_empty() {
            "-".to_string()
        } else {
            report.summary.nodes.join(", ")
        }
    )?;
    let levels: Vec<String> = report
        .summary
        .level_counts
        .iter()
        .map(|(level, count)| format!("{level} {count}"))
        .collect();
    writeln!(
        w,
        "| 级别 | {} |",
        if levels.is_empty() {
            "-".to_string()
        } else {
            levels.join(" · ")
        }
    )?;
    writeln!(
        w,
        "| 事件 | {} 条参与分析 / {} 条读入 |",
        report.summary.events_after_filter, report.summary.events_total
    )?;
    if report.summary.distinct_offsets.len() > 1 {
        writeln!(w, "| ⚠ 时区 | 混合偏移 {} | ", report.summary.distinct_offsets.join(" / "))?;
    }
    for (path, reason) in &report.skipped_inputs {
        writeln!(w, "| ⚠ 跳过 | `{path}` ({reason:?}) |")?;
    }
    writeln!(w)?;

    writeln!(w, "## 发现(按严重度)")?;
    writeln!(w)?;
    if report.findings.is_empty() {
        writeln!(w, "未发现已知故障模式;请查看下方「未识别的高频错误」是否有线索。")?;
        writeln!(w)?;
    }
    // Collapsed symptoms render inside their root's block, not flat.
    let counts: std::collections::HashMap<&str, u64> =
        report.findings.iter().map(|f| (f.rule_id.as_str(), f.count)).collect();
    for finding in report.findings.iter().filter(|f| f.collapsed_into.is_none()) {
        writeln!(
            w,
            "### [{}] {} — {} (×{})",
            finding.severity.label(),
            finding.rule_id,
            finding.title,
            finding.count
        )?;
        writeln!(w)?;
        if let (Some(first), Some(last)) = (finding.first_seen, finding.last_seen) {
            writeln!(w, "- 首次: {} · 末次: {}", first.to_rfc3339(), last.to_rfc3339())?;
        }
        if !finding.caused.is_empty() {
            let items: Vec<String> = finding
                .caused
                .iter()
                .map(|id| format!("`{id}` ×{}", counts.get(id.as_str()).copied().unwrap_or(0)))
                .collect();
            writeln!(w, "- 级联症状: {}", items.join(", "))?;
        }
        writeln!(w, "- 节点: {}", finding.nodes.iter().cloned().collect::<Vec<_>>().join(", "))?;
        writeln!(w, "- 诊断: {}", finding.diagnosis)?;
        writeln!(w, "- 建议: {}", finding.suggestion)?;
        for (field, values) in &finding.evidence {
            let shown: Vec<&str> = values.values.iter().map(String::as_str).collect();
            let overflow = if values.overflow > 0 {
                format!(" (+{} 更多)", values.overflow)
            } else {
                String::new()
            };
            writeln!(w, "- 证据 `{field}`: {}{overflow}", shown.join(", "))?;
        }
        if !finding.samples.is_empty() {
            writeln!(w)?;
            writeln!(w, "```json")?;
            for sample in &finding.samples {
                writeln!(w, "{}", serde_json::to_string(sample).map_err(io::Error::other)?)?;
            }
            writeln!(w, "```")?;
        }
        writeln!(w)?;
    }

    if !report.low_confidence.is_empty() {
        writeln!(w, "## 低频提示(命中数低于规则阈值)")?;
        writeln!(w)?;
        for finding in &report.low_confidence {
            writeln!(w, "- {} ×{} — {}", finding.rule_id, finding.count, finding.title)?;
        }
        writeln!(w)?;
    }

    writeln!(w, "## 未识别的高频错误(规则库未覆盖,Top {})", report.unmatched_top.len())?;
    writeln!(w)?;
    if report.unmatched_top.is_empty() {
        writeln!(w, "(无)")?;
    } else {
        writeln!(w, "| 次数 | 级别 | 模板 |")?;
        writeln!(w, "|---|---|---|")?;
        for cluster in &report.unmatched_top {
            writeln!(w, "| {} | {} | `{}` |", cluster.count, cluster.level, cluster.template)?;
        }
    }
    writeln!(w)?;

    if !report.timeline.is_empty() {
        writeln!(w, "## 时间线(UTC)")?;
        writeln!(w)?;
        writeln!(w, "| 起始 | ERROR | WARN | 其他 |")?;
        writeln!(w, "|---|---|---|---|")?;
        for bucket in &report.timeline {
            writeln!(
                w,
                "| {} | {} | {} | {} |",
                bucket.start.format("%Y-%m-%d %H:%M"),
                bucket.error,
                bucket.warn,
                bucket.other
            )?;
        }
    }
    Ok(())
}
