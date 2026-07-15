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

//! `rustfs diagnose` — offline log fault analysis (rustfs/backlog#1288).
//!
//! Runs before any observability/storage/network initialization, like
//! `rustfs info` / `rustfs tls inspect`: the report goes to stdout and must
//! not be wrapped by the JSON logger.
//!
//! Exit codes: 0 = diagnosis completed (findings or not), 2 = bad
//! arguments or no readable input. Findings never fail the process — this
//! is a diagnosis tool, not a CI gate.

use crate::config::{DiagnoseFormat, DiagnoseOpts};
use chrono::{DateTime, FixedOffset, Local};
use rustfs_log_analyzer::{
    AnalyzeOptions, Analyzer, IngestOptions, IngestReport, LogLevel, ReportFormat, RuleEngine, ingest_path, ingest_reader,
    render, seed_rule_set,
};
use std::io::Result;
use std::path::Path;

pub fn execute_diagnose(opts: &DiagnoseOpts) -> Result<()> {
    let analyze_opts = match build_analyze_options(opts) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("diagnose: {message}");
            std::process::exit(2);
        }
    };

    let mut analyzer = Analyzer::new(RuleEngine::new(seed_rule_set()), analyze_opts);
    let ingest_opts = IngestOptions::default();
    let mut merged = IngestReport::default();
    let mut readable_inputs = 0usize;

    for path in &opts.paths {
        let result = if path == "-" {
            let stdin = std::io::stdin();
            let mut lock = stdin.lock();
            ingest_reader(&mut lock, "stdin", &ingest_opts, &mut |event| analyzer.observe(event))
        } else {
            ingest_path(Path::new(path), &ingest_opts, &mut |event| analyzer.observe(event))
        };
        match result {
            Ok(report) => {
                merged.merge(report);
                readable_inputs += 1;
            }
            Err(err) => eprintln!("diagnose: cannot read '{path}': {err}"),
        }
    }
    if readable_inputs == 0 {
        eprintln!("diagnose: no readable inputs");
        std::process::exit(2);
    }

    let report = analyzer.finalize(merged);
    let stdout = std::io::stdout();
    render(&report, report_format(opts.format), &mut stdout.lock())
}

fn report_format(format: DiagnoseFormat) -> ReportFormat {
    match format {
        DiagnoseFormat::Text => ReportFormat::Text,
        DiagnoseFormat::Json => ReportFormat::Json,
        DiagnoseFormat::Md => ReportFormat::Markdown,
    }
}

fn build_analyze_options(opts: &DiagnoseOpts) -> std::result::Result<AnalyzeOptions, String> {
    let since = opts.since.as_deref().map(parse_time_arg).transpose()?;
    let until = opts.until.as_deref().map(parse_time_arg).transpose()?;
    let min_level = opts
        .min_level
        .as_deref()
        .map(|raw| LogLevel::parse(raw).ok_or_else(|| format!("invalid --min-level '{raw}' (trace|debug|info|warn|error)")))
        .transpose()?;
    Ok(AnalyzeOptions {
        since,
        until,
        min_level,
        max_samples: opts.samples,
        top_unmatched: opts.top,
        redact: opts.redact,
    })
}

/// RFC-3339, or a relative "30m" / "24h" / "7d" counted back from now.
fn parse_time_arg(raw: &str) -> std::result::Result<DateTime<FixedOffset>, String> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(raw) {
        return Ok(ts);
    }
    let (digits, unit) = raw.split_at(raw.len().saturating_sub(1));
    let amount: i64 = digits
        .parse()
        .map_err(|_| format!("invalid time '{raw}' (RFC-3339 or relative like 30m/24h/7d)"))?;
    let duration = match unit {
        "m" => chrono::Duration::minutes(amount),
        "h" => chrono::Duration::hours(amount),
        "d" => chrono::Duration::days(amount),
        _ => return Err(format!("invalid time '{raw}' (RFC-3339 or relative like 30m/24h/7d)")),
    };
    Ok((Local::now() - duration).fixed_offset())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_time_arg_accepts_rfc3339_and_relative() {
        assert!(parse_time_arg("2026-07-15T03:00:00+08:00").is_ok());
        let day_ago = parse_time_arg("24h").expect("relative");
        let now = Local::now().fixed_offset();
        let delta = now.signed_duration_since(day_ago).num_minutes();
        assert!((delta - 24 * 60).abs() <= 1, "got {delta} minutes");
        assert!(parse_time_arg("7x").is_err());
        assert!(parse_time_arg("").is_err());
        assert!(parse_time_arg("h").is_err());
    }
}
