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
//! Exit codes: 0 = diagnosis completed (findings or not); 2 = a rejected
//! argument value (bad `--since`/`--until`/`--min-level`/`--rules`) or no
//! readable input; 1 = a clap-level usage error (missing path, unknown flag),
//! handled by the shared arg parser before this subcommand runs. Findings
//! never fail the process — this is a diagnosis tool, not a CI gate.

use crate::config::{DiagnoseFormat, DiagnoseOpts};
use chrono::{DateTime, FixedOffset, Local};
use rustfs_log_analyzer::{
    AnalyzeOptions, Analyzer, IngestOptions, IngestReport, LogLevel, ReportFormat, RuleEngine, RuleSet, ingest_path,
    ingest_reader, render, seed_rule_set, seed_rules_with_external,
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
    let rule_set = match load_rule_set(opts) {
        Ok(set) => set,
        Err(message) => {
            // Fail fast: never analyze with a half-broken rule set. The
            // message lists every problem at once (RuleSetError::Multiple).
            eprintln!("diagnose: {message}");
            std::process::exit(2);
        }
    };

    let mut analyzer = Analyzer::new(RuleEngine::new(rule_set), analyze_opts);
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

/// Built-in seed rules, optionally merged with `--rules <file.json>`
/// (rustfs/backlog#1290 sub-item C): same-id external rules override
/// built-ins; the merged set validates as a whole.
fn load_rule_set(opts: &DiagnoseOpts) -> std::result::Result<RuleSet, String> {
    let Some(path) = &opts.rules else {
        return Ok(seed_rule_set());
    };
    let json = std::fs::read_to_string(path).map_err(|err| format!("cannot read rules file '{}': {err}", path.display()))?;
    seed_rules_with_external(&json).map_err(|err| format!("invalid rules file '{}': {err}", path.display()))
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
    // u32 rejects a leading '-': relative times count back from now, so a
    // negative one would silently mean "in the future".
    let amount: u32 = digits
        .parse()
        .map_err(|_| format!("invalid time '{raw}' (RFC-3339 or relative like 30m/24h/7d)"))?;
    let amount = i64::from(amount);
    let duration = match unit {
        "m" => chrono::Duration::minutes(amount),
        "h" => chrono::Duration::hours(amount),
        "d" => chrono::Duration::days(amount),
        _ => return Err(format!("invalid time '{raw}' (RFC-3339 or relative like 30m/24h/7d)")),
    };
    // `checked_sub_signed`: an absurd amount (e.g. `100000000d`) underflows the
    // representable date range — return an error instead of panicking.
    Local::now()
        .checked_sub_signed(duration)
        .map(|t| t.fixed_offset())
        .ok_or_else(|| format!("invalid time '{raw}' (out of representable range)"))
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
        // Negative relative times would mean "in the future" — rejected.
        assert!(parse_time_arg("-1h").is_err());
        assert!(parse_time_arg("-24h").is_err());
        // Absurd amounts underflow the representable date range: return an
        // error rather than panicking (checked_sub_signed).
        assert!(parse_time_arg("100000000d").is_err());
        assert!(parse_time_arg("4000000000h").is_err());
    }
}
