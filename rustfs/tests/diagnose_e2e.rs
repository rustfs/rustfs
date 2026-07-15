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

//! MVP acceptance scenarios for `rustfs diagnose` (rustfs/backlog#1281 §MVP,
//! rustfs/backlog#1288). Pure file-based tests — no server, no network —
//! exercising the same library pipeline the subcommand drives, plus CLI
//! parsing regressions for the legacy argv preprocessor.

use rustfs::config::{CommandResult, Opt};
use rustfs_log_analyzer::{
    AnalysisReport, AnalyzeOptions, Analyzer, IngestOptions, ReportFormat, RuleEngine, ingest_path, render, seed_rule_set,
};
use std::io::Write;
use std::path::Path;

fn json_line(ts: &str, level: &str, message: &str, extra_fields: &[(&str, &str)]) -> String {
    let mut obj = serde_json::json!({
        "timestamp": ts,
        "level": level,
        "target": "rustfs::storage::erasure",
        "message": message,
    });
    for (name, value) in extra_fields {
        obj[*name] = serde_json::Value::String((*value).to_string());
    }
    obj.to_string()
}

fn analyze_dir(path: &Path) -> AnalysisReport {
    let mut analyzer = Analyzer::new(RuleEngine::new(seed_rule_set()), AnalyzeOptions::default());
    let ingest = ingest_path(path, &IngestOptions::default(), &mut |ev| analyzer.observe(ev)).expect("ingest");
    analyzer.finalize(ingest)
}

fn quorum_and_faulty_lines() -> String {
    let mut lines = Vec::new();
    for disk in ["disk3", "disk7"] {
        lines.push(json_line(
            "2026-07-15T03:10:00+08:00",
            "ERROR",
            "Disk health check marked disk faulty",
            &[("reason", "faulty_disk"), ("disk", disk)],
        ));
    }
    for i in 0..30 {
        lines.push(json_line(
            &format!("2026-07-15T03:{:02}:00+08:00", 12 + i % 40),
            "ERROR",
            "erasure write quorum (required=8, achieved=5, failed=3)",
            &[("offline-disks", "2/16"), ("required", "8"), ("achieved", "5")],
        ));
    }
    let mut joined = lines.join("\n");
    joined.push('\n');
    joined
}

/// MVP scenario 1: directory with a live log + a zstd-rotated archive.
#[test]
fn directory_with_zst_archive_yields_quorum_and_disk_findings() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("rustfs.log"), quorum_and_faulty_lines()).expect("write");
    let archived = zstd::stream::encode_all(
        std::io::Cursor::new(json_line(
            "2026-07-15T02:00:00+08:00",
            "ERROR",
            "erasure write quorum (required=8, achieved=4, failed=4)",
            &[("offline-disks", "3/16")],
        )),
        3,
    )
    .expect("zstd");
    std::fs::write(dir.path().join("20260715030001.000000-0.rustfs.log.zst"), archived).expect("write");

    let report = analyze_dir(dir.path());
    let ids: Vec<&str> = report.findings.iter().map(|f| f.rule_id.as_str()).collect();
    assert!(ids.contains(&"ec-write-quorum"), "findings: {ids:?}");
    assert!(ids.contains(&"disk-marked-faulty"), "findings: {ids:?}");

    let quorum = report
        .findings
        .iter()
        .find(|f| f.rule_id == "ec-write-quorum")
        .expect("quorum");
    assert_eq!(quorum.count, 31, "live log + archive");
    let offline = quorum.evidence.get("offline-disks").expect("offline-disks evidence");
    assert!(offline.values.contains("2/16") && offline.values.contains("3/16"));
    // The quorum finding must sort above the P2 disk finding.
    assert!(ids.iter().position(|id| *id == "ec-write-quorum") < ids.iter().position(|id| *id == "disk-marked-faulty"));
}

/// MVP scenario 2: multi-node zip — evidence is attributed per node.
#[test]
fn multi_node_zip_attributes_findings_to_nodes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let zip_path = dir.path().join("customer.zip");
    let mut writer = zip::ZipWriter::new(std::fs::File::create(&zip_path).expect("create"));
    for node in ["node1", "node2"] {
        writer
            .start_file(format!("{node}/rustfs.log"), zip::write::SimpleFileOptions::default())
            .expect("start_file");
        writer.write_all(quorum_and_faulty_lines().as_bytes()).expect("write");
    }
    writer.finish().expect("finish");

    let report = analyze_dir(&zip_path);
    assert_eq!(report.summary.nodes, vec!["node1", "node2"]);
    let quorum = report
        .findings
        .iter()
        .find(|f| f.rule_id == "ec-write-quorum")
        .expect("quorum");
    assert!(quorum.nodes.contains("node1") && quorum.nodes.contains("node2"));
}

/// MVP scenario 3: kubectl-logs CRI prefixes must not hurt the parse rate.
#[test]
fn cri_prefixed_logs_parse_cleanly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut content = String::new();
    for line in quorum_and_faulty_lines().lines() {
        content.push_str("2026-07-14T19:10:00.123456789Z stdout F ");
        content.push_str(line);
        content.push('\n');
    }
    std::fs::write(dir.path().join("kubectl.log"), content).expect("write");

    let report = analyze_dir(dir.path());
    assert!(report.summary.parse.prefixed_json > 0);
    assert!(report.summary.parse.json_ratio() > 0.99, "ratio: {}", report.summary.parse.json_ratio());
    assert!(report.findings.iter().any(|f| f.rule_id == "ec-write-quorum"));
}

/// MVP scenario 4: a stderr panic block folds into a P1 finding.
#[test]
fn panic_block_becomes_a_process_panic_finding() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("stderr.log"),
        "thread 'tokio-runtime-worker' panicked at crates/ecstore/src/set_disk.rs:100:17:\n\
         called `Option::unwrap()` on a `None` value\n\
         note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n",
    )
    .expect("write");

    let report = analyze_dir(dir.path());
    let panic = report
        .findings
        .iter()
        .find(|f| f.rule_id == "process-panic")
        .expect("panic finding");
    assert_eq!(panic.count, 1);
    let location = panic.evidence.get("panic_location").expect("panic_location");
    assert!(location.values.contains("crates/ecstore/src/set_disk.rs:100:17"));
    assert!(panic.samples[0].message.contains("panicked at"));
}

/// MVP scenario 5: JSON output is stable and machine-readable.
#[test]
fn json_format_has_a_stable_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("rustfs.log"), quorum_and_faulty_lines()).expect("write");
    let report = analyze_dir(dir.path());

    let mut out = Vec::new();
    render(&report, ReportFormat::Json, &mut out).expect("render");
    let value: serde_json::Value = serde_json::from_slice(&out).expect("parse");
    assert_eq!(value["schema_version"], 1);
    assert!(value["findings"].as_array().expect("findings").len() >= 2);
    assert!(value["summary"]["parse"]["json_ok"].as_u64().expect("json_ok") > 0);

    let mut md = Vec::new();
    render(&report, ReportFormat::Markdown, &mut md).expect("render md");
    assert!(String::from_utf8(md).expect("utf8").starts_with("# RustFS 日志诊断报告"));
}

/// CLI parsing: diagnose is a first-class subcommand and the legacy
/// `rustfs <volume>` form still routes to the server.
#[test]
fn cli_parses_diagnose_and_keeps_legacy_server_routing() {
    let parsed = Opt::parse_command(vec![
        "rustfs".to_string(),
        "diagnose".to_string(),
        "/tmp/customer-logs".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "--since".to_string(),
        "24h".to_string(),
        "--redact".to_string(),
    ])
    .expect("parse");
    let CommandResult::Diagnose(opts) = parsed else {
        panic!("expected Diagnose command");
    };
    assert_eq!(opts.paths, vec!["/tmp/customer-logs"]);
    assert!(opts.redact);
    assert_eq!(opts.since.as_deref(), Some("24h"));

    // Legacy preprocessor regression: a bare volume still means `server`.
    let legacy = Opt::parse_command(vec!["rustfs".to_string(), "/data".to_string()]);
    assert!(matches!(legacy, Ok(CommandResult::Server(_))));
}

/// True-binary smoke test. Ignored by default: building the full rustfs
/// binary is expensive; run locally with
/// `cargo test -p rustfs --test diagnose_e2e -- --ignored`.
#[test]
#[ignore = "builds the full rustfs binary; run with -- --ignored"]
fn binary_smoke_diagnose_json() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("rustfs.log"), quorum_and_faulty_lines()).expect("write");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_rustfs"))
        .arg("diagnose")
        .arg(dir.path())
        .args(["--format", "json"])
        .output()
        .expect("run rustfs diagnose");
    assert!(output.status.success(), "stderr: {}", String::from_utf8_lossy(&output.stderr));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("stdout JSON");
    assert_eq!(value["schema_version"], 1);
}
