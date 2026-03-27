#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT_DIR}/artifacts/distributed-chaos}"
TARGET="${1:-latest}"
OUT_FILE="${OUT_FILE:-}"
FORMAT="${FORMAT:-text}"
FAIL_ON_FINDINGS="${FAIL_ON_FINDINGS:-1}"
FAIL_ON_LAYOUT_GAPS="${FAIL_ON_LAYOUT_GAPS:-1}"
MAX_TOTAL_ERRORS="${MAX_TOTAL_ERRORS:-0}"
MIN_SEGMENT_MEDIAN_MIBPS="${MIN_SEGMENT_MEDIAN_MIBPS:-0}"
MIN_SEGMENT_SLOWEST_MIBPS="${MIN_SEGMENT_SLOWEST_MIBPS:-0}"
MAX_DELETE_P99_MS="${MAX_DELETE_P99_MS:-0}"
MAX_STAT_P99_MS="${MAX_STAT_P99_MS:-0}"
MAX_GET_TTFB_P99_MS="${MAX_GET_TTFB_P99_MS:-0}"

usage() {
    cat <<'EOF'
Usage:
  evaluate-chaos-run.sh [latest|<run-dir>|<attempt-dir>]

Environment:
  ARTIFACT_ROOT             Root artifacts directory
  OUT_FILE                  Optional file to save the summary
  FORMAT                    text or json
  FAIL_ON_FINDINGS          Exit non-zero when findings.log contains matches
  FAIL_ON_LAYOUT_GAPS       Exit non-zero when object-layout.txt reports missing placements
  MAX_TOTAL_ERRORS          Maximum allowed Warp total_errors
  MIN_SEGMENT_MEDIAN_MIBPS  Minimum allowed Warp segmented median throughput
  MIN_SEGMENT_SLOWEST_MIBPS Minimum allowed Warp segmented slowest throughput
  MAX_DELETE_P99_MS         Maximum allowed DELETE p99 latency
  MAX_STAT_P99_MS           Maximum allowed STAT p99 latency
  MAX_GET_TTFB_P99_MS       Maximum allowed GET first-byte p99 latency
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

main() {
    if [[ $# -gt 1 ]]; then
        usage
        exit 1
    fi

    require_bin python3

    local output
    set +e
    output="$(
        python3 - "${ARTIFACT_ROOT}" "${TARGET}" "${FORMAT}" <<'PY'
import json
import os
import re
import subprocess
import sys
from pathlib import Path

artifact_root = Path(sys.argv[1])
target = sys.argv[2]
fmt = sys.argv[3]

fail_on_findings = os.environ.get("FAIL_ON_FINDINGS", "1") == "1"
fail_on_layout_gaps = os.environ.get("FAIL_ON_LAYOUT_GAPS", "1") == "1"
max_total_errors = float(os.environ.get("MAX_TOTAL_ERRORS", "0"))
min_segment_median_mibps = float(os.environ.get("MIN_SEGMENT_MEDIAN_MIBPS", "0"))
min_segment_slowest_mibps = float(os.environ.get("MIN_SEGMENT_SLOWEST_MIBPS", "0"))
max_delete_p99_ms = float(os.environ.get("MAX_DELETE_P99_MS", "0"))
max_stat_p99_ms = float(os.environ.get("MAX_STAT_P99_MS", "0"))
max_get_ttfb_p99_ms = float(os.environ.get("MAX_GET_TTFB_P99_MS", "0"))


def resolve_target(raw: str) -> tuple[Path, list[Path]]:
    if raw == "latest":
        candidates = sorted((artifact_root / "repros").glob("mixed-corruption-*"))
        if not candidates:
            raise SystemExit("no mixed-corruption run directories found")
        run_dir = candidates[-1]
        attempts = sorted(run_dir.glob("attempt-*"))
        if not attempts:
            raise SystemExit(f"no attempt directories found under {run_dir}")
        return run_dir, attempts

    path = Path(raw)
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        raise SystemExit(f"target does not exist: {path}")

    if path.is_dir() and any(path.glob("attempt-*")):
        attempts = sorted(path.glob("attempt-*"))
        return path, attempts

    if path.is_dir() and ((path / "warp").exists() or (path / "findings.log").exists()):
        return path.parent, [path]

    raise SystemExit(f"unsupported target path: {path}")


def load_warp_json(attempt_dir: Path):
    candidates = sorted((attempt_dir / "warp").glob("*.json.zst")) + sorted((attempt_dir / "warp").glob("*.json"))
    if not candidates:
        return None, None
    warp_path = candidates[-1]
    if warp_path.suffix == ".zst":
        raw = subprocess.check_output(["zstdcat", str(warp_path)])
    else:
        raw = warp_path.read_bytes()
    return warp_path, json.loads(raw)


def parse_findings(attempt_dir: Path):
    findings_path = attempt_dir / "findings.log"
    if not findings_path.exists():
        return findings_path, 0, None
    text = findings_path.read_text(errors="ignore")
    lines = [line for line in text.splitlines() if line.strip()]
    suspect = None
    patterns = [
        re.compile(r'bucket: "([^"]+)", object: "([^"]+)"'),
        re.compile(r'Data corruption detected for ([^/]+)/(.+?):'),
        re.compile(r'Storage resources are insufficient for the write operation: ([^/]+)/(.+)'),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            suspect = f"{match.group(1)}/{match.group(2)}"
            break
    return findings_path, len(lines), suspect


def parse_layout(attempt_dir: Path):
    layout_path = attempt_dir / "object-layout.txt"
    if not layout_path.exists():
        return layout_path, None
    text = layout_path.read_text(errors="ignore")
    summary_match = re.search(r"summary present=(\d+) missing=(\d+) expected=(\d+)", text)
    bucket_match = re.search(r"^bucket=(.+)$", text, re.MULTILINE)
    object_match = re.search(r"^object=(.+)$", text, re.MULTILINE)
    data = {
        "bucket": bucket_match.group(1) if bucket_match else None,
        "object": object_match.group(1) if object_match else None,
    }
    if summary_match:
        data["present"] = int(summary_match.group(1))
        data["missing"] = int(summary_match.group(2))
        data["expected"] = int(summary_match.group(3))
    return layout_path, data


def get_single_sized_metric(op_block, key):
    requests_by_client = op_block.get("requests_by_client") or {}
    if not requests_by_client:
        return None
    client = next(iter(requests_by_client))
    entries = requests_by_client[client]
    if not entries:
        return None
    block = entries[0].get("single_sized_requests") or {}
    return block.get(key)


def get_max_get_ttfb_p99(op_block):
    requests_by_client = op_block.get("requests_by_client") or {}
    if not requests_by_client:
        return None
    client = next(iter(requests_by_client))
    entries = requests_by_client[client]
    if not entries:
        return None
    block = entries[0].get("multi_sized_requests") or {}
    values = []
    for size_bucket in block.get("by_size") or []:
        first_byte = size_bucket.get("first_byte") or {}
        if "p99_millis" in first_byte:
            values.append(first_byte["p99_millis"])
    return max(values) if values else None


def mib_per_sec_from_bytes(bytes_per_sec):
    return bytes_per_sec / 1024 / 1024


def analyze_attempt(attempt_dir: Path):
    findings_path, findings_count, suspect_from_findings = parse_findings(attempt_dir)
    layout_path, layout = parse_layout(attempt_dir)
    warp_path, warp = load_warp_json(attempt_dir)

    summary = {
        "attempt": attempt_dir.name,
        "attempt_dir": str(attempt_dir),
        "findings_path": str(findings_path) if findings_path.exists() else None,
        "findings_count": findings_count,
        "object_layout_path": str(layout_path) if layout_path.exists() else None,
        "warp_path": str(warp_path) if warp_path else None,
        "layout": layout,
        "suspect_object": suspect_from_findings,
        "warp": None,
        "reasons": [],
    }

    if layout and layout.get("bucket") and layout.get("object"):
        summary["suspect_object"] = f"{layout['bucket']}/{layout['object']}"

    if warp is not None:
        total = warp["total"]
        throughput = total["throughput"]
        segmented = throughput.get("segmented") or {}
        delete_p99 = None
        stat_p99 = None
        get_ttfb_p99 = None
        if "DELETE" in warp["by_op_type"]:
            delete_p99 = get_single_sized_metric(warp["by_op_type"]["DELETE"], "dur_99_millis")
        if "STAT" in warp["by_op_type"]:
            stat_p99 = get_single_sized_metric(warp["by_op_type"]["STAT"], "dur_99_millis")
        if "GET" in warp["by_op_type"]:
            get_ttfb_p99 = get_max_get_ttfb_p99(warp["by_op_type"]["GET"])
        measure_duration_ms = throughput.get("measure_duration_millis") or 0
        avg_mibps = None
        if measure_duration_ms:
            avg_mibps = mib_per_sec_from_bytes(throughput["bytes"] / (measure_duration_ms / 1000))

        summary["warp"] = {
            "total_errors": total.get("total_errors"),
            "total_requests": total.get("total_requests"),
            "avg_mibps": avg_mibps,
            "segmented_median_mibps": mib_per_sec_from_bytes(segmented["median_bps"]) if "median_bps" in segmented else None,
            "segmented_slowest_mibps": mib_per_sec_from_bytes(segmented["slowest_bps"]) if "slowest_bps" in segmented else None,
            "delete_p99_ms": delete_p99,
            "stat_p99_ms": stat_p99,
            "get_ttfb_p99_ms": get_ttfb_p99,
        }

    if fail_on_findings and findings_count > 0:
        summary["reasons"].append(f"findings.log matched {findings_count} corruption or heal-failure lines")

    if fail_on_layout_gaps and layout and layout.get("missing", 0) > 0:
        summary["reasons"].append(
            f"object-layout.txt reports missing placements: {layout['missing']} of {layout['expected']}"
        )

    if summary["warp"] is not None:
        warp_summary = summary["warp"]
        total_errors = warp_summary.get("total_errors")
        if total_errors is not None and total_errors > max_total_errors:
            summary["reasons"].append(f"Warp total_errors={total_errors} exceeds {max_total_errors:g}")

        median_mibps = warp_summary.get("segmented_median_mibps")
        if min_segment_median_mibps > 0 and median_mibps is not None and median_mibps < min_segment_median_mibps:
            summary["reasons"].append(
                f"Warp segmented median throughput={median_mibps:.2f} MiB/s is below {min_segment_median_mibps:g}"
            )

        slowest_mibps = warp_summary.get("segmented_slowest_mibps")
        if min_segment_slowest_mibps > 0 and slowest_mibps is not None and slowest_mibps < min_segment_slowest_mibps:
            summary["reasons"].append(
                f"Warp segmented slowest throughput={slowest_mibps:.2f} MiB/s is below {min_segment_slowest_mibps:g}"
            )

        delete_p99 = warp_summary.get("delete_p99_ms")
        if max_delete_p99_ms > 0 and delete_p99 is not None and delete_p99 > max_delete_p99_ms:
            summary["reasons"].append(f"DELETE p99={delete_p99:.2f} ms exceeds {max_delete_p99_ms:g}")

        stat_p99 = warp_summary.get("stat_p99_ms")
        if max_stat_p99_ms > 0 and stat_p99 is not None and stat_p99 > max_stat_p99_ms:
            summary["reasons"].append(f"STAT p99={stat_p99:.2f} ms exceeds {max_stat_p99_ms:g}")

        get_ttfb_p99 = warp_summary.get("get_ttfb_p99_ms")
        if max_get_ttfb_p99_ms > 0 and get_ttfb_p99 is not None and get_ttfb_p99 > max_get_ttfb_p99_ms:
            summary["reasons"].append(f"GET first-byte p99={get_ttfb_p99:.2f} ms exceeds {max_get_ttfb_p99_ms:g}")

    summary["verdict"] = "FAIL" if summary["reasons"] else "PASS"
    return summary


run_dir, attempts = resolve_target(target)
attempt_summaries = [analyze_attempt(attempt_dir) for attempt_dir in attempts]
overall_verdict = "FAIL" if any(item["verdict"] == "FAIL" for item in attempt_summaries) else "PASS"
report = {
    "run_dir": str(run_dir),
    "attempt_count": len(attempt_summaries),
    "overall_verdict": overall_verdict,
    "attempts": attempt_summaries,
}

if fmt == "json":
    print(json.dumps(report, indent=2))
else:
    print(f"run_dir={report['run_dir']}")
    print(f"attempt_count={report['attempt_count']}")
    print(f"overall_verdict={report['overall_verdict']}")
    for attempt in report["attempts"]:
        print()
        print(f"attempt={attempt['attempt']}")
        print(f"attempt_dir={attempt['attempt_dir']}")
        print(f"verdict={attempt['verdict']}")
        if attempt["suspect_object"]:
            print(f"suspect_object={attempt['suspect_object']}")
        print(f"findings_count={attempt['findings_count']}")
        if attempt["warp"] is not None:
            warp = attempt["warp"]
            print(f"warp_total_errors={warp['total_errors']}")
            if warp["avg_mibps"] is not None:
                print(f"warp_avg_mibps={warp['avg_mibps']:.2f}")
            if warp["segmented_median_mibps"] is not None:
                print(f"warp_segmented_median_mibps={warp['segmented_median_mibps']:.2f}")
            if warp["segmented_slowest_mibps"] is not None:
                print(f"warp_segmented_slowest_mibps={warp['segmented_slowest_mibps']:.2f}")
            if warp["delete_p99_ms"] is not None:
                print(f"delete_p99_ms={warp['delete_p99_ms']:.2f}")
            if warp["stat_p99_ms"] is not None:
                print(f"stat_p99_ms={warp['stat_p99_ms']:.2f}")
            if warp["get_ttfb_p99_ms"] is not None:
                print(f"get_ttfb_p99_ms={warp['get_ttfb_p99_ms']:.2f}")
        if attempt["layout"] is not None:
            layout = attempt["layout"]
            if all(key in layout for key in ("present", "missing", "expected")):
                print(
                    "layout_summary="
                    f"present:{layout['present']} missing:{layout['missing']} expected:{layout['expected']}"
                )
        if attempt["reasons"]:
            print("reasons=")
            for reason in attempt["reasons"]:
                print(f"- {reason}")

if overall_verdict == "FAIL":
    raise SystemExit(2)
PY
    )"
    local status=$?
    set -e

    printf '%s\n' "${output}"

    if [[ -n "${OUT_FILE}" ]]; then
        mkdir -p "$(dirname "${OUT_FILE}")"
        printf '%s\n' "${output}" > "${OUT_FILE}"
    fi

    return "${status}"
}

main "$@"
