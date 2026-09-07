#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
PROFILE="e2e-nightly"
CASE_ID="background-target-crash"
RUN_DIR=""
PLAN_ONLY=0

usage() {
    cat <<'USAGE'
Usage: scripts/run_scanner_heal_evidence_case.sh [OPTIONS]

Run one Scanner/Heal release-evidence case through the real e2e test binary,
then validate the produced receipt, nextest listing, JUnit, and case oracle.

Options:
  --case CASE          Registry case to run (default: background-target-crash)
  --profile PROFILE   Nextest profile to use (default: e2e-nightly)
  --run-dir DIR       New evidence directory (default: target/scanner-heal-evidence/CASE-TIMESTAMP)
  --plan-only         Validate registry selection and print the exact filter without running cargo
  --self-test         Run lightweight CLI/registry checks without building Rust
  -h, --help          Show this help

The script intentionally runs a single case, not the release pseudo-case. After
a successful case run it verifies that the release gate still remains blocked.
USAGE
}

case_field() {
    local case_id="$1"
    local field="$2"
    "$PYTHON_BIN" - "$ROOT/.config/scanner-heal-required-tests.json" "$case_id" "$field" <<'PY'
import json
import pathlib
import sys

registry = json.loads(pathlib.Path(sys.argv[1]).read_text())
case = registry["cases"][sys.argv[2]]
value = case[sys.argv[3]]
if not isinstance(value, str):
    raise SystemExit(f"{sys.argv[3]} is not a string")
print(value)
PY
}

test_filter_for() {
    local case_id="$1"
    "$PYTHON_BIN" - "$ROOT/.config/scanner-heal-required-tests.json" "$case_id" <<'PY'
import json
import pathlib
import re
import sys

registry = json.loads(pathlib.Path(sys.argv[1]).read_text())
case = registry["cases"][sys.argv[2]]
print("test(/^" + re.escape(case["name"]) + "$/)")
PY
}

test_binary_from_listing() {
    local listing="$1"
    local case_id="$2"
    "$PYTHON_BIN" - "$ROOT/.config/scanner-heal-required-tests.json" "$listing" "$case_id" <<'PY'
import json
import pathlib
import sys

registry = json.loads(pathlib.Path(sys.argv[1]).read_text())
listing = json.loads(pathlib.Path(sys.argv[2]).read_text())
case = registry["cases"][sys.argv[3]]
suite = listing["rust-suites"][case["suite"]]
testcase = suite["testcases"][case["name"]]
if testcase.get("ignored") is not False or testcase.get("filter-match", {}).get("status") != "matches":
    raise SystemExit("selected case is not matched by the nextest listing")
matches = 0
for listed_suite in listing.get("rust-suites", {}).values():
    for listed in listed_suite.get("testcases", {}).values():
        if listed.get("filter-match", {}).get("status") == "matches":
            matches += 1
if matches != 1:
    raise SystemExit(f"expected exactly one selected case, got {matches}")
print(suite["binary-path"])
PY
}

release_gate_must_remain_blocked() {
    local run_dir="$1"
    local output="$run_dir/release-check.txt"
    if "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal "$run_dir" release >"$output" 2>&1; then
        echo "release gate unexpectedly approved a single Scanner/Heal evidence run" >&2
        return 1
    fi
    if ! grep -Eq 'required test not selected:|pending [A-Z0-9-]+:' "$output"; then
        echo "release gate did not explain why the Scanner/Heal release remains blocked" >&2
        return 1
    fi
}

run_self_test() {
    local filter
    filter="$(test_filter_for background-target-crash)"
    case "$filter" in
        *background_target_crash*) ;;
        *)
            echo "self-test failed: crash case filter missing" >&2
            return 1
            ;;
    esac
    if "$0" --case release --plan-only >/dev/null 2>&1; then
        echo "self-test failed: release pseudo-case must not be runnable" >&2
        return 1
    fi
    "$0" --case background-target-crash --plan-only >/dev/null
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --case)
            CASE_ID="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --run-dir)
            RUN_DIR="$2"
            shift 2
            ;;
        --plan-only)
            PLAN_ONLY=1
            shift
            ;;
        --self-test)
            run_self_test
            exit $?
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ "$CASE_ID" == "release" ]]; then
    echo "release is a checker-only pseudo-case; run a concrete registry case" >&2
    exit 2
fi

case_field "$CASE_ID" name >/dev/null
TEST_FILTER="$(test_filter_for "$CASE_ID")"
if [[ -z "$RUN_DIR" ]]; then
    RUN_DIR="$ROOT/target/scanner-heal-evidence/${CASE_ID}-$(date -u +%Y%m%dT%H%M%SZ)"
elif [[ "$RUN_DIR" != /* ]]; then
    RUN_DIR="$ROOT/$RUN_DIR"
fi

if [[ "$PLAN_ONLY" == 1 ]]; then
    echo "case=$CASE_ID"
    echo "profile=$PROFILE"
    echo "filter=$TEST_FILTER"
    echo "run_dir=$RUN_DIR"
    exit 0
fi

if [[ -e "$RUN_DIR" ]]; then
    echo "evidence run directory already exists: $RUN_DIR" >&2
    exit 1
fi

cd "$ROOT"
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "commit tracked source changes before creating evidence" >&2
    exit 1
fi
mkdir -p "$(dirname "$RUN_DIR")"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-scanner-heal-evidence.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

BUILD_FEATURES="${RUSTFS_BUILD_FEATURES:-}"
cargo clean -p rustfs
if [[ -n "$BUILD_FEATURES" ]]; then
    cargo build --locked -p rustfs --bins --features "$BUILD_FEATURES"
else
    cargo build --locked -p rustfs --bins
fi
printf '%s' "$BUILD_FEATURES" >"$ROOT/target/debug/rustfs.features"

LISTING_TMP="$TMP_DIR/listing.json"
cargo nextest list --profile "$PROFILE" -p e2e_test -E "$TEST_FILTER" --message-format json >"$LISTING_TMP"
TEST_BINARY="$(test_binary_from_listing "$LISTING_TMP" "$CASE_ID")"

export RUSTFS_E2E_EXPECTED_FEATURES="${RUSTFS_E2E_EXPECTED_FEATURES:-default}"
"$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --begin-scanner-heal "$RUN_DIR" "$ROOT/target/debug/rustfs" "$TEST_BINARY"
cp "$LISTING_TMP" "$RUN_DIR/listing.json"
export RUSTFS_E2E_LOG_DIR="${RUSTFS_E2E_LOG_DIR:-$RUN_DIR/e2e-logs}"
mkdir -p "$RUSTFS_E2E_LOG_DIR"

JUNIT_PATH="$ROOT/target/nextest/$PROFILE/junit.xml"
rm -f "$JUNIT_PATH"
set +e
NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}" \
HTTP_PROXY= \
HTTPS_PROXY= \
RUSTFS_SCANNER_HEAL_RUN_DIR="$RUN_DIR" \
cargo nextest run --profile "$PROFILE" -p e2e_test -E "$TEST_FILTER" --no-tests=fail
STATUS=$?
set -e

if [[ -f "$JUNIT_PATH" ]]; then
    cp "$JUNIT_PATH" "$RUN_DIR/junit.xml"
fi
"$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --finish-scanner-heal "$RUN_DIR" "$STATUS"

if [[ "$STATUS" -ne 0 ]]; then
    echo "Scanner/Heal evidence case failed: $CASE_ID (exit $STATUS); receipt kept at $RUN_DIR" >&2
    exit "$STATUS"
fi

"$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal "$RUN_DIR" "$CASE_ID"
release_gate_must_remain_blocked "$RUN_DIR"
echo "Scanner/Heal evidence case verified: $CASE_ID"
echo "Release gate remains blocked; details: $RUN_DIR/release-check.txt"
echo "Evidence directory: $RUN_DIR"
