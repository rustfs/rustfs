#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
PROFILE=""
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
  --profile PROFILE   Nextest profile to use (default: registry lane)
  --run-dir DIR       New evidence directory (default: target/scanner-heal-evidence/CASE-TIMESTAMP)
  --plan-only         Validate registry selection and print the exact filter without running cargo
  --self-test         Run lightweight CLI/registry checks without building Rust
  -h, --help          Show this help

The script intentionally runs a single case, not the release pseudo-case. After
a successful case run it verifies that the release gate still remains blocked.
Set RUSTFS_E2E_TEST_PORT_MIN and RUSTFS_E2E_TEST_PORT_RANGE to move the e2e
port allocator when the default 20000..30000 test range is unavailable.
Set RUSTFS_SCANNER_HEAL_SKIP_CLEAN=1 to reuse an existing cargo target directory
while narrowing a case locally; release evidence should keep the default clean
build.
USAGE
}

case_ids() {
    "$PYTHON_BIN" - "$ROOT/.config/scanner-heal-required-tests.json" <<'PY'
import json
import pathlib
import sys

registry = json.loads(pathlib.Path(sys.argv[1]).read_text())
for case_id in sorted(registry["cases"]):
    print(case_id)
PY
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

runtime_profile_for() {
    local case_id="$1"
    case "$case_id" in
        background-target-crash|background-target-restart)
            echo "background-4x1"
            ;;
        background-target-crash-ec8-4|background-target-restart-ec8-4|background-target-restart-ec8-4-multi-set)
            echo "background-ec8-4"
            ;;
        background-target-crash-ec8-4-multi-pool)
            echo "background-ec8-4-multi-pool"
            ;;
        ec84-target-drive-restart)
            echo "distributed-ec8-4"
            ;;
        *)
            echo "default"
            ;;
    esac
}

apply_runtime_profile() {
    local case_id="$1"
    case "$(runtime_profile_for "$case_id")" in
        background-4x1)
            export RUSTFS_HEAL_CHAOS_OBJECT_COUNT="${RUSTFS_HEAL_CHAOS_OBJECT_COUNT:-64}"
            export RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES="${RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES:-16777216}"
            export RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS="${RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS:-120}"
            ;;
        background-ec8-4)
            export RUSTFS_HEAL_CHAOS_OBJECT_COUNT="${RUSTFS_HEAL_CHAOS_OBJECT_COUNT:-32}"
            export RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES="${RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES:-8388608}"
            export RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS="${RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS:-180}"
            ;;
        background-ec8-4-multi-pool)
            export RUSTFS_HEAL_CHAOS_OBJECT_COUNT="${RUSTFS_HEAL_CHAOS_OBJECT_COUNT:-96}"
            export RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES="${RUSTFS_HEAL_CHAOS_OBJECT_SIZE_BYTES:-4194304}"
            export RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS="${RUSTFS_HEAL_CHAOS_PARTIAL_TIMEOUT_SECS:-240}"
            ;;
    esac
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
    local output="$run_dir/release-status.json"
    if "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release "$run_dir" >"$output"; then
        echo "release gate unexpectedly approved a single Scanner/Heal evidence run" >&2
        return 1
    fi
    "$PYTHON_BIN" - "$output" <<'PY'
import json
import pathlib
import sys

status = json.loads(pathlib.Path(sys.argv[1]).read_text())
if status.get("decision") != "blocked" or status.get("release_approved") is not False:
    raise SystemExit("release status did not record a blocked decision")
if not status.get("pending_gates"):
    raise SystemExit("release status did not retain pending gates")
PY
}

run_self_test() {
    if "$0" --case release --plan-only >/dev/null 2>&1; then
        echo "self-test failed: release pseudo-case must not be runnable" >&2
        return 1
    fi
    local case_id
    while IFS= read -r case_id; do
        local expected_filter expected_profile plan
        expected_filter="$(test_filter_for "$case_id")"
        expected_profile="$(case_field "$case_id" lane)"
        expected_runtime_profile="$(runtime_profile_for "$case_id")"
        plan="$("$0" --case "$case_id" --plan-only)"
        if [[ "$plan" != *"case=$case_id"* ]] ||
            [[ "$plan" != *"profile=$expected_profile"* ]] ||
            [[ "$plan" != *"runtime_profile=$expected_runtime_profile"* ]] ||
            [[ "$plan" != *"filter=$expected_filter"* ]] ||
            [[ "$plan" != *"run_dir=$ROOT/target/scanner-heal-evidence/$case_id-"* ]]; then
            echo "self-test failed: registry case plan mismatch for $case_id" >&2
            return 1
        fi
    done < <(case_ids)
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
if [[ -z "$PROFILE" ]]; then
    PROFILE="$(case_field "$CASE_ID" lane)"
fi
apply_runtime_profile "$CASE_ID"
if [[ -z "$RUN_DIR" ]]; then
    RUN_DIR="$ROOT/target/scanner-heal-evidence/${CASE_ID}-$(date -u +%Y%m%dT%H%M%SZ)"
elif [[ "$RUN_DIR" != /* ]]; then
    RUN_DIR="$ROOT/$RUN_DIR"
fi

if [[ "$PLAN_ONLY" == 1 ]]; then
    echo "case=$CASE_ID"
    echo "profile=$PROFILE"
    echo "runtime_profile=$(runtime_profile_for "$CASE_ID")"
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
NOFILE_SOFT="$(ulimit -Sn)"
NOFILE_HARD="$(ulimit -Hn)"
if [[ "$NOFILE_SOFT" =~ ^[0-9]+$ && "$NOFILE_HARD" =~ ^[0-9]+$ && "$NOFILE_SOFT" -lt 65535 ]]; then
    if [[ "$NOFILE_HARD" -ge 65535 ]]; then
        ulimit -n 65535 || true
    elif [[ "$NOFILE_HARD" -gt "$NOFILE_SOFT" ]]; then
        ulimit -n "$NOFILE_HARD" || true
    fi
fi
mkdir -p "$(dirname "$RUN_DIR")"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-scanner-heal-evidence.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

BUILD_FEATURES="${RUSTFS_BUILD_FEATURES:-}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
DEBUG_DIR="$TARGET_DIR/debug"
if [[ "${RUSTFS_SCANNER_HEAL_SKIP_CLEAN:-0}" != "1" ]]; then
    cargo clean -p rustfs
fi
if [[ -n "$BUILD_FEATURES" ]]; then
    cargo build --locked -p rustfs --bins --features "$BUILD_FEATURES"
else
    cargo build --locked -p rustfs --bins
fi
printf '%s' "$BUILD_FEATURES" >"$DEBUG_DIR/rustfs.features"

LISTING_TMP="$TMP_DIR/listing.json"
NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}" \
HTTP_PROXY= \
HTTPS_PROXY= \
RUSTFS_SCANNER_HEAL_RUN_DIR="$RUN_DIR" \
cargo nextest run --profile "$PROFILE" -p e2e_test -E "$TEST_FILTER" --no-run --no-tests=fail
cargo nextest list --profile "$PROFILE" -p e2e_test -E "$TEST_FILTER" --message-format json >"$LISTING_TMP"
TEST_BINARY="$(test_binary_from_listing "$LISTING_TMP" "$CASE_ID")"

export RUSTFS_E2E_EXPECTED_FEATURES="${RUSTFS_E2E_EXPECTED_FEATURES:-default}"
"$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --begin-scanner-heal "$RUN_DIR" "$DEBUG_DIR/rustfs" "$TEST_BINARY"
cp "$LISTING_TMP" "$RUN_DIR/listing.json"
export RUSTFS_E2E_LOG_DIR="${RUSTFS_E2E_LOG_DIR:-$RUN_DIR/e2e-logs}"
export RUSTFS_HEAL_CHAOS_LOG_DIR="${RUSTFS_HEAL_CHAOS_LOG_DIR:-$RUSTFS_E2E_LOG_DIR}"
mkdir -p "$RUSTFS_E2E_LOG_DIR"

JUNIT_PATH="$TARGET_DIR/nextest/$PROFILE/junit.xml"
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
echo "Release gate remains blocked; status: $RUN_DIR/release-status.json"
echo "Evidence directory: $RUN_DIR"
