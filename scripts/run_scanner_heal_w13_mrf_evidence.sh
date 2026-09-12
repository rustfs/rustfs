#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
MIN_FREE_KIB="${RUSTFS_W13_MIN_FREE_KIB:-4194304}"
SOAK_SECONDS="${RUSTFS_W13_MRF_SOAK_SECONDS:-7200}"
ENOSPC_TMPFS_SIZE="${RUSTFS_W13_ENOSPC_TMPFS_SIZE:-16m}"

RUN_DIR=""
ENOSPC_ROOT="${RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT:-}"
TEST_SELECTION="all"
PLAN_ONLY=0
ALLOW_DIRTY=0
ALLOW_SHORT_SOAK=0
SKIP_BUILD=0
VERBOSE=0
ENOSPC_TMPFS_MOUNTED=0

usage() {
    cat <<'USAGE'
Usage: scripts/run_scanner_heal_w13_mrf_evidence.sh [OPTIONS]

Build the current checkout, run the W13 MRF durable replay evidence test, verify
the raw JSON artifacts, and write bundle-ready G07/G08/P4 release descriptors.

Options:
  --run-dir DIR       New evidence directory (default: target/scanner-heal-w13-evidence/TIMESTAMP)
  --out-dir DIR       Alias for --run-dir
  --test NAME         all, g07, g08, or p4 (default: all)
  --soak-seconds N    P4 soak duration in seconds (default: 7200)
  --enospc-root DIR   Pre-mounted small filesystem used for real G08 ENOSPC evidence
  --allow-short-soak  Diagnostic only: allow P4 runs shorter than release duration
  --allow-dirty      Allow tracked source changes while collecting evidence
  --skip-build       Reuse an existing target/debug/rustfs binary
  --plan-only        Print the resolved plan without building or running tests
  --dry-run          Alias for --plan-only
  --self-test        Run lightweight CLI and descriptor plumbing checks
  --verbose          Stream command output instead of storing it under the run directory
  -h, --help         Show this help

Required output files:
  g07-mrf-responsibility/G07-mrf_responsibility_oracle.json
  g07-mrf-responsibility/G07-commit_boundary_crash_matrix.json
  g08-mrf-capacity/G08-mrf_capacity_evidence.json
  g08-mrf-capacity/G08-disk_full_matrix.json
  g08-mrf-capacity/G08-replica_loss_matrix.json
  p4-mrf-soak/P4-mrf_scale_measurement.json
  p4-mrf-soak/P4-mrf_replay_cost_measurement.json
  p4-mrf-soak/P4-retained_responsibility_evidence.json
  p4-mrf-soak/P4-mrf_cleanup_gc_soak_evidence.json

Environment overrides:
  RUSTFS_SCANNER_HEAL_W13_OUTPUT_ROOT
  RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT
  RUSTFS_W13_MIN_FREE_KIB
  RUSTFS_W13_MRF_SOAK_SECONDS
  RUSTFS_W13_ENOSPC_TMPFS_SIZE

Short-soak runs are for runner diagnostics only. They validate raw artifacts but
do not validate the P4 release bundle gate.
USAGE
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

require_value() {
    local option="$1"
    local count="$2"
    if [[ "$count" -lt 2 ]]; then
        die "missing value for $option"
    fi
}

case_names() {
    case "$TEST_SELECTION" in
        all)
            printf '%s\n' g07 g08 p4
            ;;
        g07|g08|p4)
            printf '%s\n' "$TEST_SELECTION"
            ;;
        *)
            die "unknown test selection: $TEST_SELECTION"
            ;;
    esac
}

validate_test_selection() {
    case "$TEST_SELECTION" in
        all|g07|g08|p4)
            ;;
        *)
            die "unknown test selection: $TEST_SELECTION"
            ;;
    esac
}

selection_includes() {
    local needle="$1"
    [[ "$TEST_SELECTION" == "all" || "$TEST_SELECTION" == "$needle" ]]
}

normalize_path() {
    local path="$1"
    if [[ "$path" == /* ]]; then
        echo "$path"
    else
        echo "$ROOT/$path"
    fi
}

cargo_target_dir() {
    if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
        normalize_path "$CARGO_TARGET_DIR"
    else
        echo "$ROOT/target"
    fi
}

write_rustfs_features_stamp() {
    local target_dir
    target_dir="$(cargo_target_dir)"
    mkdir -p "$target_dir/debug"
    : >"$target_dir/debug/rustfs.features"
}

artifact_dir_for() {
    case "$1" in
        g07)
            echo "g07-mrf-responsibility"
            ;;
        g08)
            echo "g08-mrf-capacity"
            ;;
        p4)
            echo "p4-mrf-soak"
            ;;
        *)
            die "unknown W13 case: $1"
            ;;
    esac
}

check_empty_case_dir() {
    local dir="$1"
    if [[ -d "$dir" ]] && find "$dir" -mindepth 1 -print -quit | grep -q .; then
        die "evidence case directory is not empty: $dir"
    fi
}

ensure_min_free_space() {
    local path="$1"
    local available
    mkdir -p "$path"
    available="$(df -Pk "$path" | awk 'NR == 2 { print $4 }')"
    if [[ -z "$available" ]]; then
        die "could not determine free space for $path"
    fi
    if (( available < MIN_FREE_KIB )); then
        die "insufficient free space for W13 evidence run at $path: need ${MIN_FREE_KIB} KiB, found ${available} KiB"
    fi
}

cleanup_enospc_root() {
    if [[ "$ENOSPC_TMPFS_MOUNTED" == 1 && -n "$ENOSPC_ROOT" ]]; then
        umount "$ENOSPC_ROOT" >/dev/null 2>&1 || true
    fi
}

prepare_enospc_root() {
    if ! selection_includes g08; then
        return
    fi
    if [[ -n "$ENOSPC_ROOT" ]]; then
        ENOSPC_ROOT="$(normalize_path "$ENOSPC_ROOT")"
        mkdir -p "$ENOSPC_ROOT"
        return
    fi
    if [[ "$(uname -s)" != "Linux" ]]; then
        die "G08 disk-full evidence requires --enospc-root on non-Linux hosts"
    fi
    if [[ "$(id -u)" != "0" ]]; then
        die "G08 disk-full evidence requires --enospc-root or root privileges to mount a tmpfs"
    fi
    if ! command -v mount >/dev/null 2>&1 || ! command -v umount >/dev/null 2>&1; then
        die "G08 disk-full evidence requires mount and umount, or a pre-mounted --enospc-root"
    fi
    ENOSPC_ROOT="$RUN_DIR/enospc-root"
    mkdir -p "$ENOSPC_ROOT"
    mount -t tmpfs -o "size=$ENOSPC_TMPFS_SIZE" rustfs-w13-enospc "$ENOSPC_ROOT"
    ENOSPC_TMPFS_MOUNTED=1
}

run_logged() {
    local label="$1"
    shift
    local log="$RUN_DIR/logs/$label.log"
    mkdir -p "$(dirname "$log")"
    if [[ "$VERBOSE" == 1 ]]; then
        "$@"
        return
    fi
    if ! "$@" >"$log" 2>&1; then
        echo "$label failed; log: $log" >&2
        tail -80 "$log" >&2 || true
        return 1
    fi
    echo "PASS: $label"
}

validate_artifacts() {
    local source_revision="$1"
    "$PYTHON_BIN" - "$RUN_DIR" "$source_revision" "$TEST_SELECTION" <<'PY'
import json
import pathlib
import sys

run_dir = pathlib.Path(sys.argv[1])
source_revision = sys.argv[2]
selection = sys.argv[3]

expected = {
    "g07": [
        ("g07-mrf-responsibility/G07-mrf_responsibility_oracle.json", "G07", "mrf_responsibility_oracle", "mrf-durable-responsibility-oracle"),
        ("g07-mrf-responsibility/G07-commit_boundary_crash_matrix.json", "G07", "commit_boundary_crash_matrix", "mrf-commit-boundary-crash-matrix"),
    ],
    "g08": [
        ("g08-mrf-capacity/G08-mrf_capacity_evidence.json", "G08", "mrf_capacity_evidence", "mrf-capacity-boundary"),
        ("g08-mrf-capacity/G08-disk_full_matrix.json", "G08", "disk_full_matrix", "mrf-disk-full-enospc-matrix"),
        ("g08-mrf-capacity/G08-replica_loss_matrix.json", "G08", "replica_loss_matrix", "mrf-replica-loss-matrix"),
    ],
    "p4": [
        ("p4-mrf-soak/P4-mrf_scale_measurement.json", "P4", "mrf_scale_measurement", "mrf-scale-measurement"),
        ("p4-mrf-soak/P4-mrf_replay_cost_measurement.json", "P4", "mrf_replay_cost_measurement", "mrf-replay-cost-measurement"),
        ("p4-mrf-soak/P4-retained_responsibility_evidence.json", "P4", "retained_responsibility_evidence", "mrf-retained-responsibility-soak"),
        ("p4-mrf-soak/P4-mrf_cleanup_gc_soak_evidence.json", "P4", "mrf_cleanup_gc_soak_evidence", "mrf-cleanup-gc-soak"),
    ],
}
if selection != "all":
    expected = {selection: expected[selection]}

for artifacts in expected.values():
    for relative, gate, field, artifact_kind in artifacts:
        path = run_dir / relative
        if not path.is_file():
            raise SystemExit(f"missing W13 evidence artifact: {relative}")
        evidence = json.loads(path.read_text())
        if evidence.get("schema") != 1:
            raise SystemExit(f"{relative}: expected schema 1")
        if evidence.get("evidence_type") != "measured":
            raise SystemExit(f"{relative}: expected measured evidence")
        if evidence.get("artifact_kind") != artifact_kind:
            raise SystemExit(f"{relative}: unexpected artifact kind")
        if evidence.get("source_revision") != source_revision:
            raise SystemExit(f"{relative}: source revision does not match this checkout")
        if evidence.get("gate") != gate or evidence.get("field") != field:
            raise SystemExit(f"{relative}: unexpected gate or field")
        if gate == "G07":
            crash_points = evidence.get("crash_points")
            if not isinstance(crash_points, list) or not crash_points:
                raise SystemExit(f"{relative}: missing crash points")
        if gate == "P4":
            duration = evidence.get("duration_seconds")
            if not isinstance(duration, int) or duration <= 0:
                raise SystemExit(f"{relative}: invalid P4 duration")
        if gate == "G08" and field == "disk_full_matrix":
            if evidence.get("journal_write_enospc_observed") is not True:
                raise SystemExit(f"{relative}: journal ENOSPC was not observed")
            if evidence.get("committed_checkpoint_enospc_observed") is not True:
                raise SystemExit(f"{relative}: committed checkpoint ENOSPC was not observed")
            if evidence.get("cleanup_delete_on_full_filesystem_observed") is not True:
                raise SystemExit(f"{relative}: cleanup delete on a full filesystem was not observed")
            filler_bytes = evidence.get("enospc_filler_bytes")
            if not isinstance(filler_bytes, int) or filler_bytes <= 0:
                raise SystemExit(f"{relative}: ENOSPC filler byte count is invalid")

print("PASS: W13 raw MRF evidence artifacts verified")
PY
}

check_release_gate() {
    local descriptor="$1"
    local gate="$2"
    local output="$RUN_DIR/logs/check-${gate}.json"
    "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release-bundle-gate "$descriptor" "$gate" >"$output"
    "$PYTHON_BIN" - "$output" "$gate" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
gate = sys.argv[2]
status = json.loads(path.read_text())
if status.get("decision") != "verified" or status.get("verified_gate") != gate:
    print(path.read_text(), file=sys.stderr)
    raise SystemExit(f"{gate} release bundle gate was not verified")
print(path.read_text().strip())
PY
}

write_release_descriptor() {
    local source_revision="$1"
    local descriptor="$RUN_DIR/release-bundle-w13.json"
    "$PYTHON_BIN" "$ROOT/scripts/run_scanner_heal_mrf_evidence.py" \
        --run-dir "$RUN_DIR" \
        --out-file "$descriptor" \
        --source-revision "$source_revision" \
        --test "$TEST_SELECTION" >&2
    printf '%s\n' "$descriptor"
}

run_self_test() {
    local tmp plan
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-w13-evidence-self-test.XXXXXX")"
    trap "rm -rf '$tmp'" EXIT

    plan="$("$0" --plan-only --run-dir "$tmp/run" --test all)"
    [[ "$plan" == *"tests=g07 g08 p4"* ]]
    [[ "$plan" == *"soak_seconds=7200"* ]]
    [[ "$plan" == *"run_dir=$tmp/run"* ]]
    [[ "$(CARGO_TARGET_DIR=relative-target "$0" --plan-only --run-dir "$tmp/run" --test g07)" == *"target_dir=$ROOT/relative-target"* ]]

    if "$0" --plan-only --test not-a-case >/dev/null 2>&1; then
        echo "self-test failed: invalid test selection was accepted" >&2
        return 1
    fi
    if "$0" --plan-only --test p4 --soak-seconds 10 >/dev/null 2>&1; then
        echo "self-test failed: short P4 soak was accepted as release evidence" >&2
        return 1
    fi
    mkdir -p "$tmp/nonempty/g07-mrf-responsibility"
    : >"$tmp/nonempty/g07-mrf-responsibility/existing.json"
    if "$0" --dry-run --run-dir "$tmp/nonempty" >/dev/null 2>&1; then
        echo "self-test failed: non-empty evidence directory was accepted" >&2
        return 1
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-dir|--out-dir)
            require_value "$1" "$#"
            RUN_DIR="$2"
            shift 2
            ;;
        --test)
            require_value "$1" "$#"
            TEST_SELECTION="$2"
            shift 2
            ;;
        --soak-seconds)
            require_value "$1" "$#"
            SOAK_SECONDS="$2"
            shift 2
            ;;
        --enospc-root)
            require_value "$1" "$#"
            ENOSPC_ROOT="$2"
            shift 2
            ;;
        --allow-short-soak)
            ALLOW_SHORT_SOAK=1
            shift
            ;;
        --allow-dirty)
            ALLOW_DIRTY=1
            shift
            ;;
        --skip-build)
            SKIP_BUILD=1
            shift
            ;;
        --plan-only|--dry-run)
            PLAN_ONLY=1
            shift
            ;;
        --self-test)
            run_self_test
            exit $?
            ;;
        --verbose)
            VERBOSE=1
            shift
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

validate_test_selection
[[ "$SOAK_SECONDS" =~ ^[0-9]+$ ]] || die "--soak-seconds must be a non-negative integer"
CASES=()
while IFS= read -r case_name; do
    CASES+=("$case_name")
done < <(case_names)
if [[ " ${CASES[*]} " == *" p4 "* && "$SOAK_SECONDS" -lt 7200 && "$ALLOW_SHORT_SOAK" != 1 ]]; then
    die "P4 release evidence requires at least 7200 soak seconds; pass --allow-short-soak only for diagnostics"
fi
if [[ -z "$RUN_DIR" ]]; then
    OUTPUT_ROOT="$(normalize_path "${RUSTFS_SCANNER_HEAL_W13_OUTPUT_ROOT:-$ROOT/target/scanner-heal-w13-evidence}")"
    RUN_DIR="$OUTPUT_ROOT/$(date -u +%Y%m%dT%H%M%SZ)"
else
    RUN_DIR="$(normalize_path "$RUN_DIR")"
fi

for case_name in "${CASES[@]}"; do
    check_empty_case_dir "$RUN_DIR/$(artifact_dir_for "$case_name")"
done

if [[ "$PLAN_ONLY" == 1 ]]; then
    echo "run_dir=$RUN_DIR"
    echo "out_dir=$RUN_DIR"
    echo "tests=${CASES[*]}"
    echo "soak_seconds=$SOAK_SECONDS"
    echo "min_free_kib=$MIN_FREE_KIB"
    target_dir="$(cargo_target_dir)"
    echo "target_dir=$target_dir"
    echo "current_binary=$target_dir/debug/rustfs"
    echo "test_filter=rustfs-heal heal::mrf_queue::tests::w13_mrf_release_evidence_outputs_bundle_artifacts"
    echo "required_artifacts:"
    if [[ " ${CASES[*]} " == *" g07 "* ]]; then
        echo "  $RUN_DIR/g07-mrf-responsibility/G07-mrf_responsibility_oracle.json"
        echo "  $RUN_DIR/g07-mrf-responsibility/G07-commit_boundary_crash_matrix.json"
    fi
    if [[ " ${CASES[*]} " == *" g08 "* ]]; then
        echo "  $RUN_DIR/g08-mrf-capacity/G08-mrf_capacity_evidence.json"
        echo "  $RUN_DIR/g08-mrf-capacity/G08-disk_full_matrix.json"
        echo "  $RUN_DIR/g08-mrf-capacity/G08-replica_loss_matrix.json"
        if [[ -n "$ENOSPC_ROOT" ]]; then
            echo "enospc_root=$(normalize_path "$ENOSPC_ROOT")"
        elif [[ "$(uname -s)" == "Linux" ]]; then
            echo "enospc_root=$RUN_DIR/enospc-root"
            echo "enospc_tmpfs_size=$ENOSPC_TMPFS_SIZE"
        else
            echo "enospc_root=required-for-non-linux"
        fi
    fi
    if [[ " ${CASES[*]} " == *" p4 "* ]]; then
        echo "  $RUN_DIR/p4-mrf-soak/P4-mrf_scale_measurement.json"
        echo "  $RUN_DIR/p4-mrf-soak/P4-mrf_replay_cost_measurement.json"
        echo "  $RUN_DIR/p4-mrf-soak/P4-retained_responsibility_evidence.json"
        echo "  $RUN_DIR/p4-mrf-soak/P4-mrf_cleanup_gc_soak_evidence.json"
    fi
    exit 0
fi

cd "$ROOT"
if [[ "$ALLOW_DIRTY" != 1 && -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "commit tracked source changes before creating release evidence, or pass --allow-dirty for local diagnostics" >&2
    exit 1
fi
if [[ -e "$RUN_DIR" ]]; then
    die "evidence run directory already exists: $RUN_DIR"
fi
mkdir -p "$RUN_DIR/logs"
trap cleanup_enospc_root EXIT
if [[ -n "${TMPDIR:-}" ]]; then
    mkdir -p "$TMPDIR"
    ensure_min_free_space "$TMPDIR"
fi
ensure_min_free_space "$RUN_DIR"
prepare_enospc_root

SOURCE_REVISION="$(git rev-parse HEAD)"
printf '%s\n' "$SOURCE_REVISION" >"$RUN_DIR/source-revision.txt"

if [[ "$SKIP_BUILD" != 1 ]]; then
    run_logged build-current cargo build --locked -p rustfs --bin rustfs
    write_rustfs_features_stamp
fi

selection_csv="$(IFS=,; echo "${CASES[*]}")"
run_logged w13-mrf-evidence env \
    RUSTFS_SCANNER_HEAL_W13_EVIDENCE_DIR="$RUN_DIR" \
    RUSTFS_SCANNER_HEAL_W13_SOURCE_REVISION="$SOURCE_REVISION" \
    RUSTFS_SCANNER_HEAL_W13_SELECTION="$selection_csv" \
    RUSTFS_SCANNER_HEAL_W13_SOAK_SECONDS="$SOAK_SECONDS" \
    RUSTFS_SCANNER_HEAL_W13_ALLOW_SHORT_SOAK="$ALLOW_SHORT_SOAK" \
    RUSTFS_SCANNER_HEAL_W13_RUN_ID="w13-mrf-release-evidence-run" \
    RUSTFS_SCANNER_HEAL_W13_WINDOW_ID="w13-mrf-release-evidence-window" \
    RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT="$ENOSPC_ROOT" \
    RUSTFS_SCANNER_HEAL_W13_ENOSPC_FILL_LIMIT_BYTES="${RUSTFS_SCANNER_HEAL_W13_ENOSPC_FILL_LIMIT_BYTES:-67108864}" \
    cargo test --locked -p rustfs-heal --lib heal::mrf_queue::tests::w13_mrf_release_evidence_outputs_bundle_artifacts \
        -- --ignored --exact --nocapture

validate_artifacts "$SOURCE_REVISION"
DESCRIPTOR="$(write_release_descriptor "$SOURCE_REVISION")"
if [[ " ${CASES[*]} " == *" g07 "* ]]; then
    check_release_gate "$DESCRIPTOR" G07
fi
if [[ " ${CASES[*]} " == *" g08 "* ]]; then
    check_release_gate "$DESCRIPTOR" G08
fi
if [[ " ${CASES[*]} " == *" p4 "* ]]; then
    if [[ "$ALLOW_SHORT_SOAK" == 1 && "$SOAK_SECONDS" -lt 7200 ]]; then
        echo "SKIP: P4 release bundle gate validation for diagnostic short soak"
    else
        check_release_gate "$DESCRIPTOR" P4
    fi
fi
echo "Scanner/Heal W13 MRF release descriptors verified: $DESCRIPTOR"
echo "Scanner/Heal W13 MRF evidence verified: $RUN_DIR"
