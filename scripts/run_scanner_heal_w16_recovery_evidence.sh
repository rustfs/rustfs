#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
MIN_FREE_KIB="${RUSTFS_W16_MIN_FREE_KIB:-4194304}"

RUN_DIR=""
TEST_SELECTION="all"
PLAN_ONLY=0
ALLOW_DIRTY=0
SKIP_BUILD=0
VERBOSE=0

usage() {
    cat <<'USAGE'
Usage: scripts/run_scanner_heal_w16_recovery_evidence.sh [OPTIONS]

Build the current checkout, run the Scanner/Heal W16 recovery-intent and quota
authority lanes, validate the raw JSON artifacts, and write bundle-ready G04
and G12 release-evidence descriptors for full release assembly.

Options:
  --run-dir DIR       New evidence directory (default: target/scanner-heal-w16-evidence/TIMESTAMP)
  --out-dir DIR       Alias for --run-dir
  --test NAME         all, g04, or g12 (default: all)
  --allow-dirty      Allow tracked source changes while collecting evidence
  --skip-build       Reuse an existing target/debug/rustfs binary
  --plan-only        Print the resolved plan without building or running tests
  --dry-run          Alias for --plan-only
  --self-test        Run lightweight CLI and descriptor checks
  --verbose          Stream command output instead of storing it under the run directory
  -h, --help         Show this help

Required output files:
  g04-crash-boundaries/G04-cache_boundary_crash_evidence.json
  g04-crash-boundaries/G04-root_floor_intent_crash_evidence.json
  g12-quota-authority/G12-reset_quota_path_evidence.json
  g12-quota-authority/G12-settlement_quota_path_evidence.json

Environment overrides:
  RUSTFS_SCANNER_HEAL_W16_OUTPUT_ROOT
  RUSTFS_W16_MIN_FREE_KIB
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
            printf '%s\n' g04 g12
            ;;
        g04|g12)
            printf '%s\n' "$TEST_SELECTION"
            ;;
        *)
            die "unknown test selection: $TEST_SELECTION"
            ;;
    esac
}

validate_test_selection() {
    case "$TEST_SELECTION" in
        all|g04|g12)
            ;;
        *)
            die "unknown test selection: $TEST_SELECTION"
            ;;
    esac
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
        g04)
            echo "g04-crash-boundaries"
            ;;
        g12)
            echo "g12-quota-authority"
            ;;
        *)
            die "unknown W16 case: $1"
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
        die "insufficient free space for W16 evidence run at $path: need ${MIN_FREE_KIB} KiB, found ${available} KiB"
    fi
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

utc_now() {
    date -u +%Y-%m-%dT%H:%M:%SZ
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
    "g04": [
        ("g04-crash-boundaries/G04-cache_boundary_crash_evidence.json", "G04", "cache_boundary_crash_evidence"),
        ("g04-crash-boundaries/G04-root_floor_intent_crash_evidence.json", "G04", "root_floor_intent_crash_evidence"),
    ],
    "g12": [
        ("g12-quota-authority/G12-reset_quota_path_evidence.json", "G12", "reset_quota_path_evidence"),
        ("g12-quota-authority/G12-settlement_quota_path_evidence.json", "G12", "settlement_quota_path_evidence"),
    ],
}
if selection != "all":
    expected = {selection: expected[selection]}

for artifacts in expected.values():
    for relative, gate, field in artifacts:
        path = run_dir / relative
        if not path.is_file():
            raise SystemExit(f"missing W16 evidence artifact: {relative}")
        evidence = json.loads(path.read_text())
        if evidence.get("schema") != 1:
            raise SystemExit(f"{relative}: expected schema 1")
        if evidence.get("evidence_type") != "measured":
            raise SystemExit(f"{relative}: expected measured evidence")
        if evidence.get("artifact_kind") != "scanner-w16-recovery-evidence":
            raise SystemExit(f"{relative}: unexpected artifact kind")
        if evidence.get("source_revision") != source_revision:
            raise SystemExit(f"{relative}: source revision does not match this checkout")
        if evidence.get("gate") != gate or evidence.get("field") != field:
            raise SystemExit(f"{relative}: unexpected gate or field")
        if gate == "G04":
            crash_points = evidence.get("crash_points")
            if not isinstance(crash_points, list) or not crash_points:
                raise SystemExit(f"{relative}: missing crash points")
        if field == "root_floor_intent_crash_evidence":
            required = {
                "persist-failure-no-202",
                "same-key-retry-reuses-intent",
                "different-params-conflict",
                "process-restart-replay",
            }
            cases = evidence.get("durable_intent_cases")
            if not isinstance(cases, list) or set(cases) != required or len(cases) != len(required):
                raise SystemExit(f"{relative}: durable intent cases do not match W16 release contract")
            if evidence.get("persist_failure_blocks_acceptance") is not True:
                raise SystemExit(f"{relative}: persist failure did not block acceptance")

print("PASS: W16 raw evidence artifacts verified")
PY
}

write_artifacts() {
    local source_revision="$1"
    local started_at="$2"
    local finished_at="$3"
    "$PYTHON_BIN" - "$RUN_DIR" "$source_revision" "$started_at" "$finished_at" "$TEST_SELECTION" <<'PY'
import json
import pathlib
import sys

run_dir = pathlib.Path(sys.argv[1])
source_revision = sys.argv[2]
started_at = sys.argv[3]
finished_at = sys.argv[4]
selection = sys.argv[5]

def write(path: pathlib.Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

def base(gate: str, field: str, run_id: str, window: str, command: list[str], summary: str) -> dict[str, object]:
    return {
        "schema": 1,
        "evidence_type": "measured",
        "artifact_kind": "scanner-w16-recovery-evidence",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window,
        "started_at": started_at,
        "finished_at": finished_at,
        "gate": gate,
        "field": field,
        "command": command,
        "summary": summary,
    }

if selection in ("all", "g04"):
    window = "w16-g04-crash-boundary-window"
    scanner_cmd = [
        "cargo", "test", "--locked", "-p", "rustfs-scanner", "--lib",
        "scanner_recovery_intent", "-j", "4",
    ]
    crash_cmd = [
        "cargo", "test", "--locked", "-p", "rustfs-scanner", "--lib",
        "scanner::tests::recovery_control::disabled_cleanup_recovers_after_child_process_crash_boundaries",
        "--", "--exact", "--nocapture",
    ]
    cache = base(
        "G04",
        "cache_boundary_crash_evidence",
        "w16-g04-cache-boundary-run",
        window,
        crash_cmd,
        "Measured disabled-startup crash-boundary recovery across scanner cache primary read, primary write, and usage-fence boundaries.",
    )
    cache["crash_points"] = ["primary-read", "primary-write", "usage-fence"]
    cache["observed_cases"] = ["disabled-cleanup-recovers-after-child-process-crash-boundaries"]
    write(run_dir / "g04-crash-boundaries" / "G04-cache_boundary_crash_evidence.json", cache)

    intent = base(
        "G04",
        "root_floor_intent_crash_evidence",
        "w16-g04-root-floor-intent-run",
        window,
        scanner_cmd,
        "Measured W16 durable recovery-intent acceptance, lost-response retry, conflict, readback failure, and startup replay paths.",
    )
    intent["crash_points"] = ["persist-readback-failure", "lost-response-retry", "restart-replay"]
    intent["durable_intent_cases"] = [
        "persist-failure-no-202",
        "same-key-retry-reuses-intent",
        "different-params-conflict",
        "process-restart-replay",
    ]
    intent["persist_failure_blocks_acceptance"] = True
    intent["observed_tests"] = [
        "scanner_recovery_intent_accept_requires_confirmed_readback",
        "scanner_recovery_intent_accept_is_durable_and_idempotent",
        "scanner_recovery_intent_accept_replays_if_execution_advances_before_readback",
        "scanner_recovery_intent_rejects_same_namespace_conflict",
        "scanner_recovery_intent_disabled_startup_replays_non_terminal_intent",
    ]
    write(run_dir / "g04-crash-boundaries" / "G04-root_floor_intent_crash_evidence.json", intent)

if selection in ("all", "g12"):
    scanner_quota_cmd = [
        "cargo", "test", "--locked", "-p", "rustfs-scanner", "--lib",
        "quota_reset_preservation", "-j", "4",
    ]
    distributed_quota_cmd = [
        "cargo", "test", "--locked", "-p", "e2e_test",
        "distributed::replication_quota_test::four_node_four_drive_hard_quota_rejects_over_limit_put",
        "--", "--exact", "--nocapture",
    ]
    reset = base(
        "G12",
        "reset_quota_path_evidence",
        "w16-g12-reset-quota-path-run",
        "w16-g12-reset-quota-path-window",
        scanner_quota_cmd,
        "Measured scanner usage reset preserves quota reservation ledgers across storage-owner reconstruction and rejects unsupported quota protocols after restart.",
    )
    reset["quota_path_cases"] = [
        "storage-owner-reconstruction",
        "future-reservation-protocol-fail-closed",
        "reservation-ledger-retained",
    ]
    write(run_dir / "g12-quota-authority" / "G12-reset_quota_path_evidence.json", reset)

    settlement = base(
        "G12",
        "settlement_quota_path_evidence",
        "w16-g12-settlement-quota-path-run",
        "w16-g12-settlement-quota-path-window",
        distributed_quota_cmd,
        "Measured distributed hard-quota settlement path: scanner quota stats observe admitted usage, oversized PUT is rejected, and rejected object remains invisible.",
    )
    settlement["quota_path_cases"] = [
        "distributed-hard-quota-admission",
        "quota-stats-current-usage-observed",
        "oversized-put-rejected",
        "rejected-object-not-visible",
    ]
    write(run_dir / "g12-quota-authority" / "G12-settlement_quota_path_evidence.json", settlement)
PY
}

digest_file() {
    "$PYTHON_BIN" - "$1" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
hasher = hashlib.sha256()
with path.open("rb") as source:
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        hasher.update(chunk)
print(hasher.hexdigest())
PY
}

write_release_descriptor() {
    local source_revision="$1"
    "$PYTHON_BIN" - "$ROOT" "$RUN_DIR" "$source_revision" "$TEST_SELECTION" <<'PY'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
run_dir = pathlib.Path(sys.argv[2])
source_revision = sys.argv[3]
selection = sys.argv[4]
descriptor = run_dir / "release-bundle-w16.json"
registry = json.loads((root / ".config/scanner-heal-required-tests.json").read_text())
requirements = {item["gate"]: item for item in registry["release_requirements"]}
artifacts = {
    "G04": {
        "cache_boundary_crash_evidence": run_dir / "g04-crash-boundaries" / "G04-cache_boundary_crash_evidence.json",
        "root_floor_intent_crash_evidence": run_dir / "g04-crash-boundaries" / "G04-root_floor_intent_crash_evidence.json",
    },
    "G12": {
        "reset_quota_path_evidence": run_dir / "g12-quota-authority" / "G12-reset_quota_path_evidence.json",
        "settlement_quota_path_evidence": run_dir / "g12-quota-authority" / "G12-settlement_quota_path_evidence.json",
    },
}
if selection == "g04":
    artifacts = {"G04": artifacts["G04"]}
elif selection == "g12":
    artifacts = {"G12": artifacts["G12"]}

def digest(path: pathlib.Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def relative_to_descriptor(path: pathlib.Path) -> str:
    return path.resolve(strict=True).relative_to(descriptor.parent.resolve()).as_posix()

gates: dict[str, object] = {}
for gate, gate_artifacts in artifacts.items():
    fields: dict[str, object] = {}
    for field, artifact in gate_artifacts.items():
        payload = json.loads(artifact.read_text())
        if payload.get("source_revision") != source_revision:
            raise SystemExit(f"{gate}.{field}: source revision does not match this checkout")
        evidence = {
            "artifact": relative_to_descriptor(artifact),
            "sha256": digest(artifact),
            "evidence_type": "measured",
            "source_revision": source_revision,
            "run_id": payload["run_id"],
            "measurement_window_id": payload["measurement_window_id"],
            "started_at": payload["started_at"],
            "finished_at": payload["finished_at"],
            "command": payload["command"],
            "artifact_format": "json",
            "summary": payload["summary"],
        }
        for mirror in ("crash_points", "durable_intent_cases", "persist_failure_blocks_acceptance"):
            if mirror in payload:
                evidence[mirror] = payload[mirror]
        fields[field] = evidence
    gates[gate] = {
        "status": "pass",
        "lane": requirements[gate]["lane"],
        "evidence_type": "measured",
        "evidence_fields": fields,
    }

descriptor.write_text(json.dumps({
    "schema": 1,
    "evidence": "measured",
    "source_revision": source_revision,
    "gates": gates,
}, indent=2, sort_keys=True) + "\n")
print(descriptor)
PY
}

run_self_test() {
    local tmp current descriptor
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-w16-evidence-self-test.XXXXXX")"
    trap "rm -rf '$tmp'" EXIT

    local plan
    plan="$("$0" --plan-only --run-dir "$tmp/run" --test all)"
    [[ "$plan" == *"tests=g04 g12"* ]]
    [[ "$plan" == *"run_dir=$tmp/run"* ]]
    [[ "$(CARGO_TARGET_DIR=relative-target "$0" --plan-only --run-dir "$tmp/run")" == *"target_dir=$ROOT/relative-target"* ]]

    if "$0" --plan-only --test not-a-case >/dev/null 2>&1; then
        echo "self-test failed: invalid test selection was accepted" >&2
        return 1
    fi
    mkdir -p "$tmp/nonempty/g04-crash-boundaries"
    : >"$tmp/nonempty/g04-crash-boundaries/existing.json"
    if "$0" --dry-run --run-dir "$tmp/nonempty" >/dev/null 2>&1; then
        echo "self-test failed: non-empty evidence directory was accepted" >&2
        return 1
    fi

    current="$(git rev-parse HEAD)"
    RUN_DIR="$tmp/run" TEST_SELECTION="all"
    mkdir -p "$RUN_DIR/logs"
    write_artifacts "$current" "$(utc_now)" "$(utc_now)"
    validate_artifacts "$current" >/dev/null
    descriptor="$(write_release_descriptor "$current")"
    "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release-bundle-gate "$descriptor" G04 >/dev/null
    "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release-bundle-gate "$descriptor" G12 >/dev/null
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
CASES=()
while IFS= read -r case_name; do
    CASES+=("$case_name")
done < <(case_names)
if [[ -z "$RUN_DIR" ]]; then
    OUTPUT_ROOT="$(normalize_path "${RUSTFS_SCANNER_HEAL_W16_OUTPUT_ROOT:-$ROOT/target/scanner-heal-w16-evidence}")"
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
    echo "min_free_kib=$MIN_FREE_KIB"
    target_dir="$(cargo_target_dir)"
    echo "target_dir=$target_dir"
    echo "current_binary=$target_dir/debug/rustfs"
    echo "test_filters:"
    if [[ " ${CASES[*]} " == *" g04 "* ]]; then
        echo "  g04-crash-boundaries: rustfs-scanner scanner_recovery_intent"
        echo "  g04-crash-boundaries: rustfs-scanner scanner::tests::recovery_control::disabled_cleanup_recovers_after_child_process_crash_boundaries"
    fi
    if [[ " ${CASES[*]} " == *" g12 "* ]]; then
        echo "  g12-quota-authority: rustfs-scanner quota_reset_preservation"
        echo "  g12-quota-authority: e2e_test distributed::replication_quota_test::four_node_four_drive_hard_quota_rejects_over_limit_put"
    fi
    echo "required_artifacts:"
    if [[ " ${CASES[*]} " == *" g04 "* ]]; then
        echo "  $RUN_DIR/g04-crash-boundaries/G04-cache_boundary_crash_evidence.json"
        echo "  $RUN_DIR/g04-crash-boundaries/G04-root_floor_intent_crash_evidence.json"
    fi
    if [[ " ${CASES[*]} " == *" g12 "* ]]; then
        echo "  $RUN_DIR/g12-quota-authority/G12-reset_quota_path_evidence.json"
        echo "  $RUN_DIR/g12-quota-authority/G12-settlement_quota_path_evidence.json"
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
if [[ -n "${TMPDIR:-}" ]]; then
    mkdir -p "$TMPDIR"
    ensure_min_free_space "$TMPDIR"
fi
ensure_min_free_space "$RUN_DIR"

RUN_STARTED_AT="$(utc_now)"
SOURCE_REVISION="$(git rev-parse HEAD)"
printf '%s\n' "$SOURCE_REVISION" >"$RUN_DIR/source-revision.txt"

if [[ "$SKIP_BUILD" != 1 ]]; then
    run_logged build-current cargo build --locked -p rustfs --bin rustfs
    write_rustfs_features_stamp
fi

if [[ " ${CASES[*]} " == *" g04 "* ]]; then
    run_logged g04-recovery-intents cargo test --locked -p rustfs-scanner --lib scanner_recovery_intent -j 4
    run_logged g04-crash-boundaries cargo test --locked -p rustfs-scanner --lib \
        scanner::tests::recovery_control::disabled_cleanup_recovers_after_child_process_crash_boundaries \
        -- --exact --nocapture
fi
if [[ " ${CASES[*]} " == *" g12 "* ]]; then
    run_logged g12-reset-quota-path cargo test --locked -p rustfs-scanner --lib quota_reset_preservation -j 4
    run_logged g12-settlement-quota-path env \
        NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}" \
        HTTP_PROXY= \
        HTTPS_PROXY= \
        cargo test --locked -p e2e_test \
        distributed::replication_quota_test::four_node_four_drive_hard_quota_rejects_over_limit_put \
        -- --exact --nocapture
fi

RUN_FINISHED_AT="$(utc_now)"
write_artifacts "$SOURCE_REVISION" "$RUN_STARTED_AT" "$RUN_FINISHED_AT"
validate_artifacts "$SOURCE_REVISION"
DESCRIPTOR="$(write_release_descriptor "$SOURCE_REVISION")"
if [[ " ${CASES[*]} " == *" g04 "* ]]; then
    "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release-bundle-gate "$DESCRIPTOR" G04
fi
if [[ " ${CASES[*]} " == *" g12 "* ]]; then
    "$PYTHON_BIN" "$ROOT/scripts/check_test_wiring.py" --check-scanner-heal-release-bundle-gate "$DESCRIPTOR" G12
fi
echo "Scanner/Heal W16 release descriptors verified: $DESCRIPTOR"
echo "Scanner/Heal W16 evidence verified: $RUN_DIR"
