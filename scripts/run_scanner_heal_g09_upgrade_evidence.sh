#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
SOURCE_REPOSITORY="${RUSTFS_UPGRADE_SOURCE_REPOSITORY:-${UPGRADE_SOURCE_REPOSITORY:-rustfs/rustfs}}"
SOURCE_VERSION="${RUSTFS_UPGRADE_SOURCE_VERSION:-${UPGRADE_SOURCE_VERSION:-1.0.0-rc.5}}"
SOURCE_ASSET="${RUSTFS_UPGRADE_SOURCE_ASSET:-${UPGRADE_SOURCE_ASSET:-rustfs-linux-x86_64-gnu-v1.0.0-rc.5.zip}}"
SOURCE_SHA256="${RUSTFS_UPGRADE_SOURCE_SHA256:-${UPGRADE_SOURCE_SHA256:-3ee8df71e8edcfada533be452c4135868f697bc515460ae97b027313eade7a3d}}"
MIN_FREE_KIB="${RUSTFS_G09_MIN_FREE_KIB:-6291456}"

RUN_DIR=""
SOURCE_DIR=""
SOURCE_BINARY="${RUSTFS_UPGRADE_SOURCE_BINARY:-}"
TEST_SELECTION="all"
PLAN_ONLY=0
ALLOW_DIRTY=0
SKIP_BUILD=0
SKIP_DOWNLOAD=0
VERBOSE=0

usage() {
    cat <<'USAGE'
Usage: scripts/run_scanner_heal_g09_upgrade_evidence.sh [OPTIONS]

Build the current checkout, run the Scanner/Heal G09 upgrade compatibility
lanes, and validate the raw mixed-version/rollback evidence artifacts.

Options:
  --run-dir DIR       New evidence directory (default: target/scanner-heal-g09-evidence/TIMESTAMP)
  --out-dir DIR       Alias for --run-dir
  --source-dir DIR    Cache directory for the pinned previous release binary
  --source-binary BIN Use an existing previous-release rustfs binary
  --version VERSION   Previous release tag (default: 1.0.0-rc.5)
  --asset NAME        Previous release asset zip name
  --sha256 HEX        Expected SHA-256 for the previous release asset
  --repository OWNER/REPO
                     GitHub repository used to download the release asset (default: rustfs/rustfs)
  --test NAME         all, mixed-version, or rollback (default: all)
  --allow-dirty      Allow tracked source changes while collecting evidence
  --skip-build       Reuse an existing target/debug/rustfs binary
  --skip-download    Reuse SOURCE_DIR/rustfs instead of downloading the previous release
  --plan-only        Print the resolved plan without building or running tests
  --dry-run          Validate configuration and print the commands without running them
  --self-test        Run lightweight CLI and artifact-validator checks
  --verbose          Stream command output instead of storing it under the run directory
  -h, --help         Show this help

The default pinned release asset is Linux x86_64. Use --source-binary when
running against a custom previous-release binary on another platform. The
script requires at least 6 GiB free by default; override
RUSTFS_G09_MIN_FREE_KIB only for a deliberately smaller diagnostic run.

Environment overrides:
  RUSTFS_SCANNER_HEAL_G09_OUTPUT_ROOT
  RUSTFS_UPGRADE_SOURCE_DIR
  RUSTFS_UPGRADE_SOURCE_BINARY
  RUSTFS_UPGRADE_SOURCE_REPOSITORY / UPGRADE_SOURCE_REPOSITORY
  RUSTFS_UPGRADE_SOURCE_VERSION / UPGRADE_SOURCE_VERSION
  RUSTFS_UPGRADE_SOURCE_ASSET / UPGRADE_SOURCE_ASSET
  RUSTFS_UPGRADE_SOURCE_SHA256 / UPGRADE_SOURCE_SHA256

Per-case test environment set by the runner:
  RUSTFS_UPGRADE_SOURCE_BINARY
  RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR
  RUSTFS_E2E_LOG_DIR

Required output files:
  mixed-version-upgrade/G09-mixed_version_reader_evidence.json
  mixed-version-upgrade/G09-mixed_version_writer_evidence.json
  bucket-config-rollback/G09-rollback_payload_evidence.json
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
            printf '%s\n' mixed-version rollback
            ;;
        mixed-version|rollback)
            printf '%s\n' "$TEST_SELECTION"
            ;;
        *)
            die "unknown test selection: $TEST_SELECTION"
            ;;
    esac
}

artifact_for() {
    case "$1" in
        mixed-version)
            echo "mixed-version-upgrade"
            ;;
        rollback)
            echo "bucket-config-rollback"
            ;;
        *)
            die "unknown G09 case: $1"
            ;;
    esac
}

test_filter_for() {
    case "$1" in
        mixed-version)
            echo "upgrade_compatibility_test::rolling_upgrade_from_rc2_preserves_mixed_version_contracts"
            ;;
        rollback)
            echo "upgrade_compatibility_test::rollback_to_previous_release_reads_current_bucket_metadata"
            ;;
        *)
            die "unknown G09 case: $1"
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
    : > "$target_dir/debug/rustfs.features"
}

ensure_default_asset_platform() {
    if [[ -n "$SOURCE_BINARY" ]]; then
        return
    fi
    local os arch
    os="$(uname -s)"
    arch="$(uname -m)"
    if [[ "$os" != "Linux" || ( "$arch" != "x86_64" && "$arch" != "amd64" ) ]]; then
        echo "default previous-release asset requires Linux x86_64; pass --source-binary for this platform" >&2
        exit 2
    fi
}

verify_sha256() {
    local archive="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s  %s\n' "$SOURCE_SHA256" "$archive" | sha256sum --check --strict
    elif command -v shasum >/dev/null 2>&1; then
        printf '%s  %s\n' "$SOURCE_SHA256" "$archive" | shasum -a 256 --check
    else
        echo "sha256sum or shasum is required to verify $SOURCE_ASSET" >&2
        exit 1
    fi
}

validate_source_sha256() {
    [[ "$SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "--sha256 must be a 64-character lowercase hex digest"
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
        echo "could not determine free space for $path" >&2
        exit 1
    fi
    if (( available < MIN_FREE_KIB )); then
        echo "insufficient free space for G09 evidence run at $path: need ${MIN_FREE_KIB} KiB, found ${available} KiB" >&2
        exit 1
    fi
}

resolve_source_binary() {
    if [[ -n "$SOURCE_BINARY" ]]; then
        SOURCE_BINARY="$(normalize_path "$SOURCE_BINARY")"
        test -x "$SOURCE_BINARY"
        echo "$SOURCE_BINARY"
        return
    fi

    ensure_default_asset_platform
    if [[ -z "$SOURCE_DIR" ]]; then
        SOURCE_DIR="$ROOT/target/scanner-heal-g09-source/$SOURCE_VERSION"
    else
        SOURCE_DIR="$(normalize_path "$SOURCE_DIR")"
    fi

    local binary="$SOURCE_DIR/rustfs"
    if [[ -x "$binary" ]]; then
        echo "$binary"
        return
    fi
    if [[ "$SKIP_DOWNLOAD" == 1 ]]; then
        die "previous release binary does not exist or is not executable: $binary"
    fi

    mkdir -p "$SOURCE_DIR"
    local archive="$SOURCE_DIR/$SOURCE_ASSET"
    curl --fail --location --retry 3 --output "$archive" \
        "https://github.com/$SOURCE_REPOSITORY/releases/download/$SOURCE_VERSION/$SOURCE_ASSET"
    verify_sha256 "$archive"
    unzip -q "$archive" -d "$SOURCE_DIR"
    chmod +x "$binary"
    test -x "$binary"
    echo "$binary"
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
    "mixed-version": [
        ("mixed-version-upgrade/G09-mixed_version_reader_evidence.json",
         "mixed_version_reader_evidence", "mixed-version-reader"),
        ("mixed-version-upgrade/G09-mixed_version_writer_evidence.json",
         "mixed_version_writer_evidence", "mixed-version-writer"),
    ],
    "rollback": [
        ("bucket-config-rollback/G09-rollback_payload_evidence.json",
         "rollback_payload_evidence", "rollback-payload"),
    ],
}

if selection != "all":
    expected = {selection: expected[selection]}

for _, artifacts in expected.items():
    for relative, field, role in artifacts:
        path = run_dir / relative
        if not path.is_file():
            raise SystemExit(f"missing G09 evidence artifact: {relative}")
        evidence = json.loads(path.read_text())
        if evidence.get("schema") != 1:
            raise SystemExit(f"{relative}: expected schema 1")
        if evidence.get("evidence_type") != "measured":
            raise SystemExit(f"{relative}: expected measured evidence")
        if evidence.get("artifact_kind") != "upgrade-compatibility-e2e":
            raise SystemExit(f"{relative}: unexpected artifact kind")
        if evidence.get("gate") != "G09":
            raise SystemExit(f"{relative}: unexpected gate")
        if evidence.get("field") != field:
            raise SystemExit(f"{relative}: expected field {field}")
        if evidence.get("mixed_version_role") != role:
            raise SystemExit(f"{relative}: expected role {role}")
        if evidence.get("current_revision") != source_revision:
            raise SystemExit(f"{relative}: current revision does not match this checkout")
        versions = evidence.get("versions")
        if not isinstance(versions, list) or len(versions) != 2:
            raise SystemExit(f"{relative}: versions must contain previous and current revisions")
        if versions[0] == versions[1]:
            raise SystemExit(f"{relative}: previous and current revisions must differ")
        if field == "rollback_payload_evidence" and evidence.get("rollback_payload_replayed") is not True:
            raise SystemExit(f"{relative}: rollback payload was not marked replayed")

print("PASS: G09 raw evidence artifacts verified")
PY
}

run_self_test() {
    local tmp
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-g09-evidence-self-test.XXXXXX")"
    trap "rm -rf '$tmp'" EXIT

    local plan
    plan="$("$0" --plan-only --run-dir "$tmp/run" --source-binary "$tmp/rustfs-prev" --test all)"
    [[ "$plan" == *"tests=mixed-version rollback"* ]]
    [[ "$plan" == *"run_dir=$tmp/run"* ]]
    [[ "$(CARGO_TARGET_DIR=relative-target "$0" --plan-only --run-dir "$tmp/run" --source-binary "$tmp/rustfs-prev")" == *"target_dir=$ROOT/relative-target"* ]]

    if "$0" --plan-only --test not-a-case >/dev/null 2>&1; then
        echo "self-test failed: invalid test selection was accepted" >&2
        return 1
    fi
    if "$0" --dry-run --sha256 bad >/dev/null 2>&1; then
        echo "self-test failed: invalid SHA-256 was accepted" >&2
        return 1
    fi
    mkdir -p "$tmp/nonempty/mixed-version-upgrade"
    touch "$tmp/nonempty/mixed-version-upgrade/existing.json"
    if "$0" --dry-run --run-dir "$tmp/nonempty" --sha256 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa >/dev/null 2>&1; then
        echo "self-test failed: non-empty evidence directory was accepted" >&2
        return 1
    fi

    mkdir -p "$tmp/run/mixed-version-upgrade" "$tmp/run/bucket-config-rollback"
    local current previous
    current="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    previous="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    cat >"$tmp/run/mixed-version-upgrade/G09-mixed_version_reader_evidence.json" <<JSON
{"schema":1,"evidence_type":"measured","artifact_kind":"upgrade-compatibility-e2e","gate":"G09","field":"mixed_version_reader_evidence","mixed_version_role":"mixed-version-reader","current_revision":"$current","previous_revision":"$previous","versions":["$previous","$current"]}
JSON
    cat >"$tmp/run/mixed-version-upgrade/G09-mixed_version_writer_evidence.json" <<JSON
{"schema":1,"evidence_type":"measured","artifact_kind":"upgrade-compatibility-e2e","gate":"G09","field":"mixed_version_writer_evidence","mixed_version_role":"mixed-version-writer","current_revision":"$current","previous_revision":"$previous","versions":["$previous","$current"]}
JSON
    cat >"$tmp/run/bucket-config-rollback/G09-rollback_payload_evidence.json" <<JSON
{"schema":1,"evidence_type":"measured","artifact_kind":"upgrade-compatibility-e2e","gate":"G09","field":"rollback_payload_evidence","mixed_version_role":"rollback-payload","current_revision":"$current","previous_revision":"$previous","versions":["$previous","$current"],"rollback_payload_replayed":true}
JSON
    RUN_DIR="$tmp/run" TEST_SELECTION="all" validate_artifacts "$current" >/dev/null
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-dir|--out-dir)
            require_value "$1" "$#"
            RUN_DIR="$2"
            shift 2
            ;;
        --source-dir)
            require_value "$1" "$#"
            SOURCE_DIR="$2"
            shift 2
            ;;
        --source-binary)
            require_value "$1" "$#"
            SOURCE_BINARY="$2"
            shift 2
            ;;
        --version)
            require_value "$1" "$#"
            SOURCE_VERSION="$2"
            shift 2
            ;;
        --asset)
            require_value "$1" "$#"
            SOURCE_ASSET="$2"
            shift 2
            ;;
        --sha256)
            require_value "$1" "$#"
            SOURCE_SHA256="$2"
            shift 2
            ;;
        --repository)
            require_value "$1" "$#"
            SOURCE_REPOSITORY="$2"
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
        --skip-download)
            SKIP_DOWNLOAD=1
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

CASES=()
while IFS= read -r case_name; do
    CASES+=("$case_name")
done < <(case_names)
if [[ -z "$RUN_DIR" ]]; then
    RUN_DIR="$ROOT/target/scanner-heal-g09-evidence/$(date -u +%Y%m%dT%H%M%SZ)"
else
    RUN_DIR="$(normalize_path "$RUN_DIR")"
fi
if [[ -n "$SOURCE_DIR" ]]; then
    SOURCE_DIR="$(normalize_path "$SOURCE_DIR")"
fi

validate_source_sha256
for case_name in "${CASES[@]}"; do
    check_empty_case_dir "$RUN_DIR/$(artifact_for "$case_name")"
done

if [[ "$PLAN_ONLY" == 1 ]]; then
    echo "run_dir=$RUN_DIR"
    echo "out_dir=$RUN_DIR"
    echo "tests=${CASES[*]}"
    echo "source_repository=$SOURCE_REPOSITORY"
    echo "source_version=$SOURCE_VERSION"
    echo "min_free_kib=$MIN_FREE_KIB"
    if [[ -n "$SOURCE_BINARY" ]]; then
        echo "source_binary=$(normalize_path "$SOURCE_BINARY")"
    else
        if [[ -z "$SOURCE_DIR" ]]; then
            echo "source_dir=$ROOT/target/scanner-heal-g09-source/$SOURCE_VERSION"
        else
            echo "source_dir=$SOURCE_DIR"
        fi
        echo "source_asset=$SOURCE_ASSET"
        echo "download_url=https://github.com/$SOURCE_REPOSITORY/releases/download/$SOURCE_VERSION/$SOURCE_ASSET"
    fi
    target_dir="$(cargo_target_dir)"
    echo "target_dir=$target_dir"
    echo "current_binary=$target_dir/debug/rustfs"
    echo "test_filters:"
    for case_name in "${CASES[@]}"; do
        echo "  $(artifact_for "$case_name"): $(test_filter_for "$case_name")"
    done
    echo "required_artifacts:"
    for case_name in "${CASES[@]}"; do
        artifact="$(artifact_for "$case_name")"
        case "$case_name" in
            mixed-version)
                echo "  $RUN_DIR/$artifact/G09-mixed_version_reader_evidence.json"
                echo "  $RUN_DIR/$artifact/G09-mixed_version_writer_evidence.json"
                ;;
            rollback)
                echo "  $RUN_DIR/$artifact/G09-rollback_payload_evidence.json"
                ;;
        esac
    done
    exit 0
fi

cd "$ROOT"
if [[ "$ALLOW_DIRTY" != 1 && -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "commit tracked source changes before creating release evidence, or pass --allow-dirty for local diagnostics" >&2
    exit 1
fi
if [[ -e "$RUN_DIR" ]]; then
    echo "evidence run directory already exists: $RUN_DIR" >&2
    exit 1
fi
mkdir -p "$RUN_DIR/logs"
if [[ -n "${TMPDIR:-}" ]]; then
    mkdir -p "$TMPDIR"
    ensure_min_free_space "$TMPDIR"
fi
ensure_min_free_space "$RUN_DIR"

SOURCE_BINARY="$(resolve_source_binary)"
export RUSTFS_UPGRADE_SOURCE_BINARY="$SOURCE_BINARY"
export RUSTFS_E2E_LOG_DIR="${RUSTFS_E2E_LOG_DIR:-$RUN_DIR/server-logs}"
mkdir -p "$RUSTFS_E2E_LOG_DIR"

if [[ "$SKIP_BUILD" != 1 ]]; then
    run_logged build-current cargo build --locked -p rustfs --bin rustfs
    write_rustfs_features_stamp
fi

SOURCE_REVISION="$(git rev-parse HEAD)"
printf '%s\n' "$SOURCE_REVISION" >"$RUN_DIR/source-revision.txt"
printf '%s\n' "$SOURCE_VERSION" >"$RUN_DIR/previous-release-version.txt"

for case_name in "${CASES[@]}"; do
    artifact="$(artifact_for "$case_name")"
    test_filter="$(test_filter_for "$case_name")"
    evidence_dir="$RUN_DIR/$artifact"
    mkdir -p "$evidence_dir"
    run_logged "$case_name" env \
        NO_PROXY="${NO_PROXY:-127.0.0.1,localhost}" \
        HTTP_PROXY= \
        HTTPS_PROXY= \
        RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR="$evidence_dir" \
        cargo test --locked -p e2e_test "$test_filter" -- --ignored --exact --nocapture
done

validate_artifacts "$SOURCE_REVISION"
echo "Scanner/Heal G09 evidence verified: $RUN_DIR"
