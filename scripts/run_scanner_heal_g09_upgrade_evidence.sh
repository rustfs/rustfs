#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_scanner_heal_g09_upgrade_evidence.sh [options]

Runs the Scanner/Heal G09 mixed-version and rollback upgrade evidence lanes
against the pinned previous RustFS release binary.

Options:
  --out-dir DIR        Evidence output root (default: target/scanner-heal-g09-upgrade-evidence/<utc timestamp>)
  --source-dir DIR     Directory used for the pinned previous release binary
  --version VERSION    Previous release tag (default: 1.0.0-rc.5)
  --asset NAME         Previous release asset zip name
  --sha256 HEX         Expected SHA-256 for the previous release asset
  --repository OWNER/REPO
                      GitHub repository used to download the release asset (default: rustfs/rustfs)
  --skip-build         Reuse target/debug/rustfs instead of building it first
  --skip-download      Reuse SOURCE_DIR/rustfs instead of downloading the previous release
  --dry-run            Validate configuration and print the commands without running them
  -h, --help           Show this help

Environment overrides:
  RUSTFS_SCANNER_HEAL_G09_OUTPUT_ROOT
  RUSTFS_UPGRADE_SOURCE_DIR
  UPGRADE_SOURCE_VERSION
  UPGRADE_SOURCE_ASSET
  UPGRADE_SOURCE_SHA256
  UPGRADE_SOURCE_REPOSITORY

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

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

is_absolute_path() {
  case "$1" in
    /*) return 0 ;;
    *) return 1 ;;
  esac
}

resolve_path() {
  local value="$1"
  if is_absolute_path "$value"; then
    printf '%s\n' "$value"
  else
    printf '%s/%s\n' "$PROJECT_ROOT" "$value"
  fi
}

validate_sha256() {
  local value="$1"
  [[ "$value" =~ ^[0-9a-f]{64}$ ]] || die "--sha256 must be a 64-character lowercase hex digest"
}

check_empty_case_dir() {
  local dir="$1"
  if [[ -d "$dir" ]] && find "$dir" -mindepth 1 -print -quit | grep -q .; then
    die "evidence case directory is not empty: $dir"
  fi
}

sha256_check() {
  local expected="$1"
  local archive="$2"
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s  %s\n' "$expected" "$archive" | sha256sum --check --strict
  elif command -v shasum >/dev/null 2>&1; then
    printf '%s  %s\n' "$expected" "$archive" | shasum -a 256 -c
  else
    die "required tool not found: sha256sum or shasum"
  fi
}

run_case() {
  local artifact="$1"
  local test_name="$2"
  shift 2
  local expected_files=("$@")
  local case_dir="$OUT_DIR/$artifact"
  local case_log_dir="$LOG_DIR/$artifact"

  check_empty_case_dir "$case_dir"
  mkdir -p "$case_dir" "$case_log_dir"

  echo "==> running $artifact"
  RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR="$case_dir" \
  RUSTFS_E2E_LOG_DIR="$case_log_dir" \
    cargo test --locked -p e2e_test \
      "upgrade_compatibility_test::$test_name" \
      -- --ignored --exact --nocapture

  local expected
  for expected in "${expected_files[@]}"; do
    test -s "$case_dir/$expected" || die "missing G09 evidence artifact: $case_dir/$expected"
  done
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UTC_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

UPGRADE_SOURCE_VERSION="${UPGRADE_SOURCE_VERSION:-1.0.0-rc.5}"
UPGRADE_SOURCE_ASSET="${UPGRADE_SOURCE_ASSET:-rustfs-linux-x86_64-gnu-v1.0.0-rc.5.zip}"
UPGRADE_SOURCE_SHA256="${UPGRADE_SOURCE_SHA256:-3ee8df71e8edcfada533be452c4135868f697bc515460ae97b027313eade7a3d}"
UPGRADE_SOURCE_REPOSITORY="${UPGRADE_SOURCE_REPOSITORY:-rustfs/rustfs}"
OUT_DIR="${RUSTFS_SCANNER_HEAL_G09_OUTPUT_ROOT:-$PROJECT_ROOT/target/scanner-heal-g09-upgrade-evidence/$UTC_STAMP}"
SOURCE_DIR="${RUSTFS_UPGRADE_SOURCE_DIR:-$PROJECT_ROOT/target/rustfs-upgrade-source/$UPGRADE_SOURCE_VERSION}"
SKIP_BUILD=0
SKIP_DOWNLOAD=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      [[ $# -ge 2 ]] || die "missing value for --out-dir"
      OUT_DIR="$2"
      shift 2
      ;;
    --source-dir)
      [[ $# -ge 2 ]] || die "missing value for --source-dir"
      SOURCE_DIR="$2"
      shift 2
      ;;
    --version)
      [[ $# -ge 2 ]] || die "missing value for --version"
      UPGRADE_SOURCE_VERSION="$2"
      shift 2
      ;;
    --asset)
      [[ $# -ge 2 ]] || die "missing value for --asset"
      UPGRADE_SOURCE_ASSET="$2"
      shift 2
      ;;
    --sha256)
      [[ $# -ge 2 ]] || die "missing value for --sha256"
      UPGRADE_SOURCE_SHA256="$2"
      shift 2
      ;;
    --repository)
      [[ $# -ge 2 ]] || die "missing value for --repository"
      UPGRADE_SOURCE_REPOSITORY="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-download)
      SKIP_DOWNLOAD=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

OUT_DIR="$(resolve_path "$OUT_DIR")"
SOURCE_DIR="$(resolve_path "$SOURCE_DIR")"
LOG_DIR="$OUT_DIR/server-logs"
PREVIOUS_BINARY="$SOURCE_DIR/rustfs"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target}"
TARGET_DIR="$(resolve_path "$TARGET_DIR")"
CURRENT_BINARY="$TARGET_DIR/debug/rustfs"
ARCHIVE="$SOURCE_DIR/$UPGRADE_SOURCE_ASSET"
DOWNLOAD_URL="https://github.com/$UPGRADE_SOURCE_REPOSITORY/releases/download/$UPGRADE_SOURCE_VERSION/$UPGRADE_SOURCE_ASSET"

validate_sha256 "$UPGRADE_SOURCE_SHA256"
check_empty_case_dir "$OUT_DIR/mixed-version-upgrade"
check_empty_case_dir "$OUT_DIR/bucket-config-rollback"

if [[ "$DRY_RUN" -eq 1 ]]; then
  cat <<DRYRUN
project_root=$PROJECT_ROOT
out_dir=$OUT_DIR
source_dir=$SOURCE_DIR
previous_binary=$PREVIOUS_BINARY
current_binary=$CURRENT_BINARY
target_dir=$TARGET_DIR
download_url=$DOWNLOAD_URL
tests:
  mixed-version-upgrade: upgrade_compatibility_test::rolling_upgrade_from_rc2_preserves_mixed_version_contracts
  bucket-config-rollback: upgrade_compatibility_test::rollback_to_previous_release_reads_current_bucket_metadata
required_artifacts:
  $OUT_DIR/mixed-version-upgrade/G09-mixed_version_reader_evidence.json
  $OUT_DIR/mixed-version-upgrade/G09-mixed_version_writer_evidence.json
  $OUT_DIR/bucket-config-rollback/G09-rollback_payload_evidence.json
DRYRUN
  exit 0
fi

[[ "$(uname -s)" == "Linux" ]] || die "G09 upgrade evidence runner requires Linux"
case "$(uname -m)" in
  x86_64|amd64) ;;
  *) die "G09 upgrade evidence runner requires x86_64/amd64" ;;
esac

require_tool cargo
require_tool curl
require_tool unzip
require_tool git

mkdir -p "$SOURCE_DIR" "$OUT_DIR" "$LOG_DIR"

if [[ "$SKIP_DOWNLOAD" -eq 0 ]]; then
  echo "==> downloading pinned previous release $UPGRADE_SOURCE_VERSION"
  curl --fail --location --retry 3 --output "$ARCHIVE" "$DOWNLOAD_URL"
  sha256_check "$UPGRADE_SOURCE_SHA256" "$ARCHIVE"
  unzip -q -o "$ARCHIVE" -d "$SOURCE_DIR"
fi

[[ -f "$PREVIOUS_BINARY" ]] || die "previous release binary does not exist: $PREVIOUS_BINARY"
test -x "$PREVIOUS_BINARY" || chmod +x "$PREVIOUS_BINARY"
test -x "$PREVIOUS_BINARY" || die "previous release binary is not executable: $PREVIOUS_BINARY"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "==> building current RustFS binary"
  cargo build --locked -p rustfs --bin rustfs
  mkdir -p "$TARGET_DIR/debug"
  : > "$TARGET_DIR/debug/rustfs.features"
fi

test -x "$CURRENT_BINARY" || die "current RustFS binary is not executable: $CURRENT_BINARY"
export RUSTFS_UPGRADE_SOURCE_BINARY="$PREVIOUS_BINARY"

run_case \
  "mixed-version-upgrade" \
  "rolling_upgrade_from_rc2_preserves_mixed_version_contracts" \
  "G09-mixed_version_reader_evidence.json" \
  "G09-mixed_version_writer_evidence.json"

run_case \
  "bucket-config-rollback" \
  "rollback_to_previous_release_reads_current_bucket_metadata" \
  "G09-rollback_payload_evidence.json"

echo "==> Scanner/Heal G09 upgrade evidence complete"
echo "evidence_dir=$OUT_DIR"
