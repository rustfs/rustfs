#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
UPGRADE_WORKFLOW="${PROJECT_ROOT}/.github/workflows/e2e-upgrade.yml"

SOURCE_DIR=""
OUT_DIR=""
SKIP_BUILD="false"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_scanner_heal_g09_upgrade_evidence.sh [--source-dir <dir>] [--out-dir <dir>] [--skip-build]

Runs the Scanner/Heal G09 upgrade evidence lanes against the pinned previous
Linux x86_64 RustFS release used by the e2e-upgrade workflow:

  - rolling mixed-version reader/writer evidence
  - rollback payload replay evidence

The script builds the current PR head by default, downloads and verifies the
pinned previous release binary, runs the ignored e2e tests, and fails unless all
G09 JSON evidence artifacts are present and non-empty.
USAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --source-dir)
        [[ $# -ge 2 ]] || die "--source-dir requires a value"
        SOURCE_DIR="$2"
        shift 2
        ;;
      --out-dir)
        [[ $# -ge 2 ]] || die "--out-dir requires a value"
        OUT_DIR="$2"
        shift 2
        ;;
      --skip-build)
        SKIP_BUILD="true"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown arg: $1"
        ;;
    esac
  done
}

workflow_env_value() {
  local key="$1"
  awk -v key="${key}:" '$1 == key { gsub(/["'\'']/, "", $2); print $2; exit }' "$UPGRADE_WORKFLOW"
}

require_linux_x86_64() {
  local kernel machine
  kernel="$(uname -s)"
  machine="$(uname -m)"
  [[ "$kernel" == "Linux" ]] || die "G09 upgrade evidence uses the pinned Linux release binary; run this on Linux"
  [[ "$machine" == "x86_64" || "$machine" == "amd64" ]] || die "G09 upgrade evidence requires x86_64/amd64"
}

source_binary_path() {
  printf '%s/rustfs\n' "$SOURCE_DIR"
}

ensure_previous_release_binary() {
  local version asset sha archive url binary
  version="${UPGRADE_SOURCE_VERSION:-$(workflow_env_value UPGRADE_SOURCE_VERSION)}"
  asset="${UPGRADE_SOURCE_ASSET:-$(workflow_env_value UPGRADE_SOURCE_ASSET)}"
  sha="${UPGRADE_SOURCE_SHA256:-$(workflow_env_value UPGRADE_SOURCE_SHA256)}"
  [[ -n "$version" ]] || die "UPGRADE_SOURCE_VERSION is missing"
  [[ -n "$asset" ]] || die "UPGRADE_SOURCE_ASSET is missing"
  [[ -n "$sha" ]] || die "UPGRADE_SOURCE_SHA256 is missing"

  SOURCE_DIR="${SOURCE_DIR:-${PROJECT_ROOT}/target/scanner-heal-g09-source/${version}}"
  mkdir -p "$SOURCE_DIR"

  binary="$(source_binary_path)"
  if [[ -x "$binary" ]]; then
    return
  fi

  archive="${SOURCE_DIR}/${asset}"
  url="https://github.com/rustfs/rustfs/releases/download/${version}/${asset}"
  curl --fail --location --retry 3 --output "$archive" "$url"
  echo "${sha}  ${archive}" | sha256sum --check --strict
  unzip -q -o "$archive" -d "$SOURCE_DIR"
  chmod +x "$binary"
  [[ -x "$binary" ]] || die "downloaded archive did not provide an executable rustfs binary"
}

build_current_rustfs() {
  if [[ "$SKIP_BUILD" == "true" ]]; then
    return
  fi
  "${CARGO:-cargo}" build --locked -p rustfs --bin rustfs
  : > "${PROJECT_ROOT}/target/debug/rustfs.features"
}

prepare_output_dir() {
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/scanner-heal-g09-evidence/${stamp}}"
  mkdir -p "$OUT_DIR"
}

run_case() {
  local artifact="$1"
  local test_name="$2"
  shift 2
  local case_dir="${OUT_DIR}/${artifact}"
  mkdir -p "$case_dir"
  RUSTFS_UPGRADE_SOURCE_BINARY="$(source_binary_path)" \
    RUSTFS_E2E_LOG_DIR="${OUT_DIR}/server-logs/${artifact}" \
    RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR="$case_dir" \
    "${CARGO:-cargo}" test --locked -p e2e_test \
      "upgrade_compatibility_test::${test_name}" \
      -- --ignored --exact --nocapture

  local expected
  for expected in "$@"; do
    [[ -s "${case_dir}/${expected}" ]] || die "missing non-empty ${artifact}/${expected}"
  done
}

main() {
  parse_args "$@"
  [[ -f "$UPGRADE_WORKFLOW" ]] || die "missing e2e-upgrade workflow"
  require_linux_x86_64
  cd "$PROJECT_ROOT"
  ensure_previous_release_binary
  build_current_rustfs
  prepare_output_dir
  run_case \
    "mixed-version-upgrade" \
    "rolling_upgrade_from_rc2_preserves_mixed_version_contracts" \
    "G09-mixed_version_reader_evidence.json" \
    "G09-mixed_version_writer_evidence.json"
  run_case \
    "bucket-config-rollback" \
    "rollback_to_previous_release_reads_current_bucket_metadata" \
    "G09-rollback_payload_evidence.json"
  echo "PASS: Scanner/Heal G09 upgrade evidence written under ${OUT_DIR}"
}

main "$@"
