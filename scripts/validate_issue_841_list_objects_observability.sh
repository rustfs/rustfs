#!/usr/bin/env bash
set -euo pipefail

# Acceptance runner for rustfs/backlog#841.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue-841-acceptance-$(date +%Y%m%d-%H%M%S)}"

log_info() { printf '[INFO] %s\n' "$*"; }
log_error() { printf '[ERROR] %s\n' "$*" >&2; }

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_841_list_objects_observability.sh

Environment:
  OUT_DIR      output directory (default: target/issue-841-acceptance-<ts>)
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

run_unit_checks() {
  log_info "Running ListObjects observability unit checks"
  local test_log="$OUT_DIR/issue-841-tests.log"
  : > "$test_log"

  (
    cd "$PROJECT_ROOT"
    cargo test -p rustfs-io-metrics list_objects_metrics -- --nocapture
    cargo test -p rustfs-ecstore list_objects -- --nocapture
  ) 2>&1 | tee "$test_log"

  log_info "Unit test log: $test_log"
}

run_static_checks() {
  log_info "Running ListObjects observability static checks"
  local static_log="$OUT_DIR/static-checks.log"
  : > "$static_log"

  local required_patterns=(
    "rustfs_s3_list_objects_gather_total"
    "rustfs_s3_list_objects_gather_scan_amplification"
    "rustfs_s3_list_objects_merge_fan_in"
    "record_list_objects_gather"
    "record_list_objects_merge"
    "init_list_objects_metrics"
    "listobjects-v2-baseline-fixtures"
  )

  local pattern
  for pattern in "${required_patterns[@]}"; do
    if rg --no-ignore -n "$pattern" "$PROJECT_ROOT/crates" "$PROJECT_ROOT/rustfs" "$PROJECT_ROOT/docs" "$PROJECT_ROOT/scripts" >> "$static_log"; then
      log_info "Found required symbol: $pattern"
    else
      log_error "Missing required symbol: $pattern"
      log_error "Static check log: $static_log"
      return 1
    fi
  done

  log_info "Static check log: $static_log"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "unknown arg: $1"
        usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_args "$@"
  require_cmd cargo
  require_cmd rg
  require_cmd tee

  mkdir -p "$OUT_DIR"
  run_unit_checks
  run_static_checks

  log_info "Validation summary:"
  log_info "  - logs: $OUT_DIR"
  log_info "  - status: pass"
}

main "$@"
