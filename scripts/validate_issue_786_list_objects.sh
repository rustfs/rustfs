#!/usr/bin/env bash
set -euo pipefail

# Acceptance runner for rustfs/backlog#786 and PR #4072 follow-up
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue-786-acceptance-$(date +%Y%m%d-%H%M%S)}"
SKIP_LIVE="${SKIP_LIVE:-true}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"

log_info() { printf '[INFO] %s\n' "$*"; }
log_warn() { printf '[WARN] %s\n' "$*"; }
log_error() { printf '[ERROR] %s\n' "$*" >&2; }

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_786_list_objects.sh [--full] [--no-skip-live]

Environment:
  OUT_DIR      output directory (default: target/issue-786-acceptance-<ts>)
  SKIP_LIVE    skip live S3 endpoint checks when true (default: true)
  ENDPOINT     rustfs endpoint used by optional live checks (default: http://127.0.0.1:9000)
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

run_unit_checks() {
  log_info "Running focused rustfs-ecstore tests for issue-786 cursor fallback"
  mkdir -p "$OUT_DIR"

  local test_log="$OUT_DIR/issue-786-ecstore-tests.log"
  : > "$test_log"

  local tests=(
    list_path_marker_round_trip_preserves_set_index
    list_path_marker_parser_uses_trailing_cache_tag
    list_path_marker_parser_accepts_legacy_v1_tag
    list_path_marker_parser_return_tag_forces_cache_refresh
    list_path_marker_parser_recovers_from_corrupt_set_index
  )

  local test
  for test in "${tests[@]}"; do
    echo "[TEST] $test" | tee -a "$test_log"
    (cd "$PROJECT_ROOT" && cargo test -p rustfs-ecstore "$test" -- --nocapture | tee -a "$test_log")
  done

  log_info "Unit test log: $test_log"
}

run_static_checks() {
  log_info "Running issue-786 static marker checks"
  local static_log="$OUT_DIR/static-checks.log"

  : > "$static_log"

  if rg -n "struct ListContinuationV2|return:\\]|rustfs_cache:v2|MARKER_TAG_VERSION" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" | tee -a "$static_log"; then
    log_info "ListContinuationV2 symbols found in list_objects.rs"
  else
    log_error "Required cursor symbols not found in list_objects.rs"
    exit 1
  fi

  log_info "Static check log: $static_log"
}

run_live_smoke() {
  if [[ "$SKIP_LIVE" == "true" ]]; then
    log_warn "SKIP_LIVE=true, skipping live endpoint checks"
    return 0
  fi

  log_info "Running optional live readiness check: $ENDPOINT"
  if ! curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; then
    log_error "Health ready endpoint check failed: ${ENDPOINT}/health/ready"
    return 1
  fi
  log_info "Live endpoint reachable"

  log_warn "Optional live pagination verification remains manual for this follow-up."
  log_warn "Please run your preferred client workload (mc/warp/rclone) and verify token continuity manually."
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full|--no-skip-live)
        SKIP_LIVE="false"
        shift
        ;;
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
  require_cmd curl

  mkdir -p "$OUT_DIR"

  run_unit_checks
  run_static_checks

  if [[ "$SKIP_LIVE" != "true" ]]; then
    run_live_smoke
  fi

  log_info "Validation summary:"
  log_info "  - logs: $OUT_DIR"
  log_info "  - status: pass"
}

main "$@"
