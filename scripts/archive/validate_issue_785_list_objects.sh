#!/usr/bin/env bash
set -euo pipefail

# Acceptance runner for rustfs/backlog#785 and PR #4072
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue-785-acceptance-$(date +%Y%m%d-%H%M%S)}"
SKIP_LIVE="${SKIP_LIVE:-true}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"

log_info() { printf '[INFO] %s\n' "$*"; }
log_warn() { printf '[WARN] %s\n' "$*"; }
log_error() { printf '[ERROR] %s\n' "$*" >&2; }

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_785_list_objects.sh [--full] [--no-skip-live]

Environment:
  OUT_DIR      output directory (default: target/issue-785-acceptance-<ts>)
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
  log_info "Running focused rustfs-ecstore tests for list_objects hot path"
  mkdir -p "$OUT_DIR"

  local test_log="$OUT_DIR/issue-785-ecstore-tests.log"
  : > "$test_log"

  local tests=(
    list_path_gather_results_returns_after_limit_without_waiting_for_input_close
    list_path_gather_results_keeps_marker_entry_for_version_marker_listing
    list_path_gather_results_skips_marker_entry_by_default
    list_path_forward_past_is_idempotent_for_same_marker
    list_path_parse_marker_replay_still_stable
    normalize_list_quorum_falls_back_to_strict
    list_objects_quorum_from_env_defaults_to_optimal
  )

  local test
  for test in "${tests[@]}"; do
    echo "[TEST] $test" | tee -a "$test_log"
    (cd "$PROJECT_ROOT" && cargo test -p rustfs-ecstore "$test" -- --nocapture | tee -a "$test_log")
  done

  log_info "Unit test log: $test_log"
}

run_static_checks() {
  log_info "Running acceptance-focused static checks"
  local static_log="$OUT_DIR/static-checks.log"

  : > "$static_log"

  if rg -n "store_list_objects_list_path|store_list_objects_list_merged|sets_list_objects_list_path|sets_list_objects_list_merged|set_disks_list_objects_list_path|store_list_objects_gather" \
    "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" | tee -a "$static_log"; then
    log_info "Metric and stage names exist in list_objects.rs"
  else
    log_error "Required metric symbols not found in list_objects.rs"
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

  log_warn "Optional live S3 pagination/candidates check is intentionally not enforced in this runner."
  log_warn "Please run your preferred client workload (mc/warp/rclone) and verify page continuity + duplicates manually."
}

run_metrics_context() {
  log_info "Collecting local metric symbol index for review"
  local metric_log="$OUT_DIR/issue-785-metrics-context.log"
  if rg -n "record_stage_duration\(" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" > "$metric_log"; then
    log_info "Metric context saved: $metric_log"
  else
    log_error "No metric stage context found"
    return 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full)
        SKIP_LIVE="false"
        shift
        ;;
      --no-skip-live)
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

  :
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
  run_metrics_context

  if [[ "$SKIP_LIVE" != "true" ]]; then
    run_live_smoke
  fi

  log_info "Validation summary:"
  log_info "  - logs: $OUT_DIR"
  log_info "  - status: pass"
}

main "$@"
