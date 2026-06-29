#!/usr/bin/env bash
set -euo pipefail

# Validation runner for rustfs/backlog#787: list quorum tuning and index-readiness baseline
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue-787-list-quorum-$(date +%Y%m%d-%H%M%S)}"
SKIP_LIVE="${SKIP_LIVE:-true}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
LIVE_LIST_BUCKET="${LIVE_LIST_BUCKET:-}"
LIVE_LIST_PREFIX="${LIVE_LIST_PREFIX:-}"
LIVE_LIST_MAX_KEYS="${LIVE_LIST_MAX_KEYS:-1000}"
LIVE_LIST_REGION="${LIVE_LIST_REGION:-us-east-1}"
MODE_LIST="${MODE_LIST:-strict,optimal,reduced}"
SERVER_RESTART_CMD="${SERVER_RESTART_CMD:-}"
SERVER_WAIT_SECONDS="${SERVER_WAIT_SECONDS:-20}"

log_info() { printf '[INFO] %s\n' "$*"; }
log_warn() { printf '[WARN] %s\n' "$*"; }
log_error() { printf '[ERROR] %s\n' "$*" >&2; }

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_787_list_quorum.sh [--full] [--no-skip-live]

Environment:
  OUT_DIR             output directory (default: target/issue-787-list-quorum-<ts>)
  SKIP_LIVE           skip live S3 endpoint checks when true (default: true)
  ENDPOINT            rustfs endpoint for live checks (default: http://127.0.0.1:9000)
  LIVE_LIST_BUCKET    bucket used for live pagination smoke (required when running live)
  LIVE_LIST_PREFIX    optional prefix for list smoke (default: empty)
  LIVE_LIST_MAX_KEYS  max-keys for live smoke page samples (default: 1000)
  LIVE_LIST_REGION    signing region for curl (default: us-east-1)
  MODE_LIST           comma-separated list of list quorum modes (default: strict,optimal,reduced)
  SERVER_RESTART_CMD   optional command to restart server with RUSTFS_TUNING_ENV_FILE
  SERVER_WAIT_SECONDS  seconds to wait after restart command before next checks (default: 20)

Run modes:
  --full or --no-skip-live   run live checks
  --help                     this help
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

timestamp_ms() {
  python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

extract_xml_keys() {
  local xml_text="$1"
  printf '%s\n' "$xml_text" | sed -n 's:.*<Key>\(.*\)</Key>.*:\1:p'
}

wait_for_health() {
  local waited=0
  while (( waited < SERVER_WAIT_SECONDS )); do
    if curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done

  return 1
}

run_unit_checks() {
  log_info "Running focused rustfs-ecstore tests for #787 list quorum tuning"
  mkdir -p "$OUT_DIR"

  local test_log="$OUT_DIR/issue-787-ecstore-tests.log"
  : > "$test_log"

  local tests=(
    list_path_gather_results_returns_after_limit_without_waiting_for_input_close
    list_path_marker_parser_uses_trailing_cache_tag
    normalize_list_quorum_accepts_supported_values
    normalize_list_quorum_falls_back_to_strict
    list_quorum_from_env_defaults_to_strict
    list_quorum_from_env_honors_supported_value
    list_objects_quorum_from_env_defaults_to_optimal
    list_objects_quorum_from_env_honors_supported_value
  )

  local test
  for test in "${tests[@]}"; do
    echo "[TEST] $test" | tee -a "$test_log"
    (cd "$PROJECT_ROOT" && cargo test -p rustfs-ecstore "$test" -- --nocapture | tee -a "$test_log")
  done

  log_info "Unit test log: $test_log"
}

run_static_checks() {
  log_info "Running #787 static checks for list-quorum and list path hot path"
  local static_log="$OUT_DIR/static-checks.log"
  : > "$static_log"

  if rg -n "ENV_API_LIST_OBJECTS_QUORUM|list_objects_quorum_from_env\(|ask_disks: list_objects_quorum_from_env\(\)" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" | tee -a "$static_log"; then
    log_info "Required list quorum symbols found in list_objects.rs"
  else
    log_error "list quorum symbols missing in list_objects.rs"
    exit 1
  fi

  if rg -n "store_list_objects_list_merged|sets_list_objects_list_merged|set_disks_list_objects_list_path|store list_merged finished|sets list_merged finished|set_disks list_path selected listing quorum" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" | tee -a "$static_log"; then
    log_info "List path merged/path logging symbols found"
  else
    log_error "List path merged symbols missing, static guard failed"
    exit 1
  fi

  if rg -n "index-backed|list index|list index path|list_index" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" >/dev/null 2>&1; then
    log_info "Index-backed scaffold symbols detected in list_objects.rs"
  else
    log_warn "Index-backed symbols not yet present in list_objects.rs (expected for current phase)"
  fi

  log_info "Static check log: $static_log"
}

run_metrics_context() {
  log_info "Collecting local metric symbol index for list-quorum review"
  local metric_log="$OUT_DIR/list-quorum-metrics-context.log"

  if rg -n "record_stage_duration\(\"(store|sets|set_disks)_list_objects|store_list_objects_gather|set_disks_list_objects_list_path\"" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" > "$metric_log"; then
    log_info "Metric context saved: $metric_log"
  else
    log_error "No metric symbols found in list_objects.rs"
    return 1
  fi
}

run_live_listing_two_page_smoke() {
  local mode="$1"
  local log_file="$OUT_DIR/live-${mode}.log"
  : > "$log_file"

  if ! command -v python3 >/dev/null 2>&1; then
    log_warn "python3 missing, skip live smoke"
    echo "live pagination smoke: skipped (missing python3)" | tee -a "$log_file"
    return 0
  fi

  local access_key="${RUSTFS_ACCESS_KEY:-${WARP_ACCESS_KEY:-}}"
  local secret_key="${RUSTFS_SECRET_KEY:-${WARP_SECRET_KEY:-}}"
  if [[ -z "$access_key" || -z "$secret_key" ]]; then
    log_warn "Missing credentials, skip live smoke"
    echo "live pagination smoke: skipped (missing credentials)" | tee -a "$log_file"
    return 0
  fi

  if [[ -z "$LIVE_LIST_BUCKET" ]]; then
    log_warn "LIVE_LIST_BUCKET is empty, skip live smoke"
    echo "live pagination smoke: skipped (bucket not configured)" | tee -a "$log_file"
    return 0
  fi

  if ! curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; then
    log_error "health check failed: ${ENDPOINT}/health/ready"
    echo "live pagination smoke: failed (health check)" | tee -a "$log_file"
    return 1
  fi

  local base_path="$LIVE_LIST_BUCKET"
  if [[ -n "$LIVE_LIST_PREFIX" ]]; then
    base_path="${LIVE_LIST_BUCKET}/${LIVE_LIST_PREFIX}"
  fi

  local base_url="${ENDPOINT}/${base_path}?list-type=2&max-keys=${LIVE_LIST_MAX_KEYS}"
  local sig_target="aws:amz:${LIVE_LIST_REGION}:s3"

  local start_ms page1 page2 token1 page1_key_count page2_key_count
  start_ms="$(timestamp_ms)"
  page1=$(curl -fsS \
    --user "${access_key}:${secret_key}" \
    --aws-sigv4 "$sig_target" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "$base_url")
  page1_duration_ms="$(( $(timestamp_ms) - start_ms ))"

  page1_key_count=$(extract_xml_keys "$page1" | grep -cv '^$' || true)
  token1=$(printf '%s' "$page1" | sed -n 's:.*<NextContinuationToken>\(.*\)</NextContinuationToken>.*:\1:p' | head -n 1)
  local -a page1_keys
  mapfile -t page1_keys < <(extract_xml_keys "$page1")

  log_info "Mode=${mode} first page keys=${page1_key_count} duration=${page1_duration_ms}ms" | tee -a "$log_file"

  if [[ -z "$token1" ]]; then
    log_info "Mode=${mode} live pagination smoke: single page only" | tee -a "$log_file"
    echo "live pagination smoke: pass (single page, key_count=${page1_key_count}, duration_ms=${page1_duration_ms})" | tee -a "$log_file"
    return 0
  fi

  local encoded_token
  encoded_token=$(printf '%s' "$token1" | python3 - <<'PY'
import urllib.parse,sys
print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))
PY)

  start_ms="$(timestamp_ms)"
  page2=$(curl -fsS \
    --user "${access_key}:${secret_key}" \
    --aws-sigv4 "$sig_target" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "${base_url}&continuation-token=${encoded_token}")
  page2_duration_ms="$(( $(timestamp_ms) - start_ms ))"

  page2_key_count=$(extract_xml_keys "$page2" | grep -cv '^$' || true)

  if (( page2_key_count == 0 )); then
    log_error "Mode=${mode} live smoke: page2 empty"
    return 1
  fi

  local duplicate=0
  local dkeys=()
  local -a page2_keys
  mapfile -t page2_keys < <(extract_xml_keys "$page2")

  for key in "${page2_keys[@]}"; do
    if printf '%s\n' "${page1_keys[@]}" | grep -Fx -- "$key" >/dev/null 2>&1; then
      duplicate=1
      dkeys+=("$key")
    fi
  done

  if (( duplicate )); then
    log_error "Mode=${mode} live pagination duplicates detected: ${dkeys[*]}"
    echo "live pagination smoke: fail (duplicate keys)" | tee -a "$log_file"
    return 1
  fi

  echo "Mode=${mode}" | tee -a "$log_file"
  echo "first-page-count=${page1_key_count}" | tee -a "$log_file"
  echo "second-page-count=${page2_key_count}" | tee -a "$log_file"
  echo "page1-duration-ms=${page1_duration_ms}" | tee -a "$log_file"
  echo "page2-duration-ms=${page2_duration_ms}" | tee -a "$log_file"
  echo "live pagination smoke: pass (two-page continuity)" | tee -a "$log_file"

  return 0
}

run_mode_series() {
  if [[ -z "${MODE_LIST}" ]]; then
    log_warn "MODE_LIST is empty; skip mode series"
    return 0
  fi

  local mode
  local mode_count=0
  IFS=',' read -r -a modes <<< "$MODE_LIST"

  for mode in "${modes[@]}"; do
    [[ -z "$mode" ]] && continue
    log_info "Live check for list quorum mode=${mode}"
    mode=$(printf '%s' "$mode" | tr -d '[:space:]')

    if [[ -n "$SERVER_RESTART_CMD" ]]; then
      local mode_cmd=""
      local mode_env_file=""

      if [[ "$mode" == "default" ]]; then
        mode_cmd="$SERVER_RESTART_CMD"
      else
        mode_env_file="$OUT_DIR/.issue787-${mode}.env"
        printf 'RUSTFS_LIST_OBJECTS_QUORUM=%s\n' "$mode" > "$mode_env_file"
        mode_cmd="RUSTFS_TUNING_ENV_FILE=${mode_env_file} $SERVER_RESTART_CMD"
      fi

      log_info "Restarting server for mode=${mode}"
      (cd "$PROJECT_ROOT" && eval "$mode_cmd")

      if ! wait_for_health; then
        log_error "health check timeout during mode=${mode} restart"
        return 1
      fi
    elif (( mode_count > 0 )); then
      log_warn "SERVER_RESTART_CMD unset, mode=${mode} does not auto-switch. Skip remaining modes."
      break
    fi

    mode_count=$(( mode_count + 1 ))

    run_live_listing_two_page_smoke "$mode"
  done
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
  run_metrics_context

  if [[ "$SKIP_LIVE" != "true" ]]; then
    require_cmd python3
    run_mode_series
  fi

  log_info "Validation summary:"
  log_info "  - logs: $OUT_DIR"
  log_info "  - status: pass"
}

main "$@"
