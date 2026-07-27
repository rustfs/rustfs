#!/usr/bin/env bash
#
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

ENDPOINT=""
JOB_ID=""
ADMIN_TOKEN=""
LOG_GLOB="/var/log/rustfs/*.log"
METRIC_TYPES="1"
METRIC_SAMPLES="1"
METRIC_INTERVAL=""
BUCKET_FILTER=""
SHOW_HELP=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_debug.sh --endpoint <admin-api-base> --job-id <job-id> [options]

Required:
  --endpoint          Admin endpoint base, e.g. http://127.0.0.1:9000
  --job-id            Manual transition job_id (UUID)

Optional:
  --admin-token       Bearer token for /admin/v3 endpoints
  --log-glob          Log glob to scan, defaults to /var/log/rustfs/*.log
  --metric-types      Metrics types bitmask, defaults to 1 (SCANNER)
  --metric-samples    Metrics samples, defaults to 1
  --metric-interval   Metrics interval parameter, passed as-is to query string
  --bucket            Optional bucket filter for focused log output
  -h, --help          Show this help

Output sections:
  - lifecycle_worker_state structured events (job-scoped)
  - manual-transition admin job status
  - lifecycle_tier_operation_failed and admin_ilm_transition_state traces
  - metrics snapshot (aggregated.scanner.lifecycle_transition)
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

api_get() {
  local path="$1"
  local url="${ENDPOINT%/}${path}"

  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl -sS -H "Authorization: Bearer ${ADMIN_TOKEN}" "$url"
    return
  fi

  curl -sS "$url"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint)
        ENDPOINT="${2:-}"
        shift 2
        ;;
      --job-id)
        JOB_ID="${2:-}"
        shift 2
        ;;
      --admin-token)
        ADMIN_TOKEN="${2:-}"
        shift 2
        ;;
      --log-glob)
        LOG_GLOB="${2:-}"
        shift 2
        ;;
      --metric-types)
        METRIC_TYPES="${2:-}"
        shift 2
        ;;
      --metric-samples)
        METRIC_SAMPLES="${2:-}"
        shift 2
        ;;
      --metric-interval)
        METRIC_INTERVAL="${2:-}"
        shift 2
        ;;
      --bucket)
        BUCKET_FILTER="${2:-}"
        shift 2
        ;;
      -h|--help)
        SHOW_HELP=true
        shift
        ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

validate() {
  if [[ "$SHOW_HELP" == true ]]; then
    usage
    exit 0
  fi

  if [[ -z "$ENDPOINT" || -z "$JOB_ID" ]]; then
    echo "ERROR: --endpoint and --job-id are required" >&2
    usage
    exit 1
  fi

  if [[ -z "$LOG_GLOB" ]]; then
    echo "ERROR: --log-glob must not be empty" >&2
    exit 1
  fi
}

build_admin_metrics_path() {
  local path="/rustfs/admin/v3/metrics?types=${METRIC_TYPES}&n=${METRIC_SAMPLES}"
  if [[ -n "$METRIC_INTERVAL" ]]; then
    path="${path}&interval=${METRIC_INTERVAL}"
  fi
  printf '%s' "$path"
}

run_json_stream() {
  local query="$1"
  local filter="$2"
  local log_lines; log_lines="$(mktemp)"
  local log_errors; log_errors="$(mktemp)"
  local log_paths=()
  local match

  while IFS= read -r match; do
    [[ -n "$match" ]] && log_paths+=("$match")
  done < <(compgen -G "$LOG_GLOB" || true)
  if [[ "${#log_paths[@]}" -eq 0 ]]; then
    log_paths=("$LOG_GLOB")
  fi

  if ! rg -n --color=never -- "$query" -- "${log_paths[@]}" >"$log_lines" 2>"$log_errors"; then
    :
  fi

  if [[ -s "$log_errors" ]]; then
    sed 's/^/DEBUG: /' "$log_errors"
  fi

  jq -R "$filter" <"$log_lines"

  rm -f "$log_lines" "$log_errors"
}

run_worker_logs() {
  local base_filter='fromjson? | select(.event == "lifecycle_worker_state")'
  local manual_filter='fromjson? | select(.event == "lifecycle_worker_state" and .job_id == env.JOB_ID and (.state | startswith("manual_transition_")))'
  local dist_filter='fromjson? | select(.event == "lifecycle_worker_state" and (.state | startswith("manual_transition_")) ) | .state'

  if [[ -n "$BUCKET_FILTER" ]]; then
    manual_filter='fromjson? | select(.event == "lifecycle_worker_state" and .job_id == env.JOB_ID and (.state | startswith("manual_transition_")) and .bucket == env.BUCKET_FILTER)'
    dist_filter='fromjson? | select(.event == "lifecycle_worker_state" and (.state | startswith("manual_transition_")) and .bucket == env.BUCKET_FILTER) | .state'
  fi

  echo "## lifecycle_worker_state (all)"
  run_json_stream "lifecycle_worker_state" "$base_filter"

  echo "## lifecycle_worker_state (manual_transition + job filter: ${JOB_ID})"
  JOB_ID="$JOB_ID" BUCKET_FILTER="$BUCKET_FILTER" \
    run_json_stream "lifecycle_worker_state" "$manual_filter" || true

  echo "## lifecycle_worker_state state distribution (manual_transition only)"
  JOB_ID="$JOB_ID" BUCKET_FILTER="$BUCKET_FILTER" \
    run_json_stream "lifecycle_worker_state" "$dist_filter" | jq -r . | sort | uniq -c | sort -nr || true
}

run_admin_job() {
  echo "## manual transition job status"
  api_get "/rustfs/admin/v3/ilm/transition/jobs/${JOB_ID}" \
    | jq '{status, mode, job_id, failure_reason, report: .report, queue_snapshot: .queue_snapshot}'
}

run_tier_operation_failures() {
  echo "## lifecycle_tier_operation_failed events"
  local filter='fromjson? | select(.event == "lifecycle_tier_operation_failed") | {timestamp, bucket: .bucket, object: .object, version_id: .version_id, tier: .tier, operation: .operation, error: .error}'
  if [[ -n "$BUCKET_FILTER" ]]; then
    filter='fromjson? | select(.event == "lifecycle_tier_operation_failed" and .bucket == env.BUCKET_FILTER) | {timestamp, bucket: .bucket, object: .object, version_id: .version_id, tier: .tier, operation: .operation, error: .error}'
  fi

  JOB_ID="$JOB_ID" BUCKET_FILTER="$BUCKET_FILTER" \
    run_json_stream "lifecycle_tier_operation_failed" "$filter" || true
}

run_admin_events() {
  echo "## admin_ilm_transition_state (job filter)"
  local filter='fromjson? | select(.event == "admin_ilm_transition_state" and .job_id == env.JOB_ID) | {timestamp, operation: .operation, state: .state, result: .result, failure_reason: .failure_reason, bucket: .bucket, prefix: .prefix}'
  if [[ -n "$BUCKET_FILTER" ]]; then
    filter='fromjson? | select(.event == "admin_ilm_transition_state" and .job_id == env.JOB_ID and .bucket == env.BUCKET_FILTER) | {timestamp, operation: .operation, state: .state, result: .result, failure_reason: .failure_reason, bucket: .bucket, prefix: .prefix}'
  fi

  JOB_ID="$JOB_ID" BUCKET_FILTER="$BUCKET_FILTER" \
    run_json_stream "admin_ilm_transition_state" "$filter" || true
}

run_metrics() {
  echo "## metrics: scanner.lifecycle_transition"
  local path
  path="$(build_admin_metrics_path)"
  api_get "$path" | jq '.aggregated.scanner.lifecycle_transition'

  echo "## metrics: scanner lifecycle cycle"
  api_get "$path" | jq '.aggregated.scanner | {lifecycle_transition: .lifecycle_transition, current_cycle_ilm_actions: .current_cycle_ilm_actions, current_cycle_lifecycle_transition_actions: .current_cycle_lifecycle_transition_actions}'
}

main() {
  parse_args "$@"
  validate

  require_cmd curl
  require_cmd jq
  require_cmd rg

  run_worker_logs
  run_admin_events
  run_tier_operation_failures
  run_admin_job
  run_metrics
}

main "$@"
