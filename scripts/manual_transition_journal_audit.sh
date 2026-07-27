#!/usr/bin/env bash
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENDPOINT=""
JOB_ID=""
ADMIN_TOKEN=""
LOG_GLOB="/var/log/rustfs/*.log"
METRIC_TYPES="1"
METRIC_SAMPLES="1"
METRIC_INTERVAL=""
TASK_BUCKET_FILTER=""
OUT_DIR=""

SYS_BUCKET="${RUSTFS_META_BUCKET:-.rustfs.sys}"
TASK_PREFIX="ilm/manual-transition/tasks"
RESULT_PREFIX="ilm/manual-transition/results"

MC_BIN="${MC_BIN:-mc}"
MC_ALIAS=""
MC_ENDPOINT=""
MC_ACCESS_KEY=""
MC_SECRET_KEY=""
MC_ALIAS_CREATED=false

SHOW_HELP=false
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_journal_audit.sh --endpoint <admin-api> --job-id <job-uuid> [options]

Required:
  --endpoint         Admin endpoint base, e.g. https://127.0.0.1:9000
  --job-id           Manual transition job_id

Optional:
  --admin-token      Bearer token for /admin/v3 endpoints
  --log-glob         Log glob, default /var/log/rustfs/*.log
  --metric-types     /admin/v3/metrics type flags, default 1
  --metric-samples   /admin/v3/metrics sample count, default 1
  --metric-interval  /admin/v3/metrics interval query param
  --bucket-filter    Optional bucket filter for logs
  --sys-bucket       Meta bucket for journal read (default .rustfs.sys)
  --mc-alias         Reuse existing mc alias (skip alias set)
  --mc-endpoint      mc endpoint for .rustfs.sys access
  --mc-access-key    mc access key
  --mc-secret-key    mc secret key
  --out-dir          Artifact output dir
  --dry-run
  --help             Show usage

Output:
  - Journal CSV/JSON samples for manual transition tasks and worker results
  - Reconcile report (task_count/result_count/missing/mismatch)
  - /admin/v3 lifecycle_worker_state / admin_ilm_transition_state events
  - lifecycle_tier_operation_failed logs
  - aggregated lifecycle transition metrics (admin endpoint sample)
USAGE
}

require_bash4() {
  if (( BASH_VERSINFO[0] < 4 )); then
    echo "ERROR: manual transition journal audit requires bash >= 4.0" >&2
    exit 1
  fi
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "ERROR: missing value for $flag" >&2
    exit 1
  fi
  printf '%s' "$value"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: command not found: $cmd" >&2
    exit 1
  fi
}

require_nonempty() {
  local value="$1"
  local label="$2"
  if [[ -z "$value" ]]; then
    echo "ERROR: $label is required" >&2
    exit 1
  fi
}

normalize_uuid() {
  local raw="${1//-/}"
  printf '%s' "${raw,,}"
}

validate_uuid() {
  local normalized
  normalized="$(normalize_uuid "$1")"
  if ! [[ "$normalized" =~ ^[0-9a-f]{32}$ ]]; then
    echo "ERROR: invalid uuid (expect UUIDv4): $1" >&2
    exit 1
  fi
  printf '%s' "$normalized"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --job-id) JOB_ID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --admin-token) ADMIN_TOKEN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --log-glob) LOG_GLOB="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-types) METRIC_TYPES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-samples) METRIC_SAMPLES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-interval) METRIC_INTERVAL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket-filter) TASK_BUCKET_FILTER="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sys-bucket) SYS_BUCKET="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-alias) MC_ALIAS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-endpoint) MC_ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-access-key) MC_ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-secret-key) MC_SECRET_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) SHOW_HELP=true; shift ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

validate_args() {
  if [[ "$SHOW_HELP" == true ]]; then
    usage
    exit 0
  fi

  require_nonempty "$ENDPOINT" "--endpoint"
  require_nonempty "$JOB_ID" "--job-id"

  JOB_ID="$(validate_uuid "$JOB_ID")"
  if [[ "$LOG_GLOB" == "" ]]; then
    echo "ERROR: --log-glob must not be empty" >&2
    exit 1
  fi
  if [[ -z "$MC_ALIAS" ]]; then
    if [[ -z "$MC_ENDPOINT" || -z "$MC_ACCESS_KEY" || -z "$MC_SECRET_KEY" ]]; then
      echo "ERROR: either --mc-alias or (--mc-endpoint + --mc-access-key + --mc-secret-key) is required" >&2
      exit 1
    fi
  fi
}

cleanup_mc_alias() {
  if [[ "$MC_ALIAS_CREATED" == true ]]; then
    "${MC_BIN}" alias remove "$MC_ALIAS" >/dev/null 2>&1 || true
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-journal-audit/$(date +%Y%m%dT%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  SUMMARY_TXT="${OUT_DIR}/audit_summary.txt"
  TASKS_CSV="${OUT_DIR}/tasks.csv"
  RESULTS_CSV="${OUT_DIR}/results.csv"
  RECONCILE_CSV="${OUT_DIR}/journal_reconcile.csv"
  COMMANDS_TXT="${OUT_DIR}/commands.txt"
  {
    echo "task_key,bucket,object,version_id,storage_class,queued_at_unix_nanos,task_object"
  } > "$TASKS_CSV"
  {
    echo "task_key,result,completion_reason,completed_at_unix_nanos,result_object"
  } > "$RESULTS_CSV"
  {
    echo "task_key,bucket,object,version_id,storage_class,task_has_result,result,result_reason,task_job_id_match,result_job_id_match,task_path,result_path"
  } > "$RECONCILE_CSV"
}

setup_mc_alias() {
  if [[ "$MC_ALIAS" == "" ]]; then
    MC_ALIAS="mt-journal-audit-${JOB_ID}"
    MC_ALIAS_CREATED=true
    if [[ "$DRY_RUN" == true ]]; then
      echo "[DRY-RUN] mc alias set ${MC_ALIAS} ${MC_ENDPOINT} ${MC_ACCESS_KEY} ***"
    else
      "${MC_BIN}" alias set "${MC_ALIAS}" "${MC_ENDPOINT}" "${MC_ACCESS_KEY}" "${MC_SECRET_KEY}" >/dev/null
    fi
  fi
  if ! [[ "$DRY_RUN" == true ]]; then
    "${MC_BIN}" ls "${MC_ALIAS}/${SYS_BUCKET}" >/dev/null
  fi
  trap cleanup_mc_alias EXIT
}

api_get() {
  local path="$1"
  local url="${ENDPOINT%/}${path}"
  if [[ -n "$ADMIN_TOKEN" ]]; then
    curl -sS -H "Authorization: Bearer ${ADMIN_TOKEN}" "$url"
  else
    curl -sS "$url"
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

  if [[ -s "$log_lines" ]]; then
    jq -R "$filter" <"$log_lines"
  else
    echo "[]"
  fi

  rm -f "$log_lines" "$log_errors"
}

run_worker_logs() {
  local base_filter='fromjson? | select(.event == "lifecycle_worker_state")'
  local manual_filter='fromjson? | select(.event == "lifecycle_worker_state" and .job_id == env.JOB_ID and (.state | startswith("manual_transition_")))'
  local dist_filter='fromjson? | select(.event == "lifecycle_worker_state" and (.state | startswith("manual_transition_")) ) | .state'

  if [[ -n "$TASK_BUCKET_FILTER" ]]; then
    manual_filter='fromjson? | select(.event == "lifecycle_worker_state" and .job_id == env.JOB_ID and (.state | startswith("manual_transition_")) and .bucket == env.TASK_BUCKET_FILTER)'
    dist_filter='fromjson? | select(.event == "lifecycle_worker_state" and (.state | startswith("manual_transition_")) and .bucket == env.TASK_BUCKET_FILTER) | .state'
  fi

  echo "## lifecycle_worker_state all"
  run_json_stream "lifecycle_worker_state" "$base_filter" || true
  echo
  echo "## lifecycle_worker_state manual_transition + job filter"
  JOB_ID="$JOB_ID" TASK_BUCKET_FILTER="$TASK_BUCKET_FILTER" run_json_stream "lifecycle_worker_state" "$manual_filter" || true
  echo
  echo "## lifecycle_worker_state state distribution"
  JOB_ID="$JOB_ID" TASK_BUCKET_FILTER="$TASK_BUCKET_FILTER" run_json_stream "lifecycle_worker_state" "$dist_filter" | jq -r . | sort | uniq -c | sort -nr || true
}

run_admin_job() {
  echo "## manual transition job status"
  api_get "/rustfs/admin/v3/ilm/transition/jobs/${JOB_ID}" \
    | jq '{status, mode, job_id, report: .report, queue_snapshot: .queue_snapshot, cancel_requested}'
}

run_admin_events() {
  echo "## admin_ilm_transition_state events"
  local filter='fromjson? | select(.event == "admin_ilm_transition_state" and .job_id == env.JOB_ID) | {timestamp, operation: .operation, state: .state, result: .result, failure_reason: .failure_reason, bucket: .bucket, prefix: .prefix}'
  if [[ -n "$TASK_BUCKET_FILTER" ]]; then
    filter='fromjson? | select(.event == "admin_ilm_transition_state" and .job_id == env.JOB_ID and .bucket == env.TASK_BUCKET_FILTER) | {timestamp, operation: .operation, state: .state, result: .result, failure_reason: .failure_reason, bucket: .bucket, prefix: .prefix}'
  fi
  JOB_ID="$JOB_ID" TASK_BUCKET_FILTER="$TASK_BUCKET_FILTER" \
    run_json_stream "admin_ilm_transition_state" "$filter" || true
}

run_tier_operation_failures() {
  echo "## lifecycle_tier_operation_failed events"
  local filter='fromjson? | select(.event == "lifecycle_tier_operation_failed") | {timestamp, bucket: .bucket, object: .object, version_id: .version_id, tier: .tier, operation: .operation, error: .error}'
  if [[ -n "$TASK_BUCKET_FILTER" ]]; then
    filter='fromjson? | select(.event == "lifecycle_tier_operation_failed" and .bucket == env.TASK_BUCKET_FILTER) | {timestamp, bucket: .bucket, object: .object, version_id: .version_id, tier: .tier, operation: .operation, error: .error}'
  fi
  JOB_ID="$JOB_ID" TASK_BUCKET_FILTER="$TASK_BUCKET_FILTER" \
    run_json_stream "lifecycle_tier_operation_failed" "$filter" || true
}

run_metrics() {
  echo "## metrics: scanner.lifecycle_transition"
  local path
  path="$(build_admin_metrics_path)"
  api_get "$path" | jq '.aggregated.scanner.lifecycle_transition'
}

journal_key_prefix() {
  local prefix="$1"
  local shard_a="${JOB_ID:0:2}"
  local shard_b="${JOB_ID:2:2}"
  printf '%s/%s/%s/%s' "$prefix" "$shard_a" "$shard_b" "$JOB_ID"
}

csv_escape() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '\"%s\"' "$value"
}

read_json_file() {
  local object_key="$1"
  if [[ "$DRY_RUN" == true ]]; then
    echo ""
    return 0
  fi
  "${MC_BIN}" cat "${MC_ALIAS}/${SYS_BUCKET}/${object_key}"
}

collect_journals() {
  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] skip .rustfs.sys object reads."
    echo "dry_run=true" > "$SUMMARY_TXT"
    return 0
  fi

  local task_prefix result_prefix
  task_prefix="$(journal_key_prefix "$TASK_PREFIX")"
  result_prefix="$(journal_key_prefix "$RESULT_PREFIX")"

  local task_objs
  local result_objs
  local object_name
  local task_key
  local raw
  local record_task_job record_result_job
  local expected_job
  expected_job="$JOB_ID"

  local -A task_bucket task_object task_version task_storage task_queued task_job task_path
  local -A result_status result_reason result_completed result_job result_path
  local -A task_seen
  local -A result_seen

  task_objs="$("${MC_BIN}" --json ls --recursive "${MC_ALIAS}/${SYS_BUCKET}/${task_prefix}" | jq -r 'select(.type == "file") | .key')"
  result_objs="$("${MC_BIN}" --json ls --recursive "${MC_ALIAS}/${SYS_BUCKET}/${result_prefix}" | jq -r 'select(.type == "file") | .key')"

  while IFS= read -r object_name; do
    [[ -z "$object_name" ]] && continue
    task_key="${object_name##*/}"
    task_key="${task_key%.json}"
    if ! [[ "$task_key" =~ ^[0-9a-f]{64}$ ]]; then
      echo "WARN: ignore invalid task object name: ${object_name}" >&2
      continue
    fi
    raw="$(read_json_file "$object_name")"
    if ! record_task_job="$(printf '%s' "$raw" | jq -r '.record.job_id // empty' 2>/dev/null)"; then
      echo "WARN: parse error task object: ${object_name}" >&2
      continue
    fi
    if [[ -z "$record_task_job" ]]; then
      echo "WARN: task object missing record.job_id: ${object_name}" >&2
      continue
    fi
    task_seen["$task_key"]="1"
    task_bucket["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.bucket // empty' )"
    task_object["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.object // empty' )"
    task_version["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.version_id // empty' )"
    task_storage["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.storage_class // empty' )"
    task_queued["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.queued_at_unix_nanos // empty' )"
    task_job["$task_key"]="$(printf '%s' "$record_task_job")"
    task_path["$task_key"]="$object_name"
    printf '%s,%s,%s,%s,%s,%s,%s\n' \
      "$(csv_escape "$task_key")" \
      "$(csv_escape "${task_bucket[$task_key]}")" \
      "$(csv_escape "${task_object[$task_key]}")" \
      "$(csv_escape "${task_version[$task_key]}")" \
      "$(csv_escape "${task_storage[$task_key]}")" \
      "$(csv_escape "${task_queued[$task_key]}")" \
      "$(csv_escape "$object_name")" >> "$TASKS_CSV"
  done <<< "$task_objs"

  while IFS= read -r object_name; do
    [[ -z "$object_name" ]] && continue
    task_key="${object_name##*/}"
    task_key="${task_key%.json}"
    if ! [[ "$task_key" =~ ^[0-9a-f]{64}$ ]]; then
      echo "WARN: ignore invalid result object name: ${object_name}" >&2
      continue
    fi
    raw="$(read_json_file "$object_name")"
    if ! record_result_job="$(printf '%s' "$raw" | jq -r '.record.job_id // empty' 2>/dev/null)"; then
      echo "WARN: parse error result object: ${object_name}" >&2
      continue
    fi
    if [[ -z "$record_result_job" ]]; then
      echo "WARN: result object missing record.job_id: ${object_name}" >&2
      continue
    fi
    result_seen["$task_key"]="1"
    result_status["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.result // .result // empty' )"
    result_reason["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.failure_reason // empty' )"
    result_completed["$task_key"]="$(printf '%s' "$raw" | jq -r '.record.completed_at_unix_nanos // empty' )"
    result_job["$task_key"]="$(printf '%s' "$record_result_job")"
    result_path["$task_key"]="$object_name"
    printf '%s,%s,%s,%s,%s\n' \
      "$(csv_escape "$task_key")" \
      "$(csv_escape "${result_status[$task_key]}")" \
      "$(csv_escape "${result_reason[$task_key]}")" \
      "$(csv_escape "${result_completed[$task_key]}")" \
      "$(csv_escape "$object_name")" >> "$RESULTS_CSV"
  done <<< "$result_objs"

  local task_count=0
  local result_count=0
  local missing_count=0
  local mismatch_count=0
  local completed_count=0
  local failed_count=0
  local task_job_id_match
  local result_job_id_match
  local task_record_job
  local result_record_job
  local -A failure_by_reason=()

  task_count="${#task_seen[@]}"
  result_count="${#result_seen[@]}"

  for task_key in "${!task_seen[@]}"; do
    local result=""
    local reason=""
    local task_has_result="false"
    if [[ -n "${result_seen[$task_key]+x}" ]]; then
      task_has_result="true"
      result="${result_status[$task_key]}"
      reason="${result_reason[$task_key]}"
      if [[ "${result}" == "Completed" ]]; then
        completed_count=$((completed_count + 1))
      elif [[ "${result}" == "TierFailure" ]]; then
        failed_count=$((failed_count + 1))
        reason="${reason:-Unknown}"
        failure_by_reason["$reason"]=$(( ${failure_by_reason["$reason"]:-0} + 1 ))
      fi
    else
      missing_count=$((missing_count + 1))
    fi

    task_job_id_match="false"
    result_job_id_match="false"

    task_record_job="${task_job[$task_key]-}"
    if [[ -n "$task_record_job" && "$(normalize_uuid "$task_record_job")" == "$expected_job" ]]; then
      task_job_id_match="true"
    fi
    result_record_job="${result_job[$task_key]-}"
    if [[ -n "$result_record_job" && "$(normalize_uuid "$result_record_job")" == "$expected_job" ]]; then
      result_job_id_match="true"
    fi
    if [[ "$task_job_id_match" != "true" ]] || [[ -n "${result_seen[$task_key]+x}" && "$result_job_id_match" != "true" ]]; then
      mismatch_count=$((mismatch_count + 1))
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$(csv_escape "$task_key")" \
      "$(csv_escape "${task_bucket[$task_key]}")" \
      "$(csv_escape "${task_object[$task_key]}")" \
      "$(csv_escape "${task_version[$task_key]}")" \
      "$(csv_escape "${task_storage[$task_key]}")" \
      "$(csv_escape "$task_has_result")" \
      "$(csv_escape "${result_status[$task_key]}")" \
      "$(csv_escape "${result_reason[$task_key]}")" \
      "$(csv_escape "$task_job_id_match")" \
      "$(csv_escape "$result_job_id_match")" \
      "$(csv_escape "${task_path[$task_key]}")" \
      "$(csv_escape "${result_path[$task_key]-}")" >> "$RECONCILE_CSV"
  done

  local orphan_count=0
  for task_key in "${!result_seen[@]}"; do
    if [[ -z "${task_seen[$task_key]+x}" ]]; then
      orphan_count=$((orphan_count + 1))
      printf 'orphan_result_task_key=%s\n' "$task_key" >> "$SUMMARY_TXT"
    fi
  done

  {
    echo "job_id=$JOB_ID"
    echo "sys_bucket=$SYS_BUCKET"
    echo "expected_task_prefix=$task_prefix"
    echo "expected_result_prefix=$result_prefix"
    echo "task_count=${task_count}"
    echo "result_count=${result_count}"
    echo "missing_result_count=${missing_count}"
    echo "job_id_mismatch_count=${mismatch_count}"
    echo "orphan_result_count=${orphan_count}"
    echo "completed_result_count=${completed_count}"
    echo "failed_result_count=${failed_count}"
    for reason in "${!failure_by_reason[@]}"; do
      echo "result_failure_${reason// /_}=${failure_by_reason[$reason]}"
    done
  } > "$SUMMARY_TXT"

  {
    echo
    echo "## journal summary"
    echo "- tasks: ${task_count}"
    echo "- results: ${result_count}"
    echo "- missing result for task: ${missing_count}"
    echo "- orphaned result rows: ${orphan_count}"
    echo "- completed result: ${completed_count}"
    echo "- failed result: ${failed_count}"
  } >> "$SUMMARY_TXT"
}

build_command_notes() {
  {
    echo "# Run snippets (to use manually)"
    echo
    echo "curl -sS ${ENDPOINT}/rustfs/admin/v3/metrics?types=${METRIC_TYPES}&n=${METRIC_SAMPLES} | jq '.aggregated.scanner.lifecycle_transition'"
    echo "scripts/manual_transition_journal_audit.sh --endpoint ${ENDPOINT} --job-id ${JOB_ID} --admin-token <token> --sys-bucket ${SYS_BUCKET}"
    echo "scripts/manual_transition_journal_audit.sh --endpoint ${ENDPOINT} --job-id ${JOB_ID} --admin-token <token> --sys-bucket ${SYS_BUCKET} --out-dir ${OUT_DIR}"
    if [[ "$DRY_RUN" == true ]]; then
      echo "# Add --mc-endpoint, --mc-access-key, --mc-secret-key when writing to .rustfs.sys."
    fi
  } > "$COMMANDS_TXT"
}

main() {
  parse_args "$@"
  if [[ "$SHOW_HELP" == true ]]; then
    usage
    exit 0
  fi
  require_bash4
  validate_args
  require_cmd "$MC_BIN"
  require_cmd curl
  require_cmd jq
  require_cmd rg
  setup_output
  setup_mc_alias

  build_command_notes
  run_worker_logs
  echo
  run_admin_events
  echo
  run_tier_operation_failures
  echo
  run_admin_job
  echo
  run_metrics
  echo
  collect_journals
  echo "Artifacts: $OUT_DIR"
  echo "  - $TASKS_CSV"
  echo "  - $RESULTS_CSV"
  echo "  - $RECONCILE_CSV"
  echo "  - $SUMMARY_TXT"
  echo "  - $COMMANDS_TXT"
}

main "$@"
