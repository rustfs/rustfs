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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENDPOINT=""
ADMIN_TOKEN=""
OUT_DIR=""
LOG_GLOB="/var/log/rustfs/*.log"
METRIC_TYPES="1"
METRIC_SAMPLES="1"
METRIC_INTERVAL=""
BUCKET_FILTER=""
MIN_DISTINCT_REASONS="2"
DRY_RUN=false
SHOW_HELP=false

declare -a SAMPLES=()
REASON_COUNTS_JSONL=""
MATCHED_EXPECTED_TXT=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_failure_samples.sh --endpoint <admin-api> --sample <label:job-id[:expected_reason]> [options]

Required for real runs:
  --endpoint              Admin endpoint base, e.g. https://127.0.0.1:9000
  --sample                Failure sample spec. Repeatable. Example: auth:<job-id>:RemoteAuth

Optional:
  --admin-token           Bearer token for /admin/v3 endpoints
  --log-glob              Log glob to scan, default /var/log/rustfs/*.log
  --metric-types          /admin/v3/metrics type flags, default 1
  --metric-samples        /admin/v3/metrics sample count, default 1
  --metric-interval       /admin/v3/metrics interval query param
  --bucket                Optional bucket filter for focused log output
  --min-distinct-reasons  Minimum distinct tier failure reasons required, default 2
  --out-dir               Artifact output dir
  --dry-run               Generate the sample command template without admin calls
  --help                  Show usage

Output:
  - failure_attribution_summary.csv
  - sample_plan.md
  - commands.txt
  - per-sample job_status.json, metrics.json, and log_events.json

Typical #1509 evidence target:
  scripts/manual_transition_failure_samples.sh \
    --endpoint https://127.0.0.1:9000 \
    --admin-token "$TOKEN" \
    --sample auth:<JOB_ID_AUTH>:RemoteAuth \
    --sample network:<JOB_ID_NETWORK>:RemoteNetwork \
    --min-distinct-reasons 2 \
    --out-dir target/manual-transition-failure-samples
USAGE
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
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

normalize_uuid() {
  local raw="${1//-/}"
  printf '%s' "${raw,,}"
}

validate_uuid() {
  local raw="$1"
  local normalized
  normalized="$(normalize_uuid "$raw")"
  if ! [[ "$normalized" =~ ^[0-9a-f]{32}$ ]]; then
    echo "ERROR: invalid uuid in --sample: $raw" >&2
    exit 1
  fi
  printf '%s' "$normalized"
}

csv_escape() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --admin-token) ADMIN_TOKEN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sample) SAMPLES+=("$(arg_value "$1" "${2:-}")"); shift 2 ;;
      --log-glob) LOG_GLOB="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-types) METRIC_TYPES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-samples) METRIC_SAMPLES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metric-interval) METRIC_INTERVAL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket) BUCKET_FILTER="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --min-distinct-reasons) MIN_DISTINCT_REASONS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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
  if [[ -z "$ENDPOINT" ]]; then
    echo "ERROR: --endpoint is required" >&2
    exit 1
  fi
  if ! [[ "$MIN_DISTINCT_REASONS" =~ ^[0-9]+$ ]] || (( MIN_DISTINCT_REASONS < 1 )); then
    echo "ERROR: --min-distinct-reasons must be a positive integer" >&2
    exit 1
  fi
  if [[ "$DRY_RUN" != true && "${#SAMPLES[@]}" -eq 0 ]]; then
    echo "ERROR: at least one --sample is required unless --dry-run is set" >&2
    exit 1
  fi
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-failure-samples/$(date +%Y%m%dT%H%M%S)"
  fi
}

sample_label() {
  local spec="$1"
  local label="${spec%%:*}"
  if [[ -z "$label" || "$label" == "$spec" ]]; then
    echo "ERROR: invalid --sample spec: $spec" >&2
    exit 1
  fi
  if ! [[ "$label" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: invalid --sample label: $label" >&2
    exit 1
  fi
  printf '%s' "$label"
}

sample_job_id() {
  local spec="$1"
  local rest="${spec#*:}"
  local job_id="${rest%%:*}"
  if [[ -z "$job_id" || "$job_id" == "$rest" && "$rest" == "$spec" ]]; then
    echo "ERROR: invalid --sample spec: $spec" >&2
    exit 1
  fi
  validate_uuid "$job_id"
}

sample_expected_reason() {
  local spec="$1"
  local rest="${spec#*:}"
  local job_id="${rest%%:*}"
  if [[ "$rest" == "$job_id" ]]; then
    printf ''
    return
  fi
  printf '%s' "${rest#*:}"
}

curl_json() {
  local path="$1"
  local out_file="$2"
  local url="${ENDPOINT%/}${path}"
  local args=(-fsS)
  if [[ -n "$ADMIN_TOKEN" ]]; then
    args+=(-H "Authorization: Bearer ${ADMIN_TOKEN}")
  fi
  curl "${args[@]}" "$url" > "$out_file"
}

metrics_path() {
  local path="/rustfs/admin/v3/metrics?types=${METRIC_TYPES}&n=${METRIC_SAMPLES}"
  if [[ -n "$METRIC_INTERVAL" ]]; then
    path="${path}&interval=${METRIC_INTERVAL}"
  fi
  printf '%s' "$path"
}

write_dry_run_plan() {
  mkdir -p "$OUT_DIR"
  cat >"${OUT_DIR}/sample_plan.md" <<'PLAN'
# Manual transition failure attribution sample plan

Use this artifact to turn #1509 auth/network/timeout checks into repeatable evidence.

## Prepare controlled samples

- auth: configure a temporary remote tier with invalid credentials, run one async manual transition, keep the returned job_id
- network: configure a temporary remote tier endpoint that is unreachable or refuses connections, run one async manual transition, keep the returned job_id
- timeout: configure a throttled or blackholed remote tier endpoint with a short client timeout, run one async manual transition, keep the returned job_id

## Capture evidence

Replace the placeholders and run:

```bash
scripts/manual_transition_failure_samples.sh \
  --endpoint <admin-api> \
  --admin-token "$TOKEN" \
  --sample auth:<JOB_ID_AUTH>:<expected_reason_key> \
  --sample network:<JOB_ID_NETWORK>:<expected_reason_key> \
  --sample timeout:<JOB_ID_TIMEOUT>:<expected_reason_key> \
  --min-distinct-reasons 2 \
  --out-dir target/manual-transition-failure-samples/<run-id>
```

## Close gate

- at least two samples have `matched_expected_reason=true`
- `distinct_reason_count` across all samples is greater than or equal to `--min-distinct-reasons`
- no status/log artifact exposes secrets or full credential material
- terminal partial/failed counts agree with `tier_failure_by_reason`
PLAN
  cat >"${OUT_DIR}/commands.txt" <<COMMANDS
scripts/manual_transition_failure_samples.sh --endpoint ${ENDPOINT} --sample auth:<JOB_ID_AUTH>:<expected_reason_key> --sample network:<JOB_ID_NETWORK>:<expected_reason_key> --min-distinct-reasons ${MIN_DISTINCT_REASONS} --out-dir ${OUT_DIR}
COMMANDS
  echo "[DRY-RUN] generated ${OUT_DIR}/sample_plan.md"
  echo "[DRY-RUN] generated ${OUT_DIR}/commands.txt"
}

collect_logs() {
  local job_id="$1"
  local out_file="$2"
  local raw_file
  local err_file
  local log_paths=()
  local match
  raw_file="$(mktemp)"
  err_file="$(mktemp)"

  while IFS= read -r match; do
    [[ -n "$match" ]] && log_paths+=("$match")
  done < <(compgen -G "$LOG_GLOB" || true)
  if [[ "${#log_paths[@]}" -eq 0 ]]; then
    log_paths=("$LOG_GLOB")
  fi

  if ! rg --no-filename --color=never -- "${job_id}|lifecycle_tier_operation_failed|lifecycle_worker_state" -- "${log_paths[@]}" >"$raw_file" 2>"$err_file"; then
    :
  fi

  JOB_ID="$job_id" BUCKET_FILTER="$BUCKET_FILTER" jq -R -s '
    split("\n")
    | map(fromjson? | select(. != null))
    | map(select(
        (.job_id? // "") == env.JOB_ID
        or (.event? // "") == "lifecycle_tier_operation_failed"
        or ((.event? // "") == "lifecycle_worker_state" and ((.state? // "") | startswith("manual_transition_")))
      ))
    | if env.BUCKET_FILTER == "" then . else map(select((.bucket? // "") == env.BUCKET_FILTER)) end
    | map({
        timestamp,
        event,
        state,
        job_id,
        bucket,
        object,
        version_id,
        tier,
        operation,
        reason,
        error,
        failure_reason
      })
  ' <"$raw_file" >"$out_file"

  if [[ -s "$err_file" ]]; then
    sed 's/^/DEBUG: /' "$err_file" >&2
  fi

  rm -f "$raw_file" "$err_file"
}

write_command_notes() {
  cat >"${OUT_DIR}/commands.txt" <<COMMANDS
# Re-run this evidence capture
scripts/manual_transition_failure_samples.sh --endpoint ${ENDPOINT} --min-distinct-reasons ${MIN_DISTINCT_REASONS} --out-dir ${OUT_DIR} \\
$(printf '  --sample %q \\\n' "${SAMPLES[@]}")

# Per-job deep dive
scripts/manual_transition_debug.sh --endpoint ${ENDPOINT} --job-id <JOB_ID> --log-glob '${LOG_GLOB}'
scripts/manual_transition_journal_audit.sh --endpoint ${ENDPOINT} --job-id <JOB_ID> --out-dir ${OUT_DIR}/journal-<JOB_ID>
COMMANDS
}

collect_sample() {
  local spec="$1"
  local summary_csv="$2"
  local label job_id expected sample_dir status_file metrics_file logs_file
  local status failure_reason reason_counts total_failures distinct_reasons matched_expected
  label="$(sample_label "$spec")"
  job_id="$(sample_job_id "$spec")"
  expected="$(sample_expected_reason "$spec")"
  sample_dir="${OUT_DIR}/${label}-${job_id}"
  status_file="${sample_dir}/job_status.json"
  metrics_file="${sample_dir}/metrics.json"
  logs_file="${sample_dir}/log_events.json"
  mkdir -p "$sample_dir"

  curl_json "/rustfs/admin/v3/ilm/transition/jobs/${job_id}" "$status_file"
  curl_json "$(metrics_path)" "$metrics_file"
  collect_logs "$job_id" "$logs_file"

  status="$(jq -r '.status // ""' "$status_file")"
  failure_reason="$(jq -r '.failure_reason // ""' "$status_file")"
  reason_counts="$(jq -c '.report.tier_failure_by_reason // {}' "$status_file")"
  total_failures="$(jq -r '(.report.tier_failure_by_reason // {}) | [.[]] | add // 0' "$status_file")"
  distinct_reasons="$(jq -r '(.report.tier_failure_by_reason // {}) | keys | length' "$status_file")"
  if [[ -z "$expected" ]]; then
    matched_expected="NA"
  else
    matched_expected="$(jq -r --arg expected "$expected" '(.report.tier_failure_by_reason // {}) | (.[$expected] // 0) > 0' "$status_file")"
  fi
  printf '%s\n' "$reason_counts" >> "$REASON_COUNTS_JSONL"
  if [[ "$matched_expected" == "true" ]]; then
    printf '%s\n' "$label" >> "$MATCHED_EXPECTED_TXT"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "$label")" \
    "$(csv_escape "$job_id")" \
    "$(csv_escape "$expected")" \
    "$(csv_escape "$status")" \
    "$(csv_escape "$failure_reason")" \
    "$(csv_escape "$total_failures")" \
    "$(csv_escape "$matched_expected")" \
    "$(csv_escape "$distinct_reasons")" \
    "$(csv_escape "$reason_counts")" \
    "$(csv_escape "$status_file")" \
    "$(csv_escape "$logs_file")" \
    "$(csv_escape "$metrics_file")" >> "$summary_csv"
}

assert_gate() {
  local distinct_count
  local matched_count
  distinct_count="$(jq -s 'map(. // {}) | add // {} | keys | length' "$REASON_COUNTS_JSONL")"
  matched_count="$(wc -l < "$MATCHED_EXPECTED_TXT" | awk '{$1=$1; print}')"

  {
    echo "min_distinct_reasons=${MIN_DISTINCT_REASONS}"
    echo "observed_distinct_reasons=${distinct_count}"
    echo "matched_expected_samples=${matched_count}"
  } > "${OUT_DIR}/gate_summary.txt"

  if (( distinct_count < MIN_DISTINCT_REASONS )); then
    echo "ERROR: observed distinct tier failure reasons (${distinct_count}) below required minimum (${MIN_DISTINCT_REASONS})" >&2
    exit 1
  fi
}

main() {
  parse_args "$@"
  validate_args
  if [[ "$DRY_RUN" == true ]]; then
    write_dry_run_plan
    exit 0
  fi

  require_cmd curl
  require_cmd jq
  require_cmd rg
  require_cmd awk
  mkdir -p "$OUT_DIR"

  local summary_csv="${OUT_DIR}/failure_attribution_summary.csv"
  {
    echo "sample,job_id,expected_reason,status,failure_reason,total_tier_failures,matched_expected_reason,distinct_reason_count,reason_counts,status_file,log_file,metrics_file"
  } > "$summary_csv"

  REASON_COUNTS_JSONL="${OUT_DIR}/reason_counts.jsonl"
  MATCHED_EXPECTED_TXT="${OUT_DIR}/matched_expected_samples.txt"
  : > "$REASON_COUNTS_JSONL"
  : > "$MATCHED_EXPECTED_TXT"

  write_command_notes
  for sample in "${SAMPLES[@]}"; do
    collect_sample "$sample" "$summary_csv"
  done
  assert_gate

  echo "Artifacts: $OUT_DIR"
  echo "  - $summary_csv"
  echo "  - ${OUT_DIR}/gate_summary.txt"
  echo "  - ${OUT_DIR}/commands.txt"
}

main "$@"
