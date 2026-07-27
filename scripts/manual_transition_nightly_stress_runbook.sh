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
WINDOW_SPEC="nightly-2h:120:16:5000:balanced,nightly-12h:720:24:8000:write-heavy,nightly-24h:1440:32:12000:read-heavy"
SOAK_RATIOS="read-heavy:90:10,balanced:70:30,write-heavy:40:60"
WORKLOAD_SIZES="4KiB,1MiB,16MiB"
JOB_BUCKET="manual-transition"
JOB_PREFIX="journal-nightly-stress"
TIER=""
RUN_ADMIN_CHECKS=true
DRY_RUN=false
UNKNOWN_FAILURE_RATIO_THRESHOLD="0.00"
QUEUE_MISMATCH_RATIO_THRESHOLD="0.00"
UNKNOWN_FAILURE_COUNT_THRESHOLD="0"

STRESS_MATRIX_SCRIPT="${PROJECT_ROOT}/scripts/manual_transition_soak_matrix.sh"
RUNBOOK_FILE=""
COMMAND_FILE=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_nightly_stress_runbook.sh --endpoint <admin-api> [options]

Required:
  --endpoint            Admin API base, e.g. https://127.0.0.1:9000

Optional:
  --admin-token         Bearer token for admin endpoints
  --window-spec         Comma-separated run spec: window:duration_min:concurrency:ops_per_min:mix_name
  --soak-ratios         Comma-separated mix ratios: label:read_pct:write_pct
  --workload-sizes      Object-size workload set
  --job-bucket          Job bucket scope for transition commands
  --job-prefix          Prefix for generated commands (default: journal-nightly-stress)
  --tier                Transition tier (default: empty)
  --out-dir             Output directory for generated artifacts
  --no-admin-checks     Omit generated admin check commands in the runbook
  --dry-run             Generate artifacts only; do not create executable runner
  --help

Artifacts:
  - nightly stress runbook markdown
  - matrix/run scripts from manual_transition_soak_matrix.sh
  - runnable command entry template (includes failure snapshot hook)
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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint)
        ENDPOINT="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --admin-token)
        ADMIN_TOKEN="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --window-spec)
        WINDOW_SPEC="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --soak-ratios)
        SOAK_RATIOS="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --workload-sizes)
        WORKLOAD_SIZES="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --job-bucket)
        JOB_BUCKET="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --job-prefix)
        JOB_PREFIX="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --tier)
        TIER="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --out-dir)
        OUT_DIR="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --no-admin-checks)
        RUN_ADMIN_CHECKS=false
        shift
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

main_matrix() {
  local args=("${STRESS_MATRIX_SCRIPT}" --endpoint "$ENDPOINT")

  args+=(--window-spec "$WINDOW_SPEC")
  args+=(--soak-ratios "$SOAK_RATIOS")
  args+=(--workload-sizes "$WORKLOAD_SIZES")
  [[ -n "$ADMIN_TOKEN" ]] && args+=(--admin-token "$ADMIN_TOKEN")
  [[ "$RUN_ADMIN_CHECKS" == true ]] || args+=(--no-admin-checks)
  [[ -n "$OUT_DIR" ]] && args+=(--out-dir "$OUT_DIR")
  [[ "$DRY_RUN" == true ]] && args+=(--dry-run)

  "${args[@]}"
}

command_template() {
  local matrix_csv="${OUT_DIR}/nightly_soak_matrix.csv"
  local matrix_cmd="${OUT_DIR}/run_soak_matrix.sh"
  local notes="${OUT_DIR}/soak_notes.md"
  RUNBOOK_FILE="${OUT_DIR}/manual_transition_nightly_stress_runbook.md"
  COMMAND_FILE="${OUT_DIR}/run_nightly_stress_plan.sh"

  cat >"$RUNBOOK_FILE" <<'RUNBOOK'
# Manual transition nightly/stress stress-runbook

## Runtime baseline

- endpoint: __ENDPOINT__
- bucket: __JOB_BUCKET__
- prefix template: __JOB_PREFIX__
- tier: __TIER__
- matrix: __MATRIX_CSV__
- matrix command: __MATRIX_CMD__
- notes: __NOTES__

## Threshold template (editable)

- unknown failure ratio threshold: __UNKNOWN_FAILURE_RATIO_THRESHOLD__
- queue-mismatch tolerance ratio: __QUEUE_MISMATCH_RATIO_THRESHOLD__
- unknown failure count threshold: __UNKNOWN_FAILURE_COUNT_THRESHOLD__

## Failure snapshot policy

- on startup failure (missing job_id, API error), write a timestamped snapshot under '__OUT_DIR__/failure-snapshots'
- if immediate status shows failure_reason, capture snapshot under '__OUT_DIR__/failure-snapshots'
- for each failed run, run manual_transition_journal_audit.sh and keep its outputs as evidence

## Usage

Use the commands below directly:

- chmod +x __COMMAND_FILE__
- bash __COMMAND_FILE__

## Files produced

- __MATRIX_CSV__
- __MATRIX_CMD__
- __NOTES__
- __RUNBOOK_FILE__
- __COMMAND_FILE__
- __OUT_DIR__/failure-snapshots/*
RUNBOOK

  perl -0pi -e "s#__ENDPOINT__#${ENDPOINT//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__JOB_BUCKET__#${JOB_BUCKET//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__JOB_PREFIX__#${JOB_PREFIX//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__TIER__#${TIER//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__MATRIX_CSV__#${matrix_csv//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__MATRIX_CMD__#${matrix_cmd//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__NOTES__#${notes//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__RUNBOOK_FILE__#${RUNBOOK_FILE//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__COMMAND_FILE__#${COMMAND_FILE//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__OUT_DIR__#${OUT_DIR//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__UNKNOWN_FAILURE_RATIO_THRESHOLD__#${UNKNOWN_FAILURE_RATIO_THRESHOLD//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__QUEUE_MISMATCH_RATIO_THRESHOLD__#${QUEUE_MISMATCH_RATIO_THRESHOLD//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__UNKNOWN_FAILURE_COUNT_THRESHOLD__#${UNKNOWN_FAILURE_COUNT_THRESHOLD//#/#}#g" "$RUNBOOK_FILE"

  cat >"$COMMAND_FILE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

SOAK_MATRIX_CSV='__SOAK_MATRIX_CSV__'
ENDPOINT='__ENDPOINT__'
ADMIN_TOKEN='__ADMIN_TOKEN__'
JOB_BUCKET='__JOB_BUCKET__'
JOB_PREFIX='__JOB_PREFIX__'
TIER='__TIER__'
OUT_DIR='__OUT_DIR__'

: "${UNKNOWN_FAILURE_RATIO_THRESHOLD:=0.0}"
: "${QUEUE_MISMATCH_RATIO_THRESHOLD:=0.0}"
: "${UNKNOWN_FAILURE_COUNT_THRESHOLD:=0}"

SNAPSHOT_DIR="${OUT_DIR}/failure-snapshots"
mkdir -p "$SNAPSHOT_DIR"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
}

url_encode() {
  local value="$1"
  jq -rn --arg v "$value" '$v|@uri'
}

snapshot_failure() {
  local run_tag="$1"
  local reason="$2"
  local job_id="$3"
  local ts
  local snapshot_dir

  ts="$(date +%Y%m%dT%H%M%S)"
  snapshot_dir="${SNAPSHOT_DIR}/${run_tag}/${ts}"
  mkdir -p "$snapshot_dir"

  {
    echo "run_tag=${run_tag}"
    echo "reason=${reason}"
    echo "job_id=${job_id}"
    echo "ts=${ts}"
  } >"${snapshot_dir}/snapshot.meta"

  if [[ -n "$job_id" && "$job_id" != "NA" ]]; then
    curl -sS ${ADMIN_TOKEN:+-H "Authorization: Bearer ${ADMIN_TOKEN}"} -X GET "${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id}" \
      >"${snapshot_dir}/job_status.json"
    ./scripts/manual_transition_journal_audit.sh --endpoint "$ENDPOINT" --job-id "$job_id" ${ADMIN_TOKEN:+--admin-token "$ADMIN_TOKEN"} --out-dir "$snapshot_dir" || true
  fi

  cp "$SOAK_MATRIX_CSV" "${snapshot_dir}/source_matrix.csv"
}

run_entry() {
  local tag="$1"
  local duration_min="$2"
  local concurrency="$3"
  local ops_per_min="$4"
  local mix_name="$5"
  local read_pct="$6"
  local write_pct="$7"
  local size="$8"
  local expected_ops="$9"
  local budget_status="${10}"

  local prefix
  local query
  local url
  local response
  local job_id
  local status
  local headers=()

  if [[ "$budget_status" == "over-budget" ]]; then
    echo "[skip] ${tag} ${size} over budget: expected_ops=${expected_ops}"
    return 0
  fi

  if [[ "$UNKNOWN_FAILURE_COUNT_THRESHOLD" -gt 0 && "$expected_ops" -gt "$UNKNOWN_FAILURE_COUNT_THRESHOLD" ]]; then
    :
  fi

  prefix="${JOB_PREFIX}/${tag}/${size}/${read_pct}r${write_pct}w"
  query="bucket=$(url_encode "$JOB_BUCKET")"
  query="${query}&prefix=$(url_encode "$prefix")"
  query="${query}&maxObjects=100000"
  query="${query}&maxDurationSeconds=$((duration_min * 60))"
  query="${query}&mode=async&dryRun=false"
  if [[ -n "$TIER" ]]; then
    query="${query}&tier=$(url_encode "$TIER")"
  fi

  url="${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/run?${query}"

  if [[ -n "$ADMIN_TOKEN" ]]; then
    headers+=("-H" "Authorization: Bearer ${ADMIN_TOKEN}")
  fi

  echo "==> run=${tag} size=${size} mix=${mix_name} read_pct=${read_pct} write_pct=${write_pct} duration_min=${duration_min}"
  if ! response="$(curl -sS "${headers[@]}" -X POST "$url")"; then
    snapshot_failure "$tag" "curl_post_failed" "NA"
    return 1
  fi
  job_id="$(printf '%s' "$response" | jq -r '.job_id // empty')"
  if [[ -z "$job_id" || "$job_id" == "null" ]]; then
    snapshot_failure "$tag" "missing_job_id" "NA"
    return 1
  fi

  status="$(curl -sS "${headers[@]}" -X GET "${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id}" | jq -r '.failure_reason // empty')"
  if [[ -n "$status" && "$status" != "null" ]]; then
    snapshot_failure "$tag" "failure_reason=${status}" "$job_id"
  fi
}

main() {
  require_cmd bash
  require_cmd curl
  require_cmd jq
  require_cmd awk

  if [[ ! -f "$SOAK_MATRIX_CSV" ]]; then
    echo "ERROR: expected matrix file missing: $SOAK_MATRIX_CSV" >&2
    echo "Run manual_transition_soak_matrix.sh first." >&2
    exit 1
  fi

  while IFS=',' read -r window duration_min concurrency ops_per_min mix_name read_pct write_pct size expected_ops run_id budget_status; do
    if [[ -z "$window" || "$window" == "window" ]]; then
      continue
    fi
    run_entry "${window}" "${duration_min}" "${concurrency}" "${ops_per_min}" "${mix_name}" "${read_pct}" "${write_pct}" "${size}" "${expected_ops}" "${budget_status}" || true
  done < <(tail -n +2 "$SOAK_MATRIX_CSV")
}

main "$@"
EOF

  perl -0pi -e "s#__SOAK_MATRIX_CSV__#${matrix_csv//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__ENDPOINT__#${ENDPOINT//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__ADMIN_TOKEN__#${ADMIN_TOKEN//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__JOB_BUCKET__#${JOB_BUCKET//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__JOB_PREFIX__#${JOB_PREFIX//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__TIER__#${TIER//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__OUT_DIR__#${OUT_DIR//#/#}#g" "$COMMAND_FILE"
  chmod +x "$COMMAND_FILE"
}

main() {
  parse_args "$@"
  if [[ -z "$ENDPOINT" ]]; then
    echo "ERROR: --endpoint is required" >&2
    usage
    exit 1
  fi

  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-nightly-stress-runbook/$(date +%Y%m%dT%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  require_cmd awk
  require_cmd jq

  main_matrix
  command_template

  echo "Generated runbook: ${RUNBOOK_FILE}"
}

main "$@"
