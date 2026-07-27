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
PHASE_MATRIX="request-only:100:0:30,canary:90:10:120,mixed:50:50:240,full-rollout:0:100:360,rollback:100:0:30"
CONCURRENCIES="8,16,32"
OBJECT_COUNTS="5k,20k,50k"
JOB_BUCKET="manual-transition"
JOB_PREFIX="journal-mixed-rollout"
TIER=""
READ_RATIO="90"
RUN_ADMIN_CHECKS=true
DRY_RUN=false

MIXED_MATRIX_SCRIPT="${PROJECT_ROOT}/scripts/manual_transition_mixed_rollout_matrix.sh"
RUNBOOK_FILE=""
COMMAND_FILE=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_mixed_rollout_runbook.sh --endpoint <admin-api> [options]

Required:
  --endpoint            Admin API base, e.g. https://127.0.0.1:9000

Optional:
  --admin-token         Bearer token for admin endpoints
  --job-bucket          Job bucket for transition scope (default: manual-transition)
  --job-prefix          Prefix for generated runbook phase names (default: journal-mixed-rollout)
  --tier                Transition tier (default: empty)
  --phase-matrix        Comma-separated phase spec: name:old_pct:new_pct:duration_min
  --concurrencies       Comma-separated concurrency list
  --object-counts       Comma-separated object-count workload list
  --read-ratio          Read ratio percent used in mixed workload commands
  --out-dir             Output directory for generated runbook artifacts
  --no-admin-checks     Omit generated admin check commands in the runbook
  --dry-run             Generate artifacts only; skip executable plan file creation
  --help

Artifacts:
  - mixed-rollout runbook markdown
  - matrix/run scripts from manual_transition_mixed_rollout_matrix.sh
  - reusable command template script
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
      --phase-matrix)
        PHASE_MATRIX="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --concurrencies)
        CONCURRENCIES="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --object-counts)
        OBJECT_COUNTS="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --read-ratio)
        READ_RATIO="$(arg_value "$1" "${2:-}")"
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
  local args=("${MIXED_MATRIX_SCRIPT}" --endpoint "$ENDPOINT")

  args+=(--phase-matrix "$PHASE_MATRIX")
  args+=(--concurrencies "$CONCURRENCIES")
  args+=(--object-counts "$OBJECT_COUNTS")
  args+=(--job-bucket "$JOB_BUCKET")
  args+=(--job-prefix "$JOB_PREFIX")
  args+=(--read-ratio "$READ_RATIO")
  [[ -n "$ADMIN_TOKEN" ]] && args+=(--admin-token "$ADMIN_TOKEN")
  [[ "$RUN_ADMIN_CHECKS" == true ]] || args+=(--no-admin-checks)
  [[ -n "$OUT_DIR" ]] && args+=(--out-dir "$OUT_DIR")
  [[ "$DRY_RUN" == true ]] && args+=(--dry-run)

  "${args[@]}"
}

command_template() {
  local matrix_csv="${OUT_DIR}/mixed_rollout_matrix.csv"
  local matrix_cmd="${OUT_DIR}/run_phase_commands.sh"
  local manifest="${OUT_DIR}/mixed_rollout_checklist.md"
  local out_cmd
  local token_note
  local tier_note

  RUNBOOK_FILE="${OUT_DIR}/manual_transition_mixed_rollout_runbook.md"
  COMMAND_FILE="${OUT_DIR}/run_mixed_rollout_plan.sh"

  if [[ -z "$TIER" ]]; then
    tier_note="No tier value is set; default transition scope is all tiers under scope settings."
  else
    tier_note="tier=${TIER}"
  fi
  if [[ -n "$ADMIN_TOKEN" ]]; then
    token_note='export ADMIN_TOKEN=<set by caller>'
  else
    token_note='export ADMIN_TOKEN="" (set this value)'
  fi

  cat >"$RUNBOOK_FILE" <<'RUNBOOK'
# Manual transition mixed-version rollout runbook

## Minimum runtime checklist

- endpoint: __ENDPOINT__
- admin token: __TOKEN_NOTE__
- matrix input: __MATRIX_CSV__
- read ratio target: __READ_RATIO__%
- scope: bucket=__JOB_BUCKET__, prefix template starts at __JOB_PREFIX__, __TIER_NOTE__
- required tools: bash, curl, jq, awk, sed, date

## Commands

Use the commands below directly:

- chmod +x __COMMAND_FILE__
- bash __COMMAND_FILE__

## Notes

- generated files:
  - __MATRIX_CSV__
  - __MATRIX_CMD__
  - __MANIFEST__
  - __RUNBOOK_FILE__
  - __COMMAND_FILE__
- generated 'run_phase_commands.sh' is a per-phase audit starter; 'run_mixed_rollout_plan.sh' is the full runnable template.
- recommended runbook order:
  1. Review 'mixed_rollout_checklist.md'.
  2. Inspect __MATRIX_CMD__ for each phase note.
  3. Run '__COMMAND_FILE__' to trigger the rollout template execution.
RUNBOOK

  perl -0pi -e "s#__ENDPOINT__#${ENDPOINT//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__TOKEN_NOTE__#${token_note//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__MATRIX_CSV__#${matrix_csv//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__READ_RATIO__#${READ_RATIO//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__JOB_BUCKET__#${JOB_BUCKET//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__JOB_PREFIX__#${JOB_PREFIX//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__TIER_NOTE__#${tier_note//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__MATRIX_CMD__#${matrix_cmd//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__MANIFEST__#${manifest//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__RUNBOOK_FILE__#${RUNBOOK_FILE//#/#}#g" "$RUNBOOK_FILE"
  perl -0pi -e "s#__COMMAND_FILE__#${COMMAND_FILE//#/#}#g" "$RUNBOOK_FILE"

  cat >"$COMMAND_FILE" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

MIXED_MATRIX_CSV='__MIXED_MATRIX_CSV__'
ENDPOINT='__ENDPOINT__'
ADMIN_TOKEN='__ADMIN_TOKEN__'
JOB_BUCKET='__JOB_BUCKET__'
JOB_PREFIX='__JOB_PREFIX__'
TIER='__TIER__'
OUT_DIR='__OUT_DIR__'

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

normalize_object_count() {
  local raw
  local lower
  raw="$1"
  raw="${raw// /}"
  lower="${raw,,}"

  if [[ "$lower" =~ ^([0-9]+)$ ]]; then
    echo "$lower"
    return 0
  fi

  if [[ "$lower" =~ ^([0-9]+)k$ ]]; then
    echo $((BASH_REMATCH[1] * 1000))
    return 0
  fi

  if [[ "$lower" =~ ^([0-9]+)m$ ]]; then
    echo $((BASH_REMATCH[1] * 1000000))
    return 0
  fi

  echo ""
  return 1
}

run_transition() {
  local phase="$1"
  local concurrency="$2"
  local object_count_label="$3"
  local duration_min="$4"
  local duration_sec
  local object_count
  local prefix
  local query
  local url
  local response
  local job_id
  local status_url
  local headers=()

  duration_sec=$((duration_min * 60))
  if ! object_count="$(normalize_object_count "$object_count_label")"; then
    echo "[warn] skip ${phase}: invalid object_count=${object_count_label}" >&2
    return 0
  fi
  if (( object_count <= 0 )); then
    echo "[warn] skip ${phase}: non-positive object_count=${object_count}" >&2
    return 0
  fi

  prefix="${JOB_PREFIX}/${phase}/${concurrency}c_${object_count_label}o"

  query="bucket=$(url_encode "$JOB_BUCKET")"
  query="${query}&prefix=$(url_encode "$prefix")"
  query="${query}&maxObjects=${object_count}"
  query="${query}&maxDurationSeconds=${duration_sec}"
  query="${query}&mode=async"
  query="${query}&dryRun=false"
  if [[ -n "$TIER" ]]; then
    query="${query}&tier=$(url_encode "$TIER")"
  fi

  url="${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/run?${query}"

  if [[ -n "$ADMIN_TOKEN" ]]; then
    headers+=("-H" "Authorization: Bearer ${ADMIN_TOKEN}")
  fi

  echo "==> phase=${phase} concurrency=${concurrency} object_count=${object_count} duration_min=${duration_min}"
  response="$(curl -sS "${headers[@]}" -X POST "$url")"
  job_id="$(printf '%s' "$response" | jq -r '.job_id // empty')"
  if [[ -z "$job_id" || "$job_id" == "null" ]]; then
    echo "ERROR: transition run did not return job_id for ${phase}" >&2
    echo "$response"
    return 1
  fi

  echo "job_id=${job_id}"
  status_url="${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id}"
  ./scripts/manual_transition_journal_audit.sh --endpoint "$ENDPOINT" --job-id "$job_id" ${ADMIN_TOKEN:+--admin-token "$ADMIN_TOKEN"} --out-dir "${OUT_DIR}/journal-audit-${phase}-${concurrency}-${object_count_label}" || true
  printf '%s\n' "$response" > "${OUT_DIR}/run-response-${phase}-${concurrency}-${object_count_label}.json"
  echo "status_url=${status_url}"
  curl -sS "${headers[@]}" -X GET "$status_url" | jq '{status, report, queue_snapshot, failure_reason}'
}

main() {
  require_cmd bash
  require_cmd curl
  require_cmd jq
  require_cmd awk
  require_cmd sed

  if [[ ! -f "$MIXED_MATRIX_CSV" ]]; then
    echo "ERROR: expected matrix file missing: $MIXED_MATRIX_CSV" >&2
    echo "Run manual_transition_mixed_rollout_matrix.sh first." >&2
    exit 1
  fi

  while IFS=',' read -r phase old_pct new_pct duration_min concurrency object_count read_ratio gate admin_check; do
    if [[ -z "$phase" || "$phase" == "phase" ]]; then
      continue
    fi
    run_transition "$phase" "$concurrency" "$object_count" "$duration_min"
    echo ""
  done < <(tail -n +2 "$MIXED_MATRIX_CSV")
}

main "$@"
EOF

  perl -0pi -e "s#__MIXED_MATRIX_CSV__#${matrix_csv//#/#}#g" "$COMMAND_FILE"
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
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-mixed-rollout-runbook/$(date +%Y%m%dT%H%M%S)"
  fi

  mkdir -p "$OUT_DIR"
  require_cmd awk
  require_cmd jq

  main_matrix
  command_template

  echo "Generated runbook: ${RUNBOOK_FILE}"
}

main "$@"
