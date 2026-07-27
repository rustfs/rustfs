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
ACCESS_KEY=""
SECRET_KEY=""
AWS_SIGV4_SCOPE="aws:amz:us-east-1:s3"
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
  --access-key          Access key for SigV4-signed admin calls
  --secret-key          Secret key for SigV4-signed admin calls
  --aws-sigv4-scope     curl --aws-sigv4 scope, default aws:amz:us-east-1:s3
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
      --access-key)
        ACCESS_KEY="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --secret-key)
        SECRET_KEY="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --aws-sigv4-scope)
        AWS_SIGV4_SCOPE="$(arg_value "$1" "${2:-}")"
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
  elif [[ -n "$ACCESS_KEY" ]]; then
    token_note='SigV4 credentials embedded in generated runner'
  else
    token_note='export ADMIN_TOKEN="" or pass --access-key/--secret-key'
  fi

  cat >"$RUNBOOK_FILE" <<'RUNBOOK'
# Manual transition mixed-version rollout runbook

## Minimum runtime checklist

- endpoint: __ENDPOINT__
- admin token: __TOKEN_NOTE__
- matrix input: __MATRIX_CSV__
- read ratio target: __READ_RATIO__%
- scope: bucket=__JOB_BUCKET__, prefix template starts at __JOB_PREFIX__, __TIER_NOTE__
- required tools: bash, curl, jq, awk, sed, date, rg

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
- for failure-oriented rollout phases, use scripts/manual_transition_failure_samples.sh to capture auth/network/timeout attribution evidence from the produced job ids.
- for #1508 strict validation, pre-seed non-empty lifecycle-matching objects before each phase and keep both run response and terminal status/readback evidence.
- for in-flight rollback validation, set IN_FLIGHT_ROLLBACK_HOOK to a script that replaces one new node with the old binary after job admission and before terminal status polling.
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
ACCESS_KEY='__ACCESS_KEY__'
SECRET_KEY='__SECRET_KEY__'
AWS_SIGV4_SCOPE='__AWS_SIGV4_SCOPE__'
JOB_BUCKET='__JOB_BUCKET__'
JOB_PREFIX='__JOB_PREFIX__'
TIER='__TIER__'
OUT_DIR='__OUT_DIR__'
POLL_SECONDS="${POLL_SECONDS:-120}"
S3_ENDPOINT="${S3_ENDPOINT:-$ENDPOINT}"
IN_FLIGHT_ROLLBACK_HOOK="${IN_FLIGHT_ROLLBACK_HOOK:-}"

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

curl_admin() {
  local method="$1"
  local url="$2"
  shift 2
  local args=(-sS -X "$method")
  if [[ -n "$ADMIN_TOKEN" ]]; then
    args+=(-H "Authorization: Bearer ${ADMIN_TOKEN}")
  elif [[ -n "$ACCESS_KEY" ]]; then
    args+=(--aws-sigv4 "$AWS_SIGV4_SCOPE" --user "${ACCESS_KEY}:${SECRET_KEY}")
  fi
  curl "${args[@]}" "$url" "$@"
}

curl_s3() {
  curl_admin "$@"
}

is_terminal_status() {
  case "$1" in
    completed|partial|failed|cancelled|unknown)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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

seed_phase_workload() {
  local phase="$1"
  local prefix="$2"
  local object_count="$3"
  local result_tag="$4"
  local lifecycle_file="${OUT_DIR}/lifecycle-${result_tag}.xml"
  local object_key="${prefix}/probe-${result_tag}.txt"

  if [[ -z "$TIER" ]]; then
    echo "ERROR: --tier is required for non-empty mixed-rollout evidence seeding" >&2
    return 1
  fi

  curl_s3 PUT "${S3_ENDPOINT%/}/${JOB_BUCKET}" -o "${OUT_DIR}/create-bucket-${result_tag}.response" -w "%{http_code}\n" \
    >"${OUT_DIR}/create-bucket-${result_tag}.http_code" || true

  cat >"$lifecycle_file" <<XML
<LifecycleConfiguration>
  <Rule>
    <ID>manual-transition-${phase}</ID>
    <Filter><Prefix>${prefix}</Prefix></Filter>
    <Status>Enabled</Status>
    <Transition><Days>0</Days><StorageClass>${TIER}</StorageClass></Transition>
  </Rule>
</LifecycleConfiguration>
XML
  curl_s3 PUT "${S3_ENDPOINT%/}/${JOB_BUCKET}?lifecycle" -H 'Content-Type: application/xml' --data-binary "@${lifecycle_file}" \
    -o "${OUT_DIR}/put-lifecycle-${result_tag}.response" -w "%{http_code}\n" >"${OUT_DIR}/put-lifecycle-${result_tag}.http_code"

  curl_s3 PUT "${S3_ENDPOINT%/}/${JOB_BUCKET}/${object_key}" --data-binary "rustfs #1508 ${phase} non-empty transition probe" \
    -o "${OUT_DIR}/put-object-${result_tag}.response" -w "%{http_code}\n" >"${OUT_DIR}/put-object-${result_tag}.http_code"

  echo "$object_key" >"${OUT_DIR}/object-key-${result_tag}.txt"
  if (( object_count > 1 )); then
    echo "[info] seeded one required probe object for ${phase}; caller may pre-seed the remaining $((object_count - 1)) objects under ${prefix}"
  fi
}

head_phase_probe() {
  local result_tag="$1"
  local object_key
  object_key="$(cat "${OUT_DIR}/object-key-${result_tag}.txt")"
  curl_s3 HEAD "${S3_ENDPOINT%/}/${JOB_BUCKET}/${object_key}" -D "${OUT_DIR}/head-object-${result_tag}.headers" \
    -o /dev/null -w "%{http_code}\n" >"${OUT_DIR}/head-object-${result_tag}.http_code" || true
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
  local status_json
  local result_tag
  local status

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
  result_tag="${phase}-${concurrency}-${object_count_label}"
  seed_phase_workload "$phase" "$prefix" "$object_count" "$result_tag"

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

  echo "==> phase=${phase} concurrency=${concurrency} object_count=${object_count} duration_min=${duration_min}"
  response="$(curl_admin POST "$url")"
  printf '%s\n' "$response" > "${OUT_DIR}/run-response-${result_tag}.json"
  job_id="$(printf '%s' "$response" | jq -r '.job_id // empty')"
  if [[ -z "$job_id" || "$job_id" == "null" ]]; then
    echo "ERROR: transition run did not return job_id for ${phase}" >&2
    printf '%s\n' "$response" > "${OUT_DIR}/run-response-missing-job-${result_tag}.json"
    return 1
  fi

  echo "job_id=${job_id}"
  status_url="${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id}"
  if [[ -n "$IN_FLIGHT_ROLLBACK_HOOK" ]]; then
    "$IN_FLIGHT_ROLLBACK_HOOK" "$phase" "$job_id" "$status_url" "$OUT_DIR"
  fi
  ./scripts/manual_transition_journal_audit.sh --endpoint "$ENDPOINT" --job-id "$job_id" ${ADMIN_TOKEN:+--admin-token "$ADMIN_TOKEN"} --out-dir "${OUT_DIR}/journal-audit-${phase}-${concurrency}-${object_count_label}" || true
  echo "status_url=${status_url}"
  for _ in $(seq 1 "$POLL_SECONDS"); do
    status_json="$(curl_admin GET "$status_url")"
    printf '%s\n' "$status_json" > "${OUT_DIR}/status-${result_tag}.json"
    status="$(printf '%s' "$status_json" | jq -r '.status // ""')"
    if is_terminal_status "$status"; then
      break
    fi
    sleep 1
  done
  head_phase_probe "$result_tag"
  printf '%s\n' "$status_json" | jq '{job_id, status, bucket, prefix, tier, report, queue_snapshot, failure_reason}'
}

main() {
  require_cmd bash
  require_cmd curl
  require_cmd jq
  require_cmd awk
  require_cmd sed
  if [[ -n "$ADMIN_TOKEN" && ( -n "$ACCESS_KEY" || -n "$SECRET_KEY" ) ]]; then
    echo "ERROR: --admin-token cannot be combined with --access-key/--secret-key" >&2
    exit 1
  fi
  if [[ -n "$ACCESS_KEY" && -z "$SECRET_KEY" || -z "$ACCESS_KEY" && -n "$SECRET_KEY" ]]; then
    echo "ERROR: --access-key and --secret-key must be provided together" >&2
    exit 1
  fi

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
  perl -0pi -e "s#__ACCESS_KEY__#${ACCESS_KEY//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__SECRET_KEY__#${SECRET_KEY//#/#}#g" "$COMMAND_FILE"
  perl -0pi -e "s#__AWS_SIGV4_SCOPE__#${AWS_SIGV4_SCOPE//#/#}#g" "$COMMAND_FILE"
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
  if [[ -n "$ADMIN_TOKEN" && ( -n "$ACCESS_KEY" || -n "$SECRET_KEY" ) ]]; then
    echo "ERROR: --admin-token cannot be combined with --access-key/--secret-key" >&2
    exit 1
  fi
  if [[ -n "$ACCESS_KEY" && -z "$SECRET_KEY" || -z "$ACCESS_KEY" && -n "$SECRET_KEY" ]]; then
    echo "ERROR: --access-key and --secret-key must be provided together" >&2
    exit 1
  fi

  main_matrix
  command_template

  echo "Generated runbook: ${RUNBOOK_FILE}"
}

main "$@"
