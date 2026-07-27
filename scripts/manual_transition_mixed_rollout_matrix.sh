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
ADMIN_TOKEN=""
OUT_DIR=""
PHASE_MATRIX="request-only:100:0:30,canary:90:10:120,mixed:50:50:240,full-rollout:0:100:360,rollback:100:0:30"
CONCURRENCIES="8,16,32"
OBJECT_COUNTS="5k,20k,50k"
JOB_BUCKET="manual-transition"
JOB_PREFIX="journal-mixed-rollout"
READ_RATIO="90"
RUN_ADMIN_CHECKS=true
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_mixed_rollout_matrix.sh --endpoint <admin-api> [options]

Required:
  --endpoint         Admin API base, e.g. https://127.0.0.1:9000

Optional:
  --admin-token      Bearer token for admin calls
  --phase-matrix     Comma-separated phase spec: name:old_pct:new_pct:duration_min
  --concurrencies    Comma-separated concurrency list
  --object-counts    Comma-separated queue-size list (for plan rows)
  --job-bucket       Manual transition source bucket for sample commands
  --job-prefix       Prefix for generated job notes
  --read-ratio       Target read ratio percentage for mixed workload
  --no-admin-checks  Skip generated admin check commands
  --out-dir          Output dir
  --dry-run
  --help

Examples:
  scripts/manual_transition_mixed_rollout_matrix.sh \
    --endpoint https://127.0.0.1:9000 \
    --admin-token "$TOKEN" \
    --phase-matrix "request-only:100:0:30,canary:90:10:120,full:0:100:240"
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
    echo "ERROR: command not found: $cmd" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --admin-token) ADMIN_TOKEN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --phase-matrix) PHASE_MATRIX="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --object-counts) OBJECT_COUNTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --job-bucket) JOB_BUCKET="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --job-prefix) JOB_PREFIX="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --read-ratio) READ_RATIO="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --no-admin-checks) RUN_ADMIN_CHECKS=false; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

parse_phase_row() {
  local spec="$1"
  local name old_ratio new_ratio duration_min

  name="$(echo "$spec" | cut -d':' -f1)"
  old_ratio="$(echo "$spec" | cut -d':' -f2)"
  new_ratio="$(echo "$spec" | cut -d':' -f3)"
  duration_min="$(echo "$spec" | cut -d':' -f4)"
  name="$(trim "$name")"
  old_ratio="$(trim "$old_ratio")"
  new_ratio="$(trim "$new_ratio")"
  duration_min="$(trim "$duration_min")"

  if [[ -z "$name" || -z "$old_ratio" || -z "$new_ratio" || -z "$duration_min" ]]; then
    echo "ERROR: invalid phase spec: $spec" >&2
    exit 1
  fi
  if ! [[ "$old_ratio" =~ ^[0-9]+$ && "$new_ratio" =~ ^[0-9]+$ && "$duration_min" =~ ^[0-9]+$ ]]; then
    echo "ERROR: phase ratio/duration must be integers: $spec" >&2
    exit 1
  fi
  echo "$name|$old_ratio|$new_ratio|$duration_min"
}

gate_for_phase() {
  case "$1" in
    request-only)
      echo "No mixed-version errors; unknown failures should stay 0 in job report; log ratio must trend down."
      ;;
    canary)
      echo "old/new mismatch in worker results = 0; no Unknown job failures."
      ;;
    mixed)
      echo "request-only + canary acceptance gates both pass for 2+ windows, then proceed."
      ;;
    full-rollout|full)
      echo "full canary-to-full transition completed, no tier_failure in report, worker_result mismatch = 0."
      ;;
    rollback)
      echo "all rollout checks reverse cleanly; queued -> completed in job terminal state."
      ;;
    *)
      echo "run admin job status + journal reconcile + metric delta sanity check."
      ;;
  esac
}

admin_check_cmd() {
  local phase="$1"
  local job_id_ref="$2"
  local metric_types="$3"
  local metric_samples="$4"
  local metric_path
  local job_url
  local metric_url
  local job_filter='.report, .queue_snapshot, .failure_reason'
  local metric_filter='.aggregated.scanner.lifecycle_transition'

  if [[ "$RUN_ADMIN_CHECKS" != true ]]; then
    return
  fi
  metric_path="/rustfs/admin/v3/metrics?types=${metric_types}&n=${metric_samples}"
  job_url="${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id_ref}"
  metric_url="${ENDPOINT%/}${metric_path}"

  {
    printf '# check-%s\n' "$phase"
    printf 'curl -sS'
    if [[ -n "$ADMIN_TOKEN" ]]; then
      printf " -H \"Authorization: Bearer \${ADMIN_TOKEN}\""
    fi
    printf ' -X GET "%s" | jq '\''%s'\''\n' "$job_url" "$job_filter"
    printf 'curl -sS'
    if [[ -n "$ADMIN_TOKEN" ]]; then
      printf " -H \"Authorization: Bearer \${ADMIN_TOKEN}\""
    fi
    printf ' -X GET "%s" | jq '\''%s'\''\n' "$metric_url" "$metric_filter"
    printf './scripts/manual_transition_journal_audit.sh --endpoint %s --job-id "%s"' "$ENDPOINT" "$job_id_ref"
    if [[ -n "$ADMIN_TOKEN" ]]; then
      printf " --admin-token \"\${ADMIN_TOKEN}\""
    fi
    printf ' --sys-bucket .rustfs.sys\n'
  }
}

run_rows() {
  local matrix_csv="${OUT_DIR}/mixed_rollout_matrix.csv"
  local run_script="${OUT_DIR}/run_phase_commands.sh"
  local checklist="${OUT_DIR}/mixed_rollout_checklist.md"
  local command_manifest="${OUT_DIR}/run_phase_notes.txt"
  local admin_check
  local admin_check_csv
  local job_id_ref

  {
    echo "phase,old_pct,new_pct,duration_min,concurrency,object_count,read_ratio,gate,admin_check"
  } > "$matrix_csv"

  {
    echo "#!/usr/bin/env bash"
    echo "set -euo pipefail"
    echo ""
    echo "# generated matrix commands: fill in JOB_ID after each manual transition run"
    echo ""
  } > "$run_script"

  {
    echo "# mixed-version rollout command template"
    echo "Endpoint: ${ENDPOINT}"
    echo "Bucket: ${JOB_BUCKET}"
    echo "Prefix: ${JOB_PREFIX}"
    echo "Read ratio: ${READ_RATIO}%"
    echo ""
  } > "$checklist"

  while IFS=',' read -r phase_spec; do
    [[ -z "$phase_spec" ]] && continue
    parsed="$(parse_phase_row "$phase_spec")"
    IFS='|' read -r phase_name old_ratio new_ratio duration_min <<< "$parsed"

    gate="$(gate_for_phase "$phase_name")"
    IFS=',' read -r -a counts <<< "$OBJECT_COUNTS"
    IFS=',' read -r -a concs <<< "$CONCURRENCIES"
    for conc in "${concs[@]}"; do
      conc="$(trim "$conc")"
      [[ -z "$conc" ]] && continue
      for count in "${counts[@]}"; do
        count="$(trim "$count")"
        [[ -z "$count" || -z "$conc" ]] && continue

        job_id_ref="<JOB_ID_${phase_name}_c${conc}_q${count}>"
        admin_check="$(admin_check_cmd "$phase_name" "$job_id_ref" 1 1 | tr '\n' ';')"
        admin_check_csv="${admin_check//\"/\"\"}"
        echo "${phase_name},${old_ratio},${new_ratio},${duration_min},${conc},${count},${READ_RATIO},\"${gate}\",\"${admin_check_csv}\"" >> "$matrix_csv"

        {
          echo "# phase: ${phase_name}  concurrency: ${conc}  object_count: ${count}"
          echo "# old:new = ${old_ratio}:${new_ratio}, duration=${duration_min}min"
          printf './scripts/manual_transition_journal_audit.sh --endpoint %s --job-id "%s"' "$ENDPOINT" "$job_id_ref"
          if [[ -n "$ADMIN_TOKEN" ]]; then
            printf " --admin-token \"\${ADMIN_TOKEN}\""
          fi
          printf '\n'
          echo ""
        } >> "$run_script"
      done
    done
  done < <(tr ',' '\n' <<< "$PHASE_MATRIX")

  {
    echo "## Mixed-version rollout matrix artifacts"
    echo ""
    echo "- matrix: ${matrix_csv}"
    echo "- commands: ${run_script}"
    echo "- checklist: ${checklist}"
  } > "$command_manifest"
}

main() {
  parse_args "$@"
  if [[ -z "$ENDPOINT" ]]; then
    echo "ERROR: --endpoint is required" >&2
    usage
    exit 1
  fi
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-mixed-rollout/$(date +%Y%m%dT%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  require_cmd awk
  run_rows

  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] generated files:"
  fi
  echo "Generated:"
  echo "  - ${OUT_DIR}/mixed_rollout_matrix.csv"
  echo "  - ${OUT_DIR}/run_phase_commands.sh"
  echo "  - ${OUT_DIR}/mixed_rollout_checklist.md"
  echo "  - ${OUT_DIR}/run_phase_notes.txt"
}

main "$@"
