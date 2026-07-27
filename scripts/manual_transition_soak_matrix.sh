#!/usr/bin/env bash
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# software distributed under the License is distributed on an "AS IS" BASIS,
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
WINDOW_SPEC="nightly-2h:120:16:5000:balanced,nightly-12h:720:24:8000:write-heavy,nightly-24h:1440:32:12000:read-heavy"
SOAK_RATIOS="read-heavy:90:10,balanced:70:30,write-heavy:40:60"
WORKLOAD_SIZES="4KiB,1MiB,16MiB"
RUN_ADMIN_CHECKS=true
COST_BUDGET="$((24*1024))"
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_soak_matrix.sh --endpoint <admin-api> [options]

Required:
  --endpoint         Admin API base

Optional:
  --admin-token      Bearer token for admin calls
  --window-spec      CSV of <name:duration_min:concurrency:ops_per_minute:mix_name>
  --workload-sizes   Comma-separated object size set
  --soak-ratios      Comma-separated mix spec: label:read_pct:write_pct
  --cost-budget      Max budget units for each run, default 24576
  --out-dir          Output dir
  --no-admin-checks  Skip admin check command blocks
  --dry-run
  --help
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
      --window-spec) WINDOW_SPEC="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --workload-sizes) WORKLOAD_SIZES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --soak-ratios) SOAK_RATIOS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cost-budget) COST_BUDGET="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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

parse_window() {
  local spec="$1"
  local name duration_min concurrency ops_per_min mix_name
  name="$(echo "$spec" | cut -d':' -f1)"
  duration_min="$(echo "$spec" | cut -d':' -f2)"
  concurrency="$(echo "$spec" | cut -d':' -f3)"
  ops_per_min="$(echo "$spec" | cut -d':' -f4)"
  mix_name="$(echo "$spec" | cut -d':' -f5)"
  name="$(trim "$name")"
  duration_min="$(trim "$duration_min")"
  concurrency="$(trim "$concurrency")"
  ops_per_min="$(trim "$ops_per_min")"
  mix_name="$(trim "$mix_name")"
  if [[ -z "$name" || -z "$duration_min" || -z "$concurrency" || -z "$ops_per_min" || -z "$mix_name" ]]; then
    echo "ERROR: invalid window spec: $spec" >&2
    exit 1
  fi
  if ! [[ "$duration_min" =~ ^[0-9]+$ && "$concurrency" =~ ^[0-9]+$ && "$ops_per_min" =~ ^[0-9]+$ ]]; then
    echo "ERROR: window duration/concurrency/ops must be integers: $spec" >&2
    exit 1
  fi
  echo "$name|$duration_min|$concurrency|$ops_per_min|$mix_name"
}

parse_ratio() {
  local spec="$1"
  local name read_pct write_pct
  name="$(echo "$spec" | cut -d':' -f1)"
  read_pct="$(echo "$spec" | cut -d':' -f2)"
  write_pct="$(echo "$spec" | cut -d':' -f3)"
  name="$(trim "$name")"
  read_pct="$(trim "$read_pct")"
  write_pct="$(trim "$write_pct")"
  if [[ -z "$name" || -z "$read_pct" || -z "$write_pct" ]]; then
    echo "ERROR: invalid mix ratio: $spec" >&2
    exit 1
  fi
  if ! [[ "$read_pct" =~ ^[0-9]+$ && "$write_pct" =~ ^[0-9]+$ ]]; then
    echo "ERROR: mix percentages must be integers: $spec" >&2
    exit 1
  fi
  if (( read_pct + write_pct != 100 )); then
    echo "ERROR: read/write percentages must sum to 100: $spec" >&2
    exit 1
  fi
  echo "$name|$read_pct|$write_pct"
}

resolve_mix() {
  local name="$1"
  local line
  line="$(awk -v target="$name" -F: '{
    if ($1 == target) { print $0; exit }
  }' < <(tr ',' '\n' <<< "$SOAK_RATIOS"))"
  if [[ -z "$line" ]]; then
    return 1
  fi
  parse_ratio "$line"
}

run_checks() {
  local tag="$1"
  local job_id="$2"
  local token_hdr=""
  if [[ -z "$job_id" ]]; then
    job_id="<JOB_ID_${tag}>"
  fi
  if [[ -n "$ADMIN_TOKEN" ]]; then
    token_hdr="-H \"Authorization: Bearer \${ADMIN_TOKEN}\" "
  fi
  if [[ "$RUN_ADMIN_CHECKS" != true ]]; then
    return
  fi
  echo "# admin checks for ${tag}"
  echo "curl -sS ${token_hdr}-X GET \"${ENDPOINT%/}/rustfs/admin/v3/ilm/transition/jobs/${job_id}\" | jq '.status, .report, .queue_snapshot, .failure_reason'"
  echo "curl -sS ${token_hdr}-X GET \"${ENDPOINT%/}/rustfs/admin/v3/metrics?types=1&n=1\" | jq '.aggregated.scanner.lifecycle_transition'"
  printf './scripts/manual_transition_journal_audit.sh --endpoint %s --job-id "%s"' "$ENDPOINT" "$job_id"
  if [[ -n "$ADMIN_TOKEN" ]]; then
    printf " --admin-token \"\${ADMIN_TOKEN}\""
  fi
  printf '\n'
}

write_matrix_files() {
  local matrix_csv="${OUT_DIR}/nightly_soak_matrix.csv"
  local run_matrix="${OUT_DIR}/run_soak_matrix.sh"
  local notes="${OUT_DIR}/soak_notes.md"
  local run_id=1

  {
    echo "window,window_duration_min,concurrency,ops_per_min,mix_name,read_pct,write_pct,size,expected_ops,expected_run_id,run_check"
  } > "$matrix_csv"

  echo "#!/usr/bin/env bash" > "$run_matrix"
  echo "set -euo pipefail" >> "$run_matrix"
  echo "" >> "$run_matrix"

  {
    echo "# Nightly/night soak matrix for manual transition stress"
    echo "- endpoint: ${ENDPOINT}"
    echo "- cost-budget: ${COST_BUDGET}"
    echo ""
    echo "Recommended gate for each row: no unknown failure_reason spikes, job result drift is 0, and queued/task/result counts reconcile."
    echo ""
  } > "$notes"

  IFS=',' read -r -a sizes <<< "$WORKLOAD_SIZES"
  while IFS=',' read -r window_spec; do
    [[ -z "$window_spec" ]] && continue
    parsed="$(parse_window "$window_spec")"
    IFS='|' read -r window_name duration_min concurrency ops_per_min mix_name <<< "$parsed"

    mix="$(resolve_mix "$mix_name" || true)"
    if [[ -z "$mix" ]]; then
      echo "ERROR: unknown mix name in window spec: ${window_name} -> ${mix_name}" >&2
      exit 1
    fi
    IFS='|' read -r resolved_mix read_pct write_pct <<< "$mix"

    for size in "${sizes[@]}"; do
      size="$(trim "$size")"
      [[ -z "$size" ]] && continue
      expected_ops=$(( duration_min * ops_per_min ))
      if (( expected_ops > COST_BUDGET )); then
        budget_status="over-budget"
      else
        budget_status="within-budget"
      fi
      echo "${window_name},${duration_min},${concurrency},${ops_per_min},${resolved_mix},${read_pct},${write_pct},${size},${expected_ops},${run_id},${budget_status}" >> "$matrix_csv"
      {
        echo "# window=${window_name} size=${size} mix=${resolved_mix} concurrency=${concurrency} duration=${duration_min}m ops_per_min=${ops_per_min}"
        echo "job_id=\"<JOB_ID_${window_name}_${size}_${concurrency}_${run_id}>\""
        echo "read_pct=${read_pct}"
        echo "write_pct=${write_pct}"
        echo "expected_ops=${expected_ops}"
        echo "budget_status=${budget_status}"
        run_checks "${window_name}_${size}_${concurrency}_${run_id}" "<JOB_ID_${window_name}_${size}_${concurrency}_${run_id}>"
        echo
      } >> "$run_matrix"
      run_id=$((run_id + 1))
    done
  done < <(tr ',' '\n' <<< "$WINDOW_SPEC")

  {
    echo "# run target:"
    echo "bash ${run_matrix}"
  } >> "$notes"
}

main() {
  parse_args "$@"
  if [[ -z "$ENDPOINT" ]]; then
    echo "ERROR: --endpoint is required" >&2
    usage
    exit 1
  fi
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/manual-transition-nightly-soak/$(date +%Y%m%dT%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  require_cmd awk
  write_matrix_files
  if [[ "$DRY_RUN" == true ]]; then
    echo "[DRY-RUN] generated matrix files:"
  fi
  echo "Generated:"
  echo "  - ${OUT_DIR}/nightly_soak_matrix.csv"
  echo "  - ${OUT_DIR}/run_soak_matrix.sh"
  echo "  - ${OUT_DIR}/soak_notes.md"
}

main "$@"
