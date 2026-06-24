#!/usr/bin/env bash
set -euo pipefail

# Helper artifact collector for rustfs/backlog#706 large-object PUT stage-breakdown runs.
# Designed to pair with scripts/run_put_large_stage_breakdown.sh and store
# supporting evidence under:
#   <run-root>/captures/<label>/
#
# Captures:
# - health and readiness snapshots
# - signed admin metrics snapshots per endpoint
# - optional plain Prometheus text snapshots
# - optional host telemetry (pidstat/iostat/mpstat)
# - per-sample process snapshots when a rustfs pid is available

RUN_ROOT=""
LABEL=""
ENDPOINT=""
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-}"
SECRET_KEY=""
SECRET_KEY_ENV="RUSTFS_SECRET_KEY"
REGION="us-east-1"
METRICS_ENDPOINTS=""
PROM_METRICS_URLS=""
DURATION_SECS=180
INTERVAL_SECS=15
RUSTFS_PID=""
SKIP_HOST_TELEMETRY=false
AWSCURL_BIN="awscurl"
CURL_BIN="curl"
JQ_BIN="jq"
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/collect_put_large_stage_breakdown_artifacts.sh --run-root <dir> \
    --endpoint <url> [options]

Required:
  --run-root <dir>                  Benchmark run root, usually from
                                    scripts/run_put_large_stage_breakdown.sh
  --endpoint <url>                  RustFS endpoint, e.g. http://127.0.0.1:9000

Auth / signed admin metrics:
  --access-key <ak>                 Override RUSTFS_ACCESS_KEY
  --secret-key-env <var>            Secret-key environment variable
                                    (default: RUSTFS_SECRET_KEY)
  --region <name>                   SigV4 region (default: us-east-1)

Capture options:
  --label <name>                    Capture label (default: capture-<timestamp>)
  --metrics-endpoints <csv>         Signed admin metrics endpoints. If omitted,
                                    defaults to --endpoint.
  --prom-metrics-urls <csv>         Optional plain Prometheus text endpoints
                                    captured with curl.
  --duration-secs <n>               Total capture duration (default: 180)
  --interval-secs <n>               Sample interval (default: 15)
  --rustfs-pid <pid>                RustFS pid. Auto-detect with pidof rustfs if omitted.
  --skip-host-telemetry             Skip pidstat/iostat/mpstat collection.
  --dry-run                         Print intended actions without live capture.

Output:
  <run-root>/captures/<label>/
    capture-meta.txt
    capture-layout.txt
    capture-report.md
    health/
    metrics/
    prom/
    host/
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
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
  printf '%s\n' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --run-root) RUN_ROOT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --label) LABEL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key-env) SECRET_KEY_ENV="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metrics-endpoints) METRICS_ENDPOINTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --prom-metrics-urls) PROM_METRICS_URLS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration-secs) DURATION_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --interval-secs) INTERVAL_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rustfs-pid) RUSTFS_PID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --skip-host-telemetry) SKIP_HOST_TELEMETRY=true; shift ;;
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

is_nonnegative_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

validate_args() {
  if [[ -z "$RUN_ROOT" || -z "$ENDPOINT" ]]; then
    echo "ERROR: --run-root and --endpoint are required" >&2
    exit 1
  fi
  if ! [[ "$SECRET_KEY_ENV" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "ERROR: --secret-key-env must be a valid environment variable name" >&2
    exit 1
  fi
  if ! is_nonnegative_integer "$DURATION_SECS" || ! is_positive_integer "$INTERVAL_SECS"; then
    echo "ERROR: --duration-secs must be >= 0 and --interval-secs must be > 0" >&2
    exit 1
  fi
  SECRET_KEY="${!SECRET_KEY_ENV:-}"
  if [[ -z "$LABEL" ]]; then
    LABEL="capture-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
}

setup_output() {
  CAPTURE_ROOT="${RUN_ROOT}/captures/${LABEL}"
  HEALTH_DIR="${CAPTURE_ROOT}/health"
  METRICS_DIR="${CAPTURE_ROOT}/metrics"
  PROM_DIR="${CAPTURE_ROOT}/prom"
  HOST_DIR="${CAPTURE_ROOT}/host"
  mkdir -p "$HEALTH_DIR" "$METRICS_DIR" "$PROM_DIR" "$HOST_DIR"
}

endpoint_label() {
  local endpoint="$1"
  local label
  label="${endpoint#*://}"
  label="${label%%/*}"
  printf '%s' "$label" | tr -c 'A-Za-z0-9._-' '_'
}

csv_to_lines() {
  local csv="$1"
  local raw item
  IFS=',' read -r -a arr <<< "$csv"
  for raw in "${arr[@]}"; do
    item="$(echo "$raw" | awk '{$1=$1;print}')"
    [[ -z "$item" ]] && continue
    echo "$item"
  done
}

resolve_metrics_endpoints() {
  if [[ -n "$METRICS_ENDPOINTS" ]]; then
    csv_to_lines "$METRICS_ENDPOINTS"
  else
    printf '%s\n' "$ENDPOINT"
  fi
}

resolve_pid() {
  if [[ -n "$RUSTFS_PID" ]]; then
    echo "$RUSTFS_PID"
    return
  fi
  pidof rustfs 2>/dev/null | awk '{print $1}' || true
}

health_url() {
  printf '%s/health\n' "${1%/}"
}

ready_url() {
  printf '%s/health/ready\n' "${1%/}"
}

admin_metrics_url() {
  printf '%s/rustfs/admin/v3/metrics?types=1&by-host=true&n=1\n' "${1%/}"
}

write_layout_file() {
  cat > "${CAPTURE_ROOT}/capture-layout.txt" <<'EOF'
capture-meta.txt
capture-layout.txt
capture-report.md

health/
- health.<endpoint>.<index>.<timestamp>.txt
- ready.<endpoint>.<index>.<timestamp>.txt

metrics/
- admin-metrics.<endpoint>.<index>.<timestamp>.ndjson

prom/
- prometheus.<endpoint>.<index>.<timestamp>.prom

host/
- pidstat.txt
- iostat.txt
- mpstat.txt
- proc-status.<index>.<timestamp>.txt
- proc-io.<index>.<timestamp>.txt
- ps.<index>.<timestamp>.txt
EOF
}

write_meta_file() {
  local git_commit git_branch
  git_commit="$(git rev-parse HEAD 2>/dev/null || echo "unknown")"
  git_branch="$(git branch --show-current 2>/dev/null || echo "unknown")"
  cat > "${CAPTURE_ROOT}/capture-meta.txt" <<EOF
created_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_root=${RUN_ROOT}
label=${LABEL}
endpoint=${ENDPOINT}
metrics_endpoints=${METRICS_ENDPOINTS:-$ENDPOINT}
prom_metrics_urls=${PROM_METRICS_URLS:-N/A}
region=${REGION}
duration_secs=${DURATION_SECS}
interval_secs=${INTERVAL_SECS}
rustfs_pid=${RUSTFS_PID:-auto}
resolved_pid=${RESOLVED_PID:-N/A}
skip_host_telemetry=${SKIP_HOST_TELEMETRY}
dry_run=${DRY_RUN}
git_branch=${git_branch}
git_commit=${git_commit}
access_key_present=$([[ -n "$ACCESS_KEY" ]] && echo true || echo false)
secret_key_env=${SECRET_KEY_ENV}
EOF
}

capture_health_sample() {
  local index="$1"
  local ts="$2"
  local endpoint="$3"
  local label file
  label="$(endpoint_label "$endpoint")"

  file="${HEALTH_DIR}/health.${label}.${index}.${ts}.txt"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "dry run" > "$file"
  else
    "$CURL_BIN" -fsS "$(health_url "$endpoint")" > "$file" 2>&1 || true
  fi

  file="${HEALTH_DIR}/ready.${label}.${index}.${ts}.txt"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "dry run" > "$file"
  else
    "$CURL_BIN" -fsS "$(ready_url "$endpoint")" > "$file" 2>&1 || true
  fi
}

capture_admin_metrics_sample() {
  local index="$1"
  local ts="$2"
  local endpoint="$3"
  local label file
  label="$(endpoint_label "$endpoint")"
  file="${METRICS_DIR}/admin-metrics.${label}.${index}.${ts}.ndjson"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "dry run" > "$file"
    return
  fi

  if [[ -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "missing access/secret for signed metrics capture" > "$file"
    return
  fi

  AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
  AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
  AWS_DEFAULT_REGION="$REGION" \
  "$AWSCURL_BIN" \
    --service s3 \
    --region "$REGION" \
    --request GET \
    "$(admin_metrics_url "$endpoint")" \
    > "$file" 2>&1 || true
}

capture_prometheus_sample() {
  local index="$1"
  local ts="$2"
  local endpoint="$3"
  local label file
  label="$(endpoint_label "$endpoint")"
  file="${PROM_DIR}/prometheus.${label}.${index}.${ts}.prom"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "dry run" > "$file"
  else
    "$CURL_BIN" -fsS "$endpoint" > "$file" 2>&1 || true
  fi
}

capture_proc_snapshot() {
  local index="$1"
  local ts="$2"
  local pid="$3"
  [[ -n "$pid" ]] || return 0

  if [[ -r "/proc/${pid}/status" ]]; then
    cp "/proc/${pid}/status" "${HOST_DIR}/proc-status.${index}.${ts}.txt" || true
  fi
  if [[ -r "/proc/${pid}/io" ]]; then
    cp "/proc/${pid}/io" "${HOST_DIR}/proc-io.${index}.${ts}.txt" || true
  fi
  if command -v ps >/dev/null 2>&1; then
    ps -p "$pid" -o pid,ppid,stat,pcpu,pmem,rss,vsz,etime,args > "${HOST_DIR}/ps.${index}.${ts}.txt" 2>&1 || true
  fi
}

start_host_telemetry() {
  HOST_TELEMETRY_PIDS=()
  if [[ "$SKIP_HOST_TELEMETRY" == "true" ]]; then
    return
  fi

  local count
  count=$(( DURATION_SECS / INTERVAL_SECS + 1 ))
  (( count < 1 )) && count=1

  if [[ -n "$RESOLVED_PID" ]] && command -v pidstat >/dev/null 2>&1; then
    pidstat -durwh -p "$RESOLVED_PID" "$INTERVAL_SECS" "$count" > "${HOST_DIR}/pidstat.txt" 2>&1 &
    HOST_TELEMETRY_PIDS+=("$!")
  fi

  if command -v iostat >/dev/null 2>&1; then
    iostat -xz "$INTERVAL_SECS" "$count" > "${HOST_DIR}/iostat.txt" 2>&1 &
    HOST_TELEMETRY_PIDS+=("$!")
  fi

  if command -v mpstat >/dev/null 2>&1; then
    mpstat "$INTERVAL_SECS" "$count" > "${HOST_DIR}/mpstat.txt" 2>&1 &
    HOST_TELEMETRY_PIDS+=("$!")
  fi
}

wait_host_telemetry() {
  local pid
  for pid in "${HOST_TELEMETRY_PIDS[@]:-}"; do
    wait "$pid" || true
  done
}

capture_one_sample() {
  local index="$1"
  local ts endpoint prom_url
  ts="$(date -u +%Y%m%dT%H%M%SZ)"

  while IFS= read -r endpoint; do
    capture_health_sample "$index" "$ts" "$endpoint"
    capture_admin_metrics_sample "$index" "$ts" "$endpoint"
  done < <(resolve_metrics_endpoints)

  if [[ -n "$PROM_METRICS_URLS" ]]; then
    while IFS= read -r prom_url; do
      capture_prometheus_sample "$index" "$ts" "$prom_url"
    done < <(csv_to_lines "$PROM_METRICS_URLS")
  fi

  if [[ "$DRY_RUN" != "true" ]]; then
    capture_proc_snapshot "$index" "$ts" "$RESOLVED_PID"
  fi
}

capture_series() {
  local total_samples index
  total_samples=$(( DURATION_SECS / INTERVAL_SECS + 1 ))
  (( total_samples < 1 )) && total_samples=1

  for ((index = 1; index <= total_samples; index++)); do
    capture_one_sample "$index"
    if (( index < total_samples )); then
      sleep "$INTERVAL_SECS"
    fi
  done
}

count_artifacts() {
  local dir="$1"
  local pattern="$2"
  find "$dir" -type f -name "$pattern" 2>/dev/null | wc -l | tr -d ' '
}

write_report() {
  local health_count metrics_count prom_count proc_count
  health_count="$(count_artifacts "$HEALTH_DIR" 'health.*.txt')"
  metrics_count="$(count_artifacts "$METRICS_DIR" 'admin-metrics.*.ndjson')"
  prom_count="$(count_artifacts "$PROM_DIR" 'prometheus.*.prom')"
  proc_count="$(count_artifacts "$HOST_DIR" 'proc-status.*.txt')"

  cat > "${CAPTURE_ROOT}/capture-report.md" <<EOF
## Large PUT Stage-Breakdown Capture Report

Run root: ${RUN_ROOT}
Label: ${LABEL}
Endpoint: ${ENDPOINT}
Duration seconds: ${DURATION_SECS}
Interval seconds: ${INTERVAL_SECS}
Resolved pid: ${RESOLVED_PID:-N/A}

## Artifact Summary

- Health snapshots: ${health_count}
- Signed admin metrics snapshots: ${metrics_count}
- Plain Prometheus snapshots: ${prom_count}
- Process snapshots: ${proc_count}
- Host telemetry present: $([[ -s "${HOST_DIR}/pidstat.txt" || -s "${HOST_DIR}/iostat.txt" || -s "${HOST_DIR}/mpstat.txt" ]] && echo yes || echo no)

## Review Checklist

- Align capture timestamps with benchmark windows in \`runs/cXX/\`.
- Check whether health or readiness degraded during the benchmark window.
- Compare admin metrics snapshots with the Prometheus / Grafana queries used for stage interpretation.
- Attach pidstat/iostat/mpstat when evaluating CPU, RSS, and disk pressure.
- Use the capture directory together with \`aggregate_median_summary.csv\` when filling the stage-breakdown report template.
EOF
}

main() {
  parse_args "$@"
  validate_args
  require_cmd bash
  require_cmd "$CURL_BIN"
  require_cmd awk
  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd date
  fi
  if [[ "$DRY_RUN" != "true" && -n "$ACCESS_KEY" && -n "$SECRET_KEY" ]]; then
    require_cmd "$AWSCURL_BIN"
  fi

  setup_output
  RESOLVED_PID="$(resolve_pid)"
  write_layout_file
  write_meta_file
  start_host_telemetry
  capture_series
  wait_host_telemetry
  write_report

  echo "Capture artifacts written to ${CAPTURE_ROOT}"
}

main "$@"
