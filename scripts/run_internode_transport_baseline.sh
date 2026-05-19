#!/usr/bin/env bash
set -euo pipefail

# Internode transport baseline runner.
# Reuses scripts/run_object_batch_bench.sh and exports reproducible artifacts:
# - object benchmark summaries per scenario/workload/concurrency
# - optional internode operation metric deltas from a Prometheus text endpoint

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OBJECT_BENCH_SCRIPT="${PROJECT_ROOT}/scripts/run_object_batch_bench.sh"

TOOL="warp"
ACCESS_KEY=""
SECRET_KEY=""
REGION="us-east-1"
BUCKET_PREFIX="rustfs-internode-bench"
OUT_DIR=""
SIZES="4KiB,1MiB,16MiB,128MiB,1GiB"
CONCURRENCIES="1,16,64,128"
DURATION="90s"
SAMPLES=20000
INSECURE=false
EXTRA_ARGS=""
WARP_BIN="warp"
S3BENCH_BIN="s3bench"

# Scenario format: name=endpoint
SCENARIOS="local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001"

# Optional Prometheus text exposition URL.
# If empty, metrics delta collection is skipped.
INTERNODE_METRICS_URL=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_internode_transport_baseline.sh --access-key <ak> --secret-key <sk> [options]

Required:
  --access-key <ak>
  --secret-key <sk>

Optional:
  --tool <warp|s3bench>              Default: warp
  --region <name>                    Default: us-east-1
  --bucket-prefix <prefix>           Default: rustfs-internode-bench
  --scenarios <name=url,...>         Default: local=http://127.0.0.1:9000,distributed=http://127.0.0.1:9001
  --sizes <csv>                      Default: 4KiB,1MiB,16MiB,128MiB,1GiB
  --concurrencies <csv>              Default: 1,16,64,128
  --duration <dur>                   Default: 90s
  --samples <n>                      Default: 20000 (for s3bench)
  --warp-bin <path>                  Default: warp
  --s3bench-bin <path>               Default: s3bench
  --metrics-url <url>                Prometheus text endpoint for internode metrics delta
  --out-dir <dir>                    Default: target/bench/internode-transport-<timestamp>
  --extra-args "<args>"              Passed to run_object_batch_bench.sh --extra-args
  --insecure                         TLS insecure (self-signed)
  --dry-run                          Print commands only
  -h, --help

Notes:
  - This baseline covers S3 PUT/GET workloads and records internode metric deltas when --metrics-url is set.
  - Healing/replication-specific workloads should be run separately and appended to the same artifact directory.
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

DRY_RUN=false
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --tool) TOOL="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$2"; shift 2 ;;
      --scenarios) SCENARIOS="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --samples) SAMPLES="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --s3bench-bin) S3BENCH_BIN="$2"; shift 2 ;;
      --metrics-url) INTERNODE_METRICS_URL="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --extra-args) EXTRA_ARGS="$2"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
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

validate_args() {
  if [[ -z "${ACCESS_KEY}" || -z "${SECRET_KEY}" ]]; then
    echo "ERROR: --access-key and --secret-key are required" >&2
    exit 1
  fi
  if [[ "${TOOL}" != "warp" && "${TOOL}" != "s3bench" ]]; then
    echo "ERROR: --tool must be warp or s3bench" >&2
    exit 1
  fi
  if [[ -z "${SCENARIOS}" ]]; then
    echo "ERROR: --scenarios cannot be empty" >&2
    exit 1
  fi
}

setup_output() {
  if [[ -z "${OUT_DIR}" ]]; then
    OUT_DIR="target/bench/internode-transport-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "${OUT_DIR}"
  echo "scenario,endpoint,workload,concurrency,size,status,throughput,requests_per_sec,avg_latency,log_file,run_dir" > "${OUT_DIR}/summary.csv"
  echo "scenario,workload,concurrency,size,metric,operation,before,after,delta" > "${OUT_DIR}/internode_metric_deltas.csv"
}

collect_internode_snapshot() {
  local snapshot_file="$1"
  if [[ -z "${INTERNODE_METRICS_URL}" ]]; then
    : > "${snapshot_file}"
    return 0
  fi
  if [[ "${DRY_RUN}" == "true" ]]; then
    : > "${snapshot_file}"
    return 0
  fi
  if ! curl -fsSL "${INTERNODE_METRICS_URL}" > "${snapshot_file}"; then
    echo "WARN: failed to fetch metrics from ${INTERNODE_METRICS_URL}, skipping metrics delta for this run" >&2
    : > "${snapshot_file}"
  fi
}

extract_internode_rows() {
  local src="$1"
  if [[ ! -s "${src}" ]]; then
    return 0
  fi
  awk '
  $1 ~ /^rustfs_system_network_internode_operation_/ {
    metric = $1
    op = "all"
    if (match($0, /operation="[^"]+"/)) {
      op = substr($0, RSTART + 11, RLENGTH - 12)
    }
    n = split($0, parts, " ")
    value = parts[n]
    gsub(/[[:space:]]+/, "", value)
    if (value ~ /^[0-9]+([.][0-9]+)?$/) {
      print metric "," op "," value
    }
  }' "${src}"
}

append_metric_deltas() {
  local scenario="$1"
  local workload="$2"
  local conc="$3"
  local size="$4"
  local before_file="$5"
  local after_file="$6"

  local before_rows after_rows
  before_rows="$(mktemp)"
  after_rows="$(mktemp)"
  extract_internode_rows "${before_file}" > "${before_rows}"
  extract_internode_rows "${after_file}" > "${after_rows}"

  awk -F',' -v scenario="${scenario}" -v workload="${workload}" -v conc="${conc}" -v size="${size}" '
    FNR==NR {
      key = $1 SUBSEP $2
      before[key] = $3
      next
    }
    {
      key = $1 SUBSEP $2
      metric = $1
      operation = $2
      afterv = $3 + 0
      beforev = (key in before ? before[key] + 0 : 0)
      delta = afterv - beforev
      printf "%s,%s,%s,%s,%s,%s,%.0f,%.0f,%.0f\n", scenario, workload, conc, size, metric, operation, beforev, afterv, delta
    }
  ' "${before_rows}" "${after_rows}" >> "${OUT_DIR}/internode_metric_deltas.csv"

  rm -f "${before_rows}" "${after_rows}"
}

append_object_summary() {
  local scenario="$1"
  local endpoint="$2"
  local workload="$3"
  local conc="$4"
  local run_dir="$5"

  local src="${run_dir}/summary.csv"
  if [[ ! -f "${src}" ]]; then
    echo "WARN: missing summary file: ${src}" >&2
    return 0
  fi

  awk -F',' -v scenario="${scenario}" -v endpoint="${endpoint}" -v workload="${workload}" -v run_dir="${run_dir}" '
    NR == 1 { next }
    {
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", scenario, endpoint, workload, $3, $1, $4, $5, $6, $7, $8, run_dir
    }
  ' "${src}" >> "${OUT_DIR}/summary.csv"
}

run_workload() {
  local scenario="$1"
  local endpoint="$2"
  local workload="$3"
  local conc="$4"

  local run_dir="${OUT_DIR}/${scenario}/${workload}/concurrency-${conc}"
  local bucket="${BUCKET_PREFIX}-${scenario}-${workload}-c${conc}"
  local before_metrics="${run_dir}/metrics_before.prom"
  local after_metrics="${run_dir}/metrics_after.prom"

  mkdir -p "${run_dir}"
  collect_internode_snapshot "${before_metrics}"

  local cmd=(
    "${OBJECT_BENCH_SCRIPT}"
    --tool "${TOOL}"
    --endpoint "${endpoint}"
    --access-key "${ACCESS_KEY}"
    --secret-key "${SECRET_KEY}"
    --bucket "${bucket}"
    --region "${REGION}"
    --concurrency "${conc}"
    --sizes "${SIZES}"
    --out-dir "${run_dir}"
  )
  if [[ "${TOOL}" == "warp" ]]; then
    cmd+=(--duration "${DURATION}" --warp-mode "${workload}" --warp-bin "${WARP_BIN}")
  else
    cmd+=(--samples "${SAMPLES}" --s3bench-bin "${S3BENCH_BIN}")
  fi
  if [[ "${INSECURE}" == "true" ]]; then
    cmd+=(--insecure)
  fi
  if [[ -n "${EXTRA_ARGS}" ]]; then
    cmd+=(--extra-args "${EXTRA_ARGS}")
  fi
  if [[ "${DRY_RUN}" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  printf '==> scenario=%s workload=%s concurrency=%s endpoint=%s\n' "${scenario}" "${workload}" "${conc}" "${endpoint}"
  "${cmd[@]}"

  collect_internode_snapshot "${after_metrics}"
  append_object_summary "${scenario}" "${endpoint}" "${workload}" "${conc}" "${run_dir}"
  append_metric_deltas "${scenario}" "${workload}" "${conc}" "all_sizes" "${before_metrics}" "${after_metrics}"
}

run_all() {
  local scenario_pair scenario endpoint
  local workload conc

  IFS=',' read -r -a scenario_list <<< "${SCENARIOS}"
  IFS=',' read -r -a conc_list <<< "${CONCURRENCIES}"

  for scenario_pair in "${scenario_list[@]}"; do
    scenario="${scenario_pair%%=*}"
    endpoint="${scenario_pair#*=}"
    if [[ -z "${scenario}" || -z "${endpoint}" || "${scenario}" == "${endpoint}" ]]; then
      echo "ERROR: invalid scenario entry: ${scenario_pair} (expected name=url)" >&2
      exit 1
    fi

    for workload in put get; do
      for conc in "${conc_list[@]}"; do
        conc="$(echo "${conc}" | xargs)"
        if ! [[ "${conc}" =~ ^[0-9]+$ ]] || [[ "${conc}" -le 0 ]]; then
          echo "ERROR: invalid concurrency: ${conc}" >&2
          exit 1
        fi
        run_workload "${scenario}" "${endpoint}" "${workload}" "${conc}"
      done
    done
  done
}

main() {
  parse_args "$@"
  validate_args
  require_cmd awk
  require_cmd curl
  require_cmd rg
  if [[ ! -x "${OBJECT_BENCH_SCRIPT}" ]]; then
    echo "ERROR: benchmark script not executable: ${OBJECT_BENCH_SCRIPT}" >&2
    exit 1
  fi

  setup_output
  run_all

  echo "Artifacts:"
  echo "  ${OUT_DIR}/summary.csv"
  echo "  ${OUT_DIR}/internode_metric_deltas.csv"
}

main "$@"
