#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_COMPOSE="${CLUSTER_COMPOSE:-${PROJECT_ROOT}/.docker/compose/docker-compose.cluster.local-build.yml}"
OBS_COMPOSE="${OBS_COMPOSE:-${PROJECT_ROOT}/.docker/observability/docker-compose.yml}"
PROJECT_NAME="${PROJECT_NAME:-rustfs-four-node-test}"
IMAGE_TAG="${IMAGE_TAG:-rustfs/rustfs:local-4node}"
WITH_OBSERVABILITY="${WITH_OBSERVABILITY:-true}"
BUILD_LOCAL_IMAGE="${BUILD_LOCAL_IMAGE:-true}"
RUN_FAILOVER="${RUN_FAILOVER:-true}"
RUN_BENCHMARK="${RUN_BENCHMARK:-true}"
KEEP_UP="${KEEP_UP:-false}"

RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_OBS_ENDPOINT="${RUSTFS_OBS_ENDPOINT:-http://127.0.0.1:4318}"
RUSTFS_UNSAFE_BYPASS_DISK_CHECK="${RUSTFS_UNSAFE_BYPASS_DISK_CHECK:-true}"

WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
FAILOVER_NODE="${FAILOVER_NODE:-node4}"
FAILOVER_WARMUP_SECS="${FAILOVER_WARMUP_SECS:-5}"
FAILOVER_SAMPLE_SECS="${FAILOVER_SAMPLE_SECS:-60}"
FAILOVER_INTERVAL_SECS="${FAILOVER_INTERVAL_SECS:-1}"

BENCH_ENDPOINT="${BENCH_ENDPOINT:-http://127.0.0.1:9000}"
BENCH_BUCKET="${BENCH_BUCKET:-rustfs-four-node-bench}"
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-64}"
BENCH_DURATION="${BENCH_DURATION:-60s}"
BENCH_SIZES="${BENCH_SIZES:-1KiB,4KiB,11Mi}"

OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/bench/four-node-failover-$(date +%Y%m%d-%H%M%S)}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_four_node_cluster_failover_bench.sh [options]

Options:
  --cluster-compose <path>      4-node compose file
  --obs-compose <path>          observability compose file
  --project-name <name>         docker compose project name
  --image-tag <tag>             image tag to build/use
  --with-observability          bring up .docker/observability stack together
  --without-observability       only bring up 4-node cluster
  --skip-build                  skip docker build from Dockerfile.source
  --skip-failover               skip failover recovery validation
  --skip-bench                  skip benchmark phase
  --failover-node <nodeN>       node to stop during failover test (default: node4)
  --obs-endpoint <url>          RUSTFS_OBS_ENDPOINT (default: http://127.0.0.1:4318)
  --bench-endpoint <url>        benchmark endpoint (default: http://127.0.0.1:9000)
  --bench-sizes <sizes>         comma list (default: 1KiB,4KiB,11Mi)
  --bench-concurrency <n>       benchmark concurrency
  --bench-duration <dur>        benchmark duration
  --out-dir <path>              output directory
  --keep-up                     keep compose services running after script exits
  -h, --help                    show help

Environment:
  CLUSTER_COMPOSE OBS_COMPOSE PROJECT_NAME IMAGE_TAG
  WITH_OBSERVABILITY BUILD_LOCAL_IMAGE RUN_FAILOVER RUN_BENCHMARK KEEP_UP
  RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY RUSTFS_OBS_ENDPOINT
  WAIT_TIMEOUT_SECS FAILOVER_NODE FAILOVER_WARMUP_SECS FAILOVER_SAMPLE_SECS
  FAILOVER_INTERVAL_SECS BENCH_ENDPOINT BENCH_BUCKET BENCH_CONCURRENCY
  BENCH_DURATION BENCH_SIZES OUT_DIR
USAGE
}

log_info() {
  printf '[INFO] %s\n' "$*"
}

log_warn() {
  printf '[WARN] %s\n' "$*"
}

log_error() {
  printf '[ERROR] %s\n' "$*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

compose() {
  if [[ "${WITH_OBSERVABILITY}" == "true" ]]; then
    docker compose \
      --project-name "${PROJECT_NAME}" \
      --project-directory "${PROJECT_ROOT}" \
      -f "${OBS_COMPOSE}" \
      -f "${CLUSTER_COMPOSE}" \
      "$@"
  else
    docker compose \
      --project-name "${PROJECT_NAME}" \
      --project-directory "${PROJECT_ROOT}" \
      -f "${CLUSTER_COMPOSE}" \
      "$@"
  fi
}

resolve_bool() {
  local key="$1"
  local value="$2"
  case "${value}" in
    true|false) ;;
    *)
      log_error "invalid ${key}: ${value} (expected true|false)"
      exit 1
      ;;
  esac
}

node_port() {
  case "$1" in
    node1) echo "9000" ;;
    node2) echo "9001" ;;
    node3) echo "9002" ;;
    node4) echo "9003" ;;
    *)
      log_error "unknown node name: $1 (expected node1..node4)"
      exit 1
      ;;
  esac
}

wait_http_ok() {
  local url="$1"
  local start now
  start="$(date +%s)"

  while true; do
    if curl -fsS --connect-timeout 2 --max-time 3 "${url}" >/dev/null 2>&1; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= WAIT_TIMEOUT_SECS )); then
      log_error "timed out waiting for ${url}"
      return 1
    fi
    sleep 2
  done
}

wait_cluster_ready() {
  local port
  for port in 9000 9001 9002 9003; do
    wait_http_ok "http://127.0.0.1:${port}/health"
    wait_http_ok "http://127.0.0.1:${port}/health/ready"
  done
}

probe_survivors_ready() {
  local failover_port="$1"
  local port
  for port in 9000 9001 9002 9003; do
    if [[ "${port}" == "${failover_port}" ]]; then
      continue
    fi
    if ! curl -fsS --connect-timeout 1 --max-time 2 "http://127.0.0.1:${port}/health/ready" >/dev/null 2>&1; then
      return 1
    fi
  done
  return 0
}

run_failover_validation() {
  local failover_port
  local probe_file
  local summary_file
  local event_epoch
  local end_epoch
  local ts
  local first_fail
  local first_recover
  local recovery_secs

  failover_port="$(node_port "${FAILOVER_NODE}")"
  probe_file="${OUT_DIR}/failover-probe.csv"
  summary_file="${OUT_DIR}/failover-summary.txt"

  log_info "Running failover validation: stopping ${FAILOVER_NODE}"
  sleep "${FAILOVER_WARMUP_SECS}"

  compose stop "${FAILOVER_NODE}" >/dev/null
  event_epoch="$(date +%s)"
  end_epoch="$((event_epoch + FAILOVER_SAMPLE_SECS))"

  echo "timestamp_epoch,status" > "${probe_file}"
  while (( "$(date +%s)" <= end_epoch )); do
    ts="$(date +%s)"
    if probe_survivors_ready "${failover_port}"; then
      echo "${ts},ok" >> "${probe_file}"
    else
      echo "${ts},fail" >> "${probe_file}"
    fi
    sleep "${FAILOVER_INTERVAL_SECS}"
  done

  first_fail="$(awk -F',' 'NR>1 && $2=="fail" {print $1; exit}' "${probe_file}")"
  if [[ -z "${first_fail}" ]]; then
    recovery_secs="0"
    {
      echo "failover_node=${FAILOVER_NODE}"
      echo "outage_observed=false"
      echo "recovery_seconds=${recovery_secs}"
      echo "note=no survivor readiness interruption observed in probe window"
    } > "${summary_file}"
  else
    first_recover="$(awk -F',' -v fail_ts="${first_fail}" 'NR>1 && $1>fail_ts && $2=="ok" {print $1; exit}' "${probe_file}")"
    if [[ -z "${first_recover}" ]]; then
      {
        echo "failover_node=${FAILOVER_NODE}"
        echo "outage_observed=true"
        echo "recovery_seconds=unrecovered_within_${FAILOVER_SAMPLE_SECS}s"
        echo "first_fail_epoch=${first_fail}"
      } > "${summary_file}"
    else
      recovery_secs="$((first_recover - first_fail))"
      {
        echo "failover_node=${FAILOVER_NODE}"
        echo "outage_observed=true"
        echo "first_fail_epoch=${first_fail}"
        echo "first_recover_epoch=${first_recover}"
        echo "recovery_seconds=${recovery_secs}"
      } > "${summary_file}"
    fi
  fi

  log_info "Restarting ${FAILOVER_NODE}"
  compose start "${FAILOVER_NODE}" >/dev/null
  wait_http_ok "http://127.0.0.1:${failover_port}/health"
  wait_http_ok "http://127.0.0.1:${failover_port}/health/ready"
}

run_benchmark() {
  local bench_out_dir
  bench_out_dir="${OUT_DIR}/benchmark"
  mkdir -p "${bench_out_dir}"

  if ! command -v warp >/dev/null 2>&1; then
    log_error "warp is required for benchmark phase. Please install warp or run with --skip-bench."
    exit 1
  fi

  (
    cd "${PROJECT_ROOT}"
    ./scripts/run_object_batch_bench.sh \
      --tool warp \
      --endpoint "${BENCH_ENDPOINT}" \
      --access-key "${RUSTFS_ACCESS_KEY}" \
      --secret-key "${RUSTFS_SECRET_KEY}" \
      --bucket "${BENCH_BUCKET}" \
      --concurrency "${BENCH_CONCURRENCY}" \
      --duration "${BENCH_DURATION}" \
      --sizes "${BENCH_SIZES}" \
      --out-dir "${bench_out_dir}"
  )
}

cleanup() {
  if [[ "${KEEP_UP}" == "true" ]]; then
    log_info "KEEP_UP=true, leaving containers running"
    return
  fi

  log_info "Stopping compose services"
  compose down --remove-orphans >/dev/null 2>&1 || true
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cluster-compose)
        CLUSTER_COMPOSE="$2"
        shift 2
        ;;
      --obs-compose)
        OBS_COMPOSE="$2"
        shift 2
        ;;
      --project-name)
        PROJECT_NAME="$2"
        shift 2
        ;;
      --image-tag)
        IMAGE_TAG="$2"
        shift 2
        ;;
      --with-observability)
        WITH_OBSERVABILITY=true
        shift
        ;;
      --without-observability)
        WITH_OBSERVABILITY=false
        shift
        ;;
      --skip-build)
        BUILD_LOCAL_IMAGE=false
        shift
        ;;
      --skip-failover)
        RUN_FAILOVER=false
        shift
        ;;
      --skip-bench)
        RUN_BENCHMARK=false
        shift
        ;;
      --keep-up)
        KEEP_UP=true
        shift
        ;;
      --failover-node)
        FAILOVER_NODE="$2"
        shift 2
        ;;
      --obs-endpoint)
        RUSTFS_OBS_ENDPOINT="$2"
        shift 2
        ;;
      --bench-endpoint)
        BENCH_ENDPOINT="$2"
        shift 2
        ;;
      --bench-sizes)
        BENCH_SIZES="$2"
        shift 2
        ;;
      --bench-concurrency)
        BENCH_CONCURRENCY="$2"
        shift 2
        ;;
      --bench-duration)
        BENCH_DURATION="$2"
        shift 2
        ;;
      --out-dir)
        OUT_DIR="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "unknown argument: $1"
        usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_args "$@"

  resolve_bool "WITH_OBSERVABILITY" "${WITH_OBSERVABILITY}"
  resolve_bool "BUILD_LOCAL_IMAGE" "${BUILD_LOCAL_IMAGE}"
  resolve_bool "RUN_FAILOVER" "${RUN_FAILOVER}"
  resolve_bool "RUN_BENCHMARK" "${RUN_BENCHMARK}"
  resolve_bool "KEEP_UP" "${KEEP_UP}"

  require_cmd docker
  require_cmd curl
  require_cmd awk

  if [[ ! -f "${CLUSTER_COMPOSE}" ]]; then
    log_error "cluster compose file not found: ${CLUSTER_COMPOSE}"
    exit 1
  fi
  if [[ "${WITH_OBSERVABILITY}" == "true" && ! -f "${OBS_COMPOSE}" ]]; then
    log_error "observability compose file not found: ${OBS_COMPOSE}"
    exit 1
  fi

  if [[ "${RUSTFS_OBS_ENDPOINT}" == "http://127.0.0.1:4318" ]]; then
    log_warn "RUSTFS_OBS_ENDPOINT is set to container loopback default (${RUSTFS_OBS_ENDPOINT})."
    log_warn "If you need host collector routing, consider: --obs-endpoint http://host.docker.internal:4318"
  fi

  mkdir -p "${OUT_DIR}"

  trap cleanup EXIT INT TERM

  export RUSTFS_IMAGE="${IMAGE_TAG}"
  export RUSTFS_ACCESS_KEY
  export RUSTFS_SECRET_KEY
  export RUSTFS_OBS_ENDPOINT
  export RUSTFS_UNSAFE_BYPASS_DISK_CHECK

  if [[ "${BUILD_LOCAL_IMAGE}" == "true" ]]; then
    log_info "Building local image from Dockerfile.source: ${IMAGE_TAG}"
    docker build -f "${PROJECT_ROOT}/Dockerfile.source" -t "${IMAGE_TAG}" "${PROJECT_ROOT}"
  else
    log_info "Skipping image build"
  fi

  log_info "Starting compose stack"
  compose up -d

  log_info "Waiting for 4-node cluster readiness"
  wait_cluster_ready

  if [[ "${RUN_FAILOVER}" == "true" ]]; then
    run_failover_validation
  else
    log_info "Skipping failover validation"
  fi

  if [[ "${RUN_BENCHMARK}" == "true" ]]; then
    run_benchmark
  else
    log_info "Skipping benchmark"
  fi

  log_info "Validation finished"
  log_info "Artifacts directory: ${OUT_DIR}"
  log_info "Failover summary: ${OUT_DIR}/failover-summary.txt"
  log_info "Failover probe: ${OUT_DIR}/failover-probe.csv"
  log_info "Benchmark summary: ${OUT_DIR}/benchmark/summary.csv"
}

main "$@"
