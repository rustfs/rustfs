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
PRECHECK_AUTO_CLEANUP="${PRECHECK_AUTO_CLEANUP:-true}"
WAIT_PROBE_MODE="${WAIT_PROBE_MODE:-service}"

RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_OBS_ENDPOINT="${RUSTFS_OBS_ENDPOINT:-}"
RUSTFS_UNSAFE_BYPASS_DISK_CHECK="${RUSTFS_UNSAFE_BYPASS_DISK_CHECK:-true}"

WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
BENCH_READY_TIMEOUT_SECS="${BENCH_READY_TIMEOUT_SECS:-180}"
FAILOVER_NODE="${FAILOVER_NODE:-node4}"
FAILOVER_WARMUP_SECS="${FAILOVER_WARMUP_SECS:-5}"
FAILOVER_SAMPLE_SECS="${FAILOVER_SAMPLE_SECS:-60}"
FAILOVER_INTERVAL_SECS="${FAILOVER_INTERVAL_SECS:-1}"
BENCH_WAIT_MODE="${BENCH_WAIT_MODE:-ready}"

BENCH_ENDPOINT="${BENCH_ENDPOINT:-http://127.0.0.1:9000}"
BENCH_BUCKET="${BENCH_BUCKET:-rustfs-four-node-bench}"
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-}"
BENCH_CONCURRENCIES="${BENCH_CONCURRENCIES:-}"
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
  --obs-endpoint <url>          RUSTFS_OBS_ENDPOINT (default: auto-select by mode)
  --bench-endpoint <url>        benchmark endpoint (default: http://127.0.0.1:9000)
  --bench-sizes <sizes>         comma list (default: 1KiB,4KiB,11Mi)
  --bench-concurrency <n>       benchmark concurrency
  --bench-concurrencies <list>  benchmark concurrency list (default: 8,16,32,64,128)
  --bench-duration <dur>        benchmark duration
  --out-dir <path>              output directory
  --keep-up                     keep compose services running after script exits
  -h, --help                    show help

Environment:
  CLUSTER_COMPOSE OBS_COMPOSE PROJECT_NAME IMAGE_TAG
  WITH_OBSERVABILITY BUILD_LOCAL_IMAGE RUN_FAILOVER RUN_BENCHMARK KEEP_UP
  RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY RUSTFS_OBS_ENDPOINT
  PRECHECK_AUTO_CLEANUP (true|false, default: true)
  WAIT_PROBE_MODE (service|ready, default: service)
  WAIT_TIMEOUT_SECS FAILOVER_NODE FAILOVER_WARMUP_SECS FAILOVER_SAMPLE_SECS
  FAILOVER_INTERVAL_SECS BENCH_ENDPOINT BENCH_BUCKET BENCH_CONCURRENCY
  BENCH_CONCURRENCIES BENCH_DURATION BENCH_SIZES OUT_DIR
  BENCH_WAIT_MODE (ready|service, default: ready)
  BENCH_READY_TIMEOUT_SECS (default: 180)
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
      -f "${OBS_COMPOSE}" \
      -f "${CLUSTER_COMPOSE}" \
      "$@"
  else
    docker compose \
      --project-name "${PROJECT_NAME}" \
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

resolve_probe_mode() {
  case "${WAIT_PROBE_MODE}" in
    service|ready) ;;
    *)
      log_error "invalid WAIT_PROBE_MODE: ${WAIT_PROBE_MODE} (expected service|ready)"
      exit 1
      ;;
  esac
}

resolve_bench_wait_mode() {
  case "${BENCH_WAIT_MODE}" in
    ready|service) ;;
    *)
      log_error "invalid BENCH_WAIT_MODE: ${BENCH_WAIT_MODE} (expected ready|service)"
      exit 1
      ;;
  esac
}

resolve_bench_concurrency() {
  if [[ -n "${BENCH_CONCURRENCIES}" && -n "${BENCH_CONCURRENCY}" && "${BENCH_CONCURRENCIES}" != "${BENCH_CONCURRENCY}" ]]; then
    log_warn "BENCH_CONCURRENCY is ignored because BENCH_CONCURRENCIES is set"
    return
  fi

  if [[ -n "${BENCH_CONCURRENCIES}" ]]; then
    return
  fi

  if [[ -n "${BENCH_CONCURRENCY}" ]]; then
    BENCH_CONCURRENCIES="${BENCH_CONCURRENCY}"
    return
  fi

#  BENCH_CONCURRENCIES="8,16,32,64,128"
  BENCH_CONCURRENCIES="8,16"
}

cluster_compose_uses_otel_network() {
  # Detect whether any service in cluster compose joins otel-network.
  grep -Eq '^[[:space:]]*-[[:space:]]*otel-network([[:space:]]*#.*)?$' "${CLUSTER_COMPOSE}"
}

obs_compose_has_otel_collector() {
  grep -Eq '^[[:space:]]*otel-collector:[[:space:]]*$' "${OBS_COMPOSE}"
}

resolve_default_obs_endpoint() {
  if [[ "${WITH_OBSERVABILITY}" != "true" ]]; then
    RUSTFS_OBS_ENDPOINT="http://host.docker.internal:4318"
    log_info "Auto-selected RUSTFS_OBS_ENDPOINT=${RUSTFS_OBS_ENDPOINT} (observability stack disabled)"
    return
  fi

  if cluster_compose_uses_otel_network && obs_compose_has_otel_collector; then
    RUSTFS_OBS_ENDPOINT="http://otel-collector:4318"
    log_info "Auto-selected RUSTFS_OBS_ENDPOINT=${RUSTFS_OBS_ENDPOINT} (shared docker network detected)"
    return
  fi

  RUSTFS_OBS_ENDPOINT="http://host.docker.internal:4318"
  log_info "Auto-selected RUSTFS_OBS_ENDPOINT=${RUSTFS_OBS_ENDPOINT} (cross-network fallback)"
}

docker_daemon_ready() {
  docker info >/dev/null 2>&1
}

port_is_occupied() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi

  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :${port}" 2>/dev/null | awk 'NR>1 {found=1} END{exit found?0:1}'
    return $?
  fi

  if command -v netstat >/dev/null 2>&1; then
    netstat -an 2>/dev/null | grep -E "[\.\:]${port}[[:space:]].*LISTEN" >/dev/null 2>&1
    return $?
  fi

  # Fallback: no tool available; treat as unknown (not occupied) and rely on compose failure.
  return 1
}

print_port_owner() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null | awk 'NR==1 || NR==2 {print "    " $0}'
    return
  fi

  if command -v ss >/dev/null 2>&1; then
    ss -ltnp "sport = :${port}" 2>/dev/null | awk 'NR==1 || NR==2 {print "    " $0}'
  fi
}

cleanup_existing_project_containers() {
  local existing_ids
  existing_ids="$(docker ps -aq --filter "label=com.docker.compose.project=${PROJECT_NAME}")"

  if [[ -z "${existing_ids}" ]]; then
    return 0
  fi

  log_warn "Found existing containers for project ${PROJECT_NAME}."
  docker ps -a --filter "label=com.docker.compose.project=${PROJECT_NAME}" --format '  - {{.Names}} ({{.Status}})'

  if [[ "${PRECHECK_AUTO_CLEANUP}" == "true" ]]; then
    log_info "PRECHECK_AUTO_CLEANUP=true, removing existing project containers."
    # shellcheck disable=SC2086
    docker rm -f ${existing_ids} >/dev/null
  else
    log_error "existing project containers detected and PRECHECK_AUTO_CLEANUP=false"
    log_error "run docker compose down --remove-orphans first, or set PRECHECK_AUTO_CLEANUP=true"
    exit 1
  fi
}

check_required_ports_free() {
  local required_ports=(
    9000 9001 9002 9003
  )
  local occupied_ports=()
  local port

  if [[ "${WITH_OBSERVABILITY}" == "true" ]]; then
    required_ports+=(
      1888 3000 3100 3200 4040 4317 4318 55679 8888 8889 9090 13133 14269 16686
    )
  fi

  for port in "${required_ports[@]}"; do
    if port_is_occupied "${port}"; then
      occupied_ports+=("${port}")
    fi
  done

  if [[ "${#occupied_ports[@]}" -gt 0 ]]; then
    log_error "required host ports are occupied: ${occupied_ports[*]}"
    for port in "${occupied_ports[@]}"; do
      print_port_owner "${port}" || true
    done
    log_error "free these ports or run with a different compose/profile before retrying"
    exit 1
  fi
}

ensure_runtime_image_exists() {
  if ! docker image inspect "${IMAGE_TAG}" >/dev/null 2>&1; then
    log_error "image not found: ${IMAGE_TAG}"
    log_error "build it first or rerun without --skip-build"
    exit 1
  fi
}

check_cluster_volumes_writable() {
  local node_idx
  local disk_idx
  local volume_name

  log_info "Checking cluster data volumes writable"
  # Do not pre-create compose-managed volumes here.
  # If we create them via plain docker run, compose will warn:
  # "already exists but was not created by Docker Compose".
  for node_idx in 1 2 3 4; do
    for disk_idx in 1 2 3 4; do
      volume_name="${PROJECT_NAME}_node${node_idx}_data_${disk_idx}"
      if ! docker volume inspect "${volume_name}" >/dev/null 2>&1; then
        log_info "volume not present yet (will be created by compose): ${volume_name}"
        continue
      fi
      if ! docker run --rm --entrypoint sh -v "${volume_name}:/probe" "${IMAGE_TAG}" -c \
        'set -e; touch /probe/.rwtest; rm -f /probe/.rwtest' >/dev/null 2>&1; then
        log_error "volume write check failed: ${volume_name}"
        exit 1
      fi
    done
  done
}

run_precheck_before_build() {
  log_info "Running precheck: docker daemon, residue containers, host ports"

  if ! docker_daemon_ready; then
    log_error "cannot connect to docker daemon (permission or runtime not ready)"
    exit 1
  fi

  cleanup_existing_project_containers
  check_required_ports_free
}

run_precheck_after_build() {
  log_info "Running precheck: image exists, cluster volumes writable"
  ensure_runtime_image_exists
  check_cluster_volumes_writable
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

probe_node_service_ok() {
  local port="$1"
  local health_code root_code

  health_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "http://127.0.0.1:${port}/health" || true)"
  if [[ "${health_code}" != "200" ]]; then
    return 1
  fi

  if [[ "${WAIT_PROBE_MODE}" == "ready" ]]; then
    local ready_code
    ready_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "http://127.0.0.1:${port}/health/ready" || true)"
    [[ "${ready_code}" == "200" ]]
    return $?
  fi

  # Service mode: keep startup probe permissive to avoid local false negatives.
  # Benchmark phase has its own stricter readiness gate via wait_bench_endpoint_ready.
  root_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "http://127.0.0.1:${port}/" || true)"
  case "${root_code}" in
    [1-5][0-9][0-9]) return 0 ;;
    *) return 1 ;;
  esac
}

probe_bench_endpoint_ok() {
  local endpoint health_url ready_url root_url
  local health_code ready_code root_code
  endpoint="${BENCH_ENDPOINT%/}"
  health_url="${endpoint}/health"
  ready_url="${endpoint}/health/ready"
  root_url="${endpoint}/"

  health_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "${health_url}" || true)"
  if [[ "${health_code}" != "200" ]]; then
    return 1
  fi

  if [[ "${BENCH_WAIT_MODE}" == "ready" ]]; then
    ready_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "${ready_url}" || true)"
    [[ "${ready_code}" == "200" ]]
    return $?
  fi

  root_code="$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time 3 "${root_url}" || true)"
  case "${root_code}" in
    2[0-9][0-9]|3[0-9][0-9]|401|403|404) return 0 ;;
    *) return 1 ;;
  esac
}

wait_bench_endpoint_ready() {
  local start now
  start="$(date +%s)"

  while true; do
    if probe_bench_endpoint_ok; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= BENCH_READY_TIMEOUT_SECS )); then
      log_error "timed out waiting for benchmark endpoint ${BENCH_ENDPOINT} (mode=${BENCH_WAIT_MODE})"
      return 1
    fi
    sleep 2
  done
}

wait_node_probe_ok() {
  local port="$1"
  local start now
  start="$(date +%s)"

  while true; do
    if probe_node_service_ok "${port}"; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= WAIT_TIMEOUT_SECS )); then
      log_error "timed out waiting for node probe on 127.0.0.1:${port} (mode=${WAIT_PROBE_MODE})"
      return 1
    fi
    sleep 2
  done
}

wait_cluster_ready() {
  local port
  for port in 9000 9001 9002 9003; do
    wait_node_probe_ok "${port}"
  done
}

probe_survivors_ready() {
  local failover_port="$1"
  local port
  for port in 9000 9001 9002 9003; do
    if [[ "${port}" == "${failover_port}" ]]; then
      continue
    fi
    if ! probe_node_service_ok "${port}"; then
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
  mkdir -p "$(dirname "${probe_file}")"

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
  wait_node_probe_ok "${failover_port}"
  wait_cluster_ready
}

run_benchmark() {
  local bench_out_dir
  local conc
  local conc_dir
  bench_out_dir="${OUT_DIR}/benchmark"
  mkdir -p "${bench_out_dir}"

  if ! command -v warp >/dev/null 2>&1; then
    log_error "warp is required for benchmark phase. Please install warp or run with --skip-bench."
    exit 1
  fi

  log_info "Waiting for benchmark endpoint readiness (mode=${BENCH_WAIT_MODE})"
  wait_bench_endpoint_ready

  IFS=',' read -r -a conc_list <<< "${BENCH_CONCURRENCIES}"
  for conc in "${conc_list[@]}"; do
    conc="$(echo "${conc}" | xargs)"
    if [[ -z "${conc}" ]]; then
      continue
    fi
    if ! [[ "${conc}" =~ ^[0-9]+$ ]] || [[ "${conc}" -le 0 ]]; then
      log_error "invalid concurrency in BENCH_CONCURRENCIES: ${conc}"
      exit 1
    fi

    conc_dir="${bench_out_dir}/concurrency-${conc}"
    log_info "Running benchmark sequentially with concurrency=${conc}"
    (
      cd "${PROJECT_ROOT}"
      ./scripts/run_object_batch_bench.sh \
        --tool warp \
        --endpoint "${BENCH_ENDPOINT}" \
        --access-key "${RUSTFS_ACCESS_KEY}" \
        --secret-key "${RUSTFS_SECRET_KEY}" \
        --bucket "${BENCH_BUCKET}" \
        --concurrency "${conc}" \
        --duration "${BENCH_DURATION}" \
        --sizes "${BENCH_SIZES}" \
        --out-dir "${conc_dir}"
    )
  done
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
        BENCH_CONCURRENCIES="$2"
        shift 2
        ;;
      --bench-concurrencies)
        BENCH_CONCURRENCIES="$2"
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
  resolve_bool "PRECHECK_AUTO_CLEANUP" "${PRECHECK_AUTO_CLEANUP}"
  resolve_probe_mode
  resolve_bench_wait_mode
  resolve_bench_concurrency

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

  if [[ -z "${RUSTFS_OBS_ENDPOINT}" ]]; then
    resolve_default_obs_endpoint
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

  run_precheck_before_build

  if [[ "${BUILD_LOCAL_IMAGE}" == "true" ]]; then
    log_info "Building local image from Dockerfile.source: ${IMAGE_TAG}"
    docker build -f "${PROJECT_ROOT}/Dockerfile.source" -t "${IMAGE_TAG}" "${PROJECT_ROOT}"
  else
    log_info "Skipping image build"
  fi

  run_precheck_after_build

  log_info "Starting compose stack"
  compose up -d

  log_info "Waiting for 4-node cluster readiness (mode=${WAIT_PROBE_MODE})"
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
