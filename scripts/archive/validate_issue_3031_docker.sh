#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_COMPOSE="${CLUSTER_COMPOSE:-${PROJECT_ROOT}/.docker/compose/docker-compose.cluster.local-build.yml}"
PROJECT_NAME="${PROJECT_NAME:-rustfs-issue3031}"
RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:local-4node}"
BUILD_LOCAL_IMAGE="${BUILD_LOCAL_IMAGE:-true}"
FORCE_BUILD="${FORCE_BUILD:-false}"
KEEP_UP="${KEEP_UP:-false}"
PRECHECK_AUTO_CLEANUP="${PRECHECK_AUTO_CLEANUP:-true}"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
S3_READY_TIMEOUT_SECS="${S3_READY_TIMEOUT_SECS:-120}"

RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs-cluster-admin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs-cluster-secret}"
RUSTFS_UNSAFE_BYPASS_DISK_CHECK="${RUSTFS_UNSAFE_BYPASS_DISK_CHECK:-true}"
RUSTFS_ISSUE3031_DIAG_ENABLE="${RUSTFS_ISSUE3031_DIAG_ENABLE:-true}"
RUSTFS_OBS_LOG_STDOUT_ENABLED="${RUSTFS_OBS_LOG_STDOUT_ENABLED:-true}"
RUSTFS_OBS_USE_STDOUT="${RUSTFS_OBS_USE_STDOUT:-false}"
RUSTFS_OBS_LOGGER_LEVEL="${RUSTFS_OBS_LOGGER_LEVEL:-info}"
RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT="${RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT:-5}"
RUSTFS_LOCK_ACQUIRE_TIMEOUT="${RUSTFS_LOCK_ACQUIRE_TIMEOUT:-5}"

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
WARP_HOST="${WARP_HOST:-node1:9000}"
BUCKET="${BUCKET:-rustfs-multipart-repro}"
SMOKE_OBJECTS="${SMOKE_OBJECTS:-16}"
SMOKE_OBJECT_SIZE_MB="${SMOKE_OBJECT_SIZE_MB:-32}"
SMOKE_PARALLELISM="${SMOKE_PARALLELISM:-8}"

WARP_DURATION="${WARP_DURATION:-5m}"
WARP_CONCURRENT="${WARP_CONCURRENT:-16}"
WARP_PARTS="${WARP_PARTS:-16}"
WARP_PART_SIZE="${WARP_PART_SIZE:-16MiB}"
WARP_PART_CONCURRENT="${WARP_PART_CONCURRENT:-4}"

MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"
WARP_IMAGE="${WARP_IMAGE:-minio/warp:latest}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue3031/$(date +%Y%m%d-%H%M%S)}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_3031_docker.sh [options]

Options:
  --skip-build                  skip local Docker image build
  --force-build                 rebuild even if the local image already exists
  --keep-up                     keep cluster running after the script exits
  --project-name <name>         docker compose project name
  --out-dir <path>              output directory
  --warp-duration <dur>         warp duration (default: 5m)
  --warp-concurrent <n>         warp --concurrent (default: 16)
  --warp-parts <n>              warp --parts (default: 16)
  --warp-part-size <size>       warp --part.size (default: 16MiB)
  --warp-part-concurrent <n>    warp --part.concurrent (default: 4)
  --smoke-objects <n>           plain write smoke object count (default: 16)
  --smoke-object-size-mb <n>    plain write smoke object size MiB (default: 32)
  --smoke-parallelism <n>       plain write smoke parallelism (default: 8)
  -h, --help                    show help

Environment:
  CLUSTER_COMPOSE PROJECT_NAME RUSTFS_IMAGE BUILD_LOCAL_IMAGE FORCE_BUILD KEEP_UP
  RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY RUSTFS_UNSAFE_BYPASS_DISK_CHECK
  RUSTFS_ISSUE3031_DIAG_ENABLE RUSTFS_OBS_LOG_STDOUT_ENABLED
  RUSTFS_OBS_USE_STDOUT RUSTFS_OBS_LOGGER_LEVEL
  RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT
  RUSTFS_LOCK_ACQUIRE_TIMEOUT
  S3_READY_TIMEOUT_SECS
  ENDPOINT BUCKET
  WARP_HOST
  SMOKE_OBJECTS SMOKE_OBJECT_SIZE_MB SMOKE_PARALLELISM
  WARP_DURATION WARP_CONCURRENT WARP_PARTS WARP_PART_SIZE WARP_PART_CONCURRENT
  MC_IMAGE WARP_IMAGE OUT_DIR
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
  docker compose \
    --project-name "${PROJECT_NAME}" \
    -f "${CLUSTER_COMPOSE}" \
    "$@"
}

cleanup() {
  if [[ "${KEEP_UP}" == "true" ]]; then
    log_info "KEEP_UP=true, leaving cluster running"
    return
  fi

  log_info "Stopping docker compose services"
  compose down -v >/dev/null 2>&1 || true
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --skip-build)
        BUILD_LOCAL_IMAGE=false
        shift
        ;;
      --keep-up)
        KEEP_UP=true
        shift
        ;;
      --force-build)
        FORCE_BUILD=true
        shift
        ;;
      --project-name)
        PROJECT_NAME="$2"
        shift 2
        ;;
      --out-dir)
        OUT_DIR="$2"
        shift 2
        ;;
      --warp-duration)
        WARP_DURATION="$2"
        shift 2
        ;;
      --warp-concurrent)
        WARP_CONCURRENT="$2"
        shift 2
        ;;
      --warp-parts)
        WARP_PARTS="$2"
        shift 2
        ;;
      --warp-part-size)
        WARP_PART_SIZE="$2"
        shift 2
        ;;
      --warp-part-concurrent)
        WARP_PART_CONCURRENT="$2"
        shift 2
        ;;
      --smoke-objects)
        SMOKE_OBJECTS="$2"
        shift 2
        ;;
      --smoke-object-size-mb)
        SMOKE_OBJECT_SIZE_MB="$2"
        shift 2
        ;;
      --smoke-parallelism)
        SMOKE_PARALLELISM="$2"
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
    exit 1
  fi
}

build_image_if_needed() {
  if [[ "${BUILD_LOCAL_IMAGE}" != "true" ]]; then
    log_info "Skipping local image build"
    return
  fi

  if [[ "${FORCE_BUILD}" != "true" ]] && docker image inspect "${RUSTFS_IMAGE}" >/dev/null 2>&1; then
    log_info "Reusing existing local image ${RUSTFS_IMAGE}; pass --force-build to rebuild"
    return
  fi

  log_info "Building ${RUSTFS_IMAGE} from Dockerfile.source"
  docker build -f "${PROJECT_ROOT}/Dockerfile.source" -t "${RUSTFS_IMAGE}" "${PROJECT_ROOT}"
}

start_cluster() {
  mkdir -p "${OUT_DIR}"
  cleanup_existing_project_containers

  log_info "Starting 4-node cluster from ${CLUSTER_COMPOSE}"
  compose up -d

  log_info "Waiting for cluster health endpoint"
  wait_http_ok "${ENDPOINT}/health"

  log_info "Waiting for cluster readiness endpoint"
  wait_http_ok "${ENDPOINT}/health/ready"

  log_info "Waiting for S3 API readiness via mc alias/list"
  wait_s3_api_ready
}

cluster_network() {
  echo "${PROJECT_NAME}_rustfs-cluster-net"
}

run_mc_in_network() {
  docker run --rm --network "$(cluster_network)" "${MC_IMAGE}" "$@"
}

run_mc_shell_in_network() {
  docker run --rm --network "$(cluster_network)" --entrypoint /bin/sh "${MC_IMAGE}" -lc "$1"
}

run_warp_in_network() {
  docker run --rm --network "$(cluster_network)" -v "${OUT_DIR}:/out" "${WARP_IMAGE}" "$@"
}

wait_s3_api_ready() {
  local start now
  start="$(date +%s)"

  while true; do
    if run_mc_in_network alias set rustfs "http://node1:9000" "${RUSTFS_ACCESS_KEY}" "${RUSTFS_SECRET_KEY}" >/dev/null 2>&1; then
      if run_mc_in_network ls rustfs >/dev/null 2>&1; then
        return 0
      fi
    fi

    now="$(date +%s)"
    if (( now - start >= S3_READY_TIMEOUT_SECS )); then
      log_error "timed out waiting for S3 API readiness via mc alias/list"
      return 1
    fi

    sleep 2
  done
}

collect_cluster_info() {
  local node

  for node in node1 node2 node3 node4; do
    log_info "Collecting rustfs info from ${node}"
    compose exec -T "${node}" rustfs info --all --json > "${OUT_DIR}/${node}-info.json"
  done
}

run_plain_smoke() {
  local prefix
  prefix="plain-smoke-$(date +%s)"

  log_info "Preparing bucket ${BUCKET}"
  run_mc_in_network alias set rustfs "http://node1:9000" "${RUSTFS_ACCESS_KEY}" "${RUSTFS_SECRET_KEY}" >/dev/null
  run_mc_in_network ls "rustfs/${BUCKET}" >/dev/null 2>&1 || run_mc_in_network mb "rustfs/${BUCKET}" >/dev/null

  log_info "Running plain smoke write/read/delete: objects=${SMOKE_OBJECTS} size_mb=${SMOKE_OBJECT_SIZE_MB} parallelism=${SMOKE_PARALLELISM}"
  run_mc_shell_in_network "
    set -euo pipefail
    i=1
    while [ \"\$i\" -le \"${SMOKE_OBJECTS}\" ]; do
      end=\$((i + ${SMOKE_PARALLELISM} - 1))
      [ \"\$end\" -le \"${SMOKE_OBJECTS}\" ] || end='${SMOKE_OBJECTS}'
      j=\"\$i\"
      while [ \"\$j\" -le \"\$end\" ]; do
        (
          dd if=/dev/urandom bs='${SMOKE_OBJECT_SIZE_MB}M' count=1 2>/dev/null | \
            mc pipe 'rustfs/${BUCKET}/${prefix}/obj-\${j}' >/dev/null
        ) &
        j=\$((j + 1))
      done
      wait
      i=\$((end + 1))
    done
    mc rm --recursive --force 'rustfs/${BUCKET}/${prefix}' >/dev/null
  "
}

run_warp_multipart() {
  log_info "Running warp multipart-put against ${WARP_HOST}"
  run_warp_in_network multipart-put \
    --host "${WARP_HOST}" \
    --access-key "${RUSTFS_ACCESS_KEY}" \
    --secret-key "${RUSTFS_SECRET_KEY}" \
    --bucket "${BUCKET}-warp" \
    --lookup path \
    --duration "${WARP_DURATION}" \
    --concurrent "${WARP_CONCURRENT}" \
    --parts "${WARP_PARTS}" \
    --part.size "${WARP_PART_SIZE}" \
    --part.concurrent "${WARP_PART_CONCURRENT}" \
    --benchdata /out/warp.csv.zst \
    --analyze.v \
    --no-color \
    | tee "${OUT_DIR}/warp.log"
}

count_matches() {
  local pattern="$1"
  shift
  local total=0
  local file count

  for file in "$@"; do
    count="$(grep -cE "${pattern}" "${file}" 2>/dev/null || true)"
    total=$((total + ${count:-0}))
  done

  echo "${total}"
}

collect_cluster_logs() {
  local node

  for node in node1 node2 node3 node4; do
    compose logs --no-color "${node}" > "${OUT_DIR}/${node}.log" || true
  done
}

summarize_results() {
  local warp_log summary
  local warp_errors quorum_not_reached connection_refused storage_insufficient
  local lock_timeout erasure_write_quorum readiness_probe_failed liveness_probe_failed
  local diag_read_parts diag_complete_part diag_rename_part
  local startup_range_oob startup_volume_not_found startup_remote_network startup_remote_faulty startup_remote_lock_rpc

  warp_log="${OUT_DIR}/warp.log"
  summary="${OUT_DIR}/summary.txt"

  warp_errors="$(count_matches 'warp: <ERROR>' "${warp_log}")"
  quorum_not_reached="$(count_matches 'Quorum not reached' "${warp_log}")"
  connection_refused="$(count_matches 'connection refused' "${warp_log}")"
  storage_insufficient="$(count_matches 'Storage resources are insufficient for the write operation' "${warp_log}")"
  lock_timeout="$(count_matches 'Lock acquisition timeout' "${warp_log}")"
  erasure_write_quorum="$(count_matches 'erasure write quorum' "${warp_log}")"
  readiness_probe_failed="$(count_matches 'readiness_probe_failed' "${warp_log}")"
  liveness_probe_failed="$(count_matches 'liveness_probe_failed' "${warp_log}")"

  diag_read_parts="$(count_matches 'issue3031_read_parts_part_quorum' "${OUT_DIR}"/node*.log)"
  diag_complete_part="$(count_matches 'issue3031_complete_part_error' "${OUT_DIR}"/node*.log)"
  diag_rename_part="$(count_matches 'issue3031_rename_part_context' "${OUT_DIR}"/node*.log)"

  startup_range_oob="$(count_matches 'Range \\[0, 4\\) is out of bounds' "${OUT_DIR}"/node*.log)"
  startup_volume_not_found="$(count_matches 'volume not found' "${OUT_DIR}"/node*.log)"
  startup_remote_network="$(count_matches 'Remote disk operation returned a network-like error' "${OUT_DIR}"/node*.log)"
  startup_remote_faulty="$(count_matches 'Remote disk marked faulty after timeout' "${OUT_DIR}"/node*.log)"
  startup_remote_lock_rpc="$(count_matches 'Evicting cached remote lock connection after RPC failure' "${OUT_DIR}"/node*.log)"

  cat > "${summary}" <<EOF
issue=3031
endpoint=${ENDPOINT}
bucket=${BUCKET}
warp_duration=${WARP_DURATION}
warp_concurrent=${WARP_CONCURRENT}
warp_parts=${WARP_PARTS}
warp_part_size=${WARP_PART_SIZE}
warp_part_concurrent=${WARP_PART_CONCURRENT}

warp_errors=${warp_errors}
quorum_not_reached=${quorum_not_reached}
connection_refused=${connection_refused}
storage_insufficient=${storage_insufficient}
lock_timeout=${lock_timeout}
erasure_write_quorum=${erasure_write_quorum}
readiness_probe_failed=${readiness_probe_failed}
liveness_probe_failed=${liveness_probe_failed}

issue3031_read_parts_part_quorum=${diag_read_parts}
issue3031_complete_part_error=${diag_complete_part}
issue3031_rename_part_context=${diag_rename_part}

startup_range_oob=${startup_range_oob}
startup_volume_not_found=${startup_volume_not_found}
startup_remote_network_error=${startup_remote_network}
startup_remote_faulty=${startup_remote_faulty}
startup_remote_lock_rpc=${startup_remote_lock_rpc}
EOF

  log_info "Summary written to ${summary}"
  cat "${summary}"
}

main() {
  parse_args "$@"
  require_cmd docker
  require_cmd curl
  require_cmd grep
  require_cmd tee

  mkdir -p "${OUT_DIR}"
  trap cleanup EXIT INT TERM

  export RUSTFS_IMAGE
  export RUSTFS_ACCESS_KEY
  export RUSTFS_SECRET_KEY
  export RUSTFS_UNSAFE_BYPASS_DISK_CHECK
  export RUSTFS_ISSUE3031_DIAG_ENABLE
  export RUSTFS_OBS_LOG_STDOUT_ENABLED
  export RUSTFS_OBS_USE_STDOUT
  export RUSTFS_OBS_LOGGER_LEVEL
  export RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT
  export RUSTFS_LOCK_ACQUIRE_TIMEOUT

  build_image_if_needed
  start_cluster
  collect_cluster_info
  run_plain_smoke
  run_warp_multipart
  collect_cluster_logs
  summarize_results
}

main "$@"
