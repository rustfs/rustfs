#!/usr/bin/env bash

# Manual Docker validation for rustfs/backlog#2704.
#
# The script recreates the low-concurrency multipart UploadPart shape reported in
# rustfs/rustfs#8271: one single-node RustFS container with four data paths and a
# Warp multipart workload whose PUTPART concurrency is five by default. It is a
# diagnostic harness, not a CI gate. Without --run and --ack-isolated it reports
# SKIP and exits successfully.

set -euo pipefail

RUN=0
ACK_ISOLATED=0
STRICT=0
KEEP_CONTAINERS=0
UNSAFE_BYPASS_DISK_CHECK=0
IMAGE="rustfs/rustfs:1.0.0"
CASES="reported"
OUT_DIR=""
CONTAINER_PREFIX="rustfs-2704"
PORT=19000
CONSOLE_PORT=19001
DURATION="5m"
CLIENTS=5
CONCURRENT=1
PART_CONCURRENT=1
PARTS=100
PART_SIZE="5MiB"
BUCKET="rustfs-2704-uploadpart"
WARP_BIN="warp"
SAMPLE_INTERVAL="1"
TLS_MODE="auto"
TLS_DIR=""
DATA_ROOT=""
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
MEMORY_LIMIT=""
CPU_LIMIT=""
DOCKER_PLATFORM=""
EXTRA_RUSTFS_ENV=()
CURRENT_CASE_DIR=""
CURRENT_CONTAINER_NAME=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue2704_uploadpart_memory.sh --run --ack-isolated [options]

Default workload:
  * Docker RustFS image rustfs/rustfs:1.0.0
  * one node, four bind-mounted data directories
  * TLS when a TLS bundle can be provided or generated
  * Warp multipart-put with five client streams, --concurrent=1 shape approximated
    as local PUTPART concurrency 5, --part.concurrent=1, --part.size=5MiB

Options:
  --run                         Execute the workload. Without this, prints SKIP.
  --ack-isolated                Confirm the Docker host is dedicated to this run.
  --strict                      Missing prerequisites are FAIL instead of SKIP.
  --image IMAGE                 RustFS image (default: rustfs/rustfs:1.0.0).
  --case LIST                   Comma-separated cases to run (default: reported).
                                Cases: reported,no-cache-env,data-cache-disabled,
                                write-reclaim,direct-write,tls-off,metrics-off.
  --out-dir PATH                Artifact directory (default: temporary).
  --data-root PATH              Use existing/prepared data root; creates rustfs0..3.
  --container-prefix NAME       Docker container name prefix (default: rustfs-2704).
  --port PORT                   Host S3 port for the first case (default: 19000).
  --console-port PORT           Host console port for the first case (default: 19001).
  --duration DURATION           Warp duration (default: 5m).
  --clients N                   Local client streams / PUTPART concurrency (default: 5).
  --concurrent N                Warp --concurrent per stream shape (default: 1).
  --part.concurrent N           Warp --part.concurrent (default: 1).
  --parts N                     Multipart parts per object (default: 100).
  --part.size SIZE              Multipart part size (default: 5MiB).
  --bucket NAME                 Warp bucket (default: rustfs-2704-uploadpart).
  --warp-bin PATH               Warp binary (default: warp on PATH).
  --sample-interval SECONDS     Sampler interval (default: 1).
  --tls auto|on|off             TLS behavior (default: auto).
  --tls-dir PATH                Existing RustFS TLS bundle with rustfs_cert.pem/rustfs_key.pem.
  --memory-limit BYTES          Optional Docker --memory value; omitted by default.
  --cpu-limit VALUE             Optional Docker --cpus value; omitted by default.
  --docker-platform PLATFORM    Optional Docker --platform value.
  --env KEY=VALUE               Extra RustFS env, repeatable.
  --keep-containers             Leave containers for manual inspection.
  --unsafe-bypass-disk-check    Set RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true for
                                local same-device approximations only. Do not use
                                this for the real RHEL/XFS four-disk reproduction.
  -h, --help                    Show this help.

Artifacts per case:
  * summary.env                 command, image, host, fs, cgroup and case metadata
  * sampler.csv                 process, cgroup, dirty/page-cache and Docker memory samples
  * smaps_rollup/ and status/   periodic /proc snapshots on Linux
  * cgroup/                     periodic memory.stat and memory.events on Linux cgroup v2
  * rustfs.log                  Docker logs after the run
  * warp.log and warp*.zst      Warp output and bench data

The script never writes credentials to artifacts beyond the local test access key
name. Use only disposable local credentials.
USAGE
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

skip_or_fail() {
  local message=$1
  if ((STRICT)); then
    fail "$message"
  fi
  printf 'SKIP: %s\n' "$message"
  exit 0
}

need_value() {
  (($# >= 2)) || fail "option $1 requires a value"
}

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

split_cases() {
  local input=$1
  local IFS=','
  read -r -a CASE_LIST <<<"$input"
}

is_positive_int() {
  [[ ${1:-} =~ ^[0-9]+$ ]] && (($1 > 0))
}

is_nonnegative_int() {
  [[ ${1:-} =~ ^[0-9]+$ ]]
}

normalize_case_name() {
  local case_name=$1
  case "$case_name" in
    reported|no-cache-env|data-cache-disabled|write-reclaim|direct-write|tls-off|metrics-off)
      printf '%s\n' "$case_name"
      ;;
    *)
      fail "unknown case '$case_name'"
      ;;
  esac
}

parse_args() {
  while (($#)); do
    case "$1" in
      --run) RUN=1; shift ;;
      --ack-isolated) ACK_ISOLATED=1; shift ;;
      --strict) STRICT=1; shift ;;
      --keep-containers) KEEP_CONTAINERS=1; shift ;;
      --unsafe-bypass-disk-check) UNSAFE_BYPASS_DISK_CHECK=1; shift ;;
      --image) need_value "$@"; IMAGE=$2; shift 2 ;;
      --case|--cases) need_value "$@"; CASES=$2; shift 2 ;;
      --out-dir) need_value "$@"; OUT_DIR=$2; shift 2 ;;
      --data-root) need_value "$@"; DATA_ROOT=$2; shift 2 ;;
      --container-prefix) need_value "$@"; CONTAINER_PREFIX=$2; shift 2 ;;
      --port) need_value "$@"; PORT=$2; shift 2 ;;
      --console-port) need_value "$@"; CONSOLE_PORT=$2; shift 2 ;;
      --duration) need_value "$@"; DURATION=$2; shift 2 ;;
      --clients) need_value "$@"; CLIENTS=$2; shift 2 ;;
      --concurrent) need_value "$@"; CONCURRENT=$2; shift 2 ;;
      --part.concurrent|--part-concurrent) need_value "$@"; PART_CONCURRENT=$2; shift 2 ;;
      --parts) need_value "$@"; PARTS=$2; shift 2 ;;
      --part.size|--part-size) need_value "$@"; PART_SIZE=$2; shift 2 ;;
      --bucket) need_value "$@"; BUCKET=$2; shift 2 ;;
      --warp-bin) need_value "$@"; WARP_BIN=$2; shift 2 ;;
      --sample-interval) need_value "$@"; SAMPLE_INTERVAL=$2; shift 2 ;;
      --tls) need_value "$@"; TLS_MODE=$2; shift 2 ;;
      --tls-dir) need_value "$@"; TLS_DIR=$2; shift 2 ;;
      --memory-limit) need_value "$@"; MEMORY_LIMIT=$2; shift 2 ;;
      --cpu-limit) need_value "$@"; CPU_LIMIT=$2; shift 2 ;;
      --docker-platform) need_value "$@"; DOCKER_PLATFORM=$2; shift 2 ;;
      --env) need_value "$@"; EXTRA_RUSTFS_ENV+=("$2"); shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) fail "unknown option: $1" ;;
    esac
  done
}

require_prerequisites() {
  ((RUN)) || skip_or_fail "pass --run to execute the #2704 Docker workload"
  ((ACK_ISOLATED)) || skip_or_fail "pass --ack-isolated after confirming the Docker host is dedicated to this memory run"
  command -v docker >/dev/null 2>&1 || skip_or_fail "docker is required"
  command -v "$WARP_BIN" >/dev/null 2>&1 || skip_or_fail "warp is required; install it or pass --warp-bin"
  command -v curl >/dev/null 2>&1 || skip_or_fail "curl is required"
  is_positive_int "$CLIENTS" || fail "--clients must be a positive integer"
  is_positive_int "$CONCURRENT" || fail "--concurrent must be a positive integer"
  is_positive_int "$PART_CONCURRENT" || fail "--part.concurrent must be a positive integer"
  is_positive_int "$PARTS" || fail "--parts must be a positive integer"
  is_positive_int "$PORT" || fail "--port must be a positive integer"
  is_positive_int "$CONSOLE_PORT" || fail "--console-port must be a positive integer"
  python3 - "$SAMPLE_INTERVAL" <<'PY' || fail "--sample-interval must be a finite positive number"
import math, sys
value = float(sys.argv[1])
raise SystemExit(0 if math.isfinite(value) and value > 0 else 1)
PY
}

prepare_out_dir() {
  if [[ -z $OUT_DIR ]]; then
    OUT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rustfs-2704-uploadpart.XXXXXX")
  else
    mkdir -p "$OUT_DIR"
  fi
  OUT_DIR=$(cd "$OUT_DIR" && pwd)
}

generate_tls_bundle() {
  local target_dir=$1
  mkdir -p "$target_dir"
  if [[ -s $target_dir/rustfs_cert.pem && -s $target_dir/rustfs_key.pem ]]; then
    return 0
  fi
  command -v openssl >/dev/null 2>&1 || return 1
  openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout "$target_dir/rustfs_key.pem" \
    -out "$target_dir/rustfs_cert.pem" >/dev/null 2>&1 || return 1
  cp "$target_dir/rustfs_cert.pem" "$target_dir/ca.crt"
  chmod a+r "$target_dir/rustfs_cert.pem" "$target_dir/rustfs_key.pem" "$target_dir/ca.crt"
}

resolve_tls_for_case() {
  local case_name=$1
  local case_dir=$2
  CASE_TLS_ENABLED=0
  CASE_TLS_DIR=""
  if [[ $case_name == tls-off || $TLS_MODE == off ]]; then
    return 0
  fi
  if [[ -n $TLS_DIR ]]; then
    [[ -s $TLS_DIR/rustfs_cert.pem && -s $TLS_DIR/rustfs_key.pem ]] || fail "--tls-dir must contain rustfs_cert.pem and rustfs_key.pem"
    CASE_TLS_ENABLED=1
    CASE_TLS_DIR=$(cd "$TLS_DIR" && pwd)
    return 0
  fi
  if [[ $TLS_MODE == on || $TLS_MODE == auto ]]; then
    local generated_dir="$case_dir/tls"
    if generate_tls_bundle "$generated_dir"; then
      CASE_TLS_ENABLED=1
      CASE_TLS_DIR=$generated_dir
      return 0
    fi
    [[ $TLS_MODE == auto ]] || fail "TLS requested but openssl is unavailable and --tls-dir was not supplied"
  fi
}

case_env_args() {
  local case_name=$1
  local metrics_export_enabled=true
  [[ $case_name == metrics-off ]] && metrics_export_enabled=false
  CASE_ENV=(
    -e "RUSTFS_ACCESS_KEY=$ACCESS_KEY"
    -e "RUSTFS_SECRET_KEY=$SECRET_KEY"
    -e "RUSTFS_ADDRESS=0.0.0.0:9000"
    -e "RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001"
    -e "RUSTFS_CONSOLE_ENABLE=true"
    -e "RUSTFS_CORS_ALLOWED_ORIGINS=*"
    -e "RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS=*"
    -e "RUSTFS_VOLUMES=/data/rustfs{0...3}"
    -e "RUSTFS_OBS_LOGS_EXPORT_ENABLED=false"
    -e "RUSTFS_OBS_TRACES_EXPORT_ENABLED=false"
    -e "RUSTFS_OBS_METRICS_EXPORT_ENABLED=$metrics_export_enabled"
    -e "RUSTFS_OBS_SERVICE_NAME=rustfs-2704-$case_name"
    -e "OTEL_RESOURCE_ATTRIBUTES=service.instance.id=rustfs-2704-$case_name"
  )

  if [[ $case_name != no-cache-env && $case_name != data-cache-disabled ]]; then
    CASE_ENV+=(
      -e "RUSTFS_OBJECT_CACHE_ENABLE=true"
      -e "RUSTFS_OBJECT_CACHE_TTL_SECS=300"
    )
  fi
  if [[ $case_name == data-cache-disabled ]]; then
    CASE_ENV+=(
      -e "RUSTFS_OBJECT_DATA_CACHE_ENABLE=false"
      -e "RUSTFS_OBJECT_DATA_CACHE_TTL_SECS=300"
    )
  fi
  if [[ $case_name == write-reclaim ]]; then
    CASE_ENV+=(
      -e "RUSTFS_OBJECT_FILE_CACHE_RECLAIM_WRITE_ENABLE=true"
    )
  fi
  if [[ $case_name == direct-write ]]; then
    CASE_ENV+=(
      -e "RUSTFS_OBJECT_DIRECT_IO_WRITE_ENABLE=true"
    )
  fi
  if ((UNSAFE_BYPASS_DISK_CHECK)); then
    CASE_ENV+=(
      -e "RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true"
    )
  fi
  if ((CASE_TLS_ENABLED)); then
    CASE_ENV+=(
      -e "RUSTFS_TLS_PATH=/opt/tls"
      -e "SSL_CERT_FILE=/opt/tls/ca.crt"
    )
  fi
  if ((${#EXTRA_RUSTFS_ENV[@]})); then
    for item in "${EXTRA_RUSTFS_ENV[@]}"; do
      [[ $item == *=* ]] || fail "--env must be KEY=VALUE, got '$item'"
      CASE_ENV+=(-e "$item")
    done
  fi
}

prepare_case_data_dirs() {
  local case_dir=$1
  if [[ -n $DATA_ROOT ]]; then
    CASE_DATA_ROOT=$DATA_ROOT
    mkdir -p "$CASE_DATA_ROOT"
  else
    CASE_DATA_ROOT="$case_dir/data"
    mkdir -p "$CASE_DATA_ROOT"
  fi
  local disk_index
  for disk_index in 0 1 2 3; do
    mkdir -p "$CASE_DATA_ROOT/rustfs$disk_index"
    chmod 0777 "$CASE_DATA_ROOT/rustfs$disk_index" || true
  done
}

container_pid() {
  local name=$1
  docker inspect -f '{{.State.Pid}}' "$name" 2>/dev/null || true
}

container_cgroup_path() {
  local pid=$1
  [[ $(uname -s) == Linux ]] || return 1
  [[ -r /proc/$pid/cgroup ]] || return 1
  local rel
  rel=$(awk -F: '$1 == "0" { print $3; exit }' "/proc/$pid/cgroup") || return 1
  [[ -n $rel ]] || return 1
  printf '/sys/fs/cgroup%s\n' "$rel"
}

read_status_field_kib() {
  local status_file=$1
  local field=$2
  awk -v field="$field:" '$1 == field { print $2; found = 1; exit } END { if (!found) print "" }' "$status_file" 2>/dev/null
}

read_status_field_raw() {
  local status_file=$1
  local field=$2
  awk -v field="$field:" '$1 == field { print $2; found = 1; exit } END { if (!found) print "" }' "$status_file" 2>/dev/null
}

meminfo_field_kib() {
  local field=$1
  awk -v field="$field:" '$1 == field { print $2; found = 1; exit } END { if (!found) print "" }' /proc/meminfo 2>/dev/null || true
}

cgroup_stat_field() {
  local cgroup_path=$1
  local field=$2
  [[ -r $cgroup_path/memory.stat ]] || return 0
  awk -v field="$field" '$1 == field { print $2; found = 1; exit } END { if (!found) print "" }' "$cgroup_path/memory.stat" 2>/dev/null
}

cgroup_event_field() {
  local cgroup_path=$1
  local field=$2
  [[ -r $cgroup_path/memory.events ]] || return 0
  awk -v field="$field" '$1 == field { print $2; found = 1; exit } END { if (!found) print "" }' "$cgroup_path/memory.events" 2>/dev/null
}

start_sampler() {
  local case_dir=$1
  local container_name=$2
  local pid=$3
  local cgroup_path=${4:-}
  local csv="$case_dir/sampler.csv"
  mkdir -p "$case_dir/status" "$case_dir/smaps_rollup" "$case_dir/cgroup"
  printf 'epoch,pid,container_running,vmrss_kib,rssanon_kib,rssfile_kib,vmswap_kib,threads,fd_count,mem_total_kib,mem_available_kib,dirty_kib,writeback_kib,cgroup_current_bytes,cgroup_max,cgroup_anon_bytes,cgroup_file_bytes,cgroup_inactive_file_bytes,cgroup_active_file_bytes,cgroup_oom,cgroup_oom_kill,docker_mem_usage\n' >"$csv"

  (
    while :; do
      local epoch running status_file smaps_file cgroup_current cgroup_max docker_mem_usage
      epoch=$(date +%s)
      running=$(docker inspect -f '{{.State.Running}}' "$container_name" 2>/dev/null || printf 'false')
      status_file="/proc/$pid/status"
      smaps_file="/proc/$pid/smaps_rollup"
      local vmrss rssanon rssfile vmswap threads fd_count
      vmrss=""; rssanon=""; rssfile=""; vmswap=""; threads=""; fd_count=""
      if [[ -r $status_file ]]; then
        cp "$status_file" "$case_dir/status/$epoch.status" 2>/dev/null || true
        vmrss=$(read_status_field_kib "$status_file" VmRSS)
        rssanon=$(read_status_field_kib "$status_file" RssAnon)
        rssfile=$(read_status_field_kib "$status_file" RssFile)
        vmswap=$(read_status_field_kib "$status_file" VmSwap)
        threads=$(read_status_field_raw "$status_file" Threads)
        fd_count=$(find "/proc/$pid/fd" -maxdepth 1 -type l 2>/dev/null | wc -l | tr -d ' ')
      fi
      if [[ -r $smaps_file ]]; then
        cp "$smaps_file" "$case_dir/smaps_rollup/$epoch.smaps_rollup" 2>/dev/null || true
      fi
      local mem_total mem_available dirty writeback
      mem_total=$(meminfo_field_kib MemTotal)
      mem_available=$(meminfo_field_kib MemAvailable)
      dirty=$(meminfo_field_kib Dirty)
      writeback=$(meminfo_field_kib Writeback)
      cgroup_current=""; cgroup_max=""
      local cg_anon cg_file cg_inactive_file cg_active_file cg_oom cg_oom_kill
      cg_anon=""; cg_file=""; cg_inactive_file=""; cg_active_file=""; cg_oom=""; cg_oom_kill=""
      if [[ -n $cgroup_path && -d $cgroup_path ]]; then
        [[ -r $cgroup_path/memory.current ]] && cgroup_current=$(<"$cgroup_path/memory.current")
        [[ -r $cgroup_path/memory.max ]] && cgroup_max=$(<"$cgroup_path/memory.max")
        [[ -r $cgroup_path/memory.stat ]] && cp "$cgroup_path/memory.stat" "$case_dir/cgroup/$epoch.memory.stat" 2>/dev/null || true
        [[ -r $cgroup_path/memory.events ]] && cp "$cgroup_path/memory.events" "$case_dir/cgroup/$epoch.memory.events" 2>/dev/null || true
        cg_anon=$(cgroup_stat_field "$cgroup_path" anon)
        cg_file=$(cgroup_stat_field "$cgroup_path" file)
        cg_inactive_file=$(cgroup_stat_field "$cgroup_path" inactive_file)
        cg_active_file=$(cgroup_stat_field "$cgroup_path" active_file)
        cg_oom=$(cgroup_event_field "$cgroup_path" oom)
        cg_oom_kill=$(cgroup_event_field "$cgroup_path" oom_kill)
      fi
      docker_mem_usage=$(docker stats --no-stream --format '{{.MemUsage}}' "$container_name" 2>/dev/null | tr ',' ';' || true)
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$epoch" "$pid" "$running" "$vmrss" "$rssanon" "$rssfile" "$vmswap" "$threads" "$fd_count" \
        "$mem_total" "$mem_available" "$dirty" "$writeback" "$cgroup_current" "$cgroup_max" \
        "$cg_anon" "$cg_file" "$cg_inactive_file" "$cg_active_file" "$cg_oom" "$cg_oom_kill" "$docker_mem_usage" >>"$csv"
      [[ $running == true ]] || break
      sleep "$SAMPLE_INTERVAL"
    done
  ) &
  SAMPLER_PID=$!
}

stop_sampler() {
  if [[ -n ${SAMPLER_PID:-} ]] && kill -0 "$SAMPLER_PID" 2>/dev/null; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
  fi
  SAMPLER_PID=""
}

write_summary_header() {
  local summary_file=$1
  local case_name=$2
  local container_name=$3
  local case_port=$4
  local case_console_port=$5
  local cgroup_path=${6:-}
  {
    printf 'issue=rustfs/backlog#2704\n'
    printf 'case=%s\n' "$case_name"
    printf 'image=%s\n' "$IMAGE"
    printf 'container=%s\n' "$container_name"
    printf 'endpoint_port=%s\n' "$case_port"
    printf 'console_port=%s\n' "$case_console_port"
    printf 'tls_enabled=%s\n' "$CASE_TLS_ENABLED"
    printf 'tls_dir=%s\n' "${CASE_TLS_DIR:-}"
    printf 'duration=%s\n' "$DURATION"
    printf 'clients=%s\n' "$CLIENTS"
    printf 'warp_concurrent=%s\n' "$CONCURRENT"
    printf 'part_concurrent=%s\n' "$PART_CONCURRENT"
    printf 'parts=%s\n' "$PARTS"
    printf 'part_size=%s\n' "$PART_SIZE"
    printf 'bucket=%s\n' "$BUCKET"
    printf 'sample_interval=%s\n' "$SAMPLE_INTERVAL"
    printf 'unsafe_bypass_disk_check=%s\n' "$UNSAFE_BYPASS_DISK_CHECK"
    if ((UNSAFE_BYPASS_DISK_CHECK)); then
      printf 'validation_scope=%s\n' "local-same-device-approximation"
    else
      printf 'validation_scope=%s\n' "distinct-disk-or-prepared-data-root"
    fi
    printf 'uname=%s\n' "$(uname -a)"
    printf 'docker_version=%s\n' "$(docker version --format '{{.Server.Version}}' 2>/dev/null || true)"
    printf 'docker_info_os=%s\n' "$(docker info --format '{{.OperatingSystem}}' 2>/dev/null || true)"
    printf 'docker_info_cgroup_driver=%s\n' "$(docker info --format '{{.CgroupDriver}}' 2>/dev/null || true)"
    printf 'docker_info_cgroup_version=%s\n' "$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || true)"
    printf 'image_id=%s\n' "$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null || true)"
    printf 'image_repo_digests=%s\n' "$(docker image inspect "$IMAGE" --format '{{json .RepoDigests}}' 2>/dev/null || true)"
    printf 'cgroup_path=%s\n' "$cgroup_path"
    printf 'data_root=%s\n' "$CASE_DATA_ROOT"
    if command -v df >/dev/null 2>&1; then
      printf 'data_filesystems<<EOF\n'
      df -T "$CASE_DATA_ROOT"/rustfs{0,1,2,3} 2>/dev/null || df "$CASE_DATA_ROOT"/rustfs{0,1,2,3} 2>/dev/null || true
      printf 'EOF\n'
    fi
    printf 'case_env<<EOF\n'
    printf '%q ' "${CASE_ENV[@]}"
    printf '\nEOF\n'
  } >"$summary_file"
}

wait_for_health() {
  local endpoint=$1
  local attempts=120
  local i
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsSk "$endpoint/health" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n ${CURRENT_CONTAINER_NAME:-} ]]; then
      local running
      running=$(docker inspect -f '{{.State.Running}}' "$CURRENT_CONTAINER_NAME" 2>/dev/null || printf 'false')
      [[ $running == true ]] || return 1
    fi
    sleep 1
  done
  return 1
}

run_warp() {
  local case_dir=$1
  local host=$2
  local cmd=(
    "$WARP_BIN" multipart-put
    --host "$host"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
  )
  if ((CASE_TLS_ENABLED)); then
    cmd+=(--tls --insecure)
  fi
  cmd+=(
    --bucket "$BUCKET"
    --concurrent "$((CLIENTS * CONCURRENT))" \
    --part.concurrent "$PART_CONCURRENT" \
    --parts "$PARTS" \
    --part.size "$PART_SIZE" \
    --duration "$DURATION" \
    --benchdata "$case_dir/warp" \
    --noclear \
    --no-color
  )
  "${cmd[@]}" >"$case_dir/warp.log" 2>&1
}

cleanup_current_container_on_exit() {
  if [[ -n ${CURRENT_CONTAINER_NAME:-} && ${KEEP_CONTAINERS:-0} -eq 0 ]]; then
    docker rm -f "$CURRENT_CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
}

cleanup_container() {
  local container_name=$1
  if docker ps -a --format '{{.Names}}' | grep -Fxq "$container_name"; then
    docker logs "$container_name" >"${CURRENT_CASE_DIR:-$OUT_DIR}/rustfs.log" 2>&1 || true
    if ((KEEP_CONTAINERS)); then
      log "keeping container $container_name"
    else
      docker rm -f "$container_name" >/dev/null 2>&1 || true
    fi
  fi
}

run_case() {
  local raw_case=$1
  local case_index=$2
  local case_name
  case_name=$(normalize_case_name "$raw_case")
  local case_dir="$OUT_DIR/$case_name"
  mkdir -p "$case_dir"
  CURRENT_CASE_DIR=$case_dir
  prepare_case_data_dirs "$case_dir"
  resolve_tls_for_case "$case_name" "$case_dir"
  case_env_args "$case_name"

  local case_port=$((PORT + case_index))
  local case_console_port=$((CONSOLE_PORT + case_index))
  local container_name="${CONTAINER_PREFIX}-${case_name}"
  CURRENT_CONTAINER_NAME=$container_name
  cleanup_container "$container_name"

  local volume_args=(
    -v "$CASE_DATA_ROOT/rustfs0:/data/rustfs0"
    -v "$CASE_DATA_ROOT/rustfs1:/data/rustfs1"
    -v "$CASE_DATA_ROOT/rustfs2:/data/rustfs2"
    -v "$CASE_DATA_ROOT/rustfs3:/data/rustfs3"
  )
  if ((CASE_TLS_ENABLED)); then
    volume_args+=(-v "$CASE_TLS_DIR:/opt/tls:ro")
  fi

  local docker_args=(run -d --name "$container_name" --security-opt no-new-privileges:true)
  [[ -n $DOCKER_PLATFORM ]] && docker_args+=(--platform "$DOCKER_PLATFORM")
  [[ -n $MEMORY_LIMIT ]] && docker_args+=(--memory "$MEMORY_LIMIT")
  [[ -n $CPU_LIMIT ]] && docker_args+=(--cpus "$CPU_LIMIT")
  docker_args+=(
    -p "127.0.0.1:${case_port}:9000"
    -p "127.0.0.1:${case_console_port}:9001"
    "${CASE_ENV[@]}"
    "${volume_args[@]}"
    "$IMAGE"
  )

  log "starting $container_name for case $case_name"
  docker "${docker_args[@]}" >"$case_dir/container.id"
  local pid cgroup_path endpoint
  pid=$(container_pid "$container_name")
  cgroup_path=$(container_cgroup_path "$pid" 2>/dev/null || true)
  write_summary_header "$case_dir/summary.env" "$case_name" "$container_name" "$case_port" "$case_console_port" "$cgroup_path"

  if ((CASE_TLS_ENABLED)); then
    endpoint="https://127.0.0.1:${case_port}"
  else
    endpoint="http://127.0.0.1:${case_port}"
  fi
  if ! wait_for_health "$endpoint"; then
    docker logs "$container_name" >"$case_dir/rustfs-startup-failed.log" 2>&1 || true
    cleanup_container "$container_name"
    fail "RustFS did not become healthy for case $case_name"
  fi

  start_sampler "$case_dir" "$container_name" "$pid" "$cgroup_path"
  local warp_status=0
  log "running warp for case $case_name"
  run_warp "$case_dir" "127.0.0.1:${case_port}" || warp_status=$?
  stop_sampler

  docker inspect "$container_name" >"$case_dir/container.inspect.json" 2>/dev/null || true
  docker logs "$container_name" >"$case_dir/rustfs.log" 2>&1 || true
  local exit_code oom_killed running
  exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$container_name" 2>/dev/null || true)
  oom_killed=$(docker inspect -f '{{.State.OOMKilled}}' "$container_name" 2>/dev/null || true)
  running=$(docker inspect -f '{{.State.Running}}' "$container_name" 2>/dev/null || true)
  {
    printf 'warp_exit_code=%s\n' "$warp_status"
    printf 'container_running_after_warp=%s\n' "$running"
    printf 'container_exit_code=%s\n' "$exit_code"
    printf 'container_oom_killed=%s\n' "$oom_killed"
  } >>"$case_dir/summary.env"

  cleanup_container "$container_name"
  if ((warp_status != 0)); then
    fail "warp failed for case $case_name; see $case_dir/warp.log"
  fi
}

main() {
  parse_args "$@"
  require_prerequisites
  prepare_out_dir
  split_cases "$CASES"
  log "artifacts: $OUT_DIR"
  local case_index=0
  for case_name in "${CASE_LIST[@]}"; do
    run_case "$case_name" "$case_index"
    case_index=$((case_index + 1))
  done
  log "completed #2704 UploadPart memory validation: $OUT_DIR"
}

trap cleanup_current_container_on_exit EXIT
main "$@"
