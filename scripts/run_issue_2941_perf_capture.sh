#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="${LABEL:-issue-2941}"
DURATION_SECS="${DURATION_SECS:-60}"
PERF_FREQ="${PERF_FREQ:-99}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/perf/${LABEL}-$(date +%Y%m%d-%H%M%S)}"
RUSTFS_PID="${RUSTFS_PID:-}"
CONTAINER_NAME="${CONTAINER_NAME:-}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
PERF_MODE="${PERF_MODE:-auto}"   # auto|on|off
SUDO_CMD="${SUDO_CMD:-}"         # example: sudo

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_issue_2941_perf_capture.sh [options]

Options:
  --label <name>           artifact label prefix
  --duration <secs>        sample duration in seconds (default: 60)
  --out-dir <dir>          artifact output directory
  --pid <pid>              rustfs pid; auto-detect if omitted
  --container <name>       docker container name/id for extra stats
  --endpoint <url>         rustfs endpoint for health probes (default: http://127.0.0.1:9000)
  --perf <auto|on|off>     whether to run perf record (default: auto)
  --perf-freq <hz>         perf sample frequency (default: 99)
  --sudo-cmd <cmd>         optional prefix for privileged perf, e.g. "sudo"
  -h, --help               show help

Environment:
  LABEL
  DURATION_SECS
  PERF_FREQ
  OUT_DIR
  RUSTFS_PID
  CONTAINER_NAME
  ENDPOINT
  PERF_MODE
  SUDO_CMD

Examples:
  scripts/run_issue_2941_perf_capture.sh --label musl-baseline --container rustfs
  scripts/run_issue_2941_perf_capture.sh --label glibc-test --pid 12345 --perf on --sudo-cmd sudo
USAGE
}

log() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --label) LABEL="$2"; shift 2 ;;
      --duration) DURATION_SECS="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --pid) RUSTFS_PID="$2"; shift 2 ;;
      --container) CONTAINER_NAME="$2"; shift 2 ;;
      --endpoint) ENDPOINT="$2"; shift 2 ;;
      --perf) PERF_MODE="$2"; shift 2 ;;
      --perf-freq) PERF_FREQ="$2"; shift 2 ;;
      --sudo-cmd) SUDO_CMD="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *)
        warn "unknown argument: $1"
        usage
        exit 1
        ;;
    esac
  done
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

write_cmd_output() {
  local out_file="$1"
  shift
  if "$@" >"$out_file" 2>&1; then
    return 0
  fi
  warn "command failed, see ${out_file}"
  return 1
}

resolve_pid() {
  if [[ -n "${RUSTFS_PID}" ]]; then
    printf '%s\n' "${RUSTFS_PID}"
    return
  fi

  if [[ -n "${CONTAINER_NAME}" ]] && command_exists docker; then
    local pid
    pid="$(docker inspect --format '{{.State.Pid}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
    if [[ -n "${pid}" && "${pid}" != "0" ]]; then
      printf '%s\n' "${pid}"
      return
    fi
  fi

  pgrep -n rustfs || true
}

snapshot_proc() {
  local pid="$1"
  local prefix="$2"
  [[ -n "${pid}" ]] || return 0

  [[ -r "/proc/${pid}/status" ]] && cp "/proc/${pid}/status" "${OUT_DIR}/${prefix}.proc-status.txt" || true
  [[ -r "/proc/${pid}/io" ]] && cp "/proc/${pid}/io" "${OUT_DIR}/${prefix}.proc-io.txt" || true
  [[ -r "/proc/${pid}/sched" ]] && cp "/proc/${pid}/sched" "${OUT_DIR}/${prefix}.proc-sched.txt" || true
  [[ -r "/proc/${pid}/smaps_rollup" ]] && cp "/proc/${pid}/smaps_rollup" "${OUT_DIR}/${prefix}.proc-smaps-rollup.txt" || true
  [[ -r "/proc/${pid}/limits" ]] && cp "/proc/${pid}/limits" "${OUT_DIR}/${prefix}.proc-limits.txt" || true

  if command_exists ps; then
    ps -p "${pid}" -o pid,ppid,stat,pcpu,pmem,rss,vsz,etime,args >"${OUT_DIR}/${prefix}.ps.txt" 2>&1 || true
    ps -L -p "${pid}" -o pid,tid,psr,pcpu,stat,wchan:32,comm >"${OUT_DIR}/${prefix}.threads.txt" 2>&1 || true
  fi

  if command_exists top; then
    top -H -b -n 1 -p "${pid}" >"${OUT_DIR}/${prefix}.top.txt" 2>&1 || true
  fi
}

capture_host_info() {
  write_cmd_output "${OUT_DIR}/uname.txt" uname -a || true
  command_exists lscpu && write_cmd_output "${OUT_DIR}/lscpu.txt" lscpu || true
  command_exists free && write_cmd_output "${OUT_DIR}/free.txt" free -h || true
  command_exists df && write_cmd_output "${OUT_DIR}/df.txt" df -h || true
  command_exists mount && write_cmd_output "${OUT_DIR}/mount.txt" mount || true
}

capture_endpoint_info() {
  if command_exists curl; then
    curl -fsS "${ENDPOINT}/health" >"${OUT_DIR}/health.txt" 2>&1 || true
    curl -fsS "${ENDPOINT}/health/ready" >"${OUT_DIR}/health-ready.txt" 2>&1 || true
  fi
}

capture_container_info() {
  [[ -n "${CONTAINER_NAME}" ]] || return 0
  command_exists docker || return 0

  docker inspect "${CONTAINER_NAME}" >"${OUT_DIR}/docker-inspect.json" 2>&1 || true
  docker logs --tail 500 "${CONTAINER_NAME}" >"${OUT_DIR}/docker-logs-tail.txt" 2>&1 || true
  docker stats --no-stream --format '{{json .}}' "${CONTAINER_NAME}" >"${OUT_DIR}/docker-stats-once.jsonl" 2>&1 || true
}

sample_container_stats_loop() {
  [[ -n "${CONTAINER_NAME}" ]] || return 0
  command_exists docker || return 0

  local out_file="${OUT_DIR}/docker-stats-loop.jsonl"
  : >"${out_file}"
  local end_ts=$((SECONDS + DURATION_SECS))
  while (( SECONDS < end_ts )); do
    docker stats --no-stream --format '{{json .}}' "${CONTAINER_NAME}" >>"${out_file}" 2>/dev/null || true
    sleep 1
  done
}

sample_pidstat() {
  local pid="$1"
  [[ -n "${pid}" ]] || return 0
  command_exists pidstat || return 0

  pidstat -durwh -p "${pid}" 1 "${DURATION_SECS}" >"${OUT_DIR}/pidstat.txt" 2>&1 || true
}

sample_perf() {
  local pid="$1"
  [[ -n "${pid}" ]] || return 0
  [[ "${PERF_MODE}" == "off" ]] && return 0
  command_exists perf || {
    [[ "${PERF_MODE}" == "on" ]] && warn "perf requested but not installed"
    return 0
  }

  local perf_data="${OUT_DIR}/perf.data"
  local perf_log="${OUT_DIR}/perf-record.log"
  local perf_report="${OUT_DIR}/perf-report.txt"

  local -a prefix=()
  if [[ -n "${SUDO_CMD}" ]]; then
    read -r -a prefix <<<"${SUDO_CMD}"
  fi

  if "${prefix[@]}" perf record -F "${PERF_FREQ}" -g -p "${pid}" -o "${perf_data}" -- sleep "${DURATION_SECS}" \
    >"${perf_log}" 2>&1; then
    "${prefix[@]}" perf report --stdio -i "${perf_data}" >"${perf_report}" 2>&1 || true
  else
    if [[ "${PERF_MODE}" == "on" ]]; then
      warn "perf record failed; see ${perf_log}"
    fi
  fi
}

capture_version_info() {
  local pid="$1"
  if [[ -n "${pid}" && -x "/proc/${pid}/exe" ]]; then
    readlink "/proc/${pid}/exe" >"${OUT_DIR}/binary-path.txt" 2>&1 || true
    "/proc/${pid}/exe" --help >"${OUT_DIR}/binary-help.txt" 2>&1 || true
  fi
}

main() {
  parse_args "$@"
  mkdir -p "${OUT_DIR}"

  local pid
  pid="$(resolve_pid)"
  if [[ -z "${pid}" ]]; then
    warn "failed to detect rustfs pid automatically"
  else
    log "using rustfs pid=${pid}"
  fi

  cat >"${OUT_DIR}/capture-meta.txt" <<EOF
label=${LABEL}
duration_secs=${DURATION_SECS}
perf_freq=${PERF_FREQ}
endpoint=${ENDPOINT}
container_name=${CONTAINER_NAME}
rustfs_pid=${pid}
perf_mode=${PERF_MODE}
sudo_cmd=${SUDO_CMD}
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
project_root=${PROJECT_ROOT}
git_branch=$(git -C "${PROJECT_ROOT}" branch --show-current 2>/dev/null || true)
git_head=$(git -C "${PROJECT_ROOT}" rev-parse HEAD 2>/dev/null || true)
EOF

  capture_host_info
  capture_endpoint_info
  capture_container_info
  capture_version_info "${pid}"
  snapshot_proc "${pid}" "start"

  local bg_pids=()
  sample_pidstat "${pid}" &
  bg_pids+=($!)
  sample_container_stats_loop &
  bg_pids+=($!)
  sample_perf "${pid}" &
  bg_pids+=($!)

  for bg_pid in "${bg_pids[@]}"; do
    wait "${bg_pid}" || true
  done

  snapshot_proc "${pid}" "end"
  capture_endpoint_info

  log "issue-2941 perf capture artifacts written to ${OUT_DIR}"
  find "${OUT_DIR}" -maxdepth 1 -type f | sort
}

main "$@"
