#!/usr/bin/env bash
#
# rustfs-performance-test.sh
# RustFS 4x4 集群性能压测全流程脚本
#
# Based on the Obsidian note "RustFS 性能测试". Full workflow:
#   1. Cleanup: stop & purge rustfs, remove data dirs on all nodes
#   2. Download the RustFS package on all nodes
#   3. Install RustFS on all nodes (dpkg -i), recreate volume dirs
#   4. Write /etc/default/rustfs (4-node x 4-drive MNMD), start all nodes
#      in parallel and verify the service is Running
#   5. Run the benchmark (warp GET/PUT/MIXED via rustfs-performance-testing.sh)
#   6. Analyze results (summary.tsv / summary.md)
#   7. Final cleanup: stop & purge rustfs, remove data dirs
#
# The script is driven from an admin host (e.g. a jumpbox) and operates on
# the target nodes over SSH, mirroring scripts/test/rustfs_*_test.sh.
#
# Usage:
#   ./rustfs-performance-test.sh --all                  # run all steps 1-7
#   ./rustfs-performance-test.sh --step 5               # run a single step
#   ./rustfs-performance-test.sh --steps 2,3,4          # run selected steps
#   ./rustfs-performance-test.sh --all --dry-run        # preview only
#   ./rustfs-performance-test.sh --all -y --package-url <deb URL>
#
# Notes:
#   - SSH user defaults to azureuser (passwordless sudo on the nodes);
#     pass --ssh-user root if your nodes accept root login.
#   - The benchmark runner defaults to
#     ~/Documents/Obsidian Vault/rustfs-performance-testing.sh; override with
#     --bench-script / RUSTFS_BENCH_SCRIPT. warp must be installed on the
#     admin host.
#   - Steps 1 and 7 destroy the RustFS install and all data (confirmed).
#
set -Eeuo pipefail

# ==================== Configuration (adjust to your environment) ====================

# Target nodes (4x4: 4 nodes x 4 drives each)
if [ -n "${RUSTFS_NODES:-}" ]; then
  read -r -a NODES <<<"${RUSTFS_NODES}"
else
  NODES=(vm000 vm001 vm002 vm003)
fi

SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

# Package: GitHub release tag, e.g. "1.0.0-rc.3". PACKAGE_URL is derived from
# RUSTFS_VERSION unless --package-url / RUSTFS_PACKAGE_URL is given.
RUSTFS_VERSION="${RUSTFS_VERSION:-1.0.0-rc.3}"
PACKAGE_URL="${RUSTFS_PACKAGE_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
PACKAGE_SHA256="${RUSTFS_PACKAGE_SHA256:-}"

# 4x4 topology: 4 nodes x 4 drives each, same expression on every node
DRIVES_PER_NODE="${RUSTFS_DRIVES_PER_NODE:-4}"
VOLUMES="http://rustfs-node{1...4}:9000/data/rustfs{1...4}/mnmd"

# RustFS service configuration (written to /etc/default/rustfs)
RUSTFS_CONFIG_FILE="/etc/default/rustfs"
RUSTFS_SERVICE="rustfs"
RUSTFS_PACKAGE_NAME="rustfs"
RUSTFS_USER="rustfs"
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
RUSTFS_ADDRESS=":9000"
RUSTFS_CONSOLE_ADDRESS=":9001"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"

# Benchmark runner (step 5/6): prefer the default Obsidian location, fall back
# to a rustfs-performance-testing.sh next to this script (e.g. in the repo or
# on a jumpbox).
_DEFAULT_BENCH="${HOME}/Documents/Obsidian Vault/rustfs-performance-testing.sh"
_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -x "${_DEFAULT_BENCH}" ]; then
  _BENCH_RESOLVED="${_DEFAULT_BENCH}"
elif [ -x "${_SCRIPT_DIR}/rustfs-performance-testing.sh" ]; then
  _BENCH_RESOLVED="${_SCRIPT_DIR}/rustfs-performance-testing.sh"
elif [ -x "${_SCRIPT_DIR}/rustfs_performance_testing.sh" ]; then
  _BENCH_RESOLVED="${_SCRIPT_DIR}/rustfs_performance_testing.sh"
else
  _BENCH_RESOLVED="${_DEFAULT_BENCH}"
fi
BENCH_SCRIPT="${RUSTFS_BENCH_SCRIPT:-${_BENCH_RESOLVED}}"
RESULT_DIR="${RUSTFS_RESULT_DIR:-$(pwd)/warp-bench-results-$(date +%Y%m%d-%H%M%S)}"
WARP_HOST="${RUSTFS_WARP_HOST:-rustfs-node1:9000,rustfs-node2:9000,rustfs-node3:9000,rustfs-node4:9000}"
WARP_BUCKET="${RUSTFS_WARP_BUCKET:-warp-benchmark-bucket}"
WARP_CONCURRENCY="${RUSTFS_WARP_CONCURRENCY:-64}"
WARP_DURATION="${RUSTFS_WARP_DURATION:-5m}"
WARP_GET_OBJECTS="${RUSTFS_WARP_GET_OBJECTS:-2500}"
WARP_SLEEP="${RUSTFS_WARP_SLEEP:-60}"
# Manual method/size selection (passed through to the benchmark runner; empty = full run)
WARP_METHODS="${RUSTFS_WARP_METHODS:-}"
WARP_SIZES="${RUSTFS_WARP_SIZES:-}"

# Timeouts (seconds)
SERVICE_TIMEOUT="${RUSTFS_SERVICE_TIMEOUT:-300}"
POLL_INTERVAL="${RUSTFS_POLL_INTERVAL:-10}"

# ==================== Runtime options (set by CLI) ====================
DRY_RUN=0
ASSUME_YES=0
SKIP_DOWNLOAD=0
PREFLIGHT=0
LOG_FILE=""
SELECTED_STEPS=()

# ==================== Helpers ====================

log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
  if [ "${ASSUME_YES}" -eq 1 ] || [ "${DRY_RUN}" -eq 1 ]; then return 0; fi
  printf '\033[1;33m[CONFIRM]\033[0m %s (y/N) ' "$1"
  read -r answer
  case "${answer}" in
    y|Y|yes|YES) return 0 ;;
    *) die "cancelled" ;;
  esac
}

need_cmd() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1 ($2); install it first"
}

# Run a remote script on a single node (script is read from stdin)
run_remote() {
  local node="$1" script
  script="$(cat)"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
    printf '%s\n' "${script}" | sed 's/^/    | /'
    log "DRY-RUN: ----"
    return 0
  fi
  log "==> ${node}: executing remote script"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}"
}

# Run the same remote script on all nodes in parallel (script from stdin)
run_remote_all() {
  local script pids=() i=0 fail=0
  script="$(cat)"
  for node in "${NODES[@]}"; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
      printf '%s\n' "${script}" | sed 's/^/    | /'
      log "DRY-RUN: ----"
    else
      log "==> ${node}: executing remote script"
      ( ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}" ) &
      pids[$i]=$!
      i=$((i+1))
    fi
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do
      wait "${pid}" || fail=1
    done
  fi
  [ "${fail}" -eq 0 ] || die "one or more remote executions failed"
}

rustfs_config_body() {
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${VOLUMES}"
RUSTFS_ADDRESS="${RUSTFS_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${RUSTFS_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="${RUSTFS_OBS_LOG_DIRECTORY}"
EOF
}

write_rustfs_config() {
  local node="$1" body
  body="$(rustfs_config_body)"
  log "${node}: writing config ${RUSTFS_CONFIG_FILE}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf '%s tee %s >/dev/null <<RUSTFS_EOF\n' '${SUDO}' "${RUSTFS_CONFIG_FILE}"
    printf '%s' "${body}"
    printf '\nRUSTFS_EOF\n'
    printf '${SUDO} systemctl daemon-reload\n'
  } | run_remote "${node}"
}

service_action() {
  local action="$1" node="$2"
  log "${node}: systemctl ${action} ${RUSTFS_SERVICE}"
  [ "${DRY_RUN}" -eq 1 ] && return 0
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${RUSTFS_SERVICE}; else systemctl ${action} ${RUSTFS_SERVICE}; fi" \
    || die "${node}: systemctl ${action} failed"
}

wait_service_active() {
  local node="$1" elapsed=0
  log "${node}: waiting for ${RUSTFS_SERVICE} to become active"
  [ "${DRY_RUN}" -eq 1 ] && { log "${node}: (dry-run) skip wait"; return 0; }
  while [ "${elapsed}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
        "systemctl is-active ${RUSTFS_SERVICE} 2>/dev/null" | grep -q active; then
      log "${node}: service active"
      return 0
    fi
    sleep "${POLL_INTERVAL}"
    elapsed=$((elapsed + POLL_INTERVAL))
  done
  die "${node}: ${RUSTFS_SERVICE} did not become active within ${SERVICE_TIMEOUT}s"
}

verify_service_running() {
  local node="$1"
  log "${node}: checking service status"
  if [ "${DRY_RUN}" -eq 0 ]; then
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "systemctl status ${RUSTFS_SERVICE} --no-pager | head -n 12" || true
  fi
}

build_package_url() {
  local asset
  asset="rustfs_$(printf '%s' "${RUSTFS_VERSION}" | tr '-' '.')_${ARCH}.deb"
  printf 'https://github.com/rustfs/rustfs/releases/download/%s/%s' "${RUSTFS_VERSION}" "${asset}"
}

resolve_package_url() {
  if [ -n "${PACKAGE_URL}" ]; then printf '%s' "${PACKAGE_URL}"; else build_package_url; fi
}

preflight() {
  log "preflight checks"
  need_cmd ssh "openssh client"
  need_cmd curl "http client"
  need_cmd warp "warp benchmark tool (for step 5)"
  if [ ! -x "${BENCH_SCRIPT}" ]; then
    die "benchmark script not found or not executable: ${BENCH_SCRIPT}"
  fi
  if [ "${DRY_RUN}" -eq 0 ]; then
    for node in "${NODES[@]}"; do
      ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'echo ok' >/dev/null \
        || die "cannot ssh to ${node}"
    done
    log "all nodes reachable: ${NODES[*]}"
  fi
  log "preflight OK"
}

# ==================== Steps ====================

step1_cleanup() {
  log "step 1: cleanup environment on all nodes (stop & purge rustfs, remove data dirs)"
  confirm "This DESTROYS the RustFS install and ALL data on ${NODES[*]} (irreversible). Continue?"
  local script
  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} systemctl stop rustfs 2>/dev/null || true
if \${SUDO} dpkg -l rustfs 2>/dev/null | grep -q "^ii"; then
  \${SUDO} dpkg -P rustfs
  echo "purged rustfs"
else
  echo "rustfs not installed, skip purge"
fi
for i in \$(seq 1 ${DRIVES_PER_NODE}); do
  \${SUDO} rm -rf /data/rustfs\${i}/mnmd
  \${SUDO} mkdir -p /data/rustfs\${i}/mnmd
  \${SUDO} chown -R rustfs:rustfs /data/rustfs\${i}/mnmd
done
echo "cleanup done on \$(hostname)"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
  log "step 1 complete"
}

step2_download() {
  log "step 2: download the package on all nodes"
  local url script
  url="$(resolve_package_url)"
  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
if [ -f "${PACKAGES_DIR}/${PACKAGE_FILE}" ] && [ "${SKIP_DOWNLOAD}" -eq 1 ]; then
  echo "already exists: ${PACKAGES_DIR}/${PACKAGE_FILE}, skipping download"
else
  echo "downloading ${url} ..."
  curl -fSL --retry 3 -o "/tmp/${PACKAGE_FILE}" "${url}"
  \${SUDO} mkdir -p "${PACKAGES_DIR}"
  \${SUDO} install -m 0644 "/tmp/${PACKAGE_FILE}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
  \${SUDO} rm -f "/tmp/${PACKAGE_FILE}"
fi
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || { echo "checksum verification failed"; exit 1; }
fi
ls -lh "${PACKAGES_DIR}/${PACKAGE_FILE}"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
  log "step 2 complete"
}

step3_install() {
  log "step 3: install the RustFS service on all nodes"
  confirm "About to run dpkg -i ${PACKAGE_FILE} on all nodes. Continue?"
  local script
  script="$(cat <<'EOF'
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
${SUDO} dpkg -i /home/rustfs/packages/rustfs.deb
${SUDO} systemctl daemon-reload
echo "--- installed package ---"
dpkg -l rustfs | tail -n 1
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
  log "step 3 complete"
}

step4_configure_start() {
  log "step 4: write config, start and verify the service on all nodes"
  local node
  for node in "${NODES[@]}"; do
    write_rustfs_config "${node}"
  done
  for node in "${NODES[@]}"; do
    service_action start "${node}" &
  done
  wait
  for node in "${NODES[@]}"; do
    wait_service_active "${node}"
    verify_service_running "${node}"
  done
  log "step 4 complete"
}

step5_benchmark() {
  log "step 5: run the benchmark (${BENCH_SCRIPT})"
  need_cmd warp "warp benchmark tool"
  confirm "About to run the full GET/PUT/MIXED benchmark (~6-10 hours). Continue?"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: WARP_HOST=${WARP_HOST} WARP_RESULT_DIR=${RESULT_DIR} bash ${BENCH_SCRIPT}"
    return 0
  fi
  WARP_HOST="${WARP_HOST}" \
  WARP_ACCESS_KEY="${ACCESS_KEY}" \
  WARP_SECRET_KEY="${SECRET_KEY}" \
  WARP_BUCKET="${WARP_BUCKET}" \
  WARP_CONCURRENCY="${WARP_CONCURRENCY}" \
  WARP_DURATION="${WARP_DURATION}" \
  WARP_GET_OBJECTS="${WARP_GET_OBJECTS}" \
  WARP_SLEEP_BETWEEN_ROUNDS="${WARP_SLEEP}" \
  WARP_METHODS="${WARP_METHODS}" \
  WARP_SIZES="${WARP_SIZES}" \
  WARP_RESULT_DIR="${RESULT_DIR}" \
  bash "${BENCH_SCRIPT}"
  log "step 5 complete (results in ${RESULT_DIR})"
}

step6_analyze() {
  log "step 6: analyze results from ${RESULT_DIR}"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: bash ${BENCH_SCRIPT} --parse-only ${RESULT_DIR}"
    return 0
  fi
  if [ ! -d "${RESULT_DIR}" ]; then
    die "result directory not found: ${RESULT_DIR}"
  fi
  if [ -f "${RESULT_DIR}/summary.md" ]; then
    log "summary already generated: ${RESULT_DIR}/summary.md"
  else
    log "generating summary with --parse-only"
    bash "${BENCH_SCRIPT}" --parse-only "${RESULT_DIR}"
  fi
  log "----- summary.md -----"
  cat "${RESULT_DIR}/summary.md"
  log "step 6 complete"
}

step7_cleanup() {
  log "step 7: final cleanup on all nodes (stop & purge rustfs, remove data dirs)"
  confirm "This DESTROYS the RustFS install and ALL data on ${NODES[*]} (irreversible). Continue?"
  local script
  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} systemctl stop rustfs 2>/dev/null || true
if \${SUDO} dpkg -l rustfs 2>/dev/null | grep -q "^ii"; then
  \${SUDO} dpkg -P rustfs
  echo "purged rustfs"
fi
for i in \$(seq 1 ${DRIVES_PER_NODE}); do
  \${SUDO} rm -rf /data/rustfs\${i}/mnmd
done
echo "cleanup done on \$(hostname)"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
  log "step 7 complete"
}

# ==================== CLI ====================

usage() {
  cat <<'USAGE'
Usage: ./rustfs-performance-test.sh [options]

Steps:
  1  cleanup environment (purge rustfs, remove data dirs)   [destructive]
  2  download the RustFS package on all nodes
  3  install RustFS (dpkg -i)
  4  write config, start service, verify Running
  5  run benchmark (warp GET/PUT/MIXED)
  6  analyze results (summary.tsv / summary.md)
  7  final cleanup (purge rustfs, remove data dirs)         [destructive]

Options:
  --all                     Run all steps 1-7
  --step N                  Run a single step
  --steps 1,3,5-7           Run selected steps
  --version VERSION         GitHub release tag (default 1.0.0-rc.3)
  --package-url URL         Direct deb URL (overrides --version)
  --sha256 HASH             Verify package checksum
  --skip-download           Keep an existing package file
  --bench-script PATH       Benchmark runner (default: Obsidian Vault rustfs-performance-testing.sh)
  --result-dir DIR          Benchmark result directory
  --warp-duration DUR       warp duration per round (default 5m)
  --warp-concurrency N      warp concurrency (default 64)
  --ssh-user USER           SSH user (default azureuser)
  --ssh-port PORT           SSH port (default 22)
  --preflight               Check environment and exit
  --log-file FILE           Append all output to FILE
  --dry-run                 Preview commands without executing them
  -y, --yes                 Skip all confirmation prompts
  -h, --help                Show this help

Examples:
  ./rustfs-performance-test.sh --all
  ./rustfs-performance-test.sh --all --dry-run
  ./rustfs-performance-test.sh --all -y --package-url https://dl.rustfs.com/...deb
  ./rustfs-performance-test.sh --step 5
USAGE
}

expand_steps() {
  local spec="$1" part start end i
  IFS=',' read -ra parts <<<"${spec}"
  for part in "${parts[@]}"; do
    if [[ "${part}" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      start="${BASH_REMATCH[1]}"; end="${BASH_REMATCH[2]}"
      for ((i=start; i<=end; i++)); do SELECTED_STEPS+=("${i}"); done
    elif [[ "${part}" =~ ^[0-9]+$ ]]; then
      SELECTED_STEPS+=("${part}")
    else
      die "cannot parse step spec: ${part}"
    fi
  done
}

run_steps() {
  local step
  for step in "${SELECTED_STEPS[@]}"; do
    case "${step}" in
      1) step1_cleanup ;;
      2) step2_download ;;
      3) step3_install ;;
      4) step4_configure_start ;;
      5) step5_benchmark ;;
      6) step6_analyze ;;
      7) step7_cleanup ;;
      *) die "unknown step: ${step}" ;;
    esac
    log "step ${step} completed"
  done
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt all=0
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --all) all=1 ;;
      --step) SELECTED_STEPS+=("$1"); shift ;;
      --steps) expand_steps "$1"; shift ;;
      --version) RUSTFS_VERSION="$1"; shift ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --sha256) PACKAGE_SHA256="$1"; shift ;;
      --skip-download) SKIP_DOWNLOAD=1 ;;
      --bench-script) BENCH_SCRIPT="$1"; shift ;;
      --result-dir) RESULT_DIR="$1"; shift ;;
      --warp-duration) WARP_DURATION="$1"; shift ;;
      --warp-concurrency) WARP_CONCURRENCY="$1"; shift ;;
      --ssh-user) SSH_USER="$1"; shift ;;
      --ssh-port) SSH_PORT="$1"; shift ;;
      --preflight) PREFLIGHT=1 ;;
      --log-file) LOG_FILE="$1"; shift ;;
      --dry-run) DRY_RUN=1 ;;
      -y|--yes) ASSUME_YES=1 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: ${opt} (see --help)" ;;
    esac
  done
  if [ -n "${LOG_FILE}" ]; then
    mkdir -p "$(dirname "${LOG_FILE}")"
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi
  if [ "${all}" -eq 1 ]; then
    SELECTED_STEPS=(1 2 3 4 5 6 7)
  fi
  if [ "${PREFLIGHT}" -eq 1 ]; then
    preflight
    if [ "${#SELECTED_STEPS[@]}" -eq 0 ]; then
      log "preflight only; done"
      exit 0
    fi
  fi
  [ "${#SELECTED_STEPS[@]}" -gt 0 ] || die "no steps selected (--all / --step / --steps)"
  log "nodes: ${NODES[*]}  ssh user: ${SSH_USER}  version: ${RUSTFS_VERSION}"
  log "package: $(resolve_package_url)"
  log "result dir: ${RESULT_DIR}"
  [ "${DRY_RUN}" -eq 1 ] && warn "DRY-RUN mode: only printing the commands that would run"
  run_steps
  log "all done"
}

# Allow sourcing the file for unit tests without running main.
if [ "${RUSTFS_PERF_SCRIPT_SOURCE_ONLY:-0}" != "1" ]; then
  main "$@"
fi
