#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
OUT_ROOT="${OUT_ROOT:-${ROOT_DIR}/artifacts/distributed-chaos/repros}"
STAMP="$(date +"%Y%m%d-%H%M%S")"
RUN_DIR="${OUT_ROOT}/mixed-corruption-${STAMP}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
ATTEMPTS="${ATTEMPTS:-3}"
LOG_TAIL_LINES="${LOG_TAIL_LINES:-20000}"
INSPECT_LAYOUT_ON_FINDINGS="${INSPECT_LAYOUT_ON_FINDINGS:-1}"
WARP_MODE="${WARP_MODE:-docker}"
WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE:-minio/warp:latest}"
WARP_HOST="${WARP_HOST:-rustfs-lb:9000}"
WARP_MIXED_DURATION="${WARP_MIXED_DURATION:-20s}"
WARP_MIXED_CONCURRENT="${WARP_MIXED_CONCURRENT:-16}"
WARP_MIXED_OBJECTS="${WARP_MIXED_OBJECTS:-200}"
WARP_MIXED_OBJ_SIZE="${WARP_MIXED_OBJ_SIZE:-1MiB}"
LOG_PATTERN="${LOG_PATTERN:-bitrot shard file size mismatch|Data corruption detected|erasure read quorum|Storage resources are insufficient for the write operation}"
NODES=(rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4 rustfs-lb)

usage() {
    cat <<'EOF'
Usage:
  repro-mixed-corruption.sh

Environment:
  OUT_ROOT             Output directory root for logs and Warp output
  ENDPOINT             Public RustFS endpoint for readiness checks
  TIMEOUT_SECONDS      Max seconds to wait for the Warp run
  ATTEMPTS             Number of mixed-workload attempts before giving up
  LOG_TAIL_LINES       Max log lines collected per container for each attempt
  INSPECT_LAYOUT_ON_FINDINGS  Capture an object layout report for the first suspicious object
  WARP_MODE            binary or docker
  WARP_DOCKER_IMAGE    Warp image used in docker mode
  WARP_HOST            Warp target host:port
  WARP_MIXED_DURATION  Mixed workload duration
  WARP_MIXED_CONCURRENT Mixed workload concurrency
  WARP_MIXED_OBJECTS   Mixed workload object count
  WARP_MIXED_OBJ_SIZE  Mixed workload max object size
  LOG_PATTERN          Regex used to flag suspected corruption/heal failures
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

cleanup_warp_containers() {
    if [[ "${WARP_MODE}" == "docker" ]]; then
        docker ps -aq --filter "ancestor=${WARP_DOCKER_IMAGE}" | xargs -r docker rm -f >/dev/null 2>&1 || true
    fi
}

collect_logs() {
    local since_epoch="$1"
    local attempt_dir="$2"
    local node
    mkdir -p "${attempt_dir}"
    for node in "${NODES[@]}"; do
        docker logs \
            --since "${since_epoch}" \
            --tail "${LOG_TAIL_LINES}" \
            "${node}" \
            > "${attempt_dir}/${node}.log" 2>&1 || true
    done
}

extract_first_suspect_object() {
    local findings_path="$1"
    python3 - "$findings_path" <<'PY'
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="ignore")
patterns = [
    re.compile(r'bucket: "([^"]+)", object: "([^"]+)"'),
    re.compile(r'Data corruption detected for ([^/]+)/(.+?):'),
    re.compile(r'Storage resources are insufficient for the write operation: ([^/]+)/(.+)'),
]

for pattern in patterns:
    match = pattern.search(text)
    if match:
        print(match.group(1))
        print(match.group(2))
        break
PY
}

inspect_suspect_layout() {
    local findings_path="$1"
    local attempt_dir="$2"
    local inspect_script="${ROOT_DIR}/scripts/test/distributed-chaos/inspect-object-layout.sh"
    local extracted
    local bucket
    local object

    [[ "${INSPECT_LAYOUT_ON_FINDINGS}" == "1" ]] || return 0
    [[ -x "${inspect_script}" ]] || return 0

    extracted="$(extract_first_suspect_object "${findings_path}")"
    bucket="$(printf '%s\n' "${extracted}" | sed -n '1p')"
    object="$(printf '%s\n' "${extracted}" | sed -n '2p')"

    if [[ -z "${bucket}" || -z "${object}" ]]; then
        return 0
    fi

    OUT_FILE="${attempt_dir}/object-layout.txt" "${inspect_script}" "${bucket}" "${object}" >/dev/null || true
}

run_with_timeout() {
    local timeout_seconds="$1"
    shift
    "$@" &
    local pid=$!
    local elapsed=0

    while kill -0 "${pid}" >/dev/null 2>&1; do
        if (( elapsed >= timeout_seconds )); then
            kill -TERM "${pid}" >/dev/null 2>&1 || true
            sleep 2
            kill -KILL "${pid}" >/dev/null 2>&1 || true
            cleanup_warp_containers
            return 124
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    wait "${pid}"
}

main() {
    if [[ $# -gt 0 ]]; then
        usage
        exit 1
    fi

    require_bin docker
    require_bin curl
    require_bin python3
    mkdir -p "${RUN_DIR}"

    curl -fsS "${ENDPOINT}/health" >/dev/null
    curl -fsS "${ENDPOINT}/health/ready" >/dev/null

    local attempt
    local warp_status=0
    local attempt_dir
    local since_epoch
    local last_nonzero=0
    local findings_file="${RUN_DIR}/findings.log"
    : > "${findings_file}"

    for (( attempt = 1; attempt <= ATTEMPTS; attempt++ )); do
        attempt_dir="${RUN_DIR}/attempt-$(printf '%02d' "${attempt}")"
        mkdir -p "${attempt_dir}"
        since_epoch="$(date +%s)"

        echo "Attempt ${attempt}/${ATTEMPTS}: running mixed workload"

        set +e
        run_with_timeout "${TIMEOUT_SECONDS}" \
            env \
            OUT_DIR="${attempt_dir}/warp" \
            WARP_MODE="${WARP_MODE}" \
            WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE}" \
            WARP_HOST="${WARP_HOST}" \
            WARP_MIXED_DURATION="${WARP_MIXED_DURATION}" \
            WARP_MIXED_CONCURRENT="${WARP_MIXED_CONCURRENT}" \
            WARP_MIXED_OBJECTS="${WARP_MIXED_OBJECTS}" \
            WARP_MIXED_OBJ_SIZE="${WARP_MIXED_OBJ_SIZE}" \
            "${ROOT_DIR}/scripts/test/distributed-chaos/run-warp.sh" mixed \
            > "${attempt_dir}/warp.out" 2>&1
        warp_status=$?
        set -e

        cleanup_warp_containers
        collect_logs "${since_epoch}" "${attempt_dir}"

        if rg -n -m 200 "${LOG_PATTERN}" "${attempt_dir}/warp.out" "${attempt_dir}"/rustfs-node*.log \
            > "${attempt_dir}/findings.log"; then
            inspect_suspect_layout "${attempt_dir}/findings.log" "${attempt_dir}"
            cat "${attempt_dir}/findings.log" >> "${findings_file}"
            echo "Detected suspected corruption/heal failures on attempt ${attempt}. See ${attempt_dir}/findings.log"
            exit 2
        fi

        if (( warp_status != 0 )); then
            last_nonzero="${warp_status}"
            echo "Attempt ${attempt} exited with status ${warp_status}. See ${attempt_dir}/warp.out" >&2
        else
            echo "Attempt ${attempt} finished without corruption markers."
        fi

        curl -fsS "${ENDPOINT}/health" >/dev/null
        curl -fsS "${ENDPOINT}/health/ready" >/dev/null
    done

    if (( last_nonzero != 0 )); then
        echo "Mixed workload did not reproduce corruption, but the last attempt exited with status ${last_nonzero}. Logs saved to ${RUN_DIR}" >&2
        exit "${last_nonzero}"
    fi

    echo "No corruption markers detected across ${ATTEMPTS} attempts. Logs saved to ${RUN_DIR}"
}

main "$@"
