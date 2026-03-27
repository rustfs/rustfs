#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
STACK_DIR="${ROOT_DIR}/scripts/test/distributed-chaos"
COMPOSE_FILE="${STACK_DIR}/compose.cluster.yml"
PREBUILT_COMPOSE_FILE="${STACK_DIR}/compose.cluster.prebuilt.yml"
PROJECT_NAME="${PROJECT_NAME:-rustfs-chaos}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT_DIR}/artifacts/distributed-chaos}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-300}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
BUILD_LOCAL="${BUILD_LOCAL:-1}"

usage() {
    cat <<'EOF'
Usage:
  run-compose.sh up
  run-compose.sh wait
  run-compose.sh down
  run-compose.sh reset
  run-compose.sh logs
  run-compose.sh status

Environment:
  PROJECT_NAME             Docker Compose project name
  HEALTH_TIMEOUT_SECONDS   Seconds to wait for /health and /health/ready
  ENDPOINT                 Public RustFS endpoint exposed by HAProxy
  ARTIFACT_DIR             Directory for collected logs
  BUILD_LOCAL              1 builds a local image once, 0 reuses a prebuilt image
  RUSTFS_IMAGE             Image tag to use in prebuilt mode
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

compose() {
    if [[ "${BUILD_LOCAL}" == "1" ]]; then
        docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
    else
        docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" -f "${PREBUILT_COMPOSE_FILE}" "$@"
    fi
}

build_image() {
    if [[ "${BUILD_LOCAL}" == "1" ]]; then
        compose build rustfs-node1
    else
        compose pull rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4 rustfs-lb
    fi
}

wait_ready() {
    local elapsed=0

    until curl -fsS "${ENDPOINT}/health" >/dev/null 2>&1 && curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; do
        if (( elapsed >= HEALTH_TIMEOUT_SECONDS )); then
            echo "cluster did not become ready within ${HEALTH_TIMEOUT_SECONDS}s" >&2
            mkdir -p "${ARTIFACT_DIR}"
            compose logs --no-color > "${ARTIFACT_DIR}/compose.log" 2>&1 || true
            exit 1
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
}

up() {
    require_bin docker
    require_bin curl
    mkdir -p "${ARTIFACT_DIR}"
    build_image
    compose up -d rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4 rustfs-lb
    wait_ready
    echo "RustFS distributed Compose cluster is ready at ${ENDPOINT}"
}

down() {
    require_bin docker
    compose down
}

reset() {
    require_bin docker
    compose down -v --remove-orphans
}

logs() {
    require_bin docker
    mkdir -p "${ARTIFACT_DIR}"
    compose logs --no-color | tee "${ARTIFACT_DIR}/compose.log"
}

status() {
    require_bin docker
    compose ps
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    case "$1" in
        up)
            up
            ;;
        wait)
            wait_ready
            ;;
        down)
            down
            ;;
        reset)
            reset
            ;;
        logs)
            logs
            ;;
        status)
            status
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
