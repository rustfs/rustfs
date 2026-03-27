#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.decommission.yml}"
SERVICE_NAME="${SERVICE_NAME:-rustfs-decommission-latest}"
MC_ALIAS_NAME="${MC_ALIAS_NAME:-rustfs-decom}"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9100}"
ACCESS_KEY="${ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${SECRET_KEY:-rustfsadmin}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-Dockerfile.decommission-local}"
DOCKER_IMAGE_NAME="${DOCKER_IMAGE_NAME:-rustfs-local:decommission-latest}"
POOL0_CMDLINE="${POOL0_CMDLINE:-/data/pool0/disk{1...4}}"
POOL1_CMDLINE="${POOL1_CMDLINE:-/data/pool1/disk{1...4}}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-1200}"
MC_MODE="${MC_MODE:-auto}"
MC_BIN="${MC_BIN:-mc}"
MC_DOCKER_IMAGE="${MC_DOCKER_IMAGE:-minio/mc:latest}"
MC_CONFIG_DIR="${MC_CONFIG_DIR:-.tmp/mc-config}"
MC_DOCKER_NETWORK="${MC_DOCKER_NETWORK:-rustfs-decommission-network}"
MC_DOCKER_ENDPOINT="${MC_DOCKER_ENDPOINT:-http://rustfs-decommission-latest:9000}"

usage() {
    cat <<EOF
Usage:
  decommission_docker.sh build
  decommission_docker.sh up
  decommission_docker.sh down
  decommission_docker.sh reset
  decommission_docker.sh info
  decommission_docker.sh smoke

Environment:
  COMPOSE_FILE             Compose file path. Default: ${COMPOSE_FILE}
  DOCKERFILE_PATH          Dockerfile path. Default: ${DOCKERFILE_PATH}
  DOCKER_IMAGE_NAME        Image tag to build/use. Default: ${DOCKER_IMAGE_NAME}
  MC_ALIAS_NAME            mc alias to create. Default: ${MC_ALIAS_NAME}
  MC_MODE                  mc execution mode: auto, binary, docker. Default: ${MC_MODE}
  S3_ENDPOINT              RustFS S3 endpoint. Default: ${S3_ENDPOINT}
  POOL0_CMDLINE            First pool cmdline. Default: ${POOL0_CMDLINE}
  POOL1_CMDLINE            Second pool cmdline. Default: ${POOL1_CMDLINE}
  HEALTH_TIMEOUT_SECONDS   Wait timeout. Default: ${HEALTH_TIMEOUT_SECONDS}

Smoke flow:
  1. Start a local two-pool RustFS container
  2. Configure mc alias ${MC_ALIAS_NAME}
  3. Prepare decommission test data
  4. Decommission pool 1
  5. Wait for completion and verify readability
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

mc_mode() {
    case "${MC_MODE}" in
        auto)
            if command -v "${MC_BIN}" >/dev/null 2>&1; then
                printf 'binary\n'
            else
                printf 'docker\n'
            fi
            ;;
        binary|docker)
            printf '%s\n' "${MC_MODE}"
            ;;
        *)
            echo "unsupported MC_MODE: ${MC_MODE}" >&2
            exit 1
            ;;
    esac
}

mc_cmd() {
    case "$(mc_mode)" in
        binary)
            "${MC_BIN}" "$@"
            ;;
        docker)
            require_bin docker
            mkdir -p "${MC_CONFIG_DIR}"
            docker run --rm -i \
                --network "${MC_DOCKER_NETWORK}" \
                -v "${PWD}:/work" \
                -w /work \
                -v "${MC_CONFIG_DIR}:/root/.mc" \
                "${MC_DOCKER_IMAGE}" "$@"
            ;;
    esac
}

compose() {
    docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_healthy() {
    local elapsed=0
    local container_id
    container_id="$(compose ps -q "${SERVICE_NAME}")"
    if [[ -z "${container_id}" ]]; then
        echo "container not found for service ${SERVICE_NAME}" >&2
        exit 1
    fi

    while (( elapsed < HEALTH_TIMEOUT_SECONDS )); do
        local status
        status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
        if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
            echo "container is ${status}"
            return
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done

    echo "container did not become healthy within ${HEALTH_TIMEOUT_SECONDS}s" >&2
    compose logs --tail=200 "${SERVICE_NAME}" || true
    exit 1
}

configure_alias() {
    local endpoint="${S3_ENDPOINT}"
    if [[ "$(mc_mode)" == "docker" ]]; then
        endpoint="${MC_DOCKER_ENDPOINT}"
    fi
    mc_cmd alias set "${MC_ALIAS_NAME}" "${endpoint}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null
    echo "mc alias configured: ${MC_ALIAS_NAME} -> ${endpoint}"
}

up() {
    require_bin docker
    mkdir -p deploy/data/decommission deploy/logs/decommission
    compose up -d
    wait_healthy
    configure_alias
    info
}

build_image() {
    require_bin docker
    docker build -f "${DOCKERFILE_PATH}" -t "${DOCKER_IMAGE_NAME}" .
}

down() {
    require_bin docker
    compose down
}

reset() {
    require_bin docker
    compose down -v --remove-orphans || true
    rm -rf deploy/data/decommission deploy/logs/decommission .tmp/decommission-validation
    echo "local decommission docker environment reset"
}

info() {
    cat <<EOF
Local Docker decommission environment:
  compose file : ${COMPOSE_FILE}
  dockerfile   : ${DOCKERFILE_PATH}
  image        : ${DOCKER_IMAGE_NAME}
  mc alias     : ${MC_ALIAS_NAME}
  endpoint     : ${S3_ENDPOINT}
  pool 0       : ${POOL0_CMDLINE}
  pool 1       : ${POOL1_CMDLINE}

Recommended manual flow:
  ./scripts/test/decommission_validation.sh prepare ${MC_ALIAS_NAME}
  ./scripts/test/decommission_validation.sh start ${MC_ALIAS_NAME} '${POOL1_CMDLINE}'
  ./scripts/test/decommission_validation.sh wait ${MC_ALIAS_NAME} '${POOL1_CMDLINE}' 900
  ./scripts/test/decommission_validation.sh verify ${MC_ALIAS_NAME}
EOF
}

smoke() {
    up
    ./scripts/test/decommission_validation.sh prepare "${MC_ALIAS_NAME}"
    ./scripts/test/decommission_validation.sh start "${MC_ALIAS_NAME}" "${POOL1_CMDLINE}"
    ./scripts/test/decommission_validation.sh wait "${MC_ALIAS_NAME}" "${POOL1_CMDLINE}" 900
    ./scripts/test/decommission_validation.sh verify "${MC_ALIAS_NAME}"
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    case "$1" in
        build)
            build_image
            ;;
        up)
            up
            ;;
        down)
            down
            ;;
        reset)
            reset
            ;;
        info)
            info
            ;;
        smoke)
            smoke
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
