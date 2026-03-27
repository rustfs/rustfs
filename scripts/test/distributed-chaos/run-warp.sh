#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/artifacts/distributed-chaos/warp}"
WARP_BIN="${WARP_BIN:-warp}"
WARP_MODE="${WARP_MODE:-binary}"
WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE:-minio/warp:latest}"
WARP_DOCKER_NETWORK="${WARP_DOCKER_NETWORK:-rustfs-chaos-net}"
WARP_HOST="${WARP_HOST:-127.0.0.1:9000}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
S3_REGION="${S3_REGION:-us-east-1}"
WARP_SMALL_DURATION="${WARP_SMALL_DURATION:-15m}"
WARP_SMALL_CONCURRENT="${WARP_SMALL_CONCURRENT:-256}"
WARP_SMALL_OBJECTS="${WARP_SMALL_OBJECTS:-200000}"
WARP_SMALL_OBJ_SIZE="${WARP_SMALL_OBJ_SIZE:-1MiB}"
WARP_SMALL_PUT_DURATION="${WARP_SMALL_PUT_DURATION:-10m}"
WARP_SMALL_PUT_CONCURRENT="${WARP_SMALL_PUT_CONCURRENT:-256}"
WARP_SMALL_PUT_OBJ_SIZE="${WARP_SMALL_PUT_OBJ_SIZE:-1MiB}"
WARP_MIXED_DURATION="${WARP_MIXED_DURATION:-20m}"
WARP_MIXED_CONCURRENT="${WARP_MIXED_CONCURRENT:-128}"
WARP_MIXED_OBJECTS="${WARP_MIXED_OBJECTS:-50000}"
WARP_MIXED_OBJ_SIZE="${WARP_MIXED_OBJ_SIZE:-16MiB}"
WARP_LARGE_DURATION="${WARP_LARGE_DURATION:-15m}"
WARP_LARGE_CONCURRENT="${WARP_LARGE_CONCURRENT:-32}"
WARP_LARGE_OBJ_SIZE="${WARP_LARGE_OBJ_SIZE:-1GiB}"
WARP_GET_LARGE_DURATION="${WARP_GET_LARGE_DURATION:-15m}"
WARP_GET_LARGE_CONCURRENT="${WARP_GET_LARGE_CONCURRENT:-64}"
WARP_GET_LARGE_OBJECTS="${WARP_GET_LARGE_OBJECTS:-1000}"
WARP_GET_LARGE_OBJ_SIZE="${WARP_GET_LARGE_OBJ_SIZE:-1GiB}"
WARP_DELETE_BATCH_DURATION="${WARP_DELETE_BATCH_DURATION:-10m}"
WARP_DELETE_BATCH_CONCURRENT="${WARP_DELETE_BATCH_CONCURRENT:-64}"
WARP_DELETE_BATCH_OBJECTS="${WARP_DELETE_BATCH_OBJECTS:-200000}"
WARP_DELETE_BATCH_OBJ_SIZE="${WARP_DELETE_BATCH_OBJ_SIZE:-256KiB}"

usage() {
    cat <<'EOF'
Usage:
  run-warp.sh small
  run-warp.sh small-put
  run-warp.sh mixed
  run-warp.sh large
  run-warp.sh get-large
  run-warp.sh delete-batch
  run-warp.sh all

Environment:
  WARP_BIN         Path to the Warp binary
  WARP_MODE        binary or docker
  WARP_DOCKER_IMAGE  Image used when WARP_MODE=docker
  WARP_DOCKER_NETWORK Docker network used when WARP_MODE=docker
  WARP_HOST        RustFS host:port
  S3_ACCESS_KEY    S3 access key
  S3_SECRET_KEY    S3 secret key
  S3_REGION        S3 region
  OUT_DIR          Directory where Warp writes benchmark files
  WARP_*           Optional overrides for duration, concurrency, and object counts
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

run_warp() {
    mkdir -p "${OUT_DIR}"
    (
        cd "${OUT_DIR}"
        case "${WARP_MODE}" in
            binary)
                "${WARP_BIN}" "$@"
                ;;
            docker)
                docker run --rm \
                    --network "${WARP_DOCKER_NETWORK}" \
                    -v "${PWD}:/work" \
                    -w /work \
                    "${WARP_DOCKER_IMAGE}" "$@"
                ;;
            *)
                echo "unsupported WARP_MODE: ${WARP_MODE}" >&2
                exit 1
                ;;
        esac
    )
}

common_args=(
    --host "${WARP_HOST}"
    --access-key "${S3_ACCESS_KEY}"
    --secret-key "${S3_SECRET_KEY}"
    --region "${S3_REGION}"
)

run_small() {
    run_warp mixed \
        "${common_args[@]}" \
        --duration "${WARP_SMALL_DURATION}" \
        --concurrent "${WARP_SMALL_CONCURRENT}" \
        --objects "${WARP_SMALL_OBJECTS}" \
        --obj.size "${WARP_SMALL_OBJ_SIZE}" \
        --obj.randsize \
        --get-distrib 35 \
        --stat-distrib 20 \
        --put-distrib 30 \
        --delete-distrib 15
}

run_small_put() {
    run_warp put \
        "${common_args[@]}" \
        --duration "${WARP_SMALL_PUT_DURATION}" \
        --concurrent "${WARP_SMALL_PUT_CONCURRENT}" \
        --obj.size "${WARP_SMALL_PUT_OBJ_SIZE}" \
        --obj.randsize
}

run_mixed() {
    run_warp mixed \
        "${common_args[@]}" \
        --duration "${WARP_MIXED_DURATION}" \
        --concurrent "${WARP_MIXED_CONCURRENT}" \
        --objects "${WARP_MIXED_OBJECTS}" \
        --obj.size "${WARP_MIXED_OBJ_SIZE}" \
        --obj.randsize \
        --get-distrib 45 \
        --stat-distrib 15 \
        --put-distrib 30 \
        --delete-distrib 10
}

run_large() {
    run_warp put \
        "${common_args[@]}" \
        --duration "${WARP_LARGE_DURATION}" \
        --concurrent "${WARP_LARGE_CONCURRENT}" \
        --obj.size "${WARP_LARGE_OBJ_SIZE}" \
        --obj.randsize
}

run_get_large() {
    run_warp get \
        "${common_args[@]}" \
        --duration "${WARP_GET_LARGE_DURATION}" \
        --concurrent "${WARP_GET_LARGE_CONCURRENT}" \
        --objects "${WARP_GET_LARGE_OBJECTS}" \
        --obj.size "${WARP_GET_LARGE_OBJ_SIZE}" \
        --obj.randsize
}

run_delete_batch() {
    run_warp delete \
        "${common_args[@]}" \
        --duration "${WARP_DELETE_BATCH_DURATION}" \
        --concurrent "${WARP_DELETE_BATCH_CONCURRENT}" \
        --objects "${WARP_DELETE_BATCH_OBJECTS}" \
        --obj.size "${WARP_DELETE_BATCH_OBJ_SIZE}" \
        --batch 1000
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    if [[ "${WARP_MODE}" == "binary" ]]; then
        require_bin "${WARP_BIN}"
    else
        require_bin docker
    fi

    case "$1" in
        small)
            run_small
            ;;
        small-put)
            run_small_put
            ;;
        mixed)
            run_mixed
            ;;
        large)
            run_large
            ;;
        get-large)
            run_get_large
            ;;
        delete-batch)
            run_delete_batch
            ;;
        all)
            run_small
            run_mixed
            run_large
            run_get_large
            run_delete_batch
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
