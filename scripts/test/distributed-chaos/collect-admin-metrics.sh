#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
OUT_DIR="${OUT_DIR:-${ROOT_DIR}/artifacts/distributed-chaos/admin}"
AWSCURL_BIN="${AWSCURL_BIN:-awscurl}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
S3_REGION="${S3_REGION:-us-east-1}"
BASE_URL="${BASE_URL:-http://127.0.0.1:9000}"
METRICS_INTERVAL="${METRICS_INTERVAL:-5s}"
METRICS_SAMPLES="${METRICS_SAMPLES:-60}"
SIGNING_MODE="${SIGNING_MODE:-local_awscurl}"
DOCKER_AWSCURL_IMAGE="${DOCKER_AWSCURL_IMAGE:-python:3.14-slim}"
DOCKER_NETWORK="${DOCKER_NETWORK:-rustfs-chaos-net}"

usage() {
    cat <<'EOF'
Usage:
  collect-admin-metrics.sh once
  collect-admin-metrics.sh stream
  collect-admin-metrics.sh pprof-status
  collect-admin-metrics.sh pprof-cpu [seconds]
  collect-admin-metrics.sh pprof-flamegraph [seconds]

Environment:
  AWSCURL_BIN        Path to awscurl
  BASE_URL           RustFS endpoint
  METRICS_INTERVAL   Metrics stream interval
  METRICS_SAMPLES    Number of metrics samples
  SIGNING_MODE       local_awscurl or docker_awscurl
  DOCKER_AWSCURL_IMAGE  Image used for docker_awscurl mode
  DOCKER_NETWORK     Docker network used for docker_awscurl mode
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

signed_get() {
    local url="$1"
    case "${SIGNING_MODE}" in
        local_awscurl)
            "${AWSCURL_BIN}" \
                --service s3 \
                --region "${S3_REGION}" \
                --access_key "${S3_ACCESS_KEY}" \
                --secret_key "${S3_SECRET_KEY}" \
                -X GET \
                "${url}"
            ;;
        docker_awscurl)
            docker run --rm --network "${DOCKER_NETWORK}" "${DOCKER_AWSCURL_IMAGE}" \
                sh -lc "python -m pip install -q awscurl >/dev/null 2>&1 && awscurl --service s3 --region '${S3_REGION}' --access_key '${S3_ACCESS_KEY}' --secret_key '${S3_SECRET_KEY}' -X GET '${url}'"
            ;;
        *)
            echo "unsupported SIGNING_MODE: ${SIGNING_MODE}" >&2
            exit 1
            ;;
    esac
}

timestamp() {
    date +"%Y%m%d-%H%M%S"
}

main() {
    if [[ $# -lt 1 ]]; then
        usage
        exit 1
    fi

    if [[ "${SIGNING_MODE}" == "local_awscurl" ]]; then
        require_bin "${AWSCURL_BIN}"
    else
        require_bin docker
    fi
    mkdir -p "${OUT_DIR}"

    case "$1" in
        once)
            signed_get "${BASE_URL}/rustfs/admin/v3/metrics?n=1&by-host=true&by-disk=true" \
                > "${OUT_DIR}/metrics-$(timestamp).ndjson"
            ;;
        stream)
            signed_get "${BASE_URL}/rustfs/admin/v3/metrics?n=${METRICS_SAMPLES}&interval=${METRICS_INTERVAL}&by-host=true&by-disk=true" \
                | tee "${OUT_DIR}/metrics-$(timestamp).ndjson"
            ;;
        pprof-status)
            signed_get "${BASE_URL}/rustfs/admin/debug/pprof/status" \
                | tee "${OUT_DIR}/pprof-status-$(timestamp).json"
            ;;
        pprof-cpu)
            seconds="${2:-30}"
            signed_get "${BASE_URL}/rustfs/admin/debug/pprof/profile?seconds=${seconds}&format=protobuf" \
                > "${OUT_DIR}/cpu-$(timestamp).pb"
            ;;
        pprof-flamegraph)
            seconds="${2:-30}"
            signed_get "${BASE_URL}/rustfs/admin/debug/pprof/profile?seconds=${seconds}&format=flamegraph" \
                > "${OUT_DIR}/cpu-$(timestamp).svg"
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
