#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUSTFS_BIN="${ROOT_DIR}/target/debug/rustfs"
WARP_BIN="${WARP_BIN:-/Users/zhi/go/bin/warp}"
MC_BIN="${MC_BIN:-/Users/zhi/go/bin/mc}"

PORT="${RUSTFS_BENCH_PORT:-9900}"
CONSOLE_PORT="${RUSTFS_BENCH_CONSOLE_PORT:-9901}"
HOST="127.0.0.1:${PORT}"
ACCESS_KEY="${RUSTFS_BENCH_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${RUSTFS_BENCH_SECRET_KEY:-rustfsadmin}"
BENCH_ROOT="${ROOT_DIR}/target/benchmarks"
RUN_ID="${RUSTFS_BENCH_RUN_ID:-small-put-$(date +%Y%m%d-%H%M%S)}"
RUN_DIR="${BENCH_ROOT}/${RUN_ID}"
VOLUME_DIR="${RUN_DIR}/volumes"
LOG_DIR="${RUN_DIR}/logs"
MC_CONFIG_DIR="${RUN_DIR}/mc-config"
RESULTS_MD="${RUN_DIR}/RESULTS.md"
SERVER_LOG="${LOG_DIR}/rustfs.log"
WARP_BUCKET="${RUSTFS_BENCH_BUCKET:-warp-small-put-benchmark}"
WARP_DURATION="${RUSTFS_BENCH_DURATION:-20s}"
WARP_CONCURRENCY="${RUSTFS_BENCH_CONCURRENCY:-32}"

mkdir -p "${VOLUME_DIR}" "${LOG_DIR}" "${MC_CONFIG_DIR}"
mkdir -p "${VOLUME_DIR}"/disk{1..4}

SIZES=(
  "4KiB"
  "16KiB"
  "64KiB"
  "256KiB"
  "1MiB"
)

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

echo "Starting local RustFS benchmark server..."
(
  cd "${ROOT_DIR}"
  export RUST_LOG="${RUST_LOG:-warn}"
  export RUSTFS_VOLUMES="${VOLUME_DIR}/disk{1...4}"
  export RUSTFS_ADDRESS=":${PORT}"
  export RUSTFS_CONSOLE_ENABLE=false
  export RUSTFS_CONSOLE_ADDRESS=":${CONSOLE_PORT}"
  export RUSTFS_ROOT_USER="${ACCESS_KEY}"
  export RUSTFS_ROOT_PASSWORD="${SECRET_KEY}"
  export RUSTFS_OBJECT_CACHE_ENABLE=false
  "${RUSTFS_BIN}" server >"${SERVER_LOG}" 2>&1
) &
SERVER_PID=$!

echo "Waiting for RustFS server to become ready..."
for _ in $(seq 1 60); do
  if curl -fsS "http://${HOST}/health/ready" >/dev/null 2>&1 || curl -fsS "http://${HOST}/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "http://${HOST}/health/ready" >/dev/null 2>&1 && ! curl -fsS "http://${HOST}/minio/health/ready" >/dev/null 2>&1; then
  echo "RustFS did not become ready in time. See ${SERVER_LOG}" >&2
  exit 1
fi

echo "Preparing mc alias..."
MC_CONFIG_DIR="${MC_CONFIG_DIR}" "${MC_BIN}" alias set bench "http://${HOST}" "${ACCESS_KEY}" "${SECRET_KEY}" >/dev/null
MC_CONFIG_DIR="${MC_CONFIG_DIR}" "${MC_BIN}" mb --ignore-existing "bench/${WARP_BUCKET}" >/dev/null

{
  echo "# Small PUT Benchmark Results"
  echo
  echo "- Run ID: \`${RUN_ID}\`"
  echo "- Host: \`${HOST}\`"
  echo "- Duration per size: \`${WARP_DURATION}\`"
  echo "- Concurrency: \`${WARP_CONCURRENCY}\`"
  echo "- Bucket: \`${WARP_BUCKET}\`"
  echo
} >"${RESULTS_MD}"

for size in "${SIZES[@]}"; do
  echo
  echo "Running warp PUT benchmark for size ${size}..."
  size_slug="$(echo "${size}" | tr '[:upper:]' '[:lower:]')"
  OUT_FILE="${LOG_DIR}/warp-put-${size}.log"
  BENCHDATA_FILE="${RUN_DIR}/warp-put-${size}.csv.zst"
  "${WARP_BIN}" put \
    --no-color \
    --host "${HOST}" \
    --access-key "${ACCESS_KEY}" \
    --secret-key "${SECRET_KEY}" \
    --bucket "${WARP_BUCKET}" \
    --obj.size "${size}" \
    --duration "${WARP_DURATION}" \
    --concurrent "${WARP_CONCURRENCY}" \
    --prefix "${RUN_ID}/${size_slug}" \
    --disable-multipart \
    --noclear \
    --benchdata "${BENCHDATA_FILE}" \
    --insecure \
    >"${OUT_FILE}" 2>&1

  {
    echo "## ${size}"
    echo
    echo '```text'
    tail -n 20 "${OUT_FILE}"
    echo '```'
    echo
  } >>"${RESULTS_MD}"
done

echo
echo "Benchmark completed."
echo "Results: ${RESULTS_MD}"
echo "Server log: ${SERVER_LOG}"
