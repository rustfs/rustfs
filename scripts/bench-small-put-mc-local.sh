#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUSTFS_BIN="${RUSTFS_BIN:-${ROOT_DIR}/target/debug/rustfs}"
MC_BIN="${MC_BIN:-/Users/zhi/go/bin/mc}"

PORT="${RUSTFS_BENCH_PORT:-9910}"
CONSOLE_PORT="${RUSTFS_BENCH_CONSOLE_PORT:-9911}"
HOST="127.0.0.1:${PORT}"
ACCESS_KEY="${RUSTFS_BENCH_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${RUSTFS_BENCH_SECRET_KEY:-rustfsadmin}"

OPS_PER_SIZE="${RUSTFS_BENCH_OPS_PER_SIZE:-0}"
BENCH_SECONDS="${RUSTFS_BENCH_SECONDS:-10}"
CONCURRENCY="${RUSTFS_BENCH_CONCURRENCY:-32}"
PUT_TIMEOUT_SECS="${RUSTFS_BENCH_PUT_TIMEOUT_SECS:-15}"
RUN_ID="${RUSTFS_BENCH_RUN_ID:-small-put-mc-$(date +%Y%m%d-%H%M%S)}"
BENCH_ROOT="${ROOT_DIR}/target/benchmarks"
RUN_DIR="${BENCH_ROOT}/${RUN_ID}"
VOLUME_DIR="${RUN_DIR}/volumes"
LOG_DIR="${RUN_DIR}/logs"
FILE_DIR="${RUN_DIR}/files"
MC_CONFIG_DIR="${RUN_DIR}/mc-config"
RESULTS_MD="${RUN_DIR}/RESULTS.md"
SERVER_LOG="${LOG_DIR}/rustfs.log"
BUCKET="${RUSTFS_BENCH_BUCKET:-small-put-benchmark}"

mkdir -p "${VOLUME_DIR}" "${LOG_DIR}" "${FILE_DIR}" "${MC_CONFIG_DIR}"
mkdir -p "${VOLUME_DIR}"/disk{1..4}

SIZES=(
  "4KiB"
  "16KiB"
  "64KiB"
  "256KiB"
  "1MiB"
)

size_to_bytes() {
  case "$1" in
    4KiB) echo 4096 ;;
    16KiB) echo 16384 ;;
    64KiB) echo 65536 ;;
    256KiB) echo 262144 ;;
    1MiB) echo 1048576 ;;
    *)
      echo "unsupported size: $1" >&2
      return 1
      ;;
  esac
}

now_secs() {
  perl -MTime::HiRes=time -e 'printf "%.6f\n", time'
}

prepare_sample_file() {
  local size_label="$1"
  local bytes="$2"
  local file_path="${FILE_DIR}/${size_label}.bin"
  if [[ ! -f "${file_path}" ]]; then
    dd if=/dev/zero of="${file_path}" bs="${bytes}" count=1 status=none
  fi
  echo "${file_path}"
}

run_mc_put_with_timeout() {
  local sample_file="$1"
  local key="$2"
  perl -e '
      use strict;
      use warnings;

      my $timeout = shift @ARGV;
      my $pid = fork();
      die "fork failed: $!" unless defined $pid;

      if ($pid == 0) {
        exec @ARGV;
        die "exec failed: $!";
      }

      my $timed_out = 0;
      local $SIG{ALRM} = sub {
        $timed_out = 1;
        kill "TERM", $pid;
        select undef, undef, undef, 0.2;
        kill "KILL", $pid;
      };

      alarm($timeout);
      my $waited = waitpid($pid, 0);
      alarm(0);

      exit(1) if $timed_out;
      exit(($waited == $pid && $? == 0) ? 0 : 1);
    ' "${PUT_TIMEOUT_SECS}" env MC_CONFIG_DIR="${MC_CONFIG_DIR}" "${MC_BIN}" put --quiet --disable-multipart "${sample_file}" "${key}" >/dev/null 2>&1
}

export -f run_mc_put_with_timeout

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
MC_CONFIG_DIR="${MC_CONFIG_DIR}" "${MC_BIN}" mb --ignore-existing "bench/${BUCKET}" >/dev/null

{
  echo "# Small PUT Benchmark Results"
  echo
  echo "- Run ID: \`${RUN_ID}\`"
  echo "- Host: \`${HOST}\`"
  echo "- Bucket: \`${BUCKET}\`"
  if [[ "${OPS_PER_SIZE}" -gt 0 ]]; then
    echo "- Ops per size: \`${OPS_PER_SIZE}\`"
  else
    echo "- Duration per size: \`${BENCH_SECONDS}s\`"
  fi
  echo "- Concurrency: \`${CONCURRENCY}\`"
  echo "- Per request timeout: \`${PUT_TIMEOUT_SECS}s\`"
  echo
} >"${RESULTS_MD}"

for size_label in "${SIZES[@]}"; do
  bytes="$(size_to_bytes "${size_label}")"
  sample_file="$(prepare_sample_file "${size_label}" "${bytes}")"
  raw_json="${RUN_DIR}/${size_label}.jsonl"
  prefix="$(echo "${size_label}" | tr '[:upper:]' '[:lower:]')"
  worker_dir="${RUN_DIR}/${size_label}-workers"
  mkdir -p "${worker_dir}"

  echo "Running mc PUT benchmark for size ${size_label}..."
  batch_start="$(now_secs)"
  if [[ "${OPS_PER_SIZE}" -gt 0 ]]; then
    seq 1 "${OPS_PER_SIZE}" | xargs -n1 -P "${CONCURRENCY}" -I{} \
      env MC_CONFIG_DIR="${MC_CONFIG_DIR}" MC_BIN="${MC_BIN}" PUT_TIMEOUT_SECS="${PUT_TIMEOUT_SECS}" SAMPLE_FILE="${sample_file}" BUCKET="${BUCKET}" PREFIX="${prefix}" /bin/bash -lc '
        idx="$1"
        start=$(perl -MTime::HiRes=time -e '"'"'printf "%.6f\n", time'"'"')
        if run_mc_put_with_timeout "${SAMPLE_FILE}" "bench/${BUCKET}/${PREFIX}/obj-${idx}.bin"; then
          finish=$(perl -MTime::HiRes=time -e '"'"'printf "%.6f\n", time'"'"')
          perl -e '"'"'
            use strict;
            use warnings;
            my ($idx, $start, $finish) = @ARGV;
            my $duration_ms = sprintf("%.3f", ($finish - $start) * 1000);
            print "{\"idx\":${idx},\"ok\":true,\"duration_ms\":${duration_ms}}\n";
          '"'"' -- "${idx}" "${start}" "${finish}"
        else
          finish=$(perl -MTime::HiRes=time -e '"'"'printf "%.6f\n", time'"'"')
          perl -e '"'"'
            use strict;
            use warnings;
            my ($idx, $start, $finish) = @ARGV;
            my $duration_ms = sprintf("%.3f", ($finish - $start) * 1000);
            print "{\"idx\":${idx},\"ok\":false,\"duration_ms\":${duration_ms}}\n";
          '"'"' -- "${idx}" "${start}" "${finish}"
        fi
      ' _ {} >"${raw_json}"
  else
    bench_deadline="$(perl -e 'use strict; use warnings; my ($start, $secs) = @ARGV; printf "%.6f", $start + $secs;' -- "${batch_start}" "${BENCH_SECONDS}")"
    pids=()
    for worker in $(seq 1 "${CONCURRENCY}"); do
      (
        out_file="${worker_dir}/worker-${worker}.jsonl"
        idx=0
        while true; do
          now="$(now_secs)"
          if perl -e 'use strict; use warnings; exit(($ARGV[0] >= $ARGV[1]) ? 0 : 1)' -- "${now}" "${bench_deadline}"; then
            break
          fi
          start="${now}"
          key="bench/${BUCKET}/${prefix}/worker-${worker}/obj-${idx}.bin"
          if run_mc_put_with_timeout "${sample_file}" "${key}"; then
            finish="$(now_secs)"
            perl -e '
              use strict;
              use warnings;
              my ($idx, $start, $finish) = @ARGV;
              my $duration_ms = sprintf("%.3f", ($finish - $start) * 1000);
              print "{\"idx\":${idx},\"ok\":true,\"duration_ms\":${duration_ms}}\n";
            ' -- "${idx}" "${start}" "${finish}" >>"${out_file}"
          else
            finish="$(now_secs)"
            perl -e '
              use strict;
              use warnings;
              my ($idx, $start, $finish) = @ARGV;
              my $duration_ms = sprintf("%.3f", ($finish - $start) * 1000);
              print "{\"idx\":${idx},\"ok\":false,\"duration_ms\":${duration_ms}}\n";
            ' -- "${idx}" "${start}" "${finish}" >>"${out_file}"
          fi
          idx=$((idx + 1))
        done
      ) &
      pids+=($!)
    done
    for pid in "${pids[@]}"; do
      wait "${pid}"
    done
    cat "${worker_dir}"/worker-*.jsonl >"${raw_json}"
  fi
  batch_finish="$(now_secs)"

  jq_summary="$(jq -s '
    def pct(p):
      if length == 0 then null
      else (sort_by(.duration_ms) | .[((length - 1) * p | floor)].duration_ms)
      end;
    {
      total: length,
      succeeded: (map(select(.ok == true)) | length),
      failed: (map(select(.ok == false)) | length),
      avg_ms: (if length == 0 then null else (map(.duration_ms) | add / length) end),
      p50_ms: pct(0.50),
      p90_ms: pct(0.90),
      p99_ms: pct(0.99)
    }
  ' "${raw_json}")"

  success_count="$(echo "${jq_summary}" | jq -r '.succeeded')"
  avg_ms="$(echo "${jq_summary}" | jq -r '.avg_ms')"
  p50_ms="$(echo "${jq_summary}" | jq -r '.p50_ms')"
  p90_ms="$(echo "${jq_summary}" | jq -r '.p90_ms')"
  p99_ms="$(echo "${jq_summary}" | jq -r '.p99_ms')"
  failed_count="$(echo "${jq_summary}" | jq -r '.failed')"

  wall_secs="$(perl -e 'use strict; use warnings; my ($start, $finish) = @ARGV; printf "%.6f", ($finish - $start);' -- "${batch_start}" "${batch_finish}")"
  mib_per_sec="$(perl -e 'use strict; use warnings; my ($bytes, $count, $secs) = @ARGV; printf "%.3f", (($bytes * $count) / (1024 * 1024)) / $secs;' -- "${bytes}" "${success_count}" "${wall_secs}")"
  obj_per_sec="$(perl -e 'use strict; use warnings; my ($count, $secs) = @ARGV; printf "%.3f", $count / $secs;' -- "${success_count}" "${wall_secs}")"

  {
    echo "## ${size_label}"
    echo
    echo "- Successful PUTs: \`${success_count}\`"
    echo "- Failed PUTs: \`${failed_count}\`"
    echo "- Wall time: \`${wall_secs}s\`"
    echo "- Throughput: \`${mib_per_sec} MiB/s\`"
    echo "- Object rate: \`${obj_per_sec} obj/s\`"
    echo "- Avg latency: \`${avg_ms} ms\`"
    echo "- p50 latency: \`${p50_ms} ms\`"
    echo "- p90 latency: \`${p90_ms} ms\`"
    echo "- p99 latency: \`${p99_ms} ms\`"
    echo
  } >>"${RESULTS_MD}"
done

echo
echo "Benchmark completed."
echo "Results: ${RESULTS_MD}"
echo "Server log: ${SERVER_LOG}"
