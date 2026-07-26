#!/usr/bin/env bash
set -euo pipefail

# Dedicated exact-1MiB GET attribution harness for rustfs/backlog#1434.
#
# The heavy lifting stays in run_get_codec_streaming_smoke.sh. This wrapper only
# fixes the experiment matrix so a reviewer can reproduce the isolated-host
# ABBA evidence without accidentally drifting object size, order, path proof, or
# stage-metrics capture.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SMOKE_RUNNER="${PROJECT_ROOT}/scripts/run_get_codec_streaming_smoke.sh"

ADDRESS="127.0.0.1:19030"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
BUCKET="rustfs-get-1mib-abba"
REGION="us-east-1"
CONCURRENCY=16
DURATION="30s"
ROUNDS=3
RETRY_PER_ROUND=1
ROUND_COOLDOWN_SECS=20
WARP_OBJECTS=""
WARP_OBJECT_LIFECYCLE="per-round"
WARP_PREPARE_DURATION="1s"
WARP_EXTRA_ARGS=""
WARP_WARMUP_GET_BEFORE_BENCH=false
OUTER_READER_STREAM_BUFFER_SIZES="65536,1048576"
PROFILE_ORDERS="normal,reverse"
CODEC_MIN_SIZE=1048576
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
PYTHON_BIN="python3"
OUT_DIR=""
DIAGNOSTIC_METRICS_URL="http://127.0.0.1:8889/metrics"
DIAGNOSTIC_METRICS_SETTLE_SECS="${RUSTFS_DIAGNOSTIC_METRICS_SETTLE_SECS:-2}"
DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS="${RUSTFS_DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS:-5}"
DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS="${RUSTFS_DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS:-1}"
DIAGNOSTIC_METRICS_CONNECT_TIMEOUT_SECS="${RUSTFS_DIAGNOSTIC_METRICS_CONNECT_TIMEOUT_SECS:-2}"
DIAGNOSTIC_METRICS_MAX_TIME_SECS="${RUSTFS_DIAGNOSTIC_METRICS_MAX_TIME_SECS:-15}"
DIAGNOSTIC_METRICS_FILTER_REGEX="${RUSTFS_DIAGNOSTIC_METRICS_FILTER_REGEX:-rustfs_io_get_object_}"
DIAGNOSTIC_OBS_ENDPOINT="${RUSTFS_OBS_ENDPOINT:-}"
DIAGNOSTIC_OBS_METRIC_ENDPOINT="${RUSTFS_OBS_METRIC_ENDPOINT:-}"
DIAGNOSTIC_OBS_METER_INTERVAL="${RUSTFS_OBS_METER_INTERVAL:-1}"
DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX="${RUSTFS_OBS_SERVICE_NAME:-RustFS-get-1mib-abba}"
RESOURCE_SAMPLE_INTERVAL_SECS="${RUSTFS_GET_BENCH_RESOURCE_SAMPLE_INTERVAL_SECS:-5}"
HEALTH_TIMEOUT_SECS=60
SKIP_BUILD=false
DRY_RUN=false
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_get_1mib_abba_stage_metrics.sh [options]

Purpose:
  Run the rustfs/backlog#1434 exact-1MiB isolated-host GET attribution matrix:
  - object size fixed to 1MiB / 1048576 bytes
  - legacy and codec-legacy read-path profiles
  - normal and reverse profile ordering for ABBA order-bias checks
  - two outer ReaderStream capacities via RUSTFS_GET_READER_STREAM_BUFFER_SIZE
  - output handoff attribution and GET stage metrics enabled

Core options:
  --outer-reader-stream-buffer-sizes <csv>
                                 RUSTFS_GET_READER_STREAM_BUFFER_SIZE values
                                 used for the outer response ReaderStream axis
                                 (default: 65536,1048576)
  --profile-orders <csv>         Profile orders to run (default: normal,reverse)
  --address <host:port>          RustFS listen address (default: 127.0.0.1:19030)
  --bucket <name>                Bucket prefix (default: rustfs-get-1mib-abba)
  --concurrency <n>              warp concurrency (default: 16)
  --duration <duration>          warp duration per round (default: 30s)
  --rounds <n>                   rounds per matrix cell (default: 3)
  --retry-per-round <n>          failed-attempt retries per round (default: 1)
  --round-cooldown-secs <n>      cooldown seconds after each completed round
                                 (default: 20)
  --warp-objects <n>             Number of objects prepared by warp
  --warp-object-lifecycle <mode> per-round|prepare-once|existing-only
                                 (default: per-round)
  --warp-prepare-duration <dur>  Duration used by prepare-once warmup
                                 (default: 1s)
  --warp-extra-args <args>       Extra args forwarded to warp through the smoke
                                 runner
  --warp-warmup-get-before-bench Run a GET warmup before measured rounds
  --out-dir <path>               Output directory (default target/bench/get-1mib-abba-stage-metrics-<timestamp>)

Diagnostics:
  --diagnostic-metrics-url <url> GET metrics scrape URL (default: http://127.0.0.1:8889/metrics)
  --diagnostic-metrics-settle-secs <n>
  --diagnostic-metrics-capture-attempts <n>
  --diagnostic-metrics-capture-retry-secs <n>
  --diagnostic-metrics-connect-timeout-secs <n>
  --diagnostic-metrics-max-time-secs <n>
  --diagnostic-metrics-filter-regex <regex>
  --diagnostic-obs-endpoint <url>
  --diagnostic-obs-metric-endpoint <url>
  --diagnostic-obs-meter-interval <secs>
  --diagnostic-obs-service-name-prefix <name>

Binary/options:
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --python-bin <path>            Python binary (default: python3)
  --skip-build                   Do not build RustFS in the smoke runner
  --dry-run                      Validate wiring without starting RustFS
  -h, --help                     Show this help

Output:
  <out-dir>/manifest.env
  <out-dir>/abba_matrix.csv
  <out-dir>/<order>-outer-<bytes>/manifest.env
  <out-dir>/<order>-outer-<bytes>/metrics_summary.csv
  <out-dir>/<order>-outer-<bytes>/baseline_compare.csv
  <out-dir>/<order>-outer-<bytes>/service_metrics_summary.csv
  <out-dir>/<order>-outer-<bytes>/service_metrics_round_summary.csv
  <out-dir>/<order>-outer-<bytes>/service_metrics_stage_distribution.csv
  <out-dir>/<order>-outer-<bytes>/service_metrics_round_percentiles.csv
  <out-dir>/<order>-outer-<bytes>/service_metrics_acceptance.csv
USAGE
}

die() {
  echo "error: $*" >&2
  exit 2
}

log() {
  printf '[get-1mib-abba] %s\n' "$*" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outer-reader-stream-buffer-sizes) OUTER_READER_STREAM_BUFFER_SIZES="$2"; shift 2 ;;
    --profile-orders) PROFILE_ORDERS="$2"; shift 2 ;;
    --address) ADDRESS="$2"; shift 2 ;;
    --bucket) BUCKET="$2"; shift 2 ;;
    --access-key) ACCESS_KEY="$2"; shift 2 ;;
    --secret-key) SECRET_KEY="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rounds) ROUNDS="$2"; shift 2 ;;
    --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
    --round-cooldown-secs) ROUND_COOLDOWN_SECS="$2"; shift 2 ;;
    --warp-objects) WARP_OBJECTS="$2"; shift 2 ;;
    --warp-object-lifecycle) WARP_OBJECT_LIFECYCLE="$2"; shift 2 ;;
    --warp-prepare-duration) WARP_PREPARE_DURATION="$2"; shift 2 ;;
    --warp-extra-args) WARP_EXTRA_ARGS="$2"; shift 2 ;;
    --warp-warmup-get-before-bench) WARP_WARMUP_GET_BEFORE_BENCH=true; shift ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --diagnostic-metrics-url) DIAGNOSTIC_METRICS_URL="$2"; shift 2 ;;
    --diagnostic-metrics-settle-secs) DIAGNOSTIC_METRICS_SETTLE_SECS="$2"; shift 2 ;;
    --diagnostic-metrics-capture-attempts) DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS="$2"; shift 2 ;;
    --diagnostic-metrics-capture-retry-secs) DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS="$2"; shift 2 ;;
    --diagnostic-metrics-connect-timeout-secs) DIAGNOSTIC_METRICS_CONNECT_TIMEOUT_SECS="$2"; shift 2 ;;
    --diagnostic-metrics-max-time-secs) DIAGNOSTIC_METRICS_MAX_TIME_SECS="$2"; shift 2 ;;
    --diagnostic-metrics-filter-regex) DIAGNOSTIC_METRICS_FILTER_REGEX="$2"; shift 2 ;;
    --diagnostic-obs-endpoint) DIAGNOSTIC_OBS_ENDPOINT="$2"; shift 2 ;;
    --diagnostic-obs-metric-endpoint) DIAGNOSTIC_OBS_METRIC_ENDPOINT="$2"; shift 2 ;;
    --diagnostic-obs-meter-interval) DIAGNOSTIC_OBS_METER_INTERVAL="$2"; shift 2 ;;
    --diagnostic-obs-service-name-prefix) DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX="$2"; shift 2 ;;
    --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
    --warp-bin) WARP_BIN="$2"; shift 2 ;;
    --python-bin) PYTHON_BIN="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -x "$SMOKE_RUNNER" ]] || die "missing smoke runner: $SMOKE_RUNNER"
[[ -n "$OUTER_READER_STREAM_BUFFER_SIZES" ]] || die "--outer-reader-stream-buffer-sizes must not be empty"
[[ -n "$PROFILE_ORDERS" ]] || die "--profile-orders must not be empty"

validate_positive_int() {
  local value="$1" label="$2"
  [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]] || die "${label} must be a positive integer"
}

validate_non_negative_int() {
  local value="$1" label="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "${label} must be a non-negative integer"
}

validate_positive_int "$CONCURRENCY" "--concurrency"
validate_positive_int "$ROUNDS" "--rounds"
validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
validate_non_negative_int "$ROUND_COOLDOWN_SECS" "--round-cooldown-secs"
validate_non_negative_int "$DIAGNOSTIC_METRICS_SETTLE_SECS" "--diagnostic-metrics-settle-secs"
validate_positive_int "$DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS" "--diagnostic-metrics-capture-attempts"
validate_non_negative_int "$DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS" "--diagnostic-metrics-capture-retry-secs"
validate_positive_int "$DIAGNOSTIC_METRICS_CONNECT_TIMEOUT_SECS" "--diagnostic-metrics-connect-timeout-secs"
validate_positive_int "$DIAGNOSTIC_METRICS_MAX_TIME_SECS" "--diagnostic-metrics-max-time-secs"
validate_positive_int "$DIAGNOSTIC_OBS_METER_INTERVAL" "--diagnostic-obs-meter-interval"

IFS=',' read -r -a outer_sizes <<< "$OUTER_READER_STREAM_BUFFER_SIZES"
IFS=',' read -r -a profile_orders <<< "$PROFILE_ORDERS"

cleaned_outer_sizes=()
for raw_outer_size in "${outer_sizes[@]}"; do
  clean_outer_size="${raw_outer_size//[[:space:]]/}"
  [[ -n "$clean_outer_size" ]] && cleaned_outer_sizes+=("$clean_outer_size")
done
outer_sizes=("${cleaned_outer_sizes[@]}")

cleaned_profile_orders=()
for raw_profile_order in "${profile_orders[@]}"; do
  clean_profile_order="${raw_profile_order//[[:space:]]/}"
  [[ -n "$clean_profile_order" ]] && cleaned_profile_orders+=("$clean_profile_order")
done
profile_orders=("${cleaned_profile_orders[@]}")
[[ "${#outer_sizes[@]}" -gt 0 ]] || die "--outer-reader-stream-buffer-sizes must include at least one value"
[[ "${#profile_orders[@]}" -gt 0 ]] || die "--profile-orders must include at least one value"

for outer_size in "${outer_sizes[@]}"; do
  validate_positive_int "$outer_size" "--outer-reader-stream-buffer-sizes entry"
done
for profile_order in "${profile_orders[@]}"; do
  case "$profile_order" in
    normal|reverse) ;;
    *) die "--profile-orders entries must be normal or reverse" ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="${PROJECT_ROOT}/target/bench/get-1mib-abba-stage-metrics-$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
fi
mkdir -p "$OUT_DIR"

command_line() {
  printf '%q ' "$0" "$@"
}

branch="$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD)"
git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
git_dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"
rustc_version="$(rustc --version 2>/dev/null || echo unavailable)"
cargo_version="$(cargo --version 2>/dev/null || echo unavailable)"

cat >"${OUT_DIR}/manifest.env" <<EOF
issue=rustfs/backlog#1434
generated_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
branch=${branch}
git_head=${git_head}
git_dirty_count=${git_dirty_count}
rustc_version=${rustc_version}
cargo_version=${cargo_version}
host_scope=isolated_local_single_node_four_disks
exact_size=1MiB
exact_size_bytes=1048576
read_path_profiles=legacy,codec-legacy
abba_profile_orders=${PROFILE_ORDERS}
outer_reader_stream_buffer_sizes=${OUTER_READER_STREAM_BUFFER_SIZES}
outer_reader_stream_env=RUSTFS_GET_READER_STREAM_BUFFER_SIZE
inner_legacy_duplex_buffer_policy=adaptive_duplex_buffer_size
exact_1mib_legacy_duplex_buffer_bytes=65536
codec_engine=legacy
codec_min_size=${CODEC_MIN_SIZE}
handoff_attribution=true
diagnostic_metrics=true
diagnostic_metrics_url=${DIAGNOSTIC_METRICS_URL}
stage_metrics_artifacts=service_metrics_summary.csv,service_metrics_round_summary.csv,service_metrics_stage_distribution.csv,service_metrics_round_percentiles.csv
body_header_parity_artifacts=compat_summary.csv,response_headers_legacy.json,response_headers_codec_legacy.json,body_sha256_legacy.txt,body_sha256_codec_legacy.txt
performance_conclusion=not_encoded_by_harness_collect_raw_abba_stage_metrics_first
address=${ADDRESS}
bucket=${BUCKET}
region=${REGION}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
warp_objects=${WARP_OBJECTS}
warp_object_lifecycle=${WARP_OBJECT_LIFECYCLE}
warp_prepare_duration=${WARP_PREPARE_DURATION}
warp_extra_args=${WARP_EXTRA_ARGS}
warp_warmup_get_before_bench=${WARP_WARMUP_GET_BEFORE_BENCH}
skip_build=${SKIP_BUILD}
dry_run=${DRY_RUN}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
python_bin=${PYTHON_BIN}
command_line=$(command_line "${ORIGINAL_ARGS[@]}")
EOF

matrix_csv="${OUT_DIR}/abba_matrix.csv"
cat >"$matrix_csv" <<'EOF'
order,outer_reader_stream_buffer_size,run_dir,read_path_profiles,expected_profile_sequence,stage_metrics_required,body_header_parity_required,status
EOF

run_cell() {
  local profile_order="$1"
  local outer_size="$2"
  local run_name="${profile_order}-outer-${outer_size}"
  local run_dir="${OUT_DIR}/${run_name}"
  local bucket="${BUCKET}-${profile_order}-${outer_size}"
  local profile_sequence
  case "$profile_order" in
    normal) profile_sequence="legacy>codec-legacy" ;;
    reverse) profile_sequence="codec-legacy>legacy" ;;
    *) die "unexpected profile order: $profile_order" ;;
  esac

  log "running cell order=${profile_order} outer_reader_stream_buffer_size=${outer_size} out=${run_dir}"
  local cmd=(
    "$SMOKE_RUNNER"
    --mode both
    --profile-order "$profile_order"
    --codec-engine legacy
    --codec-min-size "$CODEC_MIN_SIZE"
    --handoff-attribution
    --diagnostic-metrics
    --diagnostic-metrics-url "$DIAGNOSTIC_METRICS_URL"
    --diagnostic-metrics-settle-secs "$DIAGNOSTIC_METRICS_SETTLE_SECS"
    --diagnostic-metrics-capture-attempts "$DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS"
    --diagnostic-metrics-capture-retry-secs "$DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS"
    --diagnostic-metrics-connect-timeout-secs "$DIAGNOSTIC_METRICS_CONNECT_TIMEOUT_SECS"
    --diagnostic-metrics-max-time-secs "$DIAGNOSTIC_METRICS_MAX_TIME_SECS"
    --diagnostic-metrics-filter-regex "$DIAGNOSTIC_METRICS_FILTER_REGEX"
    --diagnostic-obs-meter-interval "$DIAGNOSTIC_OBS_METER_INTERVAL"
    --diagnostic-obs-service-name-prefix "${DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX}-${profile_order}-${outer_size}"
    --address "$ADDRESS"
    --bucket "$bucket"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --region "$REGION"
    --sizes 1MiB
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --round-cooldown-secs "$ROUND_COOLDOWN_SECS"
    --warp-object-lifecycle "$WARP_OBJECT_LIFECYCLE"
    --warp-prepare-duration "$WARP_PREPARE_DURATION"
    --out-dir "$run_dir"
    --rustfs-bin "$RUSTFS_BIN"
    --warp-bin "$WARP_BIN"
    --python-bin "$PYTHON_BIN"
    --resource-sample-interval-secs "$RESOURCE_SAMPLE_INTERVAL_SECS"
  )
  if [[ -n "$DIAGNOSTIC_OBS_ENDPOINT" ]]; then
    cmd+=(--diagnostic-obs-endpoint "$DIAGNOSTIC_OBS_ENDPOINT")
  fi
  if [[ -n "$DIAGNOSTIC_OBS_METRIC_ENDPOINT" ]]; then
    cmd+=(--diagnostic-obs-metric-endpoint "$DIAGNOSTIC_OBS_METRIC_ENDPOINT")
  fi
  if [[ -n "$WARP_OBJECTS" ]]; then
    cmd+=(--warp-objects "$WARP_OBJECTS")
  fi
  if [[ -n "$WARP_EXTRA_ARGS" ]]; then
    cmd+=(--warp-extra-args "$WARP_EXTRA_ARGS")
  fi
  if [[ "$WARP_WARMUP_GET_BEFORE_BENCH" == "true" ]]; then
    cmd+=(--warp-warmup-get-before-bench)
  fi
  if [[ "$SKIP_BUILD" == "true" ]]; then
    cmd+=(--skip-build)
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  if RUSTFS_GET_READER_STREAM_BUFFER_SIZE="$outer_size" "${cmd[@]}"; then
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$profile_order" "$outer_size" "$run_dir" "legacy;codec-legacy" "$profile_sequence" \
      "service_metrics_round_summary.csv;service_metrics_stage_distribution.csv;service_metrics_round_percentiles.csv" \
      "compat_summary.csv;response_headers_legacy.json;response_headers_codec_legacy.json;body_sha256_legacy.txt;body_sha256_codec_legacy.txt" \
      "ok" >>"$matrix_csv"
  else
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$profile_order" "$outer_size" "$run_dir" "legacy;codec-legacy" "$profile_sequence" \
      "service_metrics_round_summary.csv;service_metrics_stage_distribution.csv;service_metrics_round_percentiles.csv" \
      "compat_summary.csv;response_headers_legacy.json;response_headers_codec_legacy.json;body_sha256_legacy.txt;body_sha256_codec_legacy.txt" \
      "failed" >>"$matrix_csv"
    return 1
  fi
}

failures=0
for outer_size in "${outer_sizes[@]}"; do
  for profile_order in "${profile_orders[@]}"; do
    if ! run_cell "$profile_order" "$outer_size"; then
      failures=$((failures + 1))
    fi
  done
done

log "manifest: ${OUT_DIR}/manifest.env"
log "matrix: ${OUT_DIR}/abba_matrix.csv"
if [[ "$failures" -gt 0 ]]; then
  die "${failures} matrix cell(s) failed"
fi
log "exact-1MiB ABBA stage metrics harness finished."
