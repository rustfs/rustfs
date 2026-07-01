#!/usr/bin/env bash
set -euo pipefail

# Enhanced batch object benchmark runner for warp/s3bench:
# - Multi-round execution (default 3 rounds)
# - Retry failed round attempts automatically
# - Median aggregation per object size
# - Optional baseline CSV comparison

DEFAULT_SIZES="1KiB,4KiB,8KiB,16KiB,32KiB,100KiB,512KiB,1MiB,2MiB,5MiB,10MiB"

TOOL="warp"
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
BUCKET="rustfs-bench"
BUCKET_SIZE_SUFFIX=false
REGION="us-east-1"
CONCURRENCY=128
SIZES="$DEFAULT_SIZES"
OUT_DIR=""
INSECURE=false
DRY_RUN=false
PYTHON_BIN="python3"

# warp options
WARP_BIN="warp"
WARP_MODE="mixed"
DURATION="60s"

# s3bench options
S3BENCH_BIN="s3bench"
SAMPLES=20000

# enhancement options
ROUNDS=3
RETRY_PER_ROUND=2
RETRY_SLEEP_SECS=2
COOLDOWN_SECS=0
BASELINE_CSV=""
EXTRA_ARGS=()
FAILED_FINAL_ROUNDS=0
SERVICE_METRICS_URL=""
SERVICE_METRICS_DIR=""
SERVICE_METRICS_CAPTURE_ATTEMPTS=3
SERVICE_METRICS_CAPTURE_RETRY_SECS=1
SERVICE_METRICS_CONNECT_TIMEOUT_SECS=2
SERVICE_METRICS_MAX_TIME_SECS=15
SERVICE_METRICS_SETTLE_SECS=0
SERVICE_METRICS_FILTER_REGEX=""
SERVICE_METRICS_FILTER_REGEX_EXPLICIT=false
SERVICE_PROMETHEUS_QUERY_URL=""
SERVICE_PROMETHEUS_QUERY=""
SERVICE_PROMETHEUS_QUERY_EXPLICIT=false
SERVICE_METRICS_SERVICE_NAME=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_object_batch_bench_enhanced.sh --tool <warp|s3bench> --endpoint <host:port|url> \
    --access-key <ak> --secret-key <sk> [options]

Required:
  --tool                       warp | s3bench
  --endpoint                   S3 endpoint
  --access-key                 S3 access key
  --secret-key                 S3 secret key

Core options:
  --bucket                     Bucket name (default: rustfs-bench)
  --bucket-size-suffix         Append a sanitized object size suffix to the
                               bucket for each size
  --region                     Region (default: us-east-1)
  --concurrency                Concurrency for all sizes (default: 128)
  --sizes                      Comma-separated sizes (default: 1KiB..10MiB matrix)
  --out-dir                    Output directory (default: target/bench/object-batch-enhanced-<timestamp>)
  --insecure                   Allow insecure TLS
  --dry-run                    Print commands only, do not execute
  --python-bin                 Python binary for Prometheus query capture (default: python3)

Warp options:
  --warp-bin                   warp binary (default: warp)
  --warp-mode                  get|put|mixed (default: mixed)
  --duration                   e.g. 60s/2m (default: 60s)

s3bench options:
  --s3bench-bin                s3bench binary (default: s3bench)
  --samples                    numSamples (default: 20000)

Enhanced options:
  --rounds                     Benchmark rounds per size (default: 3)
  --retry-per-round            Retry count per failed round (default: 2)
  --retry-sleep-secs           Sleep seconds between retries (default: 2)
  --cooldown-secs              Sleep seconds between rounds/sizes (default: 0)
  --round-cooldown-secs        Compatibility alias for --cooldown-secs
  --baseline-csv               Baseline median CSV to compare
  --extra-args                 Extra args appended to tool command, quoted as one string
  --service-metrics-url        Optional Prometheus scrape URL captured before/after each round attempt
  --service-prometheus-query-url
                               Optional Prometheus HTTP API /api/v1/query URL for OTLP-exported metrics
  --service-prometheus-query   PromQL used with --service-prometheus-query-url
                               (default: auto by tool/mode)
  --service-metrics-service-name
                               Optional service.name/service_name label filter for Prometheus query results
  --service-metrics-dir        Output directory for per-round service metric snapshots
  --service-metrics-attempts   Capture attempts for each snapshot (default: 3)
  --service-metrics-retry-secs Sleep seconds between failed capture attempts (default: 1)
  --service-metrics-connect-timeout-secs
                               Curl connect timeout for each metrics capture (default: 2)
  --service-metrics-max-time-secs
                               Curl max time for each metrics capture (default: 15)
  --service-metrics-settle-secs
                               Sleep seconds before after-snapshot capture (default: 0)
  --service-metrics-filter-regex
                               Regex for retained metrics lines after scrape
                               (default: auto by tool/mode)

Output files:
  round_results.csv            One row per round attempt (with retry trace)
  median_summary.csv           Median metrics per object size
  baseline_compare.csv         Delta vs baseline (if --baseline-csv is set)
  <service-metrics-dir>/*.prom  Optional per-round service metric snapshots

Example:
  scripts/run_object_batch_bench_enhanced.sh \
    --tool warp --endpoint http://127.0.0.1:9000 \
    --access-key minioadmin --secret-key minioadmin \
    --bucket bench-obj --concurrency 128 --duration 90s \
    --rounds 3 --retry-per-round 2 --baseline-csv old/median_summary.csv
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

normalize_warp_host() {
  local raw="$1"
  raw="${raw#http://}"
  raw="${raw#https://}"
  raw="${raw%%/*}"
  raw="${raw%%\?*}"
  raw="${raw%%\#*}"
  echo "$raw"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --tool) TOOL="$2"; shift 2 ;;
      --endpoint) ENDPOINT="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --bucket-size-suffix) BUCKET_SIZE_SUFFIX=true; shift ;;
      --region) REGION="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      --python-bin) PYTHON_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --warp-mode) WARP_MODE="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --s3bench-bin) S3BENCH_BIN="$2"; shift 2 ;;
      --samples) SAMPLES="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --retry-sleep-secs) RETRY_SLEEP_SECS="$2"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$2"; shift 2 ;;
      --round-cooldown-secs) COOLDOWN_SECS="$2"; shift 2 ;;
      --baseline-csv) BASELINE_CSV="$2"; shift 2 ;;
      --service-metrics-url) SERVICE_METRICS_URL="$2"; shift 2 ;;
      --service-prometheus-query-url) SERVICE_PROMETHEUS_QUERY_URL="$2"; shift 2 ;;
      --service-prometheus-query) SERVICE_PROMETHEUS_QUERY="$2"; SERVICE_PROMETHEUS_QUERY_EXPLICIT=true; shift 2 ;;
      --service-metrics-service-name) SERVICE_METRICS_SERVICE_NAME="$2"; shift 2 ;;
      --service-metrics-dir) SERVICE_METRICS_DIR="$2"; shift 2 ;;
      --service-metrics-attempts) SERVICE_METRICS_CAPTURE_ATTEMPTS="$2"; shift 2 ;;
      --service-metrics-retry-secs) SERVICE_METRICS_CAPTURE_RETRY_SECS="$2"; shift 2 ;;
      --service-metrics-connect-timeout-secs) SERVICE_METRICS_CONNECT_TIMEOUT_SECS="$2"; shift 2 ;;
      --service-metrics-max-time-secs) SERVICE_METRICS_MAX_TIME_SECS="$2"; shift 2 ;;
      --service-metrics-settle-secs) SERVICE_METRICS_SETTLE_SECS="$2"; shift 2 ;;
      --service-metrics-filter-regex) SERVICE_METRICS_FILTER_REGEX="$2"; SERVICE_METRICS_FILTER_REGEX_EXPLICIT=true; shift 2 ;;
      --extra-args)
        # shellcheck disable=SC2206
        EXTRA_ARGS=($2)
        shift 2
        ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

validate_positive_int() {
  local v="$1"
  local n="$2"
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [[ "$v" -le 0 ]]; then
    echo "ERROR: $n must be a positive integer, got: $v" >&2
    exit 1
  fi
}

validate_nonnegative_int() {
  local v="$1"
  local n="$2"
  if ! [[ "$v" =~ ^[0-9]+$ ]]; then
    echo "ERROR: $n must be a nonnegative integer, got: $v" >&2
    exit 1
  fi
}

cooldown_sleep() {
  local secs="$1"
  if (( secs <= 0 )); then
    return 0
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] sleep ${secs}"
  else
    sleep "$secs"
  fi
}

default_service_metrics_filter_regex() {
  if [[ "$TOOL" == "warp" ]]; then
    case "$WARP_MODE" in
      get)
        echo "rustfs_io_get_object_|rustfs_s3_get_object_|rustfs_zero_copy_read|rustfs_mmap_|rustfs_get_object_"
        return
        ;;
      put)
        echo "rustfs_s3_put_object_|rustfs_io_put_object_|rustfs_zero_copy_write|rustfs_buffer_|rustfs_ec_|rustfs_io_bytespool_"
        return
        ;;
      mixed)
        echo "rustfs_s3_get_object_|rustfs_io_get_object_|rustfs_s3_put_object_|rustfs_io_put_object_|rustfs_zero_copy_|rustfs_buffer_|rustfs_ec_"
        return
        ;;
    esac
  fi

  echo "rustfs_s3_put_object_|rustfs_io_put_object_|rustfs_s3_get_object_|rustfs_io_get_object_"
}

default_service_prometheus_query() {
  if [[ "$TOOL" == "warp" ]]; then
    case "$WARP_MODE" in
      get)
        echo '{__name__=~"rustfs_(io_get_object|s3_get_object|zero_copy_read|mmap|get_object)_.*"}'
        return
        ;;
      put)
        echo '{__name__=~"rustfs_(s3_put_object|io_put_object|zero_copy_write|buffer|ec|io_bytespool)_.*"}'
        return
        ;;
      mixed)
        echo '{__name__=~"rustfs_(s3_get_object|io_get_object|s3_put_object|io_put_object|zero_copy|buffer|ec)_.*"}'
        return
        ;;
    esac
  fi

  echo '{__name__=~"rustfs_(s3_put_object|io_put_object|s3_get_object|io_get_object)_.*"}'
}

validate_args() {
  if [[ "$TOOL" != "warp" && "$TOOL" != "s3bench" ]]; then
    echo "ERROR: --tool must be warp or s3bench" >&2
    exit 1
  fi
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint/--access-key/--secret-key are required" >&2
    exit 1
  fi
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_positive_int "$RETRY_SLEEP_SECS" "--retry-sleep-secs"
  validate_nonnegative_int "$COOLDOWN_SECS" "--cooldown-secs"
  validate_positive_int "$SERVICE_METRICS_CAPTURE_ATTEMPTS" "--service-metrics-attempts"
  validate_nonnegative_int "$SERVICE_METRICS_CAPTURE_RETRY_SECS" "--service-metrics-retry-secs"
  validate_positive_int "$SERVICE_METRICS_CONNECT_TIMEOUT_SECS" "--service-metrics-connect-timeout-secs"
  validate_positive_int "$SERVICE_METRICS_MAX_TIME_SECS" "--service-metrics-max-time-secs"
  validate_nonnegative_int "$SERVICE_METRICS_SETTLE_SECS" "--service-metrics-settle-secs"
  if [[ -n "$SERVICE_METRICS_URL" && -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    echo "ERROR: --service-metrics-url and --service-prometheus-query-url are mutually exclusive" >&2
    exit 1
  fi
  if [[ -n "$SERVICE_METRICS_URL" || -n "$SERVICE_PROMETHEUS_QUERY_URL" ]] && [[ -z "$SERVICE_METRICS_DIR" ]]; then
    echo "ERROR: --service-metrics-dir is required when service metrics capture is enabled" >&2
    exit 1
  fi
  if [[ -n "$SERVICE_METRICS_URL" ]]; then
    require_cmd curl
  fi
  if [[ -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    require_cmd "$PYTHON_BIN"
  fi
  if [[ "$TOOL" == "s3bench" ]]; then
    validate_positive_int "$SAMPLES" "--samples"
  fi
  if [[ -n "$BASELINE_CSV" && ! -f "$BASELINE_CSV" ]]; then
    echo "ERROR: --baseline-csv does not exist: $BASELINE_CSV" >&2
    exit 1
  fi
  if [[ "$TOOL" == "warp" ]]; then
    if [[ "$WARP_MODE" != "get" && "$WARP_MODE" != "put" && "$WARP_MODE" != "mixed" ]]; then
      echo "ERROR: --warp-mode must be get, put, or mixed" >&2
      exit 1
    fi
    local warp_host
    warp_host="$(normalize_warp_host "$ENDPOINT")"
    if [[ -z "$warp_host" ]]; then
      echo "ERROR: invalid --endpoint for warp: $ENDPOINT" >&2
      exit 1
    fi
  fi
  if [[ "$SERVICE_METRICS_FILTER_REGEX_EXPLICIT" != "true" && -z "$SERVICE_METRICS_FILTER_REGEX" ]]; then
    SERVICE_METRICS_FILTER_REGEX="$(default_service_metrics_filter_regex)"
  fi
  if [[ "$SERVICE_PROMETHEUS_QUERY_EXPLICIT" != "true" && -z "$SERVICE_PROMETHEUS_QUERY" ]]; then
    SERVICE_PROMETHEUS_QUERY="$(default_service_prometheus_query)"
  fi
}

git_value() {
  local default_value="$1"
  shift
  git "$@" 2>/dev/null || echo "$default_value"
}

git_dirty_state() {
  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "unknown"
    return
  fi
  if ! git diff --quiet --ignore-submodules --; then
    echo "dirty"
    return
  fi
  if ! git diff --cached --quiet --ignore-submodules --; then
    echo "dirty"
    return
  fi
  echo "clean"
}

write_run_manifest() {
  local manifest_file="$OUT_DIR/run_manifest.env"
  local started_at_utc git_commit git_branch git_dirty rustc_version uname_s service_metrics_csv
  started_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  git_commit="$(git_value unknown rev-parse --short HEAD)"
  git_branch="$(git_value unknown branch --show-current)"
  git_dirty="$(git_dirty_state)"
  rustc_version="$(rustc --version 2>/dev/null || echo unknown)"
  uname_s="$(uname -a 2>/dev/null || echo unknown)"
  service_metrics_csv="${SERVICE_METRICS_CSV:-}"

  cat >"$manifest_file" <<EOF
started_at_utc=${started_at_utc}
git_commit=${git_commit}
git_branch=${git_branch}
git_dirty=${git_dirty}
rustc_version=${rustc_version}
uname=${uname_s}
tool=${TOOL}
endpoint=${ENDPOINT}
access_key=REDACTED
secret_key=REDACTED
bucket=${BUCKET}
bucket_size_suffix=${BUCKET_SIZE_SUFFIX}
region=${REGION}
concurrency=${CONCURRENCY}
sizes=${SIZES}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
retry_sleep_secs=${RETRY_SLEEP_SECS}
cooldown_secs=${COOLDOWN_SECS}
warp_bin=${WARP_BIN}
warp_mode=${WARP_MODE}
duration=${DURATION}
s3bench_bin=${S3BENCH_BIN}
samples=${SAMPLES}
baseline_csv=${BASELINE_CSV}
service_metrics_url=${SERVICE_METRICS_URL}
service_prometheus_query_url=${SERVICE_PROMETHEUS_QUERY_URL}
service_prometheus_query=${SERVICE_PROMETHEUS_QUERY}
service_prometheus_query_explicit=${SERVICE_PROMETHEUS_QUERY_EXPLICIT}
service_metrics_service_name=${SERVICE_METRICS_SERVICE_NAME}
service_metrics_dir=${SERVICE_METRICS_DIR}
service_metrics_csv=${service_metrics_csv}
service_metrics_filter_regex=${SERVICE_METRICS_FILTER_REGEX}
service_metrics_filter_regex_explicit=${SERVICE_METRICS_FILTER_REGEX_EXPLICIT}
service_metrics_capture_attempts=${SERVICE_METRICS_CAPTURE_ATTEMPTS}
service_metrics_retry_secs=${SERVICE_METRICS_CAPTURE_RETRY_SECS}
service_metrics_connect_timeout_secs=${SERVICE_METRICS_CONNECT_TIMEOUT_SECS}
service_metrics_max_time_secs=${SERVICE_METRICS_MAX_TIME_SECS}
service_metrics_settle_secs=${SERVICE_METRICS_SETTLE_SECS}
dry_run=${DRY_RUN}
EOF
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/object-batch-enhanced-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR/logs"
  if [[ -n "$SERVICE_METRICS_URL" || -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    mkdir -p "$SERVICE_METRICS_DIR"
  fi

  ROUND_CSV="$OUT_DIR/round_results.csv"
  MEDIAN_CSV="$OUT_DIR/median_summary.csv"
  COMPARE_CSV="$OUT_DIR/baseline_compare.csv"
  SERVICE_METRICS_CSV="$OUT_DIR/service_metrics_captures.csv"

  echo "size,tool,round,attempt,concurrency,status,exit_code,round_started_at_utc,round_finished_at_utc,throughput_human,throughput_bps,reqps,latency_human,latency_ms,log_file,req_p90_human,req_p90_ms,req_p99_human,req_p99_ms" > "$ROUND_CSV"
  echo "size,tool,concurrency,successful_rounds,failed_rounds,median_throughput_bps,median_reqps,median_latency_ms,median_req_p90_ms,median_req_p99_ms" > "$MEDIAN_CSV"
  if [[ -n "$SERVICE_METRICS_URL" || -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    echo "size,tool,round,attempt,phase,source,status,capture_attempt,raw_bytes,snapshot_bytes,status_file,snapshot_file,filter_regex,prometheus_query" > "$SERVICE_METRICS_CSV"
  fi
  write_run_manifest
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

sanitize_bucket_suffix() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9-' '-'
}

bucket_for_size() {
  local size="$1"
  if [[ "$BUCKET_SIZE_SUFFIX" != "true" ]]; then
    echo "$BUCKET"
    return
  fi
  echo "${BUCKET}-$(sanitize_bucket_suffix "$size")"
}

to_bps() {
  local human="$1"
  local number unit factor
  if [[ "$human" == "N/A" || -z "$human" ]]; then
    echo "N/A"
    return
  fi

  # Normalize "123MiB/s" → "123 MiB/s" when no space separator exists.
  human="$(echo "$human" | sed -E 's/^([0-9]+(\.[0-9]+)?)([A-Za-zµ])/\1 \3/')"
  number="$(echo "$human" | awk '{print $1}')"
  unit="$(echo "$human" | awk '{print $2}')"
  case "$unit" in
    GiB/s) factor=1073741824 ;;
    MiB/s) factor=1048576 ;;
    KiB/s) factor=1024 ;;
    GB/s) factor=1000000000 ;;
    MB/s) factor=1000000 ;;
    KB/s) factor=1000 ;;
    B/s) factor=1 ;;
    *)
      echo "N/A"
      return
      ;;
  esac

  awk -v n="$number" -v f="$factor" 'BEGIN { printf "%.6f\n", n * f }'
}

to_ms() {
  local human="$1"
  local number unit factor
  if [[ "$human" == "N/A" || -z "$human" ]]; then
    echo "N/A"
    return
  fi

  # Normalize "50ms" → "50 ms" when no space separator exists.
  human="$(echo "$human" | sed -E 's/^([0-9]+(\.[0-9]+)?)([A-Za-zµ])/\1 \3/')"
  number="$(echo "$human" | awk '{print $1}')"
  unit="$(echo "$human" | awk '{print $2}')"
  case "$unit" in
    s) factor=1000 ;;
    ms) factor=1 ;;
    us|µs) factor=0.001 ;;
    *)
      echo "N/A"
      return
      ;;
  esac

  awk -v n="$number" -v f="$factor" 'BEGIN { printf "%.6f\n", n * f }'
}

extract_first() {
  local regex="$1"
  local file="$2"
  rg -o "$regex" "$file" | head -n1 || true
}

normalize_duration_metric() {
  local value="$1"
  value="$(trim "${value:-N/A}")"

  if [[ -z "$value" || "$value" == "N/A" ]]; then
    echo "N/A"
    return
  fi

  if [[ "$value" =~ ^([0-9]+(\.[0-9]+)?)(ms|us|µs|s)$ ]]; then
    echo "${BASH_REMATCH[1]} ${BASH_REMATCH[3]}"
    return
  fi

  echo "$value" | awk '{ if ($1 == "" || $1 == "N/A") print "N/A"; else print $1" "$2 }'
}

extract_metrics() {
  local log_file="$1"

  local throughput reqps latency req_p90 req_p99
  throughput="$(extract_first '[0-9]+(\.[0-9]+)?[[:space:]]*(GiB/s|MiB/s|KiB/s|GB/s|MB/s|KB/s|B/s)' "$log_file")"
  reqps="$(extract_first '[0-9]+(\.[0-9]+)?[[:space:]]*(obj/s|req/s|ops/s|requests/s)' "$log_file")"
  latency="$(rg -o 'Reqs:[[:space:]]+Avg:[[:space:]]+[0-9]+(\.[0-9]+)?(ms|us|µs|s)' "$log_file" | head -n1 | sed -E 's/^Reqs:[[:space:]]+Avg:[[:space:]]+//')"
  req_p90="$(rg -o '90%:[[:space:]]+[0-9]+(\.[0-9]+)?(ms|us|µs|s)' "$log_file" | head -n1 | sed -E 's/^90%:[[:space:]]+//')"
  req_p99="$(rg -o '99%:[[:space:]]+[0-9]+(\.[0-9]+)?(ms|us|µs|s)' "$log_file" | head -n1 | sed -E 's/^99%:[[:space:]]+//')"

  if [[ -z "$latency" ]]; then
    latency="$(extract_first '[0-9]+(\.[0-9]+)?[[:space:]]*(ms|us|µs|s)' "$log_file")"
  fi

  throughput="$(trim "${throughput:-N/A}")"
  reqps="$(trim "${reqps:-N/A}")"
  latency="$(trim "${latency:-N/A}")"
  req_p90="$(trim "${req_p90:-N/A}")"
  req_p99="$(trim "${req_p99:-N/A}")"

  # Normalize to "<num> <unit>" shape for downstream conversion helpers.
  latency="$(normalize_duration_metric "$latency")"
  req_p90="$(normalize_duration_metric "$req_p90")"
  req_p99="$(normalize_duration_metric "$req_p99")"
  reqps_num="$(echo "$reqps" | awk '{print $1}')"

  echo "$throughput,${reqps_num:-N/A},$latency,$req_p90,$req_p99"
}

metric_snapshot_token() {
  local size="$1"
  local round="$2"
  local attempt="$3"
  local safe_size
  safe_size="$(printf '%s' "$size" | tr -c '[:alnum:]_.-' '_')"
  printf '%s_%s_r%s_a%s' "$TOOL" "$safe_size" "$round" "$attempt"
}

filter_service_metrics_snapshot() {
  local input_file="$1"
  local output_file="$2"

  if [[ -z "$SERVICE_METRICS_FILTER_REGEX" ]]; then
    mv "$input_file" "$output_file"
    return 0
  fi

  awk -v pattern="$SERVICE_METRICS_FILTER_REGEX" '$0 ~ pattern { print }' "$input_file" >"$output_file"
  if [[ ! -s "$output_file" ]]; then
    mv "$input_file" "$output_file"
  else
    rm -f "$input_file"
  fi
}

settle_before_after_metrics_capture() {
  if (( SERVICE_METRICS_SETTLE_SECS <= 0 )); then
    return 0
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] sleep ${SERVICE_METRICS_SETTLE_SECS}"
  else
    sleep "$SERVICE_METRICS_SETTLE_SECS"
  fi
}

capture_prometheus_query_snapshot() {
  local phase="$1"
  local snapshot_file="$2"
  local status_file="$3"

  "$PYTHON_BIN" - "$SERVICE_PROMETHEUS_QUERY_URL" "$SERVICE_PROMETHEUS_QUERY" "$phase" "$snapshot_file" "$status_file" "$SERVICE_METRICS_SERVICE_NAME" "$SERVICE_METRICS_SETTLE_SECS" <<'PY'
import json
import pathlib
import sys
import urllib.parse
import urllib.request

query_url, query, phase, snapshot_raw, status_raw, service_name, settle_secs = sys.argv[1:]
snapshot_path = pathlib.Path(snapshot_raw)
status_path = pathlib.Path(status_raw)


def write_status(status):
    status_path.write_text(
        "\n".join(
            [
                f"phase={phase}",
                f"status={status}",
                f"url={query_url}",
                f"query={query}",
                f"service_name={service_name}",
                f"settle_secs={settle_secs}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def escape_label(value):
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def sample_to_line(sample):
    metric = dict(sample.get("metric", {}))
    name = metric.pop("__name__", "")
    if not name:
        return None
    labels = ",".join(f'{key}="{escape_label(value)}"' for key, value in sorted(metric.items()))
    value = sample.get("value", [None, None])[1]
    if value is None:
        return None
    if labels:
        return f"{name}{{{labels}}} {value}"
    return f"{name} {value}"


try:
    separator = "&" if "?" in query_url else "?"
    url = f"{query_url}{separator}{urllib.parse.urlencode({'query': query})}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
except Exception as err:  # noqa: BLE001 - shell harness reports failures in status files.
    snapshot_path.write_text(f"# prometheus_query_failed error={err}\n", encoding="utf-8")
    write_status("query_failed")
    raise SystemExit(0)

if payload.get("status") != "success":
    snapshot_path.write_text(f"# prometheus_query_failed payload_status={payload.get('status')}\n", encoding="utf-8")
    write_status("query_failed")
    raise SystemExit(0)

samples = [
    sample
    for sample in payload.get("data", {}).get("result", [])
    if not service_name
    or sample.get("metric", {}).get("service.name") == service_name
    or sample.get("metric", {}).get("service_name") == service_name
]
lines = [line for sample in samples if (line := sample_to_line(sample))]
if not lines:
    snapshot_path.write_text(f"# no_matching_metrics service_name={service_name}\n", encoding="utf-8")
    write_status("no_matching_metrics")
    raise SystemExit(0)

snapshot_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
write_status("ok")
PY
}

capture_round_service_metrics() {
  local size="$1"
  local round="$2"
  local attempt="$3"
  local phase="$4"

  if [[ -z "$SERVICE_METRICS_URL" && -z "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    return 0
  fi

  local token snapshot_file status_file tmp_file filtered_file capture_attempt raw_bytes snapshot_bytes capture_status
  token="$(metric_snapshot_token "$size" "$round" "$attempt")"
  snapshot_file="${SERVICE_METRICS_DIR}/${token}_${phase}.prom"
  status_file="${SERVICE_METRICS_DIR}/${token}_${phase}.status"
  tmp_file="${snapshot_file}.tmp"
  filtered_file="${snapshot_file}.filtered.tmp"

  if [[ "$DRY_RUN" == "true" ]]; then
    : >"$snapshot_file"
    cat >"$status_file" <<EOF
size=${size}
round=${round}
attempt=${attempt}
phase=${phase}
status=not_run_dry_run
url=${SERVICE_METRICS_URL}
prometheus_query_url=${SERVICE_PROMETHEUS_QUERY_URL}
filter_regex=${SERVICE_METRICS_FILTER_REGEX}
prometheus_query=${SERVICE_PROMETHEUS_QUERY}
settle_secs=${SERVICE_METRICS_SETTLE_SECS}
EOF
    echo "$size,$TOOL,$round,$attempt,$phase,dry_run,not_run_dry_run,0,0,0,$status_file,$snapshot_file,$SERVICE_METRICS_FILTER_REGEX,$SERVICE_PROMETHEUS_QUERY" >> "$SERVICE_METRICS_CSV"
    return 0
  fi

  if [[ -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    capture_prometheus_query_snapshot "$phase" "$snapshot_file" "$status_file"
    capture_status="$(awk -F= '$1=="status" {print $2; exit}' "$status_file")"
    snapshot_bytes="$(wc -c <"$snapshot_file" | tr -d '[:space:]')"
    echo "$size,$TOOL,$round,$attempt,$phase,prometheus_query,${capture_status:-unknown},1,$snapshot_bytes,$snapshot_bytes,$status_file,$snapshot_file,$SERVICE_METRICS_FILTER_REGEX,$SERVICE_PROMETHEUS_QUERY" >> "$SERVICE_METRICS_CSV"
    return 0
  fi

  for ((capture_attempt=1; capture_attempt<=SERVICE_METRICS_CAPTURE_ATTEMPTS; capture_attempt++)); do
    if curl -fsS --noproxy '*' \
      --connect-timeout "$SERVICE_METRICS_CONNECT_TIMEOUT_SECS" \
      --max-time "$SERVICE_METRICS_MAX_TIME_SECS" \
      "$SERVICE_METRICS_URL" >"$tmp_file"; then
      if [[ -s "$tmp_file" ]]; then
        raw_bytes="$(wc -c <"$tmp_file" | tr -d '[:space:]')"
        filter_service_metrics_snapshot "$tmp_file" "$filtered_file"
        mv "$filtered_file" "$snapshot_file"
        snapshot_bytes="$(wc -c <"$snapshot_file" | tr -d '[:space:]')"
        cat >"$status_file" <<EOF
size=${size}
round=${round}
attempt=${attempt}
phase=${phase}
status=ok
capture_attempt=${capture_attempt}
url=${SERVICE_METRICS_URL}
connect_timeout_secs=${SERVICE_METRICS_CONNECT_TIMEOUT_SECS}
max_time_secs=${SERVICE_METRICS_MAX_TIME_SECS}
filter_regex=${SERVICE_METRICS_FILTER_REGEX}
raw_bytes=${raw_bytes}
snapshot_bytes=${snapshot_bytes}
settle_secs=${SERVICE_METRICS_SETTLE_SECS}
EOF
        echo "$size,$TOOL,$round,$attempt,$phase,prometheus_text,ok,$capture_attempt,$raw_bytes,$snapshot_bytes,$status_file,$snapshot_file,$SERVICE_METRICS_FILTER_REGEX,$SERVICE_PROMETHEUS_QUERY" >> "$SERVICE_METRICS_CSV"
        return 0
      fi
    fi

    rm -f "$tmp_file" "$filtered_file"
    if (( capture_attempt < SERVICE_METRICS_CAPTURE_ATTEMPTS && SERVICE_METRICS_CAPTURE_RETRY_SECS > 0 )); then
      sleep "$SERVICE_METRICS_CAPTURE_RETRY_SECS"
    fi
  done

  : >"$snapshot_file"
  cat >"$status_file" <<EOF
size=${size}
round=${round}
attempt=${attempt}
phase=${phase}
status=capture_failed
capture_attempts=${SERVICE_METRICS_CAPTURE_ATTEMPTS}
url=${SERVICE_METRICS_URL}
connect_timeout_secs=${SERVICE_METRICS_CONNECT_TIMEOUT_SECS}
max_time_secs=${SERVICE_METRICS_MAX_TIME_SECS}
filter_regex=${SERVICE_METRICS_FILTER_REGEX}
settle_secs=${SERVICE_METRICS_SETTLE_SECS}
EOF
  echo "$size,$TOOL,$round,$attempt,$phase,prometheus_text,capture_failed,$SERVICE_METRICS_CAPTURE_ATTEMPTS,N/A,N/A,$status_file,$snapshot_file,$SERVICE_METRICS_FILTER_REGEX,$SERVICE_PROMETHEUS_QUERY" >> "$SERVICE_METRICS_CSV"
  echo "WARN: failed to capture service metrics size=${size} round=${round} attempt=${attempt} phase=${phase}" >&2
}

median_from_numbers() {
  local values="$1"
  local count
  count="$(printf '%s\n' "$values" | awk 'NF{c++} END{print c+0}')"
  if [[ "$count" -eq 0 ]]; then
    echo "N/A"
    return
  fi

  printf '%s\n' "$values" | awk 'NF' | sort -n | awk '
    {a[NR]=$1}
    END{
      n=NR
      if (n==0) { print "N/A"; exit }
      if (n%2==1) {
        printf "%.6f\n", a[(n+1)/2]
      } else {
        printf "%.6f\n", (a[n/2]+a[n/2+1])/2
      }
    }'
}

run_one_attempt() {
  local size="$1"
  local round="$2"
  local attempt="$3"
  local log_file="$OUT_DIR/logs/${TOOL}_${size}_r${round}_a${attempt}.log"
  local status="ok"
  local exit_code=0
  local started_at_utc finished_at_utc
  capture_round_service_metrics "$size" "$round" "$attempt" before
  started_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if [[ "$TOOL" == "warp" ]]; then
    local warp_host bucket
    warp_host="$(normalize_warp_host "$ENDPOINT")"
    bucket="$(bucket_for_size "$size")"
    local cmd=(
      "$WARP_BIN" "$WARP_MODE"
      "--host" "$warp_host"
      "--access-key" "$ACCESS_KEY"
      "--secret-key" "$SECRET_KEY"
      "--bucket" "$bucket"
      "--obj.size" "$size"
      "--concurrent" "$CONCURRENCY"
      "--duration" "$DURATION"
      "--region" "$REGION"
    )
    if [[ "$INSECURE" == "true" ]]; then
      cmd+=("--insecure")
    fi
    if ((${#EXTRA_ARGS[@]} > 0)); then
      cmd+=("${EXTRA_ARGS[@]}")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "dry run" > "$log_file"
    else
      if "${cmd[@]}" 2>&1 | tee "$log_file" >&2; then
        :
      else
        exit_code=$?
        status="failed"
      fi
    fi
  else
    local bucket
    bucket="$(bucket_for_size "$size")"
    local cmd=(
      "$S3BENCH_BIN"
      "-accessKey=$ACCESS_KEY"
      "-secretKey=$SECRET_KEY"
      "-bucket=$bucket"
      "-endpoint=$ENDPOINT"
      "-region=$REGION"
      "-numClients=$CONCURRENCY"
      "-numSamples=$SAMPLES"
      "-objectSize=$size"
    )
    if [[ "$INSECURE" == "true" ]]; then
      cmd+=("-insecure")
    fi
    if ((${#EXTRA_ARGS[@]} > 0)); then
      cmd+=("${EXTRA_ARGS[@]}")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "dry run" > "$log_file"
    else
      if "${cmd[@]}" 2>&1 | tee "$log_file" >&2; then
        :
      else
        exit_code=$?
        status="failed"
      fi
    fi
  fi
  finished_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  settle_before_after_metrics_capture
  capture_round_service_metrics "$size" "$round" "$attempt" after

  local metrics throughput_human reqps latency_human throughput_bps latency_ms req_p90_human req_p90_ms req_p99_human req_p99_ms
  if [[ "$DRY_RUN" == "true" ]]; then
    throughput_human="N/A"
    reqps="N/A"
    latency_human="N/A"
    throughput_bps="N/A"
    latency_ms="N/A"
    req_p90_human="N/A"
    req_p90_ms="N/A"
    req_p99_human="N/A"
    req_p99_ms="N/A"
  else
    metrics="$(extract_metrics "$log_file")"
    throughput_human="$(echo "$metrics" | cut -d',' -f1)"
    reqps="$(echo "$metrics" | cut -d',' -f2)"
    latency_human="$(echo "$metrics" | cut -d',' -f3)"
    req_p90_human="$(echo "$metrics" | cut -d',' -f4)"
    req_p99_human="$(echo "$metrics" | cut -d',' -f5)"
    throughput_bps="$(to_bps "$throughput_human")"
    latency_ms="$(to_ms "$latency_human")"
    req_p90_ms="$(to_ms "$req_p90_human")"
    req_p99_ms="$(to_ms "$req_p99_human")"
  fi

  if [[ "$DRY_RUN" != "true" && "$status" == "ok" ]]; then
    if [[ "$throughput_bps" == "N/A" && "$reqps" == "N/A" ]]; then
      status="failed"
      exit_code=1
    fi
  fi

  echo "$size,$TOOL,$round,$attempt,$CONCURRENCY,$status,$exit_code,$started_at_utc,$finished_at_utc,$throughput_human,$throughput_bps,$reqps,$latency_human,$latency_ms,$log_file,$req_p90_human,$req_p90_ms,$req_p99_human,$req_p99_ms" >> "$ROUND_CSV"
  echo "$status"
}

run_size() {
  local size="$1"
  local round success attempt rc max_attempts
  max_attempts=$((RETRY_PER_ROUND + 1))

  for ((round=1; round<=ROUNDS; round++)); do
    success="no"
    for ((attempt=1; attempt<=max_attempts; attempt++)); do
      echo "==== size=$size round=$round attempt=$attempt/$max_attempts ===="
      rc="$(run_one_attempt "$size" "$round" "$attempt")"
      if [[ "$rc" == "ok" || "$DRY_RUN" == "true" ]]; then
        success="yes"
        break
      fi
      if (( attempt < RETRY_PER_ROUND+1 )); then
        echo "Round failed, retry in ${RETRY_SLEEP_SECS}s..."
        cooldown_sleep "$RETRY_SLEEP_SECS"
      fi
    done

    if [[ "$success" == "no" ]]; then
      echo "WARN: size=$size round=$round failed after retries."
      FAILED_FINAL_ROUNDS=$((FAILED_FINAL_ROUNDS + 1))
    fi

    if (( COOLDOWN_SECS > 0 && round < ROUNDS )); then
      echo "Cooldown after size=$size round=$round: ${COOLDOWN_SECS}s"
      cooldown_sleep "$COOLDOWN_SECS"
    fi
  done
}

build_median_summary() {
  local sizes_arr size
  IFS=',' read -r -a sizes_arr <<< "$SIZES"

  for raw in "${sizes_arr[@]}"; do
    size="$(trim "$raw")"
    [[ -z "$size" ]] && continue

    local ok_rounds fail_rounds t_vals r_vals l_vals p90_vals p99_vals
    ok_rounds="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" {c++} END{print c+0}' "$ROUND_CSV")"
    fail_rounds="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6!="ok" {c++} END{print c+0}' "$ROUND_CSV")"

    t_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $11!="N/A" {print $11}' "$ROUND_CSV")"
    r_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $12!="N/A" {print $12}' "$ROUND_CSV")"
    l_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $14!="N/A" {print $14}' "$ROUND_CSV")"
    p90_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $17!="N/A" {print $17}' "$ROUND_CSV")"
    p99_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $19!="N/A" {print $19}' "$ROUND_CSV")"

    local m_t m_r m_l m_p90 m_p99
    m_t="$(median_from_numbers "$t_vals")"
    m_r="$(median_from_numbers "$r_vals")"
    m_l="$(median_from_numbers "$l_vals")"
    m_p90="$(median_from_numbers "$p90_vals")"
    m_p99="$(median_from_numbers "$p99_vals")"

    echo "$size,$TOOL,$CONCURRENCY,$ok_rounds,$fail_rounds,$m_t,$m_r,$m_l,$m_p90,$m_p99" >> "$MEDIAN_CSV"
  done
}

compare_baseline() {
  if [[ -z "$BASELINE_CSV" ]]; then
    return
  fi

  echo "size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct,new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct" > "$COMPARE_CSV"

  awk -F',' '
    NR==FNR {
      if (FNR==1) next
      key=$1
      b_req[key]=$7
      b_lat[key]=$8
      b_thr[key]=$6
      next
    }
    FNR==1 {next}
    {
      key=$1
      n_thr=$6; n_req=$7; n_lat=$8
      br=(key in b_req)?b_req[key]:"N/A"
      bl=(key in b_lat)?b_lat[key]:"N/A"
      bt=(key in b_thr)?b_thr[key]:"N/A"

      dr="N/A"; dl="N/A"; dt="N/A"
      if (br!="N/A" && n_req!="N/A" && br+0!=0) dr=sprintf("%.2f", ((n_req-br)/br)*100)
      if (bl!="N/A" && n_lat!="N/A" && bl+0!=0) dl=sprintf("%.2f", ((n_lat-bl)/bl)*100)
      if (bt!="N/A" && n_thr!="N/A" && bt+0!=0) dt=sprintf("%.2f", ((n_thr-bt)/bt)*100)

      print key "," $2 "," $3 "," n_req "," br "," dr "," n_lat "," bl "," dl "," n_thr "," bt "," dt
    }
  ' "$BASELINE_CSV" "$MEDIAN_CSV" >> "$COMPARE_CSV"
}

main() {
  parse_args "$@"
  validate_args
  require_cmd rg
  require_cmd awk
  require_cmd sort
  if [[ "$TOOL" == "warp" ]]; then
    require_cmd "$WARP_BIN"
  else
    require_cmd "$S3BENCH_BIN"
  fi

  setup_output

  echo "Output dir: $OUT_DIR"
  echo "Tool: $TOOL"
  echo "Sizes: $SIZES"
  echo "Concurrency: $CONCURRENCY"
  echo "Rounds: $ROUNDS"
  echo "Retry per round: $RETRY_PER_ROUND"
  echo "Cooldown secs: $COOLDOWN_SECS"
  if [[ -n "$SERVICE_METRICS_URL" || -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    echo "Service metrics filter: $SERVICE_METRICS_FILTER_REGEX"
    if [[ -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
      echo "Service Prometheus query URL: $SERVICE_PROMETHEUS_QUERY_URL"
      echo "Service Prometheus query: $SERVICE_PROMETHEUS_QUERY"
      if [[ -n "$SERVICE_METRICS_SERVICE_NAME" ]]; then
        echo "Service metrics service name: $SERVICE_METRICS_SERVICE_NAME"
      fi
    fi
  fi

  IFS=',' read -r -a size_arr <<< "$SIZES"
  local total_sizes="${#size_arr[@]}"
  local size_index=0
  for raw in "${size_arr[@]}"; do
    size="$(trim "$raw")"
    [[ -z "$size" ]] && continue
    size_index=$(( size_index + 1 ))
    run_size "$size"
    if (( COOLDOWN_SECS > 0 && size_index < total_sizes )); then
      echo "Cooldown after size=$size: ${COOLDOWN_SECS}s"
      cooldown_sleep "$COOLDOWN_SECS"
    fi
  done

  build_median_summary
  compare_baseline

  echo
  echo "=== Median Summary ==="
  cat "$MEDIAN_CSV"

  if [[ -n "$BASELINE_CSV" ]]; then
    echo
    echo "=== Baseline Compare ==="
    cat "$COMPARE_CSV"
  fi

  if [[ "$DRY_RUN" != "true" && "$FAILED_FINAL_ROUNDS" -gt 0 ]]; then
    echo "ERROR: ${FAILED_FINAL_ROUNDS} benchmark rounds failed after retries." >&2
    exit 1
  fi
}

main "$@"
