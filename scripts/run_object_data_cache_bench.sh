#!/usr/bin/env bash
set -euo pipefail

# Dedicated GET benchmark harness for the object data cache rollout gate.
#
# This script intentionally reuses run_get_codec_streaming_smoke.sh for local
# RustFS lifecycle and warp orchestration, then adds object-cache-specific
# mode sequencing and metric acceptance checks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
GET_BENCH="${PROJECT_ROOT}/scripts/run_get_codec_streaming_smoke.sh"

ADDRESS_BASE="127.0.0.1:19130"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
BUCKET="rustfs-object-cache-bench"
REGION="us-east-1"
SIZES="256KiB,1MiB"
CONCURRENCY=16
DURATION="15s"
ROUNDS=2
RETRY_PER_ROUND=1
ROUND_COOLDOWN_SECS=5
WARP_OBJECTS=128
WARP_PREPARE_DURATION="3s"
MODES="disabled,hit_only,fill_buffered_only,fill_materialize_enabled"
BUFFERED_FILL_DIRECT_MEMORY_THRESHOLD=1048576
OUT_DIR=""
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
PYTHON_BIN="python3"
SKIP_BUILD=false
DRY_RUN=false

SERVICE_METRICS_URL=""
SERVICE_PROMETHEUS_QUERY_URL=""
SERVICE_PROMETHEUS_QUERY='{__name__=~"rustfs_object_data_cache_.*"}'
SERVICE_METRICS_FILTER_REGEX="rustfs_object_data_cache_"
SERVICE_METRICS_CAPTURE_ATTEMPTS=5
SERVICE_METRICS_CAPTURE_RETRY_SECS=1
SERVICE_METRICS_CONNECT_TIMEOUT_SECS=2
SERVICE_METRICS_MAX_TIME_SECS=15
SERVICE_METRICS_SETTLE_SECS=2
OBS_ENDPOINT="${RUSTFS_OBS_ENDPOINT:-}"
OBS_METRIC_ENDPOINT="${RUSTFS_OBS_METRIC_ENDPOINT:-}"
OBS_METER_INTERVAL="${RUSTFS_OBS_METER_INTERVAL:-1}"
OBS_SERVICE_NAME_PREFIX="${RUSTFS_OBS_SERVICE_NAME:-RustFS-object-cache}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ALLOW_MISSING_CACHE_METRICS=false

OBJECT_CACHE_MAX_BYTES=""
OBJECT_CACHE_MAX_MEMORY_PERCENT=""
OBJECT_CACHE_MAX_ENTRY_BYTES=""
OBJECT_CACHE_TTL_SECS=""
OBJECT_CACHE_TIME_TO_IDLE_SECS=""
OBJECT_CACHE_MIN_FREE_MEMORY_PERCENT=""
OBJECT_CACHE_FILL_CONCURRENCY_PER_CPU=""
OBJECT_CACHE_FILL_CONCURRENCY_MAX=""
OBJECT_CACHE_IDENTITY_KEYS_MAX=""

RUN_FAILURES=0

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_object_data_cache_bench.sh [options]

Purpose:
  Run the object data cache rollout benchmark matrix and verify that the
  cache-specific metrics prove the expected behavior before default enablement.

Default mode matrix:
  disabled
  hit_only
  fill_buffered_only
  fill_materialize_enabled

Core options:
  --modes <csv>                  Cache modes to run (default: all four modes)
  --address-base <host:port>     First local RustFS address. The port is
                                 incremented per mode (default: 127.0.0.1:19130)
  --bucket <name>                Benchmark bucket prefix
  --sizes <csv>                  Object sizes (default: 256KiB,1MiB)
  --concurrency <n>              warp concurrency (default: 16)
  --duration <duration>          warp duration per round (default: 15s)
  --rounds <n>                   benchmark rounds per size (default: 2)
  --retry-per-round <n>          failed-attempt retries per round (default: 1)
  --round-cooldown-secs <n>      cooldown after each round (default: 5)
  --warp-objects <n>             objects prepared and reused by warp (default: 128)
  --warp-prepare-duration <dur>  prepare-once warmup duration (default: 3s)
  --buffered-fill-direct-memory-threshold <n>
                                 Direct-memory threshold used only for
                                 fill_buffered_only to force a buffered_body
                                 producer (default: 1048576)
  --out-dir <path>               output directory

Metrics gate options:
  --service-metrics-url <url>    Prometheus text scrape URL
  --service-prometheus-query-url <url>
                                 Prometheus HTTP API /api/v1/query URL
  --service-prometheus-query <q> PromQL for object-cache metrics
  --service-metrics-filter-regex <regex>
  --service-metrics-settle-secs <n>
  --diagnostic-obs-endpoint <url>
  --diagnostic-obs-metric-endpoint <url>
                                 RUSTFS_OBS_METRIC_ENDPOINT for OTLP export
  --diagnostic-obs-meter-interval <n>
  --diagnostic-obs-service-name-prefix <name>
  --allow-missing-cache-metrics  Run perf-only if no service metrics endpoint is
                                 provided. Strict rollout acceptance is skipped.

Object cache env overrides:
  --object-cache-max-bytes <n>
  --object-cache-max-memory-percent <n>
  --object-cache-max-entry-bytes <n>
  --object-cache-ttl-secs <n>
  --object-cache-time-to-idle-secs <n>
  --object-cache-min-free-memory-percent <n>
  --object-cache-fill-concurrency-per-cpu <n>
  --object-cache-fill-concurrency-max <n>
  --object-cache-identity-keys-max <n>

Binary/options:
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --python-bin <path>            Python binary for summary parsing
  --skip-build                   do not build RustFS release binary
  --dry-run                      print child commands without starting RustFS

Output:
  <out-dir>/environment.txt
  <out-dir>/mode_summary.csv
  <out-dir>/cache_metrics_summary.csv
  <out-dir>/cache_metrics_acceptance.csv
  <out-dir>/default_enablement_readiness.md
  <out-dir>/<mode>/legacy/warp/median_summary.csv
  <out-dir>/<mode>/legacy/service-metrics/{before,after}.prom

Strict acceptance:
  - non-disabled modes must expose rustfs_object_data_cache_requests_total
  - hit_only must not insert fills
  - fill_buffered_only must prove inserted fill, hit bytes, and weighted bytes
  - fill_materialize_enabled must prove inserted fill, hit bytes, and weighted bytes
USAGE
}

log() {
  printf '%s\n' "$*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "command not found: $1"
  fi
}

validate_positive_int() {
  local value="$1"
  local name="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -le 0 ]]; then
    die "$name must be a positive integer, got: $value"
  fi
}

validate_non_negative_int() {
  local value="$1"
  local name="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    die "$name must be a non-negative integer, got: $value"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --modes) MODES="$2"; shift 2 ;;
      --address-base) ADDRESS_BASE="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --round-cooldown-secs) ROUND_COOLDOWN_SECS="$2"; shift 2 ;;
      --warp-objects) WARP_OBJECTS="$2"; shift 2 ;;
      --warp-prepare-duration) WARP_PREPARE_DURATION="$2"; shift 2 ;;
      --buffered-fill-direct-memory-threshold) BUFFERED_FILL_DIRECT_MEMORY_THRESHOLD="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --service-metrics-url) SERVICE_METRICS_URL="$2"; shift 2 ;;
      --service-prometheus-query-url) SERVICE_PROMETHEUS_QUERY_URL="$2"; shift 2 ;;
      --service-prometheus-query) SERVICE_PROMETHEUS_QUERY="$2"; shift 2 ;;
      --service-metrics-filter-regex) SERVICE_METRICS_FILTER_REGEX="$2"; shift 2 ;;
      --service-metrics-attempts) SERVICE_METRICS_CAPTURE_ATTEMPTS="$2"; shift 2 ;;
      --service-metrics-retry-secs) SERVICE_METRICS_CAPTURE_RETRY_SECS="$2"; shift 2 ;;
      --service-metrics-connect-timeout-secs) SERVICE_METRICS_CONNECT_TIMEOUT_SECS="$2"; shift 2 ;;
      --service-metrics-max-time-secs) SERVICE_METRICS_MAX_TIME_SECS="$2"; shift 2 ;;
      --service-metrics-settle-secs) SERVICE_METRICS_SETTLE_SECS="$2"; shift 2 ;;
      --diagnostic-obs-endpoint) OBS_ENDPOINT="$2"; shift 2 ;;
      --diagnostic-obs-metric-endpoint) OBS_METRIC_ENDPOINT="$2"; shift 2 ;;
      --diagnostic-obs-meter-interval) OBS_METER_INTERVAL="$2"; shift 2 ;;
      --diagnostic-obs-service-name-prefix) OBS_SERVICE_NAME_PREFIX="$2"; shift 2 ;;
      --allow-missing-cache-metrics) ALLOW_MISSING_CACHE_METRICS=true; shift ;;
      --object-cache-max-bytes) OBJECT_CACHE_MAX_BYTES="$2"; shift 2 ;;
      --object-cache-max-memory-percent) OBJECT_CACHE_MAX_MEMORY_PERCENT="$2"; shift 2 ;;
      --object-cache-max-entry-bytes) OBJECT_CACHE_MAX_ENTRY_BYTES="$2"; shift 2 ;;
      --object-cache-ttl-secs) OBJECT_CACHE_TTL_SECS="$2"; shift 2 ;;
      --object-cache-time-to-idle-secs) OBJECT_CACHE_TIME_TO_IDLE_SECS="$2"; shift 2 ;;
      --object-cache-min-free-memory-percent) OBJECT_CACHE_MIN_FREE_MEMORY_PERCENT="$2"; shift 2 ;;
      --object-cache-fill-concurrency-per-cpu) OBJECT_CACHE_FILL_CONCURRENCY_PER_CPU="$2"; shift 2 ;;
      --object-cache-fill-concurrency-max) OBJECT_CACHE_FILL_CONCURRENCY_MAX="$2"; shift 2 ;;
      --object-cache-identity-keys-max) OBJECT_CACHE_IDENTITY_KEYS_MAX="$2"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --python-bin) PYTHON_BIN="$2"; shift 2 ;;
      --skip-build) SKIP_BUILD=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        usage >&2
        die "unknown arg: $1"
        ;;
    esac
  done
}

validate_mode() {
  case "$1" in
    disabled|hit_only|fill_buffered_only|fill_materialize_enabled) ;;
    *) die "unsupported object cache mode: $1" ;;
  esac
}

validate_args() {
  [[ -n "$MODES" ]] || die "--modes must not be empty"
  [[ -n "$ADDRESS_BASE" ]] || die "--address-base must not be empty"
  [[ -n "$ACCESS_KEY" ]] || die "--access-key must not be empty"
  [[ -n "$SECRET_KEY" ]] || die "--secret-key must not be empty"
  [[ -n "$BUCKET" ]] || die "--bucket must not be empty"
  [[ -n "$SIZES" ]] || die "--sizes must not be empty"
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_non_negative_int "$ROUND_COOLDOWN_SECS" "--round-cooldown-secs"
  validate_positive_int "$WARP_OBJECTS" "--warp-objects"
  validate_positive_int "$BUFFERED_FILL_DIRECT_MEMORY_THRESHOLD" "--buffered-fill-direct-memory-threshold"
  validate_positive_int "$SERVICE_METRICS_CAPTURE_ATTEMPTS" "--service-metrics-attempts"
  validate_non_negative_int "$SERVICE_METRICS_CAPTURE_RETRY_SECS" "--service-metrics-retry-secs"
  validate_positive_int "$SERVICE_METRICS_CONNECT_TIMEOUT_SECS" "--service-metrics-connect-timeout-secs"
  validate_positive_int "$SERVICE_METRICS_MAX_TIME_SECS" "--service-metrics-max-time-secs"
  validate_non_negative_int "$SERVICE_METRICS_SETTLE_SECS" "--service-metrics-settle-secs"
  validate_positive_int "$OBS_METER_INTERVAL" "--diagnostic-obs-meter-interval"

  local raw mode
  IFS=',' read -r -a mode_list <<< "$MODES"
  [[ "${#mode_list[@]}" -gt 0 ]] || die "--modes must contain at least one mode"
  for raw in "${mode_list[@]}"; do
    mode="${raw//[[:space:]]/}"
    [[ -n "$mode" ]] || continue
    validate_mode "$mode"
  done

  if [[ -n "$SERVICE_METRICS_URL" && -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    die "--service-metrics-url and --service-prometheus-query-url are mutually exclusive"
  fi
  if [[ "$ALLOW_MISSING_CACHE_METRICS" != "true" && -z "$SERVICE_METRICS_URL" && -z "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    die "strict cache metrics acceptance requires --service-metrics-url or --service-prometheus-query-url; use --allow-missing-cache-metrics for perf-only runs"
  fi

  [[ -x "$GET_BENCH" ]] || die "GET benchmark script is not executable: $GET_BENCH"
  require_cmd git
  require_cmd "$PYTHON_BIN"
  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd "$WARP_BIN"
    if [[ "$SKIP_BUILD" != "true" ]]; then
      require_cmd cargo
    fi
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/object-data-cache-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
}

host_part() {
  printf '%s' "$ADDRESS_BASE" | awk -F: '{ print $1 }'
}

port_part() {
  printf '%s' "$ADDRESS_BASE" | awk -F: '{ print $NF }'
}

address_for_index() {
  local index="$1"
  local host port
  host="$(host_part)"
  port="$(port_part)"
  if ! [[ "$port" =~ ^[0-9]+$ ]]; then
    die "address-base must end with a numeric port: $ADDRESS_BASE"
  fi
  printf '%s:%s\n' "$host" "$((port + index))"
}

sanitize_name() {
  printf '%s' "$1" | tr '[:upper:]_' '[:lower:]-' | tr -c 'a-z0-9-' '-'
}

mode_values() {
  local raw mode
  IFS=',' read -r -a mode_list <<< "$MODES"
  for raw in "${mode_list[@]}"; do
    mode="${raw//[[:space:]]/}"
    [[ -n "$mode" ]] || continue
    printf '%s\n' "$mode"
  done
}

write_environment() {
  local branch git_head dirty_count rustc_version cargo_version command_line
  branch="$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD)"
  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
  dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"
  rustc_version="$(rustc --version 2>/dev/null || echo unavailable)"
  cargo_version="$(cargo --version 2>/dev/null || echo unavailable)"
  command_line="$(printf '%q ' "${BASH_SOURCE[0]}" "$@")"

  cat >"${OUT_DIR}/environment.txt" <<EOF
generated_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
branch=${branch}
git_head=${git_head}
dirty_count=${dirty_count}
rustc_version=${rustc_version}
cargo_version=${cargo_version}
os=$(uname -a)
address_base=${ADDRESS_BASE}
modes=${MODES}
bucket=${BUCKET}
region=${REGION}
sizes=${SIZES}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
warp_objects=${WARP_OBJECTS}
warp_prepare_duration=${WARP_PREPARE_DURATION}
buffered_fill_direct_memory_threshold=${BUFFERED_FILL_DIRECT_MEMORY_THRESHOLD}
service_metrics_url=${SERVICE_METRICS_URL}
service_prometheus_query_url=${SERVICE_PROMETHEUS_QUERY_URL}
service_prometheus_query=${SERVICE_PROMETHEUS_QUERY}
service_metrics_filter_regex=${SERVICE_METRICS_FILTER_REGEX}
service_metrics_settle_secs=${SERVICE_METRICS_SETTLE_SECS}
diagnostic_obs_endpoint=${OBS_ENDPOINT}
diagnostic_obs_metric_endpoint=${OBS_METRIC_ENDPOINT}
diagnostic_obs_meter_interval=${OBS_METER_INTERVAL}
diagnostic_obs_service_name_prefix=${OBS_SERVICE_NAME_PREFIX}
run_id=${RUN_ID}
allow_missing_cache_metrics=${ALLOW_MISSING_CACHE_METRICS}
RUSTFS_OBJECT_DATA_CACHE_ENABLE=mode-derived
RUSTFS_OBJECT_DATA_CACHE_MAX_BYTES=${OBJECT_CACHE_MAX_BYTES:-server-default}
RUSTFS_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT=${OBJECT_CACHE_MAX_MEMORY_PERCENT:-server-default}
RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES=${OBJECT_CACHE_MAX_ENTRY_BYTES:-server-default}
RUSTFS_OBJECT_DATA_CACHE_TTL_SECS=${OBJECT_CACHE_TTL_SECS:-server-default}
RUSTFS_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS=${OBJECT_CACHE_TIME_TO_IDLE_SECS:-server-default}
RUSTFS_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT=${OBJECT_CACHE_MIN_FREE_MEMORY_PERCENT:-server-default}
RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU=${OBJECT_CACHE_FILL_CONCURRENCY_PER_CPU:-server-default}
RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX=${OBJECT_CACHE_FILL_CONCURRENCY_MAX:-server-default}
RUSTFS_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX=${OBJECT_CACHE_IDENTITY_KEYS_MAX:-server-default}
skip_build=${SKIP_BUILD}
dry_run=${DRY_RUN}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
python_bin=${PYTHON_BIN}
command_line=${command_line% }
EOF
}

append_optional_object_cache_env() {
  if [[ -n "$OBJECT_CACHE_MAX_BYTES" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_MAX_BYTES="$OBJECT_CACHE_MAX_BYTES"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_MAX_BYTES
  fi
  if [[ -n "$OBJECT_CACHE_MAX_MEMORY_PERCENT" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT="$OBJECT_CACHE_MAX_MEMORY_PERCENT"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_MAX_MEMORY_PERCENT
  fi
  if [[ -n "$OBJECT_CACHE_MAX_ENTRY_BYTES" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES="$OBJECT_CACHE_MAX_ENTRY_BYTES"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES
  fi
  if [[ -n "$OBJECT_CACHE_TTL_SECS" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_TTL_SECS="$OBJECT_CACHE_TTL_SECS"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_TTL_SECS
  fi
  if [[ -n "$OBJECT_CACHE_TIME_TO_IDLE_SECS" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS="$OBJECT_CACHE_TIME_TO_IDLE_SECS"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_TIME_TO_IDLE_SECS
  fi
  if [[ -n "$OBJECT_CACHE_MIN_FREE_MEMORY_PERCENT" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT="$OBJECT_CACHE_MIN_FREE_MEMORY_PERCENT"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT
  fi
  if [[ -n "$OBJECT_CACHE_FILL_CONCURRENCY_PER_CPU" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU="$OBJECT_CACHE_FILL_CONCURRENCY_PER_CPU"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_PER_CPU
  fi
  if [[ -n "$OBJECT_CACHE_FILL_CONCURRENCY_MAX" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX="$OBJECT_CACHE_FILL_CONCURRENCY_MAX"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_FILL_CONCURRENCY_MAX
  fi
  if [[ -n "$OBJECT_CACHE_IDENTITY_KEYS_MAX" ]]; then
    export RUSTFS_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX="$OBJECT_CACHE_IDENTITY_KEYS_MAX"
  else
    unset RUSTFS_OBJECT_DATA_CACHE_IDENTITY_KEYS_MAX
  fi
}

run_mode() {
  local mode="$1"
  local index="$2"
  local mode_dir="${OUT_DIR}/${mode}"
  local address bucket obs_service_prefix
  local -a cmd=()

  address="$(address_for_index "$index")"
  bucket="${BUCKET}-$(sanitize_name "$mode")"
  obs_service_prefix="${OBS_SERVICE_NAME_PREFIX}-${RUN_ID}-$(sanitize_name "$mode")"
  mkdir -p "$mode_dir"

  cmd=(
    "$GET_BENCH"
    --mode legacy
    --address "$address"
    --bucket "$bucket"
    --sizes "$SIZES"
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --round-cooldown-secs "$ROUND_COOLDOWN_SECS"
    --warp-objects "$WARP_OBJECTS"
    --warp-object-lifecycle prepare-once
    --warp-prepare-duration "$WARP_PREPARE_DURATION"
    --out-dir "$mode_dir"
    --rustfs-bin "$RUSTFS_BIN"
    --warp-bin "$WARP_BIN"
    --python-bin "$PYTHON_BIN"
    --skip-compat-probe
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --region "$REGION"
  )

  if [[ "$mode" == "fill_buffered_only" ]]; then
    cmd+=(
      --direct-memory on
      --direct-memory-threshold "$BUFFERED_FILL_DIRECT_MEMORY_THRESHOLD"
    )
  fi

  if [[ "$SKIP_BUILD" == "true" || "$index" -gt 0 ]]; then
    cmd+=(--skip-build)
  fi

  if [[ -n "$SERVICE_METRICS_URL" || -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
    cmd+=(
      --diagnostic-metrics
      --diagnostic-prometheus-query "$SERVICE_PROMETHEUS_QUERY"
      --diagnostic-metrics-settle-secs "$SERVICE_METRICS_SETTLE_SECS"
      --diagnostic-metrics-capture-attempts "$SERVICE_METRICS_CAPTURE_ATTEMPTS"
      --diagnostic-metrics-capture-retry-secs "$SERVICE_METRICS_CAPTURE_RETRY_SECS"
      --diagnostic-metrics-connect-timeout-secs "$SERVICE_METRICS_CONNECT_TIMEOUT_SECS"
      --diagnostic-metrics-max-time-secs "$SERVICE_METRICS_MAX_TIME_SECS"
      --diagnostic-metrics-filter-regex "$SERVICE_METRICS_FILTER_REGEX"
      --diagnostic-obs-meter-interval "$OBS_METER_INTERVAL"
      --diagnostic-obs-service-name-prefix "$obs_service_prefix"
    )
    if [[ -n "$SERVICE_METRICS_URL" ]]; then
      cmd+=(--diagnostic-metrics-url "$SERVICE_METRICS_URL")
    fi
    if [[ -n "$SERVICE_PROMETHEUS_QUERY_URL" ]]; then
      cmd+=(--diagnostic-prometheus-query-url "$SERVICE_PROMETHEUS_QUERY_URL")
    fi
    if [[ -n "$OBS_ENDPOINT" ]]; then
      cmd+=(--diagnostic-obs-endpoint "$OBS_ENDPOINT")
    fi
    if [[ -n "$OBS_METRIC_ENDPOINT" ]]; then
      cmd+=(--diagnostic-obs-metric-endpoint "$OBS_METRIC_ENDPOINT")
    fi
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  log "==== object-cache mode=${mode} address=${address} ===="
  (
    if [[ "$mode" == "disabled" ]]; then
      export RUSTFS_OBJECT_DATA_CACHE_ENABLE=false
    else
      export RUSTFS_OBJECT_DATA_CACHE_ENABLE=true
    fi
    export RUSTFS_OBJECT_DATA_CACHE_MODE="$mode"
    append_optional_object_cache_env
    "${cmd[@]}"
  )
}

write_summaries() {
  local strict="true"
  if [[ "$ALLOW_MISSING_CACHE_METRICS" == "true" ]]; then
    strict="false"
  fi

  "$PYTHON_BIN" - "$OUT_DIR" "$MODES" "$strict" "$RUN_ID" <<'PY'
import csv
import pathlib
import re
import sys

out_dir = pathlib.Path(sys.argv[1])
modes = [mode.strip() for mode in sys.argv[2].split(",") if mode.strip()]
strict = sys.argv[3] == "true"
run_id = sys.argv[4] if len(sys.argv) > 4 else ""

LINE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+'
    r'([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)'
    r'(?:\s+[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)?\s*$'
)


def read_prom(path):
    rows = {}
    if not path.exists() or path.stat().st_size == 0:
        return rows
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw or raw.startswith("#"):
            continue
        match = LINE.match(raw)
        if match is None:
            continue
        metric, labels_raw, value_raw = match.groups()
        labels = labels_raw or ""
        rows[(metric, labels)] = float(value_raw)
    return rows


def labels_to_dict(labels):
    result = {}
    if not labels:
        return result
    parts = re.findall(r'([A-Za-z0-9_.-]+)="((?:\\.|[^"\\])*)"', labels)
    for key, value in parts:
        result[key] = value.replace(r'\"', '"').replace(r'\\', '\\')
    return result


def sanitize_mode(mode):
    return "".join(ch if ("a" <= ch <= "z" or "0" <= ch <= "9") else "-" for ch in mode.lower().replace("_", "-"))


def labels_belong_to_mode(labels, mode):
    label_map = labels_to_dict(labels)
    mode_slug = sanitize_mode(mode)
    attribution_values = [label_map.get(key, "") for key in ("job", "service_name", "otel_scope_name")]
    has_attribution = any(attribution_values)
    mode_label_matches = label_map.get("mode") == mode
    attribution_matches_mode = any(mode in value or mode_slug in value for value in attribution_values)
    attribution_matches_run = not run_id or any(run_id in value for value in attribution_values)
    if has_attribution:
        return attribution_matches_run and (mode_label_matches or attribution_matches_mode)

    if mode_label_matches:
        return True

    return False


def labels_have_mode_attribution(labels):
    label_map = labels_to_dict(labels)
    return any(label_map.get(key) for key in ("mode", "job", "service_name", "otel_scope_name"))


def labels_match_mode_or_unattributed(labels, mode):
    if labels_belong_to_mode(labels, mode):
        return True
    return not labels_have_mode_attribution(labels)


def metric_matches(metric, expected):
    return metric == expected or metric == f"{expected}_total"


def counter_delta(before, after, expected, expected_mode, **wanted_labels):
    total = 0.0
    for (metric, labels), after_value in after.items():
        if not metric_matches(metric, expected):
            continue
        if not labels_match_mode_or_unattributed(labels, expected_mode):
            continue
        label_map = labels_to_dict(labels)
        if all(label_map.get(key) == expected_value for key, expected_value in wanted_labels.items()):
            before_value = before.get((metric, labels), 0.0)
            delta = after_value - before_value
            if delta > 0:
                total += delta
    return total


def counter_observed(after, expected, expected_mode, **wanted_labels):
    total = 0.0
    for (metric, labels), value in after.items():
        if not metric_matches(metric, expected):
            continue
        if not labels_match_mode_or_unattributed(labels, expected_mode):
            continue
        label_map = labels_to_dict(labels)
        if all(label_map.get(key) == expected_value for key, expected_value in wanted_labels.items()):
            total += value
    return total


def gauge_max(before, after, expected, mode):
    values = []
    for rows in (before, after):
        for (metric, labels), value in rows.items():
            if metric_matches(metric, expected):
                if not labels_match_mode_or_unattributed(labels, mode):
                    continue
                values.append(value)
    return max(values) if values else 0.0


def metric_delta_rows(mode, before, after):
    keys = sorted(set(before) | set(after))
    for metric, labels in keys:
        if not labels_match_mode_or_unattributed(labels, mode):
            continue
        before_value = before.get((metric, labels), 0.0)
        after_value = after.get((metric, labels), 0.0)
        yield {
            "mode": mode,
            "metric": metric,
            "labels": labels,
            "before": f"{before_value:.12g}",
            "after": f"{after_value:.12g}",
            "delta": f"{after_value - before_value:.12g}",
        }


def mode_metrics(mode):
    metrics_dir = out_dir / mode / "legacy" / "service-metrics"
    before = read_prom(metrics_dir / "before.prom")
    after = read_prom(metrics_dir / "after.prom")
    requests = counter_delta(before, after, "rustfs_object_data_cache_requests_total", mode, mode=mode)
    misses = counter_delta(before, after, "rustfs_object_data_cache_requests_total", mode, mode=mode, decision="miss")
    cacheable = counter_delta(before, after, "rustfs_object_data_cache_requests_total", mode, mode=mode, decision="cacheable")
    hits = counter_delta(before, after, "rustfs_object_data_cache_requests_total", mode, mode=mode, decision="hit")
    hit_bytes = counter_delta(before, after, "rustfs_object_data_cache_hit_bytes_total", mode, mode=mode)
    fill_inserted = counter_observed(after, "rustfs_object_data_cache_fill_total", mode, mode=mode, result="inserted")
    fill_skipped_by_mode = counter_observed(
        after, "rustfs_object_data_cache_fill_total", mode, mode=mode, result="skipped_by_mode"
    )
    weighted_bytes = gauge_max(before, after, "rustfs_object_data_cache_weighted_bytes", mode)
    entries = gauge_max(before, after, "rustfs_object_data_cache_entries", mode)
    return before, after, {
        "requests_total": requests,
        "misses": misses,
        "cacheable": cacheable,
        "hits": hits,
        "hit_bytes_total": hit_bytes,
        "fill_inserted": fill_inserted,
        "fill_skipped_by_mode": fill_skipped_by_mode,
        "weighted_bytes": weighted_bytes,
        "entries": entries,
    }


def acceptance(mode, values):
    if not strict:
        return "skipped", "perf_only_run_allow_missing_cache_metrics"
    if mode == "disabled":
        return "pass", "disabled baseline does not require object-cache metrics"
    if values["requests_total"] <= 0:
        return "fail", "missing object-cache request metrics"
    if mode == "hit_only":
        if values["fill_inserted"] > 0:
            return "fail", "hit_only inserted cache fills"
        return "pass", "hit_only produced cache decisions without fills"
    if mode == "fill_buffered_only":
        missing = []
        if values["fill_inserted"] <= 0:
            missing.append("fill_inserted")
        if values["hit_bytes_total"] <= 0:
            missing.append("hit_bytes_total")
        if values["weighted_bytes"] <= 0:
            missing.append("weighted_bytes")
        if missing:
            return "fail", "missing required metrics: " + "|".join(missing)
        return "pass", "buffered fill mode proved fill, hit, and resident bytes"
    if mode == "fill_materialize_enabled":
        missing = []
        if values["fill_inserted"] <= 0:
            missing.append("fill_inserted")
        if values["hit_bytes_total"] <= 0:
            missing.append("hit_bytes_total")
        if values["weighted_bytes"] <= 0:
            missing.append("weighted_bytes")
        if missing:
            return "fail", "missing required metrics: " + "|".join(missing)
        return "pass", "materialize mode proved fill, hit, and resident bytes"
    return "fail", "unknown mode"


with (out_dir / "mode_summary.csv").open("w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow([
        "mode",
        "size",
        "tool",
        "concurrency",
        "successful_rounds",
        "failed_rounds",
        "median_throughput_bps",
        "median_reqps",
        "median_latency_ms",
        "median_req_p90_ms",
        "median_req_p99_ms",
        "source_csv",
    ])
    for mode in modes:
        median_path = out_dir / mode / "legacy" / "warp" / "median_summary.csv"
        if not median_path.exists():
            writer.writerow([mode, "N/A", "N/A", "N/A", 0, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", str(median_path)])
            continue
        with median_path.open("r", encoding="utf-8", newline="") as source:
            for row in csv.DictReader(source):
                writer.writerow([
                    mode,
                    row.get("size", "N/A"),
                    row.get("tool", "N/A"),
                    row.get("concurrency", "N/A"),
                    row.get("successful_rounds", "N/A"),
                    row.get("failed_rounds", "N/A"),
                    row.get("median_throughput_bps", "N/A"),
                    row.get("median_reqps", "N/A"),
                    row.get("median_latency_ms", "N/A"),
                    row.get("median_req_p90_ms", "N/A"),
                    row.get("median_req_p99_ms", "N/A"),
                    str(median_path),
                ])

all_acceptance = []
with (out_dir / "cache_metrics_summary.csv").open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=["mode", "metric", "labels", "before", "after", "delta"])
    writer.writeheader()
    for mode in modes:
        before, after, values = mode_metrics(mode)
        for row in metric_delta_rows(mode, before, after):
            writer.writerow(row)
        status, notes = acceptance(mode, values)
        all_acceptance.append((mode, status, notes, values))

with (out_dir / "cache_metrics_acceptance.csv").open("w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow([
        "mode",
        "status",
        "requests_total",
        "misses",
        "cacheable",
        "hits",
        "hit_bytes_total",
        "fill_inserted",
        "fill_skipped_by_mode",
        "weighted_bytes",
        "entries",
        "notes",
    ])
    for mode, status, notes, values in all_acceptance:
        writer.writerow([
            mode,
            status,
            f'{values["requests_total"]:.12g}',
            f'{values["misses"]:.12g}',
            f'{values["cacheable"]:.12g}',
            f'{values["hits"]:.12g}',
            f'{values["hit_bytes_total"]:.12g}',
            f'{values["fill_inserted"]:.12g}',
            f'{values["fill_skipped_by_mode"]:.12g}',
            f'{values["weighted_bytes"]:.12g}',
            f'{values["entries"]:.12g}',
            notes,
        ])

strict_failed = any(status == "fail" for _mode, status, _notes, _values in all_acceptance)
overall_status = "skipped" if not strict else ("fail" if strict_failed else "pass")
with (out_dir / "default_enablement_readiness.md").open("w", encoding="utf-8") as handle:
    handle.write("# Object Data Cache Default Enablement Readiness\n\n")
    handle.write(f"- strict_metrics_acceptance: {str(strict).lower()}\n")
    handle.write(f"- overall_status: {overall_status}\n")
    handle.write(f"- modes_under_test: {', '.join(modes)}\n")
    handle.write("- default_required_modes: disabled, hit_only, fill_buffered_only, fill_materialize_enabled\n")
    handle.write("- required_metrics: requests_total, fill_total, hit_bytes_total, weighted_bytes\n\n")
    handle.write("## Mode Acceptance\n\n")
    handle.write("| mode | status | requests | fill_inserted | hit_bytes | weighted_bytes | notes |\n")
    handle.write("| --- | --- | ---: | ---: | ---: | ---: | --- |\n")
    for mode, status, notes, values in all_acceptance:
        handle.write(
            f"| {mode} | {status} | {values['requests_total']:.12g} | "
            f"{values['fill_inserted']:.12g} | {values['hit_bytes_total']:.12g} | "
            f"{values['weighted_bytes']:.12g} | {notes} |\n"
        )
    handle.write("\n")
    if strict_failed:
        handle.write("Conclusion: do not enable object data cache by default.\n")
    elif strict:
        handle.write("Conclusion: metrics gate passed; default enablement can move to the next rollout review.\n")
    else:
        handle.write("Conclusion: perf-only run; default enablement remains blocked until strict metrics gate passes.\n")

if strict_failed:
    raise SystemExit(2)
PY
}

main() {
  parse_args "$@"
  validate_args
  setup_output
  write_environment "$@"

  local index=0
  local mode
  while IFS= read -r mode; do
    if run_mode "$mode" "$index"; then
      :
    else
      RUN_FAILURES=$((RUN_FAILURES + 1))
    fi
    index=$((index + 1))
  done < <(mode_values)

  if write_summaries; then
    :
  else
    RUN_FAILURES=$((RUN_FAILURES + 1))
  fi

  log "Output dir: ${OUT_DIR}"
  log "Mode summary: ${OUT_DIR}/mode_summary.csv"
  log "Cache metrics summary: ${OUT_DIR}/cache_metrics_summary.csv"
  log "Cache metrics acceptance: ${OUT_DIR}/cache_metrics_acceptance.csv"
  log "Default enablement readiness: ${OUT_DIR}/default_enablement_readiness.md"

  if [[ "$RUN_FAILURES" -gt 0 ]]; then
    die "object data cache benchmark completed with ${RUN_FAILURES} failure(s)"
  fi
}

main "$@"
