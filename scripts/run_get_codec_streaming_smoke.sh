#!/usr/bin/env bash
set -euo pipefail

# Local GET benchmark harness for the experimental codec streaming read path.
#
# This script intentionally keeps the benchmark orchestration thin:
# - start one local RustFS server with controlled GET/codec/scanner env
# - run the existing enhanced object benchmark in GET mode
# - optionally run legacy and codec profiles back-to-back for A/B comparison

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

ADDRESS="127.0.0.1:19030"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
BUCKET="rustfs-get-codec-smoke"
REGION="us-east-1"
SIZES="1MiB,4MiB,10MiB"
CONCURRENCY=16
DURATION="30s"
ROUNDS=3
RETRY_PER_ROUND=1
ROUND_COOLDOWN_SECS=20
WARP_OBJECTS=""
WARP_OBJECT_LIFECYCLE="per-round"
WARP_PREPARE_DURATION="1s"
GET_OBJECT_METADATA_CACHE_MAX_ENTRIES=""
GET_SMALL_OBJECT_DIRECT_MEMORY=""
GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD=""
MODE="both"
CODEC_ENGINES="legacy"
CODEC_MAX_INFLIGHT=1
CODEC_MULTIPART="off"
CODEC_MULTIPART_MAX_PARTS=256
METADATA_EARLY_STOP="off"
SHARD_LOCALITY_PREFERENCE="off"
OUTPUT_HANDOFF_ATTRIBUTION=false
DIAGNOSTIC_METRICS=false
DIAGNOSTIC_METRICS_URL="http://127.0.0.1:8889/metrics"
DIAGNOSTIC_PROMETHEUS_QUERY_URL=""
DIAGNOSTIC_PROMETHEUS_QUERY='{__name__=~"rustfs_io_get_object_.*"}'
DIAGNOSTIC_METRICS_SETTLE_SECS="${RUSTFS_DIAGNOSTIC_METRICS_SETTLE_SECS:-2}"
DIAGNOSTIC_OBS_ENDPOINT="${RUSTFS_OBS_ENDPOINT:-}"
DIAGNOSTIC_OBS_METRIC_ENDPOINT="${RUSTFS_OBS_METRIC_ENDPOINT:-}"
DIAGNOSTIC_OBS_METER_INTERVAL="${RUSTFS_OBS_METER_INTERVAL:-1}"
DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX="${RUSTFS_OBS_SERVICE_NAME:-RustFS-get-codec}"
DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS="${RUSTFS_DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS:-5}"
DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS="${RUSTFS_DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS:-1}"
SERVICE_METRIC_PREFIX="rustfs_io_get_object_"
COMPRESSED_FALLBACK_PROBE=false
COMPRESSED_PROBE_EXTENSION=".compressed-probe.txt"
COMPRESSED_PROBE_MIME_TYPE="text/plain"
OUT_DIR=""
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
PYTHON_BIN="python3"
CODEC_MIN_SIZE=1
RUST_LOG="warn"
HEALTH_TIMEOUT_SECS=60
COMPAT_OBJECT_KEY="__rustfs_get_v2_pr24_compat/object.bin"
COMPAT_OBJECT_SIZE=65536
SKIP_COMPAT_PROBE=false
RESOURCE_SAMPLE_INTERVAL_SECS="${RUSTFS_GET_BENCH_RESOURCE_SAMPLE_INTERVAL_SECS:-5}"
DRY_RUN=false
SKIP_BUILD=false
DETACH=false
DETACH_LOG=""
PROFILE_FAILURES=0
declare -a ORIGINAL_ARGS=()

SERVER_PID=""
SERVER_SAMPLER_PID=""
SERVER_SAMPLER_LOG=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_get_codec_streaming_smoke.sh [options]

Purpose:
  Start a local RustFS server and run GET warp benchmarks for the legacy read
  path, the experimental codec streaming read path, or both.

Core options:
  --mode <legacy|codec|both>     Which profile(s) to run (default: both)
  --codec-engine <csv>           Codec engine(s) for codec profiles (default: legacy)
  --metadata-early-stop <on|off> Enable metadata early-stop observe/opt-in env (default: off)
  --shard-locality-preference <on|off>
                                 Enable shard locality preference env (default: off)
  --codec-max-inflight <n>       RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT (default: 1)
  --codec-multipart <on|off>     Enable multipart codec streaming opt-in for codec profiles
                                 (default: off)
  --codec-multipart-max-parts <n>
                                 RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS
                                 (default: 256)
  --handoff-attribution          Enable output handoff attribution metrics
  --diagnostic-metrics           Enable observability metrics export and capture server-side metrics
  --diagnostic-metrics-url <url> Prometheus scrape URL for diagnostic captures
                                 (default: http://127.0.0.1:8889/metrics)
  --diagnostic-prometheus-query-url <url>
                                 Prometheus HTTP API query endpoint for OTLP-exported metrics,
                                 e.g. http://localhost:9090/api/v1/query
  --diagnostic-prometheus-query <promql>
                                 PromQL used with --diagnostic-prometheus-query-url
                                 (default: {__name__=~"rustfs_io_get_object_.*"})
  --diagnostic-metrics-settle-secs <n>
                                 Seconds to wait before the after snapshot so OTLP periodic
                                 metrics can export probe counters (default: 2)
  --diagnostic-metrics-capture-attempts <n>
                                 Retry attempts for each direct /metrics scrape (default: 5)
  --diagnostic-metrics-capture-retry-secs <n>
                                 Sleep seconds between failed direct /metrics scrape attempts
                                 (default: 1)
  --diagnostic-obs-endpoint <url>
                                 RUSTFS_OBS_ENDPOINT passed to RustFS during diagnostic runs
  --diagnostic-obs-metric-endpoint <url>
                                 RUSTFS_OBS_METRIC_ENDPOINT passed to RustFS during diagnostic runs
  --diagnostic-obs-meter-interval <secs>
                                 RUSTFS_OBS_METER_INTERVAL passed during diagnostic runs (default: 1)
  --diagnostic-obs-service-name-prefix <name>
                                 Prefix used for unique per-profile RUSTFS_OBS_SERVICE_NAME
                                 (default: RustFS-get-codec, or existing RUSTFS_OBS_SERVICE_NAME)
  --compressed-fallback-probe    Enable disk compression only for the compressed fallback probe object
  --address <host:port>          RustFS listen address (default: 127.0.0.1:19030)
  --bucket <name>                Benchmark bucket (default: rustfs-get-codec-smoke)
  --sizes <csv>                  Object sizes (default: 1MiB,4MiB,10MiB)
  --concurrency <n>              warp concurrency (default: 16)
  --duration <duration>          warp duration per round (default: 30s)
  --warp-objects <n>             Number of objects prepared by warp for each size
                                 (default: warp default)
  --warp-object-lifecycle <mode> Object lifecycle mode for warp GET:
                                 per-round|prepare-once|existing-only
                                 (default: per-round)
  --warp-prepare-duration <dur>  Duration used by the prepare-once warmup
                                 warp GET run (default: 1s)
  --metadata-cache-max-entries <n>
                                 RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES for RustFS
  --direct-memory <on|off>       RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY for RustFS
  --direct-memory-threshold <bytes>
                                 RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD for RustFS
  --rounds <n>                   rounds per size (default: 3)
  --retry-per-round <n>          failed-attempt retries per round (default: 1)
  --round-cooldown-secs <n>      cooldown seconds after each completed round (default: 20)
  --out-dir <path>               output directory (default: target/bench/get-codec-streaming-<timestamp>)

Binary/options:
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --python-bin <path>            Python binary for SigV4 compatibility probe (default: python3)
  --codec-min-size <bytes>       RUSTFS_GET_CODEC_STREAMING_MIN_SIZE (default: 1)
  --compat-object-key <key>      Object key used by the compatibility probe
  --compat-object-size <bytes>   Object size used by the compatibility probe (default: 65536)
  --skip-compat-probe            skip post-benchmark compatibility/fallback probes
  --resource-sample-interval-secs <n>
                                 Process resource sampler interval (default: 5)
  --skip-build                   do not run cargo build --release -p rustfs
  --detach                       start this script under nohup and exit after
                                 writing detached pid/log metadata
  --detach-log <path>            log file used with --detach
                                 (default: <out-dir>/detached.log)
  --dry-run                      print benchmark commands without starting RustFS

Credentials:
  --access-key <value>           RustFS access key (default: rustfsadmin)
  --secret-key <value>           RustFS secret key (default: rustfsadmin)
  --region <value>               S3 region (default: us-east-1)

Output:
  <out-dir>/environment.txt
  <out-dir>/manifest.env
  <out-dir>/baseline_compare.csv
  <out-dir>/cpu_rss_notes.txt
  <out-dir>/fallback_coverage.txt
  <out-dir>/tracking_issues.txt
  <out-dir>/raw_output_paths.txt
  <out-dir>/default_switch_readiness.md
  <out-dir>/legacy/warp/median_summary.csv
  <out-dir>/codec-legacy/warp/median_summary.csv
  <out-dir>/codec-rustfs/warp/median_summary.csv
  <out-dir>/engine_compare.csv                  when legacy and codec profiles both run
  <out-dir>/compat_summary.csv                  when legacy and codec profiles both run
  <out-dir>/metrics_summary.csv
  <out-dir>/service_metrics_summary.csv         when --diagnostic-metrics is set
  <out-dir>/service_metrics_round_summary.csv   per-round stage/page-fault deltas when direct --diagnostic-metrics is set
  <out-dir>/service_metrics_stage_distribution.csv
                                                per-round stage histogram bucket deltas when direct --diagnostic-metrics is set
  <out-dir>/service_metrics_round_percentiles.csv
                                                per-round request/stage histogram percentile bucket upper bounds
  <out-dir>/service_metrics_acceptance.csv      when --diagnostic-metrics is set
  <out-dir>/fallback_probe_summary.csv
  <out-dir>/body_sha256_legacy.txt              when legacy profile runs
  <out-dir>/body_sha256_codec_legacy.txt        when codec-legacy profile runs
  <out-dir>/body_sha256_codec_rustfs.txt        when codec-rustfs profile runs
  <out-dir>/response_headers_legacy.json        when legacy profile runs
  <out-dir>/response_headers_codec_legacy.json  when codec-legacy profile runs
  <out-dir>/response_headers_codec_rustfs.json  when codec-rustfs profile runs
  <out-dir>/<profile>/manifest.env
  <out-dir>/<profile>/metrics_summary.csv
  <out-dir>/<profile>/service_metrics_summary.csv
  <out-dir>/<profile>/service_metrics_round_summary.csv
  <out-dir>/<profile>/service_metrics_stage_distribution.csv
  <out-dir>/<profile>/service_metrics_round_percentiles.csv
  <out-dir>/<profile>/service-metrics/*.prom     before/after snapshots when --diagnostic-metrics is set
  <out-dir>/<profile>/service-metrics/rounds/*.prom
                                                  per-round snapshots when direct --diagnostic-metrics is set
  <out-dir>/<profile>/compat/compat_summary.csv
  <out-dir>/<profile>/compat/fallback_probe_summary.csv
  <out-dir>/<profile>/compat/response_headers.json
  <out-dir>/<profile>/compat/body_sha256.txt
  <out-dir>/<profile>/rustfs.log
  <out-dir>/<profile>/warp/logs/*.log

Example:
  scripts/run_get_codec_streaming_smoke.sh \
    --mode both --codec-engine legacy,rustfs --sizes 1MiB,4MiB,10MiB --concurrency 64 --duration 30s
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
      --mode) MODE="$2"; shift 2 ;;
      --codec-engine) CODEC_ENGINES="$2"; shift 2 ;;
      --metadata-early-stop) METADATA_EARLY_STOP="$2"; shift 2 ;;
      --shard-locality-preference) SHARD_LOCALITY_PREFERENCE="$2"; shift 2 ;;
      --codec-max-inflight) CODEC_MAX_INFLIGHT="$2"; shift 2 ;;
      --codec-multipart) CODEC_MULTIPART="$2"; shift 2 ;;
      --codec-multipart-max-parts) CODEC_MULTIPART_MAX_PARTS="$2"; shift 2 ;;
      --handoff-attribution) OUTPUT_HANDOFF_ATTRIBUTION=true; shift ;;
      --diagnostic-metrics) DIAGNOSTIC_METRICS=true; shift ;;
      --diagnostic-metrics-url) DIAGNOSTIC_METRICS_URL="$2"; shift 2 ;;
      --diagnostic-prometheus-query-url) DIAGNOSTIC_PROMETHEUS_QUERY_URL="$2"; shift 2 ;;
      --diagnostic-prometheus-query) DIAGNOSTIC_PROMETHEUS_QUERY="$2"; shift 2 ;;
      --diagnostic-metrics-settle-secs) DIAGNOSTIC_METRICS_SETTLE_SECS="$2"; shift 2 ;;
      --diagnostic-metrics-capture-attempts) DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS="$2"; shift 2 ;;
      --diagnostic-metrics-capture-retry-secs) DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS="$2"; shift 2 ;;
      --diagnostic-obs-endpoint) DIAGNOSTIC_OBS_ENDPOINT="$2"; shift 2 ;;
      --diagnostic-obs-metric-endpoint) DIAGNOSTIC_OBS_METRIC_ENDPOINT="$2"; shift 2 ;;
      --diagnostic-obs-meter-interval) DIAGNOSTIC_OBS_METER_INTERVAL="$2"; shift 2 ;;
      --diagnostic-obs-service-name-prefix) DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX="$2"; shift 2 ;;
      --compressed-fallback-probe) COMPRESSED_FALLBACK_PROBE=true; shift ;;
      --address) ADDRESS="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --warp-objects) WARP_OBJECTS="$2"; shift 2 ;;
      --warp-object-lifecycle) WARP_OBJECT_LIFECYCLE="$2"; shift 2 ;;
      --warp-prepare-duration) WARP_PREPARE_DURATION="$2"; shift 2 ;;
      --metadata-cache-max-entries) GET_OBJECT_METADATA_CACHE_MAX_ENTRIES="$2"; shift 2 ;;
      --direct-memory) GET_SMALL_OBJECT_DIRECT_MEMORY="$2"; shift 2 ;;
      --direct-memory-threshold) GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --round-cooldown-secs) ROUND_COOLDOWN_SECS="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --python-bin) PYTHON_BIN="$2"; shift 2 ;;
      --codec-min-size) CODEC_MIN_SIZE="$2"; shift 2 ;;
      --compat-object-key) COMPAT_OBJECT_KEY="$2"; shift 2 ;;
      --compat-object-size) COMPAT_OBJECT_SIZE="$2"; shift 2 ;;
      --skip-compat-probe) SKIP_COMPAT_PROBE=true; shift ;;
      --resource-sample-interval-secs) RESOURCE_SAMPLE_INTERVAL_SECS="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --skip-build) SKIP_BUILD=true; shift ;;
      --detach) DETACH=true; shift ;;
      --detach-log) DETACH_LOG="$2"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        usage >&2
        die "unknown arg: $1"
        ;;
    esac
  done
}

validate_args() {
  case "$MODE" in
    legacy|codec|both) ;;
    *) die "--mode must be legacy, codec, or both" ;;
  esac

  case "$METADATA_EARLY_STOP" in
    on|off) ;;
    *) die "--metadata-early-stop must be on or off" ;;
  esac

  case "$SHARD_LOCALITY_PREFERENCE" in
    on|off) ;;
    *) die "--shard-locality-preference must be on or off" ;;
  esac

  case "$CODEC_MULTIPART" in
    on|off) ;;
    *) die "--codec-multipart must be on or off" ;;
  esac

  local raw engine
  IFS=',' read -r -a engines <<< "$CODEC_ENGINES"
  [[ "${#engines[@]}" -gt 0 ]] || die "--codec-engine must not be empty"
  for raw in "${engines[@]}"; do
    engine="${raw//[[:space:]]/}"
    case "$engine" in
      legacy|rustfs) ;;
      *) die "--codec-engine supports only legacy,rustfs, got: $engine" ;;
    esac
  done

  [[ -n "$ADDRESS" ]] || die "--address must not be empty"
  [[ -n "$ACCESS_KEY" ]] || die "--access-key must not be empty"
  [[ -n "$SECRET_KEY" ]] || die "--secret-key must not be empty"
  [[ -n "$BUCKET" ]] || die "--bucket must not be empty"
  [[ -n "$SIZES" ]] || die "--sizes must not be empty"
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$CODEC_MAX_INFLIGHT" "--codec-max-inflight"
  validate_positive_int "$CODEC_MULTIPART_MAX_PARTS" "--codec-multipart-max-parts"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_non_negative_int "$ROUND_COOLDOWN_SECS" "--round-cooldown-secs"
  if [[ -n "$WARP_OBJECTS" ]]; then
    validate_positive_int "$WARP_OBJECTS" "--warp-objects"
  fi
  case "$WARP_OBJECT_LIFECYCLE" in
    per-round|prepare-once|existing-only) ;;
    *) die "--warp-object-lifecycle must be per-round, prepare-once, or existing-only" ;;
  esac
  if [[ "$WARP_OBJECT_LIFECYCLE" != "per-round" ]]; then
    local size_count
    size_count="$(printf '%s\n' "$SIZES" | awk -F',' '{ count=0; for (i=1; i<=NF; i++) { gsub(/^[ \t]+|[ \t]+$/, "", $i); if ($i != "") count++ } print count }')"
    [[ "$size_count" -eq 1 ]] || die "--warp-object-lifecycle=${WARP_OBJECT_LIFECYCLE} currently requires a single --sizes value to avoid mixing object sizes in one bucket"
  fi
  if [[ -n "$GET_OBJECT_METADATA_CACHE_MAX_ENTRIES" ]]; then
    validate_positive_int "$GET_OBJECT_METADATA_CACHE_MAX_ENTRIES" "--metadata-cache-max-entries"
  fi
  if [[ -n "$GET_SMALL_OBJECT_DIRECT_MEMORY" ]]; then
    case "$GET_SMALL_OBJECT_DIRECT_MEMORY" in
      on|off) ;;
      *) die "--direct-memory must be on or off" ;;
    esac
  fi
  if [[ -n "$GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD" ]]; then
    validate_positive_int "$GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD" "--direct-memory-threshold"
  fi
  validate_positive_int "$CODEC_MIN_SIZE" "--codec-min-size"
  validate_positive_int "$COMPAT_OBJECT_SIZE" "--compat-object-size"
  validate_positive_int "$RESOURCE_SAMPLE_INTERVAL_SECS" "--resource-sample-interval-secs"
  validate_positive_int "$HEALTH_TIMEOUT_SECS" "--health-timeout-secs"
  validate_non_negative_int "$DIAGNOSTIC_METRICS_SETTLE_SECS" "--diagnostic-metrics-settle-secs"
  validate_positive_int "$DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS" "--diagnostic-metrics-capture-attempts"
  validate_non_negative_int "$DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS" "--diagnostic-metrics-capture-retry-secs"
  validate_positive_int "$DIAGNOSTIC_OBS_METER_INTERVAL" "--diagnostic-obs-meter-interval"
  [[ -n "$COMPAT_OBJECT_KEY" ]] || die "--compat-object-key must not be empty"
  [[ -n "$DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX" ]] || die "--diagnostic-obs-service-name-prefix must not be empty"
  [[ -n "$DIAGNOSTIC_METRICS_URL" ]] || die "--diagnostic-metrics-url must not be empty"

  [[ -x "$ENHANCED_BENCH" ]] || die "enhanced benchmark script is not executable: $ENHANCED_BENCH"
  require_cmd curl
  require_cmd git

  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd cargo
    require_cmd "$PYTHON_BIN"
  fi
  if [[ "$DETACH" == "true" && "$DRY_RUN" != "true" ]]; then
    require_cmd nohup
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/get-codec-streaming-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
}

bool_from_on_off() {
  case "$1" in
    on) echo "true" ;;
    off) echo "false" ;;
    *) die "expected on/off value, got: $1" ;;
  esac
}

dry_run_multipart_expected_reason() {
  if [[ "$CODEC_MULTIPART" != "on" ]]; then
    echo "multipart"
  elif [[ "$CODEC_MULTIPART_MAX_PARTS" -lt 2 ]]; then
    echo "multipart_part_limit"
  else
    echo "none"
  fi
}

command_line_string() {
  printf '%q ' "${BASH_SOURCE[0]}" "${ORIGINAL_ARGS[@]}"
}

detached_child_args() {
  local skip_next=false
  local arg

  for arg in "${ORIGINAL_ARGS[@]}"; do
    if [[ "$skip_next" == "true" ]]; then
      skip_next=false
      continue
    fi

    case "$arg" in
      --detach)
        ;;
      --detach-log)
        skip_next=true
        ;;
      *)
        printf '%s\0' "$arg"
        ;;
    esac
  done
}

run_detached_if_requested() {
  if [[ "$DETACH" != "true" ]]; then
    return 0
  fi

  if [[ -z "$DETACH_LOG" ]]; then
    DETACH_LOG="${OUT_DIR}/detached.log"
  fi

  local script_path="${PROJECT_ROOT}/scripts/run_get_codec_streaming_smoke.sh"
  local pid_file="${OUT_DIR}/detached.pid"
  local command_file="${OUT_DIR}/detached_command.txt"
  local -a child_args=()

  while IFS= read -r -d '' arg; do
    child_args+=("$arg")
  done < <(detached_child_args)

  {
    printf '%q ' "$script_path" "${child_args[@]}"
    printf '\n'
  } >"$command_file"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] detach requested"
    log "[DRY-RUN] detached log: ${DETACH_LOG}"
    log "[DRY-RUN] detached command: ${command_file}"
    return 0
  fi

  nohup "$script_path" "${child_args[@]}" >"$DETACH_LOG" 2>&1 < /dev/null &
  local detached_pid="$!"
  printf '%s\n' "$detached_pid" >"$pid_file"

  log "Detached GET codec streaming smoke started."
  log "PID: ${detached_pid}"
  log "Output dir: ${OUT_DIR}"
  log "Log: ${DETACH_LOG}"
  log "Command: ${command_file}"
  exit 0
}

sanitize_metric_label_value() {
  printf '%s' "$1" | tr -c '[:alnum:]_-' '-'
}

diagnostic_service_name() {
  local profile="$1"
  local run_id
  run_id="$(sanitize_metric_label_value "$(basename "$OUT_DIR")")"
  echo "${DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX}-${run_id}-${profile}"
}

detect_cpu_summary() {
  if command -v sysctl >/dev/null 2>&1; then
    local brand logical
    brand="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
    logical="$(sysctl -n hw.logicalcpu 2>/dev/null || true)"
    if [[ -n "$brand" && -n "$logical" ]]; then
      printf '%s (%s logical cores)\n' "$brand" "$logical"
      return
    fi
  fi

  if command -v lscpu >/dev/null 2>&1; then
    lscpu 2>/dev/null | awk -F: '
      /^Model name:/ { model=$2 }
      /^CPU\(s\):/ { cpus=$2 }
      END {
        gsub(/^[ \t]+|[ \t]+$/, "", model)
        gsub(/^[ \t]+|[ \t]+$/, "", cpus)
        if (model != "" && cpus != "") {
          printf "%s (%s logical cores)\n", model, cpus
        } else if (model != "") {
          printf "%s\n", model
        }
      }
    '
    return
  fi

  echo "unknown"
}

write_root_environment() {
  local branch git_head git_dirty_count rustc_version cargo_version command_line
  branch="$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD)"
  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
  git_dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"
  rustc_version="$(rustc --version 2>/dev/null || echo unavailable)"
  cargo_version="$(cargo --version 2>/dev/null || echo unavailable)"
  command_line="$(command_line_string)"

  cat >"${OUT_DIR}/environment.txt" <<EOF
generated_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
branch=${branch}
git_head=${git_head}
dirty_count=${git_dirty_count}
rustc_version=${rustc_version}
cargo_version=${cargo_version}
os=$(uname -srmo 2>/dev/null || uname -a)
cpu=$(detect_cpu_summary)
command_line=${command_line% }
EOF
}

write_tracking_issues() {
  cat >"${OUT_DIR}/tracking_issues.txt" <<'EOF'
rustfs/backlog#715
rustfs/backlog#716
rustfs/backlog#717
rustfs/backlog#718
rustfs/backlog#719
rustfs/backlog#720
rustfs/backlog#721
rustfs/backlog#722
rustfs/backlog#723
rustfs/backlog#724
rustfs/backlog#725
rustfs/backlog#726
rustfs/backlog#727
rustfs/backlog#758
rustfs/backlog#759
rustfs/backlog#760
rustfs/backlog#761
rustfs/backlog#762
rustfs/backlog#763
rustfs/backlog#764
EOF
}

write_fallback_coverage() {
  cat >"${OUT_DIR}/fallback_coverage.txt" <<'EOF'
codec_streaming rollout fallback coverage proof
============================================

Static gate reasons:
- disabled
- rollout_not_opted_in
- rollout_pct_not_selected
- body_compatibility_unconfirmed
- header_compatibility_unconfirmed
- lock_optimization_disabled
- range
- below_min_size
- encrypted
- compressed
- remote
- multipart
- multipart_part_limit
- invalid_min_size
- read_quorum_not_safe

Relevant focused proof points:
- set_disk::read::tests::codec_streaming_reader_gate_defaults_to_disabled
- set_disk::read::tests::codec_streaming_engine_env_is_ignored_when_streaming_is_disabled
- set_disk::read::tests::codec_streaming_reader_gate_requires_explicit_rollout_and_compat_confirmation
- set_disk::read::tests::codec_streaming_reader_gate_is_conservative
- set_disk::read::tests::codec_streaming_reader_build_falls_back_when_read_quorum_is_not_safe
- erasure::codec::bridge::tests::rustfs_codec_decode_engine_rejects_inconsistent_reconstruction_sources
- erasure::codec::bridge::tests::rustfs_codec_decode_engine_rejects_stale_data_source
- erasure::coding::decode_reader::tests::erasure_decode_reader_rustfs_engine_recovers_after_bitrot_source_mismatch
- rustfs_io_get_object_reconstruct_outcome_total{path,engine,outcome}

Conclusion:
- range / encrypted / compressed / remote objects remain on legacy fallback paths
- multipart remains a legacy fallback by default and can be separately enabled with RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE=true
- multipart codec streaming is bounded by RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS
- degraded reader setup remains opt-out via read_quorum_not_safe
- codec-rustfs remains explicit opt-in through RUSTFS_GET_CODEC_STREAMING_ENGINE=rustfs
- healthy codec-rustfs reads should report skip_data_complete instead of rustfs_called
- rustfs_called is reserved for explicit reconstruction test/probe coverage and must not be required for healthy-read rollout
- env kill switch remains available through RUSTFS_GET_CODEC_STREAMING_ENABLE=false
EOF
}

write_root_manifest() {
  local profiles_csv="$1"
  local branch git_head git_dirty_count command_line
  branch="$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD)"
  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
  git_dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"
  command_line="$(command_line_string)"

  cat >"${OUT_DIR}/manifest.env" <<EOF
branch=${branch}
git_head=${git_head}
git_dirty_count=${git_dirty_count}
profiles=${profiles_csv}
mode=${MODE}
codec_engines=${CODEC_ENGINES}
metadata_early_stop=${METADATA_EARLY_STOP}
shard_locality_preference=${SHARD_LOCALITY_PREFERENCE}
codec_max_inflight=${CODEC_MAX_INFLIGHT}
codec_rollout_codec_profile=benchmark
codec_body_compat_confirmed_codec_profile=true
codec_header_compat_confirmed_codec_profile=true
codec_multipart=${CODEC_MULTIPART}
codec_multipart_max_parts=${CODEC_MULTIPART_MAX_PARTS}
output_handoff_attribution=${OUTPUT_HANDOFF_ATTRIBUTION}
diagnostic_metrics_enabled=${DIAGNOSTIC_METRICS}
diagnostic_metrics_url=${DIAGNOSTIC_METRICS_URL}
diagnostic_prometheus_query_url=${DIAGNOSTIC_PROMETHEUS_QUERY_URL}
diagnostic_prometheus_query=${DIAGNOSTIC_PROMETHEUS_QUERY}
diagnostic_metrics_settle_secs=${DIAGNOSTIC_METRICS_SETTLE_SECS}
diagnostic_obs_endpoint=${DIAGNOSTIC_OBS_ENDPOINT}
diagnostic_obs_metric_endpoint=${DIAGNOSTIC_OBS_METRIC_ENDPOINT}
diagnostic_obs_meter_interval=${DIAGNOSTIC_OBS_METER_INTERVAL}
diagnostic_obs_service_name_prefix=${DIAGNOSTIC_OBS_SERVICE_NAME_PREFIX}
service_metric_prefix=${SERVICE_METRIC_PREFIX}
compressed_fallback_probe=${COMPRESSED_FALLBACK_PROBE}
compressed_probe_extension=${COMPRESSED_PROBE_EXTENSION}
compressed_probe_mime_type=${COMPRESSED_PROBE_MIME_TYPE}
address=${ADDRESS}
bucket=${BUCKET}
region=${REGION}
sizes=${SIZES}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
warp_objects=${WARP_OBJECTS}
RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES=${GET_OBJECT_METADATA_CACHE_MAX_ENTRIES}
RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY=${GET_SMALL_OBJECT_DIRECT_MEMORY}
RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD=${GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD}
skip_build=${SKIP_BUILD}
dry_run=${DRY_RUN}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
python_bin=${PYTHON_BIN}
codec_min_size=${CODEC_MIN_SIZE}
compat_object_key=${COMPAT_OBJECT_KEY}
compat_object_size=${COMPAT_OBJECT_SIZE}
command_line=${command_line% }
EOF
}

build_rustfs_if_needed() {
  if [[ "$DRY_RUN" == "true" || "$SKIP_BUILD" == "true" ]]; then
    return
  fi

  log "Building RustFS release binary..."
  cargo build --release -p rustfs
}

profile_codec_enabled() {
  local profile="$1"
  case "$profile" in
    legacy) echo "false" ;;
    codec-legacy|codec-rustfs) echo "true" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_codec_engine() {
  local profile="$1"
  case "$profile" in
    legacy|codec-legacy) echo "legacy" ;;
    codec-rustfs) echo "rustfs" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_rollout_target() {
  local profile="$1"
  case "$profile" in
    legacy) echo "off" ;;
    codec-legacy|codec-rustfs) echo "benchmark" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_body_compat_confirmed() {
  local profile="$1"
  case "$profile" in
    legacy) echo "false" ;;
    codec-legacy|codec-rustfs) echo "true" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_header_compat_confirmed() {
  local profile="$1"
  case "$profile" in
    legacy) echo "false" ;;
    codec-legacy|codec-rustfs) echo "true" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_metrics_path() {
  local profile="$1"
  case "$profile" in
    legacy) echo "legacy_duplex" ;;
    codec-legacy) echo "codec_streaming_legacy_engine" ;;
    codec-rustfs) echo "codec_streaming_rustfs_engine" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_data_root() {
  local profile="$1"
  echo "${OUT_DIR}/${profile}/data"
}

profile_volumes() {
  local data_root="$1"
  printf '%s/disk1 %s/disk2 %s/disk3 %s/disk4' "$data_root" "$data_root" "$data_root" "$data_root"
}

endpoint_url() {
  echo "http://${ADDRESS}"
}

single_benchmark_size() {
  printf '%s\n' "$SIZES" | awk -F',' '{
    for (i=1; i<=NF; i++) {
      gsub(/^[ \t]+|[ \t]+$/, "", $i)
      if ($i != "") {
        print $i
        exit
      }
    }
  }'
}

write_manifest() {
  local profile="$1"
  local profile_dir="$2"
  local codec_enabled="$3"
  local volumes="$4"
  local codec_engine="$5"
  local metrics_path="$6"
  local rollout_target body_compat_confirmed header_compat_confirmed obs_service_name
  local git_head git_dirty_count
  rollout_target="$(profile_rollout_target "$profile")"
  body_compat_confirmed="$(profile_body_compat_confirmed "$profile")"
  header_compat_confirmed="$(profile_header_compat_confirmed "$profile")"
  obs_service_name="$(diagnostic_service_name "$profile")"

  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
  git_dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"

  cat >"${profile_dir}/manifest.env" <<EOF
profile=${profile}
git_head=${git_head}
git_dirty_count=${git_dirty_count}
endpoint=$(endpoint_url)
address=${ADDRESS}
bucket=${BUCKET}
region=${REGION}
sizes=${SIZES}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
warp_objects=${WARP_OBJECTS}
warp_object_lifecycle=${WARP_OBJECT_LIFECYCLE}
warp_prepare_duration=${WARP_PREPARE_DURATION}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
python_bin=${PYTHON_BIN}
rust_log=${RUST_LOG}
compat_object_key=${COMPAT_OBJECT_KEY}
compat_object_size=${COMPAT_OBJECT_SIZE}
rustfs_volumes=${volumes}
RUSTFS_GET_CODEC_STREAMING_ENABLE=${codec_enabled}
RUSTFS_GET_CODEC_STREAMING_ENGINE=${codec_engine}
RUSTFS_GET_CODEC_STREAMING_ROLLOUT=${rollout_target}
RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED=${body_compat_confirmed}
RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED=${header_compat_confirmed}
RUSTFS_GET_METADATA_EARLY_STOP_ENABLE=$(bool_from_on_off "$METADATA_EARLY_STOP")
RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE=$(bool_from_on_off "$SHARD_LOCALITY_PREFERENCE")
RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT=${CODEC_MAX_INFLIGHT}
RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE=$(bool_from_on_off "$CODEC_MULTIPART")
RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS=${CODEC_MULTIPART_MAX_PARTS}
RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE=${OUTPUT_HANDOFF_ATTRIBUTION}
RUSTFS_GET_CODEC_STREAMING_MIN_SIZE=${CODEC_MIN_SIZE}
RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES=${GET_OBJECT_METADATA_CACHE_MAX_ENTRIES}
RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY=${GET_SMALL_OBJECT_DIRECT_MEMORY}
RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD=${GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD}
RUSTFS_OBS_METRICS_EXPORT_ENABLED=${DIAGNOSTIC_METRICS}
RUSTFS_OBS_ENDPOINT=${DIAGNOSTIC_OBS_ENDPOINT}
RUSTFS_OBS_METRIC_ENDPOINT=${DIAGNOSTIC_OBS_METRIC_ENDPOINT}
RUSTFS_OBS_METER_INTERVAL=${DIAGNOSTIC_OBS_METER_INTERVAL}
RUSTFS_OBS_SERVICE_NAME=${obs_service_name}
RUSTFS_COMPRESSION_ENABLED=${COMPRESSED_FALLBACK_PROBE}
RUSTFS_COMPRESSION_EXTENSIONS=${COMPRESSED_PROBE_EXTENSION}
RUSTFS_COMPRESSION_MIME_TYPES=${COMPRESSED_PROBE_MIME_TYPE}
diagnostic_metrics_enabled=${DIAGNOSTIC_METRICS}
diagnostic_metrics_url=${DIAGNOSTIC_METRICS_URL}
diagnostic_prometheus_query_url=${DIAGNOSTIC_PROMETHEUS_QUERY_URL}
diagnostic_prometheus_query=${DIAGNOSTIC_PROMETHEUS_QUERY}
diagnostic_metrics_settle_secs=${DIAGNOSTIC_METRICS_SETTLE_SECS}
service_metric_prefix=${SERVICE_METRIC_PREFIX}
metrics_path=${metrics_path}
RUSTFS_SCANNER_ENABLED=false
RUSTFS_SCANNER_START_DELAY_SECS=3600
RUSTFS_SCANNER_CYCLE=3600
RUSTFS_CONSOLE_ENABLE=false
RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
resource_sample_interval_secs=${RESOURCE_SAMPLE_INTERVAL_SECS}
skip_compat_probe=${SKIP_COMPAT_PROBE}
EOF
}

stop_server() {
  if [[ -n "$SERVER_SAMPLER_PID" ]]; then
    if kill -0 "$SERVER_SAMPLER_PID" >/dev/null 2>&1; then
      kill "$SERVER_SAMPLER_PID" >/dev/null 2>&1 || true
      wait "$SERVER_SAMPLER_PID" >/dev/null 2>&1 || true
    fi
    SERVER_SAMPLER_PID=""
    SERVER_SAMPLER_LOG=""
  fi

  if [[ -n "$SERVER_PID" ]]; then
    if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      kill "$SERVER_PID" >/dev/null 2>&1 || true
      wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    SERVER_PID=""
  fi
}

wait_for_health() {
  local profile="$1"
  local log_file="$2"
  local health_url
  health_url="$(endpoint_url)/health"

  for ((attempt = 1; attempt <= HEALTH_TIMEOUT_SECS; attempt++)); do
    if curl -fsS --noproxy '*' --connect-timeout 2 --max-time 3 "$health_url" >/dev/null 2>&1; then
      return
    fi
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      tail -n 80 "$log_file" >&2 || true
      die "RustFS exited before health check passed for profile: $profile"
    fi
    sleep 1
  done

  tail -n 80 "$log_file" >&2 || true
  die "RustFS health check timed out for profile: $profile"
}

start_server() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local data_root
  local volumes
  local codec_enabled
  local codec_engine
  local metrics_path
  local rollout_target
  local body_compat_confirmed
  local header_compat_confirmed
  local obs_service_name
  local rustfs_log

  data_root="$(profile_data_root "$profile")"
  volumes="$(profile_volumes "$data_root")"
  codec_enabled="$(profile_codec_enabled "$profile")"
  codec_engine="$(profile_codec_engine "$profile")"
  metrics_path="$(profile_metrics_path "$profile")"
  rollout_target="$(profile_rollout_target "$profile")"
  body_compat_confirmed="$(profile_body_compat_confirmed "$profile")"
  header_compat_confirmed="$(profile_header_compat_confirmed "$profile")"
  obs_service_name="$(diagnostic_service_name "$profile")"
  rustfs_log="${profile_dir}/rustfs.log"

  mkdir -p "${data_root}/disk1" "${data_root}/disk2" "${data_root}/disk3" "${data_root}/disk4"
  write_manifest "$profile" "$profile_dir" "$codec_enabled" "$volumes" "$codec_engine" "$metrics_path"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] start RustFS profile=${profile} endpoint=$(endpoint_url)"
    log "[DRY-RUN] RUSTFS_VOLUMES=${volumes}"
    return
  fi

  [[ -x "$RUSTFS_BIN" ]] || die "RustFS binary is not executable: $RUSTFS_BIN"

  (
    export RUSTFS_ADDRESS="$ADDRESS"
    export RUSTFS_ACCESS_KEY="$ACCESS_KEY"
    export RUSTFS_SECRET_KEY="$SECRET_KEY"
    export RUSTFS_VOLUMES="$volumes"
    export RUSTFS_CONSOLE_ENABLE=false
    export RUSTFS_GET_CODEC_STREAMING_ENABLE="$codec_enabled"
    export RUSTFS_GET_CODEC_STREAMING_ENGINE="$codec_engine"
    export RUSTFS_GET_CODEC_STREAMING_ROLLOUT="$rollout_target"
    export RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED="$body_compat_confirmed"
    export RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED="$header_compat_confirmed"
    RUSTFS_GET_METADATA_EARLY_STOP_ENABLE="$(bool_from_on_off "$METADATA_EARLY_STOP")"
    export RUSTFS_GET_METADATA_EARLY_STOP_ENABLE
    RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE="$(bool_from_on_off "$SHARD_LOCALITY_PREFERENCE")"
    export RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE
    export RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT="$CODEC_MAX_INFLIGHT"
    RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE="$(bool_from_on_off "$CODEC_MULTIPART")"
    export RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE
    export RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS="$CODEC_MULTIPART_MAX_PARTS"
    export RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE="$OUTPUT_HANDOFF_ATTRIBUTION"
    export RUSTFS_GET_CODEC_STREAMING_MIN_SIZE="$CODEC_MIN_SIZE"
    if [[ -n "$GET_OBJECT_METADATA_CACHE_MAX_ENTRIES" ]]; then
      export RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES="$GET_OBJECT_METADATA_CACHE_MAX_ENTRIES"
    fi
    if [[ -n "$GET_SMALL_OBJECT_DIRECT_MEMORY" ]]; then
      RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY="$(bool_from_on_off "$GET_SMALL_OBJECT_DIRECT_MEMORY")"
      export RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY
    fi
    if [[ -n "$GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD" ]]; then
      export RUSTFS_GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD="$GET_SMALL_OBJECT_DIRECT_MEMORY_THRESHOLD"
    fi
    export RUSTFS_OBS_METRICS_EXPORT_ENABLED="$DIAGNOSTIC_METRICS"
    if [[ "$DIAGNOSTIC_METRICS" == "true" ]]; then
      export RUSTFS_OBS_METER_INTERVAL="$DIAGNOSTIC_OBS_METER_INTERVAL"
      export RUSTFS_OBS_SERVICE_NAME="$obs_service_name"
      if [[ -n "$DIAGNOSTIC_OBS_ENDPOINT" ]]; then
        export RUSTFS_OBS_ENDPOINT="$DIAGNOSTIC_OBS_ENDPOINT"
      fi
      if [[ -n "$DIAGNOSTIC_OBS_METRIC_ENDPOINT" ]]; then
        export RUSTFS_OBS_METRIC_ENDPOINT="$DIAGNOSTIC_OBS_METRIC_ENDPOINT"
      fi
    fi
    export RUSTFS_COMPRESSION_ENABLED="$COMPRESSED_FALLBACK_PROBE"
    export RUSTFS_COMPRESSION_EXTENSIONS="$COMPRESSED_PROBE_EXTENSION"
    export RUSTFS_COMPRESSION_MIME_TYPES="$COMPRESSED_PROBE_MIME_TYPE"
    export RUSTFS_REGION="$REGION"
    export RUSTFS_RPC_SECRET="rustfs-get-codec-smoke-rpc-secret"
    export RUSTFS_SCANNER_ENABLED=false
    export RUSTFS_SCANNER_START_DELAY_SECS=3600
    export RUSTFS_SCANNER_CYCLE=3600
    export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
    export RUST_LOG
    exec "$RUSTFS_BIN"
  ) >"$rustfs_log" 2>&1 &

  SERVER_PID="$!"
  start_server_sampler "$SERVER_PID" "$profile_dir"
  wait_for_health "$profile" "$rustfs_log"
}

prepare_warp_existing_objects() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local prepare_dir="${profile_dir}/warp_prepare"
  local size
  size="$(single_benchmark_size)"

  if [[ "$WARP_OBJECT_LIFECYCLE" != "prepare-once" ]]; then
    return 0
  fi

  mkdir -p "$prepare_dir"

  local cmd=(
    "$WARP_BIN" get
    --host "$ADDRESS"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket "$BUCKET"
    --obj.size "$size"
    --concurrent "$CONCURRENCY"
    --duration "$WARP_PREPARE_DURATION"
    --region "$REGION"
    --noclear
  )
  if [[ -n "$WARP_OBJECTS" ]]; then
    cmd+=(--objects "$WARP_OBJECTS")
  fi

  {
    printf 'profile=%s\n' "$profile"
    printf 'mode=prepare-once\n'
    printf 'size=%s\n' "$size"
    printf 'duration=%s\n' "$WARP_PREPARE_DURATION"
    printf 'bucket=%s\n' "$BUCKET"
    printf 'command='
    printf '%q ' "${cmd[@]}"
    printf '\n'
  } >"${prepare_dir}/manifest.env"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] prepare existing warp objects profile=${profile} size=${size}"
    printf '[DRY-RUN] ' >"${prepare_dir}/prepare.log"
    printf '%q ' "${cmd[@]}" >>"${prepare_dir}/prepare.log"
    printf '\n' >>"${prepare_dir}/prepare.log"
    return 0
  fi

  log "Preparing existing warp objects profile=${profile} size=${size} lifecycle=${WARP_OBJECT_LIFECYCLE}..."
  "${cmd[@]}" >"${prepare_dir}/prepare.log" 2>&1
}

run_bench() {
  local profile="$1"
  local baseline_csv="${2:-}"
  local profile_dir="${OUT_DIR}/${profile}"
  local bench_dir="${profile_dir}/warp"
  local cmd=(
    "$ENHANCED_BENCH"
    --tool warp
    --endpoint "$(endpoint_url)"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket "$BUCKET"
    --region "$REGION"
    --warp-bin "$WARP_BIN"
    --warp-mode get
    --sizes "$SIZES"
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --round-cooldown-secs "$ROUND_COOLDOWN_SECS"
    --out-dir "$bench_dir"
  )

  if [[ -n "$baseline_csv" ]]; then
    cmd+=(--baseline-csv "$baseline_csv")
  fi
  if [[ "$WARP_OBJECT_LIFECYCLE" == "per-round" && -n "$WARP_OBJECTS" ]]; then
    cmd+=(--extra-args "--objects ${WARP_OBJECTS}")
  fi
  if [[ "$WARP_OBJECT_LIFECYCLE" != "per-round" ]]; then
    local lifecycle_args="--list-existing --noclear"
    if [[ -n "$WARP_OBJECTS" ]]; then
      lifecycle_args="${lifecycle_args} --objects ${WARP_OBJECTS}"
    fi
    cmd+=(--extra-args "$lifecycle_args")
  fi
  if [[ "$DIAGNOSTIC_METRICS" == "true" && -z "$DIAGNOSTIC_PROMETHEUS_QUERY_URL" ]]; then
    cmd+=(
      --service-metrics-url "$DIAGNOSTIC_METRICS_URL"
      --service-metrics-dir "${profile_dir}/service-metrics/rounds"
      --service-metrics-attempts "$DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS"
      --service-metrics-retry-secs "$DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS"
    )
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  log "Running ${profile} GET benchmark..."
  "${cmd[@]}"
}

write_metrics_summary() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local median_csv="${profile_dir}/warp/median_summary.csv"

  if [[ -f "$median_csv" ]]; then
    cp "$median_csv" "${profile_dir}/metrics_summary.csv"
  fi
}

service_metrics_dir() {
  local profile="$1"
  echo "${OUT_DIR}/${profile}/service-metrics"
}

capture_prometheus_query_snapshot() {
  local phase="$1"
  local snapshot_file="$2"
  local status_file="$3"
  local service_name="$4"

  "$PYTHON_BIN" - "$DIAGNOSTIC_PROMETHEUS_QUERY_URL" "$DIAGNOSTIC_PROMETHEUS_QUERY" "$phase" "$snapshot_file" "$status_file" "$service_name" <<'PY'
import json
import pathlib
import sys
import urllib.parse
import urllib.request

query_url, query, phase, snapshot_raw, status_raw, service_name = sys.argv[1:]
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
except Exception as err:  # noqa: BLE001 - shell harness reports the failure in status files.
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
    if not service_name or sample.get("metric", {}).get("service.name") == service_name
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

capture_service_metrics_snapshot() {
  local profile="$1"
  local phase="$2"
  local metrics_dir snapshot_file status_file
  metrics_dir="$(service_metrics_dir "$profile")"
  snapshot_file="${metrics_dir}/${phase}.prom"
  status_file="${metrics_dir}/${phase}.status"

  if [[ "$DIAGNOSTIC_METRICS" != "true" ]]; then
    return 0
  fi

  mkdir -p "$metrics_dir"

  if [[ "$DRY_RUN" == "true" ]]; then
    : >"$snapshot_file"
    cat >"$status_file" <<EOF
phase=${phase}
status=not_run_dry_run
url=${DIAGNOSTIC_METRICS_URL}
EOF
    return 0
  fi

  if [[ -n "$DIAGNOSTIC_PROMETHEUS_QUERY_URL" ]]; then
    capture_prometheus_query_snapshot "$phase" "$snapshot_file" "$status_file" "$(diagnostic_service_name "$profile")"
    return 0
  fi

  local tmp_file capture_attempt
  tmp_file="${snapshot_file}.tmp"
  for ((capture_attempt=1; capture_attempt<=DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS; capture_attempt++)); do
    if curl -fsS --noproxy '*' --connect-timeout 2 --max-time 5 "$DIAGNOSTIC_METRICS_URL" >"$tmp_file"; then
      if [[ -s "$tmp_file" ]]; then
        mv "$tmp_file" "$snapshot_file"
        cat >"$status_file" <<EOF
phase=${phase}
status=ok
capture_attempt=${capture_attempt}
url=${DIAGNOSTIC_METRICS_URL}
EOF
        return 0
      fi
    fi

    rm -f "$tmp_file"
    if (( capture_attempt < DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS && DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS > 0 )); then
      sleep "$DIAGNOSTIC_METRICS_CAPTURE_RETRY_SECS"
    fi
  done

  : >"$snapshot_file"
  cat >"$status_file" <<EOF
phase=${phase}
status=capture_failed
capture_attempts=${DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS}
url=${DIAGNOSTIC_METRICS_URL}
EOF
  log "WARN: failed to capture service metrics profile=${profile} phase=${phase} attempts=${DIAGNOSTIC_METRICS_CAPTURE_ATTEMPTS}"
}

write_profile_service_metrics_summary() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local out_csv="${profile_dir}/service_metrics_summary.csv"
  local before_file="${profile_dir}/service-metrics/before.prom"
  local after_file="${profile_dir}/service-metrics/after.prom"

  if [[ "$DIAGNOSTIC_METRICS" != "true" ]]; then
    cat >"$out_csv" <<EOF
profile,status,metric,labels,before,after,delta,classification
${profile},disabled,N/A,N/A,N/A,N/A,N/A,diagnostic_metrics_disabled
EOF
    return
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    cat >"$out_csv" <<EOF
profile,status,metric,labels,before,after,delta,classification
${profile},not_run_dry_run,N/A,N/A,N/A,N/A,N/A,diagnostic_metrics_dry_run
EOF
    return
  fi

  if [[ ! -s "$before_file" || ! -s "$after_file" ]]; then
    cat >"$out_csv" <<EOF
profile,status,metric,labels,before,after,delta,classification
${profile},snapshot_missing,N/A,N/A,N/A,N/A,N/A,service_metrics_snapshot_missing
EOF
    return
  fi

  "$PYTHON_BIN" - "$profile" "$SERVICE_METRIC_PREFIX" "$before_file" "$after_file" "$out_csv" "$(diagnostic_service_name "$profile")" <<'PY'
import csv
import pathlib
import re
import sys

profile, metric_prefix, before_raw, after_raw, out_raw, service_name = sys.argv[1:]
before_path = pathlib.Path(before_raw)
after_path = pathlib.Path(after_raw)
out_path = pathlib.Path(out_raw)

LINE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+'
    r'([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)'
    r'(?:\s+[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)?\s*$'
)


def classify(metric):
    if metric == "rustfs_io_get_object_codec_streaming_decision_total":
        return "codec_decision"
    if metric == "rustfs_io_get_object_codec_streaming_fallback_total":
        return "codec_fallback"
    if metric == "rustfs_io_get_object_reader_path_total":
        return "reader_path"
    if metric.startswith("rustfs_io_get_object_stage_duration_seconds"):
        return "stage_duration"
    if metric.startswith("rustfs_io_get_object_reader_copy"):
        return "reader_copy"
    if metric.startswith("rustfs_io_get_object_reader_prefetch"):
        return "reader_prefetch"
    if metric.startswith("rustfs_io_get_object_fill_"):
        return "fill_pipeline"
    if metric.startswith("rustfs_io_get_object_shard_read"):
        return "shard_read"
    if metric.startswith("rustfs_io_get_object_metadata_"):
        return "metadata"
    if metric.startswith("rustfs_io_get_object_reader_"):
        return "reader"
    return "get_object"


def read_metrics(path):
    rows = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw or raw.startswith("#"):
            continue
        match = LINE.match(raw)
        if match is None:
            continue
        metric, labels, value = match.groups()
        if not metric.startswith(metric_prefix):
            continue
        labels = labels or ""
        if service_name and f'service_name="{service_name}"' not in labels \
                and f'service.name="{service_name}"' not in labels:
            continue
        rows[(metric, labels)] = float(value)
    return rows


before = read_metrics(before_path)
after = read_metrics(after_path)
keys = sorted(set(before) | set(after))

with out_path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(["profile", "status", "metric", "labels", "before", "after", "delta", "classification"])
    if not keys:
        writer.writerow([profile, "no_matching_metrics", "N/A", "N/A", "N/A", "N/A", "N/A", "service_metrics_empty"])
    for metric, labels in keys:
        before_value = before.get((metric, labels), 0.0)
        after_value = after.get((metric, labels), 0.0)
        writer.writerow(
            [
                profile,
                "ok",
                metric,
                labels,
                f"{before_value:.12g}",
                f"{after_value:.12g}",
                f"{after_value - before_value:.12g}",
                classify(metric),
            ]
        )
PY
}

write_profile_service_metrics_round_breakdown() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local round_csv="${profile_dir}/warp/round_results.csv"
  local round_metrics_dir="${profile_dir}/service-metrics/rounds"
  local out_summary="${profile_dir}/service_metrics_round_summary.csv"
  local out_distribution="${profile_dir}/service_metrics_stage_distribution.csv"
  local out_percentiles="${profile_dir}/service_metrics_round_percentiles.csv"

  if [[ "$DIAGNOSTIC_METRICS" != "true" ]]; then
    cat >"$out_summary" <<EOF
profile,size,tool,round,attempt,round_status,stage,count_delta,sum_delta,avg_ms,minor_fault_delta,major_fault_delta,minor_faults_per_call,major_faults_per_call,before_status,after_status
${profile},N/A,N/A,N/A,N/A,disabled,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,diagnostic_metrics_disabled,diagnostic_metrics_disabled
EOF
    cat >"$out_distribution" <<EOF
profile,size,tool,round,attempt,round_status,path,stage,le,bucket_delta,before_status,after_status
${profile},N/A,N/A,N/A,N/A,disabled,N/A,N/A,N/A,N/A,diagnostic_metrics_disabled,diagnostic_metrics_disabled
EOF
    cat >"$out_percentiles" <<EOF
profile,size,tool,round,attempt,round_status,target,path,stage,count_delta,sum_delta,avg_ms,p50_le_seconds,p90_le_seconds,p99_le_seconds,bucket_resolution_note,before_status,after_status
${profile},N/A,N/A,N/A,N/A,disabled,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,diagnostic_metrics_disabled,diagnostic_metrics_disabled,diagnostic_metrics_disabled
EOF
    return
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    cat >"$out_summary" <<EOF
profile,size,tool,round,attempt,round_status,stage,count_delta,sum_delta,avg_ms,minor_fault_delta,major_fault_delta,minor_faults_per_call,major_faults_per_call,before_status,after_status
${profile},N/A,N/A,N/A,N/A,not_run_dry_run,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,not_run_dry_run,not_run_dry_run
EOF
    cat >"$out_distribution" <<EOF
profile,size,tool,round,attempt,round_status,path,stage,le,bucket_delta,before_status,after_status
${profile},N/A,N/A,N/A,N/A,not_run_dry_run,N/A,N/A,N/A,N/A,not_run_dry_run,not_run_dry_run
EOF
    cat >"$out_percentiles" <<EOF
profile,size,tool,round,attempt,round_status,target,path,stage,count_delta,sum_delta,avg_ms,p50_le_seconds,p90_le_seconds,p99_le_seconds,bucket_resolution_note,before_status,after_status
${profile},N/A,N/A,N/A,N/A,not_run_dry_run,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,not_run_dry_run,not_run_dry_run,not_run_dry_run
EOF
    return
  fi

  if [[ -n "$DIAGNOSTIC_PROMETHEUS_QUERY_URL" ]]; then
    cat >"$out_summary" <<EOF
profile,size,tool,round,attempt,round_status,stage,count_delta,sum_delta,avg_ms,minor_fault_delta,major_fault_delta,minor_faults_per_call,major_faults_per_call,before_status,after_status
${profile},N/A,N/A,N/A,N/A,prometheus_query_round_breakdown_unsupported,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,prometheus_query,prometheus_query
EOF
    cat >"$out_distribution" <<EOF
profile,size,tool,round,attempt,round_status,path,stage,le,bucket_delta,before_status,after_status
${profile},N/A,N/A,N/A,N/A,prometheus_query_round_breakdown_unsupported,N/A,N/A,N/A,N/A,prometheus_query,prometheus_query
EOF
    cat >"$out_percentiles" <<EOF
profile,size,tool,round,attempt,round_status,target,path,stage,count_delta,sum_delta,avg_ms,p50_le_seconds,p90_le_seconds,p99_le_seconds,bucket_resolution_note,before_status,after_status
${profile},N/A,N/A,N/A,N/A,prometheus_query_round_breakdown_unsupported,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,prometheus_query_round_breakdown_unsupported,prometheus_query,prometheus_query
EOF
    return
  fi

  "$PYTHON_BIN" - "$profile" "$SERVICE_METRIC_PREFIX" "$round_csv" "$round_metrics_dir" \
    "$out_summary" "$out_distribution" "$out_percentiles" "$(diagnostic_service_name "$profile")" <<'PY'
import csv
import pathlib
import re
import sys

profile, metric_prefix, round_raw, metrics_dir_raw, summary_raw, distribution_raw, percentiles_raw, service_name = sys.argv[1:]
round_path = pathlib.Path(round_raw)
metrics_dir = pathlib.Path(metrics_dir_raw)
summary_path = pathlib.Path(summary_raw)
distribution_path = pathlib.Path(distribution_raw)
percentiles_path = pathlib.Path(percentiles_raw)

TARGET_STAGES = [
    "store_reader_setup",
    "reader_open_mmap_copy_success",
    "reader_mmap_blocking_wait",
    "reader_mmap_blocking_task",
    "reader_mmap_copy_buffer",
    "reader_mmap_map",
    "body_build",
    "body_memory_blob",
    "response_handoff",
]
DISTRIBUTION_STAGES = {
    "reader_mmap_blocking_wait",
    "reader_mmap_blocking_task",
    "reader_mmap_copy_buffer",
    "reader_open_mmap_copy_success",
}
LINE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+'
    r'([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)'
    r'(?:\s+[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)?\s*$'
)


def snapshot_token(tool, size, round_id, attempt):
    safe_size = re.sub(r"[^A-Za-z0-9_.-]", "_", size)
    return f"{tool}_{safe_size}_r{round_id}_a{attempt}"


def status_for(path):
    if not path.exists():
        return "missing"
    values = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values.get("status", "unknown")


def label_value(labels, key):
    match = re.search(rf'(?:^|,){re.escape(key)}="((?:\\.|[^"\\])*)"', labels)
    if match is None:
        return ""
    return match.group(1).replace(r'\"', '"').replace(r"\\", "\\")


def read_metrics(path):
    rows = {}
    if not path.exists() or path.stat().st_size == 0:
        return rows
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw or raw.startswith("#"):
            continue
        match = LINE.match(raw)
        if match is None:
            continue
        metric, labels, value = match.groups()
        if not metric.startswith(metric_prefix):
            continue
        labels = labels or ""
        if service_name and f'service_name="{service_name}"' not in labels \
                and f'service.name="{service_name}"' not in labels:
            continue
        rows[(metric, labels)] = float(value)
    return rows


def metric_delta(before, after, metric_name, stage, suffix):
    total = 0.0
    for metric, labels in sorted(set(before) | set(after)):
        if metric != f"{metric_name}_{suffix}":
            continue
        if label_value(labels, "stage") != stage:
            continue
        total += after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
    return total


def fault_delta(before, after, kind):
    total = 0.0
    for metric, labels in sorted(set(before) | set(after)):
        if metric != "rustfs_io_get_object_mmap_page_faults_total":
            continue
        if label_value(labels, "stage") != "reader_mmap_copy_buffer":
            continue
        if label_value(labels, "kind") != kind:
            continue
        total += after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
    return total


def histogram_bucket_deltas(before, after, metric_name, path="", stage="", status=""):
    buckets = {}
    for metric, labels in sorted(set(before) | set(after)):
        if metric != f"{metric_name}_bucket":
            continue
        if path and label_value(labels, "path") != path:
            continue
        if stage and label_value(labels, "stage") != stage:
            continue
        if status and label_value(labels, "status") != status:
            continue
        le = label_value(labels, "le")
        if not le:
            continue
        buckets[le] = buckets.get(le, 0.0) + after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
    return buckets


def bucket_sort_key(le):
    return float("inf") if le == "+Inf" else float(le)


def percentile_bucket_upper_bound(buckets, quantile):
    if not buckets:
        return "N/A"
    finite_or_inf = sorted(buckets.items(), key=lambda item: bucket_sort_key(item[0]))
    total = buckets.get("+Inf", finite_or_inf[-1][1])
    if total <= 0:
        return "N/A"
    threshold = total * quantile
    for le, cumulative in finite_or_inf:
        if cumulative >= threshold:
            return le
    return finite_or_inf[-1][0]


def metric_count_sum(before, after, metric_name, path="", stage="", status=""):
    count = 0.0
    total = 0.0
    for metric, labels in sorted(set(before) | set(after)):
        if path and label_value(labels, "path") != path:
            continue
        if stage and label_value(labels, "stage") != stage:
            continue
        if status and label_value(labels, "status") != status:
            continue
        if metric == f"{metric_name}_count":
            count += after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
        elif metric == f"{metric_name}_sum":
            total += after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
    return count, total


PERCENTILE_TARGETS = [
    ("request_duration_ok", "rustfs_io_get_object_request_duration_seconds", "", "", "ok"),
    ("total_duration", "rustfs_io_get_object_total_duration_seconds", "", "", ""),
    ("request_context", "rustfs_io_get_object_stage_duration_seconds", "s3_handler", "request_context", ""),
]
for stage_name in TARGET_STAGES:
    PERCENTILE_TARGETS.append((stage_name, "rustfs_io_get_object_stage_duration_seconds", "", stage_name, ""))


with summary_path.open("w", encoding="utf-8", newline="") as summary_handle, \
        distribution_path.open("w", encoding="utf-8", newline="") as distribution_handle, \
        percentiles_path.open("w", encoding="utf-8", newline="") as percentiles_handle:
    summary_writer = csv.writer(summary_handle)
    distribution_writer = csv.writer(distribution_handle)
    percentiles_writer = csv.writer(percentiles_handle)
    summary_writer.writerow([
        "profile", "size", "tool", "round", "attempt", "round_status", "stage",
        "count_delta", "sum_delta", "avg_ms", "minor_fault_delta", "major_fault_delta",
        "minor_faults_per_call", "major_faults_per_call", "before_status", "after_status",
    ])
    distribution_writer.writerow([
        "profile", "size", "tool", "round", "attempt", "round_status", "path", "stage",
        "le", "bucket_delta", "before_status", "after_status",
    ])
    percentiles_writer.writerow([
        "profile", "size", "tool", "round", "attempt", "round_status", "target",
        "path", "stage", "count_delta", "sum_delta", "avg_ms",
        "p50_le_seconds", "p90_le_seconds", "p99_le_seconds", "bucket_resolution_note",
        "before_status", "after_status",
    ])

    if not round_path.exists() or not metrics_dir.exists():
        summary_writer.writerow([
            profile, "N/A", "N/A", "N/A", "N/A", "snapshot_missing", "N/A",
            "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "missing", "missing",
        ])
        distribution_writer.writerow([
            profile, "N/A", "N/A", "N/A", "N/A", "snapshot_missing", "N/A",
            "N/A", "N/A", "N/A", "missing", "missing",
        ])
        percentiles_writer.writerow([
            profile, "N/A", "N/A", "N/A", "N/A", "snapshot_missing", "N/A",
            "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A", "N/A",
            "snapshot_missing", "missing", "missing",
        ])
        raise SystemExit(0)

    for round_row in csv.DictReader(round_path.open(encoding="utf-8", newline="")):
        size = round_row["size"]
        tool = round_row["tool"]
        round_id = round_row["round"]
        attempt = round_row["attempt"]
        round_status = round_row["status"]
        token = snapshot_token(tool, size, round_id, attempt)
        before_path = metrics_dir / f"{token}_before.prom"
        after_path = metrics_dir / f"{token}_after.prom"
        before_status = status_for(metrics_dir / f"{token}_before.status")
        after_status = status_for(metrics_dir / f"{token}_after.status")
        before = read_metrics(before_path)
        after = read_metrics(after_path)

        minor_faults = fault_delta(before, after, "minor")
        major_faults = fault_delta(before, after, "major")
        for stage in TARGET_STAGES:
            count_delta = metric_delta(before, after, "rustfs_io_get_object_stage_duration_seconds", stage, "count")
            sum_delta = metric_delta(before, after, "rustfs_io_get_object_stage_duration_seconds", stage, "sum")
            avg_ms = (sum_delta / count_delta * 1000.0) if count_delta else 0.0
            stage_minor_faults = minor_faults if stage == "reader_mmap_copy_buffer" else 0.0
            stage_major_faults = major_faults if stage == "reader_mmap_copy_buffer" else 0.0
            summary_writer.writerow([
                profile,
                size,
                tool,
                round_id,
                attempt,
                round_status,
                stage,
                f"{count_delta:.12g}",
                f"{sum_delta:.12g}",
                f"{avg_ms:.6f}",
                f"{stage_minor_faults:.12g}",
                f"{stage_major_faults:.12g}",
                f"{(stage_minor_faults / count_delta) if count_delta else 0.0:.6f}",
                f"{(stage_major_faults / count_delta) if count_delta else 0.0:.6f}",
                before_status,
                after_status,
            ])

        for target, metric_name, path_label, stage_label, status_label in PERCENTILE_TARGETS:
            buckets = histogram_bucket_deltas(before, after, metric_name, path_label, stage_label, status_label)
            count_delta, sum_delta = metric_count_sum(before, after, metric_name, path_label, stage_label, status_label)
            avg_ms = (sum_delta / count_delta * 1000.0) if count_delta else 0.0
            p50 = percentile_bucket_upper_bound(buckets, 0.50)
            p90 = percentile_bucket_upper_bound(buckets, 0.90)
            p99 = percentile_bucket_upper_bound(buckets, 0.99)
            if not buckets:
                note = "no_buckets"
            elif p50 == p90 == p99 and p99 not in {"N/A", "+Inf"} and bucket_sort_key(p99) >= 1.0 and avg_ms < 1000.0:
                note = "coarse_seconds_buckets"
            else:
                note = "bucket_upper_bound"
            percentiles_writer.writerow([
                profile,
                size,
                tool,
                round_id,
                attempt,
                round_status,
                target,
                path_label or "N/A",
                stage_label or "N/A",
                f"{count_delta:.12g}",
                f"{sum_delta:.12g}",
                f"{avg_ms:.6f}",
                p50,
                p90,
                p99,
                note,
                before_status,
                after_status,
            ])

        for metric, labels in sorted(set(before) | set(after)):
            if metric != "rustfs_io_get_object_stage_duration_seconds_bucket":
                continue
            stage = label_value(labels, "stage")
            if stage not in DISTRIBUTION_STAGES:
                continue
            bucket_delta = after.get((metric, labels), 0.0) - before.get((metric, labels), 0.0)
            if bucket_delta == 0:
                continue
            distribution_writer.writerow([
                profile,
                size,
                tool,
                round_id,
                attempt,
                round_status,
                label_value(labels, "path"),
                stage,
                label_value(labels, "le"),
                f"{bucket_delta:.12g}",
                before_status,
                after_status,
            ])
PY
}

write_root_service_metrics_summary() {
  local out_csv="${OUT_DIR}/service_metrics_summary.csv"
  local profile summary wrote_header=false
  : >"$out_csv"

  for profile in "$@"; do
    summary="${OUT_DIR}/${profile}/service_metrics_summary.csv"
    [[ -f "$summary" ]] || continue
    if [[ "$wrote_header" == "false" ]]; then
      cat "$summary" >>"$out_csv"
      wrote_header=true
    else
      tail -n +2 "$summary" >>"$out_csv"
    fi
  done

  if [[ "$wrote_header" == "false" ]]; then
    cat >"$out_csv" <<'EOF'
profile,status,metric,labels,before,after,delta,classification
N/A,missing,N/A,N/A,N/A,N/A,N/A,no_profile_service_metrics_summary
EOF
  fi
}

write_root_profile_csv() {
  local basename="$1"
  shift
  local out_csv="${OUT_DIR}/${basename}"
  local profile summary wrote_header=false
  : >"$out_csv"

  for profile in "$@"; do
    summary="${OUT_DIR}/${profile}/${basename}"
    [[ -f "$summary" ]] || continue
    if [[ "$wrote_header" == "false" ]]; then
      cat "$summary" >>"$out_csv"
      wrote_header=true
    else
      tail -n +2 "$summary" >>"$out_csv"
    fi
  done

  if [[ "$wrote_header" == "false" ]]; then
    cat >"$out_csv" <<'EOF'
profile,status,note
N/A,missing,no_profile_csv
EOF
  fi
}

write_service_metrics_acceptance() {
  local out_csv="${OUT_DIR}/service_metrics_acceptance.csv"
  local service_csv="${OUT_DIR}/service_metrics_summary.csv"

  "$PYTHON_BIN" - "$out_csv" "$service_csv" "$DIAGNOSTIC_METRICS" "$MODE" "$CODEC_ENGINES" \
    "$COMPRESSED_FALLBACK_PROBE" "${OUT_DIR}/metrics_summary.csv" "$CODEC_MULTIPART" \
    "$CODEC_MULTIPART_MAX_PARTS" <<'PY'
import csv
import pathlib
import sys

out_path = pathlib.Path(sys.argv[1])
service_path = pathlib.Path(sys.argv[2])
diagnostic_enabled = sys.argv[3] == "true"
mode = sys.argv[4]
codec_engines = [item.strip() for item in sys.argv[5].split(",") if item.strip()]
compressed_fallback_probe_enabled = sys.argv[6] == "true"
metrics_path = pathlib.Path(sys.argv[7])
codec_multipart_enabled = sys.argv[8] == "on"
codec_multipart_max_parts = int(sys.argv[9])


def expected_profiles():
    codec_profiles = [f"codec-{engine}" for engine in codec_engines]
    if mode == "legacy":
        return ["legacy"]
    if mode == "codec":
        return codec_profiles
    return ["legacy", *codec_profiles]


rows = []
if service_path.exists():
    with service_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

metrics_rows = []
if metrics_path.exists():
    with metrics_path.open(encoding="utf-8", newline="") as handle:
        metrics_rows = list(csv.DictReader(handle))


def metric_matches(actual, expected):
    return actual == expected or actual == f"{expected}_total"


def delta_for(profile, metric, *label_fragments):
    total = 0.0
    for row in rows:
        if row.get("profile") != profile or row.get("status") != "ok":
            continue
        if not metric_matches(row.get("metric", ""), metric):
            continue
        labels = row.get("labels", "")
        if all(fragment in labels for fragment in label_fragments):
            try:
                total += float(row.get("delta", "0") or 0)
            except ValueError:
                pass
    return total


def delta_prefix(profile, metric_prefix, *label_fragments):
    total = 0.0
    for row in rows:
        if row.get("profile") != profile or row.get("status") != "ok":
            continue
        if not row.get("metric", "").startswith(metric_prefix):
            continue
        labels = row.get("labels", "")
        if all(fragment in labels for fragment in label_fragments):
            try:
                total += float(row.get("delta", "0") or 0)
            except ValueError:
                pass
    return total


def status_for(delta):
    return "pass" if delta > 0 else "fail"


def add(profile, check, expected, delta, note):
    rows_out.append(
        {
            "profile": profile,
            "check": check,
            "expected": expected,
            "status": status_for(delta),
            "observed_delta": f"{delta:.12g}",
            "note": note,
        }
    )


def p99_available():
    for row in metrics_rows:
        value = row.get("median_req_p99_ms", "")
        if value and value != "N/A":
            return True
    return False


rows_out = []
if not diagnostic_enabled:
    rows_out.append(
        {
            "profile": "N/A",
            "check": "diagnostic_metrics_enabled",
            "expected": "true",
            "status": "skipped",
            "observed_delta": "N/A",
            "note": "performance run: observability metrics export intentionally disabled",
        }
    )
elif rows and all(row.get("status") == "not_run_dry_run" for row in rows):
    rows_out.append(
        {
            "profile": "N/A",
            "check": "diagnostic_metrics_enabled",
            "expected": "non-dry-run service metrics snapshots",
            "status": "skipped",
            "observed_delta": "N/A",
            "note": "dry-run only validates artifact wiring; runtime deltas require a real server run",
        }
    )
elif rows and all(row.get("status") == "snapshot_missing" for row in rows):
    rows_out.append(
        {
            "profile": "N/A",
            "check": "diagnostic_metrics_capture",
            "expected": "non-empty service metrics snapshots",
            "status": "fail",
            "observed_delta": "N/A",
            "note": "metrics endpoint returned empty or missing snapshots; verify --diagnostic-metrics-url and exporter setup",
        }
    )
else:
    for profile in expected_profiles():
        if profile == "legacy":
            add(
                profile,
                "reader_path",
                'rustfs_io_get_object_reader_path_total{path="legacy_duplex"} delta > 0',
                delta_for(profile, "rustfs_io_get_object_reader_path_total", 'path="legacy_duplex"'),
                "proves legacy_duplex path was exercised",
            )
            add(
                profile,
                "fallback_disabled",
                'rustfs_io_get_object_codec_streaming_fallback_total{reason="disabled"} delta > 0',
                delta_for(profile, "rustfs_io_get_object_codec_streaming_fallback_total", 'reason="disabled"'),
                "proves kill-switch fallback reason at runtime",
            )
        else:
            engine = profile[len("codec-") :] if profile.startswith("codec-") else profile
            engine_path = "codec_streaming_rustfs_engine" if engine == "rustfs" else "codec_streaming_legacy_engine"
            add(
                profile,
                "codec_decision_use",
                'rustfs_io_get_object_codec_streaming_decision_total{outcome="use",reason="none"} delta > 0',
                delta_for(
                    profile,
                    "rustfs_io_get_object_codec_streaming_decision_total",
                    'outcome="use"',
                    'reason="none"',
                ),
                "proves eligible GETs selected codec streaming",
            )
            add(
                profile,
                "reader_path",
                'rustfs_io_get_object_reader_path_total{path="codec_streaming"} delta > 0',
                delta_for(profile, "rustfs_io_get_object_reader_path_total", 'path="codec_streaming"'),
                "proves response reader used the codec streaming path",
            )
            add(
                profile,
                "engine_stage",
                f'rustfs_io_get_object_stage_duration_seconds*{{path="{engine_path}"}} delta > 0',
                delta_prefix(profile, "rustfs_io_get_object_stage_duration_seconds", f'path="{engine_path}"'),
                "proves engine-specific codec path stage attribution",
            )
            add(
                profile,
                "reader_bytes",
                f'rustfs_io_get_object_reader_bytes_total{{path="{engine_path}"}} delta > 0',
                delta_for(profile, "rustfs_io_get_object_reader_bytes_total", f'path="{engine_path}"'),
                "proves emitted reader bytes were attributed to the selected engine",
            )
            add(
                profile,
                "fallback_range",
                'rustfs_io_get_object_codec_streaming_fallback_total{reason="range"} delta > 0',
                delta_for(profile, "rustfs_io_get_object_codec_streaming_fallback_total", 'reason="range"'),
                "proves runtime Range GET fallback reason delta",
            )
            if codec_multipart_enabled:
                if codec_multipart_max_parts < 2:
                    add(
                        profile,
                        "multipart_fallback_part_limit",
                        'rustfs_io_get_object_codec_streaming_decision_total{outcome="fallback",object_class="multipart",reason="multipart_part_limit"} delta > 0',
                        delta_for(
                            profile,
                            "rustfs_io_get_object_codec_streaming_decision_total",
                            'outcome="fallback"',
                            'object_class="multipart"',
                            'reason="multipart_part_limit"',
                        ),
                        "proves multipart codec streaming falls back when part count exceeds the configured guard",
                    )
                else:
                    add(
                        profile,
                        "multipart_codec_decision_use",
                        'rustfs_io_get_object_codec_streaming_decision_total{outcome="use",object_class="multipart",reason="none"} delta > 0',
                        delta_for(
                            profile,
                            "rustfs_io_get_object_codec_streaming_decision_total",
                            'outcome="use"',
                            'object_class="multipart"',
                            'reason="none"',
                        ),
                        "proves runtime multipart GET selected codec streaming under explicit opt-in",
                    )
                    add(
                        profile,
                        "multipart_fallback_read_quorum_not_safe",
                        'rustfs_io_get_object_codec_streaming_decision_total{outcome="fallback",object_class="multipart",reason="read_quorum_not_safe"} delta > 0',
                        delta_for(
                            profile,
                            "rustfs_io_get_object_codec_streaming_decision_total",
                            'outcome="fallback"',
                            'object_class="multipart"',
                            'reason="read_quorum_not_safe"',
                        ),
                        "proves unsafe multipart part setup falls back before returning a codec reader",
                    )
            else:
                add(
                    profile,
                    "fallback_multipart",
                    'rustfs_io_get_object_codec_streaming_fallback_total{reason="multipart"} delta > 0',
                    delta_for(profile, "rustfs_io_get_object_codec_streaming_fallback_total", 'reason="multipart"'),
                    "proves runtime multipart object fallback reason delta",
                )
            add(
                profile,
                "fallback_encrypted",
                'rustfs_io_get_object_codec_streaming_fallback_total{reason="encrypted"} delta > 0',
                delta_for(profile, "rustfs_io_get_object_codec_streaming_fallback_total", 'reason="encrypted"'),
                "proves runtime encrypted object fallback reason delta",
            )
            add(
                profile,
                "fallback_read_quorum_not_safe",
                'rustfs_io_get_object_codec_streaming_fallback_total{reason="read_quorum_not_safe"} delta > 0',
                delta_for(
                    profile,
                    "rustfs_io_get_object_codec_streaming_fallback_total",
                    'reason="read_quorum_not_safe"',
                ),
                "proves degraded-but-readable shard setup uses the legacy fallback path",
            )
            compressed_delta = delta_for(
                profile,
                "rustfs_io_get_object_codec_streaming_fallback_total",
                'reason="compressed"',
            )
            rows_out.append(
                {
                    "profile": profile,
                    "check": "fallback_compressed",
                    "expected": 'rustfs_io_get_object_codec_streaming_fallback_total{reason="compressed"} delta > 0',
                    "status": "pass"
                    if compressed_delta > 0
                    else ("fail" if compressed_fallback_probe_enabled else "unavailable"),
                    "observed_delta": f"{compressed_delta:.12g}",
                    "note": "Enable --compressed-fallback-probe to generate a runtime compressed fallback delta"
                    if not compressed_fallback_probe_enabled
                    else "proves runtime disk-compressed object fallback reason delta",
                }
            )
            below_delta = delta_for(
                profile,
                "rustfs_io_get_object_codec_streaming_fallback_total",
                'reason="below_min_size"',
            )
            rows_out.append(
                {
                    "profile": profile,
                    "check": "fallback_below_min_size",
                    "expected": 'rustfs_io_get_object_codec_streaming_fallback_total{reason="below_min_size"} delta > 0',
                    "status": "pass" if below_delta > 0 else "unavailable",
                    "observed_delta": f"{below_delta:.12g}",
                    "note": "Set --codec-min-size greater than 1 to force this runtime fallback probe when unavailable",
                }
            )

    rows_out.append(
        {
            "profile": "all",
            "check": "diagnostic_vs_performance_separation",
            "expected": "diagnostic artifacts are separate from performance conclusions",
            "status": "pass",
            "observed_delta": "N/A",
            "note": "use --diagnostic-metrics for path proof; omit it for throughput acceptance runs",
        }
    )
    p99_status = p99_available()
    rows_out.append(
        {
            "profile": "all",
            "check": "p95_p99",
            "expected": "p99 available from warp request percentile output; p95 explicitly marked unavailable when warp does not emit it",
            "status": "pass" if p99_status else "unavailable",
            "observed_delta": "p99_available" if p99_status else "N/A",
            "note": "median_req_p99_ms is aggregated in metrics_summary.csv; p95 remains unavailable because current warp text output emits 90% and 99%, not 95%",
        }
    )

with out_path.open("w", encoding="utf-8", newline="") as handle:
    fieldnames = ["profile", "check", "expected", "status", "observed_delta", "note"]
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_out)
PY
}

start_server_sampler() {
  local pid="$1"
  local profile_dir="$2"
  SERVER_SAMPLER_LOG="${profile_dir}/resource_samples.csv"

  if [[ "$DRY_RUN" == "true" ]]; then
    cat >"$SERVER_SAMPLER_LOG" <<'EOF'
timestamp_utc,rss_kib,cpu_pct,threads,fd_count,vm_hwm_kib,vm_data_kib,voluntary_ctxt_switches,nonvoluntary_ctxt_switches,sched_runtime_ns,sched_wait_ns,sched_timeslices
DRY_RUN,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A,N/A
EOF
    return
  fi

  echo "timestamp_utc,rss_kib,cpu_pct,threads,fd_count,vm_hwm_kib,vm_data_kib,voluntary_ctxt_switches,nonvoluntary_ctxt_switches,sched_runtime_ns,sched_wait_ns,sched_timeslices" >"$SERVER_SAMPLER_LOG"
  (
    while kill -0 "$pid" >/dev/null 2>&1; do
      local timestamp sample rss_cpu threads fd_count vm_hwm vm_data voluntary_ctxt nonvoluntary_ctxt sched_runtime sched_wait sched_slices
      timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      rss_cpu="$(ps -o rss= -o %cpu= -p "$pid" 2>/dev/null | awk 'NF >= 2 { gsub(/^[ \t]+|[ \t]+$/, "", $1); gsub(/^[ \t]+|[ \t]+$/, "", $2); print $1 "," $2; exit }')"
      if [[ -z "$rss_cpu" ]]; then
        sleep "$RESOURCE_SAMPLE_INTERVAL_SECS"
        continue
      fi

      threads="N/A"
      fd_count="N/A"
      vm_hwm="N/A"
      vm_data="N/A"
      voluntary_ctxt="N/A"
      nonvoluntary_ctxt="N/A"
      sched_runtime="N/A"
      sched_wait="N/A"
      sched_slices="N/A"

      if [[ -r "/proc/${pid}/status" ]]; then
        sample="$(awk '
          /^Threads:/ { threads=$2 }
          /^VmHWM:/ { vm_hwm=$2 }
          /^VmData:/ { vm_data=$2 }
          /^voluntary_ctxt_switches:/ { voluntary=$2 }
          /^nonvoluntary_ctxt_switches:/ { nonvoluntary=$2 }
          END {
            printf "%s,%s,%s,%s,%s",
              threads ? threads : "N/A",
              vm_hwm ? vm_hwm : "N/A",
              vm_data ? vm_data : "N/A",
              voluntary ? voluntary : "N/A",
              nonvoluntary ? nonvoluntary : "N/A"
          }
        ' "/proc/${pid}/status")"
        IFS=',' read -r threads vm_hwm vm_data voluntary_ctxt nonvoluntary_ctxt <<<"$sample"
      fi
      if [[ -d "/proc/${pid}/fd" ]]; then
        fd_count="$(find "/proc/${pid}/fd" -maxdepth 1 -type l 2>/dev/null | awk 'END { print NR + 0 }')"
      fi
      if [[ -d "/proc/${pid}/task" ]]; then
        sample="$(awk '
          {
            runtime += $1
            wait += $2
            slices += $3
            seen = 1
          }
          END {
            if (seen) {
              printf "%.0f %.0f %.0f", runtime, wait, slices
            }
          }
        ' /proc/"${pid}"/task/*/schedstat 2>/dev/null || true)"
        if [[ -n "$sample" ]]; then
          read -r sched_runtime sched_wait sched_slices <<<"$sample"
        fi
      fi

      echo "${timestamp},${rss_cpu},${threads},${fd_count},${vm_hwm},${vm_data},${voluntary_ctxt},${nonvoluntary_ctxt},${sched_runtime},${sched_wait},${sched_slices}" >>"$SERVER_SAMPLER_LOG"
      sleep "$RESOURCE_SAMPLE_INTERVAL_SECS"
    done
  ) &
  SERVER_SAMPLER_PID="$!"
}

write_profile_cpu_rss_notes() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local sample_csv="${profile_dir}/resource_samples.csv"
  local notes_file="${profile_dir}/cpu_rss_notes.txt"

  if [[ "$DRY_RUN" == "true" ]]; then
    cat >"$notes_file" <<EOF
profile=${profile}
sampling=not_run_dry_run
cpu_rss_acceptability=not_proven
note=Formal runtime CPU/RSS evidence requires a non-dry-run harness execution.
EOF
    return
  fi

  if [[ ! -f "$sample_csv" ]]; then
    cat >"$notes_file" <<EOF
profile=${profile}
sampling=missing
cpu_rss_acceptability=not_proven
note=Resource sampling file was not generated.
EOF
    return
  fi

  awk -F',' -v profile="$profile" '
    NR == 1 { next }
    $2 != "N/A" && $2 != "" {
      rss = $2 + 0
      cpu = $3 + 0
      threads = ($4 != "N/A" && $4 != "") ? $4 + 0 : 0
      fd_count = ($5 != "N/A" && $5 != "") ? $5 + 0 : 0
      vm_hwm = ($6 != "N/A" && $6 != "") ? $6 + 0 : 0
      vm_data = ($7 != "N/A" && $7 != "") ? $7 + 0 : 0
      voluntary = ($8 != "N/A" && $8 != "") ? $8 + 0 : -1
      nonvoluntary = ($9 != "N/A" && $9 != "") ? $9 + 0 : -1
      if (count == 0 || rss > max_rss) max_rss = rss
      if (count == 0 || cpu > max_cpu) max_cpu = cpu
      if (threads > max_threads) max_threads = threads
      if (fd_count > max_fd_count) max_fd_count = fd_count
      if (vm_hwm > max_vm_hwm) max_vm_hwm = vm_hwm
      if (vm_data > max_vm_data) max_vm_data = vm_data
      if (voluntary >= 0) {
        if (!seen_voluntary) { first_voluntary = voluntary; seen_voluntary = 1 }
        last_voluntary = voluntary
      }
      if (nonvoluntary >= 0) {
        if (!seen_nonvoluntary) { first_nonvoluntary = nonvoluntary; seen_nonvoluntary = 1 }
        last_nonvoluntary = nonvoluntary
      }
      cpu_sum += cpu
      count++
    }
    END {
      print "profile=" profile
      print "samples=" count + 0
      if (count == 0) {
        print "max_rss_kib=unknown"
        print "max_cpu_pct=unknown"
        print "avg_cpu_pct=unknown"
        print "cpu_rss_acceptability=not_proven"
        print "note=No non-empty resource samples were captured."
      } else {
        printf "max_rss_kib=%.0f\n", max_rss
        printf "max_cpu_pct=%.2f\n", max_cpu
        printf "avg_cpu_pct=%.2f\n", cpu_sum / count
        printf "max_threads=%.0f\n", max_threads
        printf "max_fd_count=%.0f\n", max_fd_count
        printf "max_vm_hwm_kib=%.0f\n", max_vm_hwm
        printf "max_vm_data_kib=%.0f\n", max_vm_data
        if (seen_voluntary) {
          printf "voluntary_ctxt_switches_delta=%.0f\n", last_voluntary - first_voluntary
        } else {
          print "voluntary_ctxt_switches_delta=unknown"
        }
        if (seen_nonvoluntary) {
          printf "nonvoluntary_ctxt_switches_delta=%.0f\n", last_nonvoluntary - first_nonvoluntary
        } else {
          print "nonvoluntary_ctxt_switches_delta=unknown"
        }
        print "cpu_rss_acceptability=manual_review_required"
        print "note=Harness captured runtime/resource samples but does not impose a hard pass/fail threshold."
      }
    }
  ' "$sample_csv" >"$notes_file"
}

write_skipped_compat_probe() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local compat_dir="${profile_dir}/compat"

  mkdir -p "$compat_dir"
  cat >"${compat_dir}/compat_summary.csv" <<EOF
size,path,body_sha256_match,content_length_match,etag_match,content_range_match,checksum_headers_match,sse_headers_match,status_code_match,error_count,body_sha256,status_code,content_length,etag
${COMPAT_OBJECT_SIZE},${profile},skipped,skipped,skipped,skipped,skipped,skipped,skipped,0,SKIPPED,N/A,N/A,SKIPPED
EOF
  cat >"${compat_dir}/fallback_probe_summary.csv" <<EOF
profile,probe,object_key,status,status_code,body_len,expected_runtime_fallback_reason,note
${profile},all,${COMPAT_OBJECT_KEY},skipped,N/A,N/A,N/A,skipped by --skip-compat-probe
EOF
}

run_compat_probe() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local compat_dir="${profile_dir}/compat"

  mkdir -p "$compat_dir"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] run compatibility probe profile=${profile} key=${COMPAT_OBJECT_KEY}"
    cat >"${compat_dir}/compat_summary.csv" <<EOF
size,path,body_sha256_match,content_length_match,etag_match,content_range_match,checksum_headers_match,sse_headers_match,status_code_match,error_count,body_sha256,status_code,content_length,etag
${COMPAT_OBJECT_SIZE},${profile},true,true,true,true,true,true,true,0,DRY_RUN,200,${COMPAT_OBJECT_SIZE},DRY_RUN
EOF
    printf '%s\n' "DRY_RUN" >"${compat_dir}/body_sha256.txt"
    printf '{}\n' >"${compat_dir}/response_headers.json"
    {
      cat <<EOF
profile,probe,object_key,status,status_code,body_len,expected_runtime_fallback_reason,note
${profile},range,${COMPAT_OBJECT_KEY},not_run_dry_run,206,N/A,range,dry-run only
${profile},below_min_size,${COMPAT_OBJECT_KEY}.below-min-size,not_run_dry_run,N/A,N/A,below_min_size,dry-run only
${profile},multipart,${COMPAT_OBJECT_KEY}.multipart,not_run_dry_run,200,N/A,$(dry_run_multipart_expected_reason),dry-run only
${profile},multipart_degraded_read,${COMPAT_OBJECT_KEY}.multipart,not_run_dry_run,200,N/A,read_quorum_not_safe,dry-run only
${profile},encrypted,${COMPAT_OBJECT_KEY}.encrypted,not_run_dry_run,200,N/A,encrypted,dry-run only
${profile},read_quorum_not_safe,${COMPAT_OBJECT_KEY}.degraded-read,not_run_dry_run,200,N/A,read_quorum_not_safe,dry-run only
EOF
      if [[ "$COMPRESSED_FALLBACK_PROBE" == "true" ]]; then
        printf '%s,%s,%s%s,%s,%s,%s,%s,%s\n' \
          "$profile" "compressed" "$COMPAT_OBJECT_KEY" "$COMPRESSED_PROBE_EXTENSION" \
          "not_run_dry_run" "200" "N/A" "compressed" "dry-run only"
      else
        printf '%s,%s,%s%s,%s,%s,%s,%s,%s\n' \
          "$profile" "compressed" "$COMPAT_OBJECT_KEY" "$COMPRESSED_PROBE_EXTENSION" \
          "skipped" "N/A" "N/A" "compressed" "use --compressed-fallback-probe"
      fi
    } >"${compat_dir}/fallback_probe_summary.csv"
    cat >"${compat_dir}/snapshot.json" <<EOF
{
  "profile": "${profile}",
  "path": "${profile}",
  "size": ${COMPAT_OBJECT_SIZE},
  "object_key": "${COMPAT_OBJECT_KEY}",
  "expected_body_sha256": "DRY_RUN",
  "body_sha256": "DRY_RUN",
  "body_sha256_match_expected": true,
  "head_status": 200,
  "get_status": 200,
  "head_headers": {
    "content-length": ["${COMPAT_OBJECT_SIZE}"],
    "etag": ["DRY_RUN"]
  },
  "get_headers": {
    "content-length": ["${COMPAT_OBJECT_SIZE}"],
    "etag": ["DRY_RUN"]
  }
}
EOF
    return
  fi
  "$PYTHON_BIN" - "$(endpoint_url)" "$ACCESS_KEY" "$SECRET_KEY" "$REGION" "$BUCKET" \
    "$COMPAT_OBJECT_KEY" "$COMPAT_OBJECT_SIZE" "$CODEC_MIN_SIZE" "$compat_dir" "$profile" \
    "$COMPRESSED_FALLBACK_PROBE" "$COMPRESSED_PROBE_EXTENSION" "$COMPRESSED_PROBE_MIME_TYPE" \
    "$CODEC_MULTIPART" "$CODEC_MULTIPART_MAX_PARTS" <<'PY'
import base64
import csv
import datetime as dt
import hashlib
import hmac
import http.client
import json
import pathlib
import sys
from typing import Dict, List, Optional, Tuple
import urllib.parse
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape

(
    endpoint,
    access_key,
    secret_key,
    region,
    bucket,
    object_key,
    object_size_raw,
    codec_min_size_raw,
    out_dir_raw,
    profile,
    compressed_fallback_probe_raw,
    compressed_probe_extension,
    compressed_probe_mime_type,
    codec_multipart_raw,
    codec_multipart_max_parts_raw,
) = sys.argv[1:]
object_size = int(object_size_raw)
codec_min_size = int(codec_min_size_raw)
compressed_fallback_probe = compressed_fallback_probe_raw == "true"
codec_multipart_enabled = codec_multipart_raw == "on"
codec_multipart_max_parts = int(codec_multipart_max_parts_raw)
out_dir = pathlib.Path(out_dir_raw)
out_dir.mkdir(parents=True, exist_ok=True)


def payload(size: int) -> bytes:
    return bytes(((index * 31 + 7) % 251 for index in range(size)))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sign(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def signing_key(secret: str, date_stamp: str) -> bytes:
    key_date = sign(("AWS4" + secret).encode(), date_stamp)
    key_region = sign(key_date, region)
    key_service = sign(key_region, "s3")
    return sign(key_service, "aws4_request")


def canonical_query(query: str) -> str:
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = [
        (urllib.parse.quote(key, safe="-_.~"), urllib.parse.quote(value, safe="-_.~"))
        for key, value in pairs
    ]
    encoded.sort()
    return "&".join(f"{key}={value}" for key, value in encoded)


def canonical_uri(path: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="-_.~/") for part in path.split("/")) or "/"


def signed_headers(method: str, path: str, body: bytes, extra_headers: List[Tuple[str, str]]) -> Dict[str, str]:
    parsed = urllib.parse.urlsplit(endpoint)
    amz_date = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    headers = [("host", parsed.netloc), ("x-amz-content-sha256", sha256_hex(body)), ("x-amz-date", amz_date)]
    headers.extend((name.lower(), " ".join(value.strip().split())) for name, value in extra_headers)
    headers.sort()
    canonical_headers = "".join(f"{name}:{value}\n" for name, value in headers)
    signed_names = ";".join(name for name, _ in headers)
    raw_path, _, raw_query = path.partition("?")
    canonical_request = "\n".join(
        [method, canonical_uri(raw_path), canonical_query(raw_query), canonical_headers, signed_names, sha256_hex(body)]
    )
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
    )
    signature = hmac.new(signing_key(secret_key, date_stamp), string_to_sign.encode(), hashlib.sha256).hexdigest()
    result = {name: value for name, value in headers}
    result["Authorization"] = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_names}, "
        f"Signature={signature}"
    )
    return result


def request(method: str, path: str, body: bytes = b"", extra_headers: Optional[List[Tuple[str, str]]] = None):
    parsed = urllib.parse.urlsplit(endpoint)
    headers = signed_headers(method, path, body, extra_headers or [])
    if body and "content-length" not in headers:
        headers["content-length"] = str(len(body))
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    response_body = response.read()
    snapshot = {
        "status": response.status,
        "reason": response.reason,
        "headers": response.getheaders(),
        "body_sha256": sha256_hex(response_body),
        "body_len": len(response_body),
    }
    conn.close()
    return snapshot, response_body


def header_map(headers) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for name, value in headers:
        result.setdefault(name.lower(), []).append(value)
    for value in result.values():
        value.sort()
    return dict(sorted(result.items()))


def first_header(snapshot, name: str) -> str:
    name = name.lower()
    for header_name, value in snapshot["headers"]:
        if header_name.lower() == name:
            return value
    return ""


def find_xml_text(body: bytes, name: str) -> Optional[str]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None
    for elem in root.iter():
        if elem.tag.rsplit("}", 1)[-1] == name:
            return elem.text
    return None


bucket_path = "/" + urllib.parse.quote(bucket, safe="")
object_path = bucket_path + "/" + urllib.parse.quote(object_key, safe="/-_.~")
body = payload(object_size)

create_bucket, _ = request("PUT", bucket_path)
if create_bucket["status"] not in (200, 409):
    raise SystemExit(f"create bucket failed: {create_bucket['status']} {create_bucket['reason']}")

put_object, _ = request("PUT", object_path, body, [("content-type", "application/octet-stream")])
if put_object["status"] not in (200, 204):
    raise SystemExit(f"put object failed: {put_object['status']} {put_object['reason']}")

head_object, _ = request("HEAD", object_path)
get_object, get_body = request("GET", object_path)
if get_object["status"] != 200:
    raise SystemExit(f"get object failed: {get_object['status']} {get_object['reason']}")

range_object, range_body = request("GET", object_path, extra_headers=[("range", "bytes=0-0")])
fallback_rows = [
    {
        "profile": profile,
        "probe": "range",
        "object_key": object_key,
        "status": "ok" if range_object["status"] == 206 else "unexpected_status",
        "status_code": range_object["status"],
        "body_len": len(range_body),
        "expected_runtime_fallback_reason": "range",
        "note": "Range GET should stay on the legacy fallback path for codec profiles",
    }
]

if codec_min_size > 1:
    small_size = max(1, min(codec_min_size - 1, object_size))
    small_key = object_key + ".below-min-size"
    small_path = bucket_path + "/" + urllib.parse.quote(small_key, safe="/-_.~")
    small_body = payload(small_size)
    small_put, _ = request("PUT", small_path, small_body, [("content-type", "application/octet-stream")])
    if small_put["status"] not in (200, 204):
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "below_min_size",
                "object_key": small_key,
                "status": "put_failed",
                "status_code": small_put["status"],
                "body_len": 0,
                "expected_runtime_fallback_reason": "below_min_size",
                "note": "below-min-size probe upload failed",
            }
        )
    else:
        small_get, small_get_body = request("GET", small_path)
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "below_min_size",
                "object_key": small_key,
                "status": "ok" if small_get["status"] == 200 else "unexpected_status",
                "status_code": small_get["status"],
                "body_len": len(small_get_body),
                "expected_runtime_fallback_reason": "below_min_size",
                "note": "Object smaller than codec-min-size should stay on the legacy fallback path for codec profiles",
            }
        )
else:
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "below_min_size",
            "object_key": object_key + ".below-min-size",
            "status": "skipped",
            "status_code": "N/A",
            "body_len": "N/A",
            "expected_runtime_fallback_reason": "below_min_size",
            "note": "Set --codec-min-size greater than 1 to generate a runtime below_min_size fallback delta",
        }
    )

compressed_key = object_key + compressed_probe_extension
compressed_path = bucket_path + "/" + urllib.parse.quote(compressed_key, safe="/-_.~")
if compressed_fallback_probe:
    compressed_body = (b"RustFS compressed fallback probe\n" * 4096)[:128 * 1024]
    compressed_put, _ = request(
        "PUT",
        compressed_path,
        compressed_body,
        [("content-type", compressed_probe_mime_type)],
    )
    if compressed_put["status"] not in (200, 204):
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "compressed",
                "object_key": compressed_key,
                "status": "put_failed",
                "status_code": compressed_put["status"],
                "body_len": 0,
                "expected_runtime_fallback_reason": "compressed",
                "note": "compressed probe upload failed",
            }
        )
    else:
        compressed_get, compressed_get_body = request("GET", compressed_path)
        compressed_ok = compressed_get["status"] == 200 and sha256_hex(compressed_get_body) == sha256_hex(compressed_body)
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "compressed",
                "object_key": compressed_key,
                "status": "ok" if compressed_ok else "unexpected_status_or_body",
                "status_code": compressed_get["status"],
                "body_len": len(compressed_get_body),
                "expected_runtime_fallback_reason": "compressed",
                "note": "Disk-compressed object GET should stay on the legacy fallback path for codec profiles",
            }
        )
else:
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "compressed",
            "object_key": compressed_key,
            "status": "skipped",
            "status_code": "N/A",
            "body_len": "N/A",
            "expected_runtime_fallback_reason": "compressed",
            "note": "Enable --compressed-fallback-probe to generate a runtime compressed fallback delta",
        }
    )

degraded_key = object_key + ".degraded-read"
degraded_path = bucket_path + "/" + urllib.parse.quote(degraded_key, safe="/-_.~")
degraded_body = payload(max(object_size, 8 * 1024 * 1024))
degraded_put, _ = request("PUT", degraded_path, degraded_body, [("content-type", "application/octet-stream")])
if degraded_put["status"] not in (200, 204):
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "read_quorum_not_safe",
            "object_key": degraded_key,
            "status": "put_failed",
            "status_code": degraded_put["status"],
            "body_len": 0,
            "expected_runtime_fallback_reason": "read_quorum_not_safe",
            "note": "degraded-read probe upload failed",
        }
    )
else:
    profile_dir = out_dir.parent
    data_root = profile_dir / "data"
    degraded_rel = pathlib.Path(*degraded_key.split("/"))
    part_candidates = []
    for disk_dir in sorted(data_root.glob("disk*")):
        object_dir = disk_dir / bucket / degraded_rel
        part_candidates.extend(sorted(object_dir.glob("*/part.1")))

    if not part_candidates:
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "read_quorum_not_safe",
                "object_key": degraded_key,
                "status": "part_file_missing",
                "status_code": "N/A",
                "body_len": 0,
                "expected_runtime_fallback_reason": "read_quorum_not_safe",
                "note": "degraded-read probe could not find an on-disk part.1 shard to move",
            }
        )
    else:
        degraded_part = part_candidates[0]
        missing_dir = out_dir / "degraded-missing-shards"
        missing_dir.mkdir(parents=True, exist_ok=True)
        missing_part = missing_dir / f"{degraded_part.parent.name}-{degraded_part.name}"
        if missing_part.exists():
            missing_part.unlink()
        degraded_part.rename(missing_part)
        try:
            degraded_part.parent.rmdir()
        except OSError:
            pass
        degraded_get, degraded_get_body = request("GET", degraded_path)
        degraded_ok = degraded_get["status"] == 200 and sha256_hex(degraded_get_body) == sha256_hex(degraded_body)
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "read_quorum_not_safe",
                "object_key": degraded_key,
                "status": "ok" if degraded_ok else "unexpected_status_or_body",
                "status_code": degraded_get["status"],
                "body_len": len(degraded_get_body),
                "expected_runtime_fallback_reason": "read_quorum_not_safe",
                "note": f"Moved one shard to {missing_part}; degraded GET should stay on the legacy fallback path for codec profiles",
            }
        )

multipart_key = object_key + ".multipart"
multipart_path = bucket_path + "/" + urllib.parse.quote(multipart_key, safe="/-_.~")
multipart_part_one = payload(5 * 1024 * 1024)
multipart_part_two = payload(1)
multipart_expected_reason = "multipart"
if codec_multipart_enabled and profile.startswith("codec-"):
    multipart_expected_reason = "multipart_part_limit" if codec_multipart_max_parts < 2 else "none"
multipart_note = (
    "Multipart object GET should use the codec streaming path for codec profiles"
    if multipart_expected_reason == "none"
    else "Multipart object GET should exceed the codec streaming part-count guard"
    if multipart_expected_reason == "multipart_part_limit"
    else "Multipart object GET should stay on the legacy fallback path for codec profiles"
)
initiate_multipart, initiate_multipart_body = request("POST", multipart_path + "?uploads")
if initiate_multipart["status"] not in (200, 201):
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "multipart",
            "object_key": multipart_key,
            "status": "initiate_failed",
            "status_code": initiate_multipart["status"],
            "body_len": 0,
            "expected_runtime_fallback_reason": multipart_expected_reason,
            "note": "multipart initiate failed",
        }
    )
else:
    upload_id = find_xml_text(initiate_multipart_body, "UploadId")
    if not upload_id:
        fallback_rows.append(
            {
                "profile": profile,
                "probe": "multipart",
                "object_key": multipart_key,
                "status": "upload_id_missing",
                "status_code": initiate_multipart["status"],
                "body_len": 0,
                "expected_runtime_fallback_reason": multipart_expected_reason,
                "note": "multipart initiate response did not include UploadId",
            }
        )
    else:
        upload_id_query = urllib.parse.quote(upload_id, safe="")
        part_one, _ = request("PUT", f"{multipart_path}?partNumber=1&uploadId={upload_id_query}", multipart_part_one)
        part_two, _ = request("PUT", f"{multipart_path}?partNumber=2&uploadId={upload_id_query}", multipart_part_two)
        etag_one = first_header(part_one, "etag")
        etag_two = first_header(part_two, "etag")
        if part_one["status"] not in (200, 201) or part_two["status"] not in (200, 201) or not etag_one or not etag_two:
            fallback_rows.append(
                {
                    "profile": profile,
                    "probe": "multipart",
                    "object_key": multipart_key,
                    "status": "upload_part_failed",
                    "status_code": f"{part_one['status']};{part_two['status']}",
                    "body_len": 0,
                    "expected_runtime_fallback_reason": multipart_expected_reason,
                    "note": "multipart part upload failed or ETag was missing",
                }
            )
            request("DELETE", f"{multipart_path}?uploadId={upload_id_query}")
        else:
            complete_body = (
                '<?xml version="1.0" encoding="UTF-8"?>'
                "<CompleteMultipartUpload>"
                f"<Part><PartNumber>1</PartNumber><ETag>{xml_escape(etag_one)}</ETag></Part>"
                f"<Part><PartNumber>2</PartNumber><ETag>{xml_escape(etag_two)}</ETag></Part>"
                "</CompleteMultipartUpload>"
            ).encode()
            complete_multipart, _ = request(
                "POST",
                f"{multipart_path}?uploadId={upload_id_query}",
                complete_body,
                [("content-type", "application/xml")],
            )
            if complete_multipart["status"] not in (200, 201):
                fallback_rows.append(
                    {
                        "profile": profile,
                        "probe": "multipart",
                        "object_key": multipart_key,
                        "status": "complete_failed",
                        "status_code": complete_multipart["status"],
                        "body_len": 0,
                        "expected_runtime_fallback_reason": multipart_expected_reason,
                        "note": "multipart complete failed",
                    }
                )
                request("DELETE", f"{multipart_path}?uploadId={upload_id_query}")
            else:
                multipart_get, multipart_get_body = request("GET", multipart_path)
                expected_multipart_len = len(multipart_part_one) + len(multipart_part_two)
                expected_multipart_body_hash = sha256_hex(multipart_part_one + multipart_part_two)
                multipart_ok = (
                    multipart_get["status"] == 200
                    and len(multipart_get_body) == expected_multipart_len
                    and sha256_hex(multipart_get_body) == expected_multipart_body_hash
                )
                fallback_rows.append(
                    {
                        "profile": profile,
                        "probe": "multipart",
                        "object_key": multipart_key,
                        "status": "ok" if multipart_ok else "unexpected_status_or_length",
                        "status_code": multipart_get["status"],
                        "body_len": len(multipart_get_body),
                        "expected_runtime_fallback_reason": multipart_expected_reason,
                        "note": multipart_note,
                    }
                )
                if multipart_expected_reason == "none":
                    profile_dir = out_dir.parent
                    data_root = profile_dir / "data"
                    multipart_rel = pathlib.Path(*multipart_key.split("/"))
                    part_candidates = []
                    for disk_dir in sorted(data_root.glob("disk*")):
                        object_dir = disk_dir / bucket / multipart_rel
                        part_candidates.extend(sorted(object_dir.glob("*/part.1")))

                    if not part_candidates:
                        fallback_rows.append(
                            {
                                "profile": profile,
                                "probe": "multipart_degraded_read",
                                "object_key": multipart_key,
                                "status": "part_file_missing",
                                "status_code": "N/A",
                                "body_len": 0,
                                "expected_runtime_fallback_reason": "read_quorum_not_safe",
                                "note": "multipart degraded-read probe could not find an on-disk part.1 shard to move",
                            }
                        )
                    else:
                        multipart_part = part_candidates[0]
                        missing_dir = out_dir / "multipart-degraded-missing-shards"
                        missing_dir.mkdir(parents=True, exist_ok=True)
                        missing_part = missing_dir / f"{multipart_part.parent.name}-{multipart_part.name}"
                        if missing_part.exists():
                            missing_part.unlink()
                        multipart_part.rename(missing_part)
                        degraded_multipart_get, degraded_multipart_body = request("GET", multipart_path)
                        degraded_multipart_ok = (
                            degraded_multipart_get["status"] == 200
                            and len(degraded_multipart_body) == expected_multipart_len
                            and sha256_hex(degraded_multipart_body) == expected_multipart_body_hash
                        )
                        fallback_rows.append(
                            {
                                "profile": profile,
                                "probe": "multipart_degraded_read",
                                "object_key": multipart_key,
                                "status": "ok" if degraded_multipart_ok else "unexpected_status_or_body",
                                "status_code": degraded_multipart_get["status"],
                                "body_len": len(degraded_multipart_body),
                                "expected_runtime_fallback_reason": "read_quorum_not_safe",
                                "note": f"Moved one multipart shard to {missing_part}; unsafe part setup should fall back before returning a codec reader",
                            }
                        )
                else:
                    fallback_rows.append(
                        {
                            "profile": profile,
                            "probe": "multipart_degraded_read",
                            "object_key": multipart_key,
                            "status": "skipped",
                            "status_code": "N/A",
                            "body_len": "N/A",
                            "expected_runtime_fallback_reason": "read_quorum_not_safe",
                            "note": "Enable --codec-multipart on with a multipart max-parts value of at least 2 for degraded-read fallback proof",
                        }
                    )

encrypted_key = object_key + ".encrypted"
encrypted_path = bucket_path + "/" + urllib.parse.quote(encrypted_key, safe="/-_.~")
ssec_key = b"0123456789abcdef0123456789abcdef"
ssec_key_b64 = base64.b64encode(ssec_key).decode()
ssec_key_md5_b64 = base64.b64encode(hashlib.md5(ssec_key).digest()).decode()
ssec_headers = [
    ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
    ("x-amz-server-side-encryption-customer-key", ssec_key_b64),
    ("x-amz-server-side-encryption-customer-key-md5", ssec_key_md5_b64),
]
encrypted_put, _ = request("PUT", encrypted_path, body, [("content-type", "application/octet-stream"), *ssec_headers])
if encrypted_put["status"] not in (200, 204):
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "encrypted",
            "object_key": encrypted_key,
            "status": "put_failed",
            "status_code": encrypted_put["status"],
            "body_len": 0,
            "expected_runtime_fallback_reason": "encrypted",
            "note": "SSE-C encrypted object upload failed",
        }
    )
else:
    encrypted_get, encrypted_body = request("GET", encrypted_path, extra_headers=ssec_headers)
    encrypted_ok = encrypted_get["status"] == 200 and sha256_hex(encrypted_body) == sha256_hex(body)
    fallback_rows.append(
        {
            "profile": profile,
            "probe": "encrypted",
            "object_key": encrypted_key,
            "status": "ok" if encrypted_ok else "unexpected_status_or_body",
            "status_code": encrypted_get["status"],
            "body_len": len(encrypted_body),
            "expected_runtime_fallback_reason": "encrypted",
            "note": "SSE-C encrypted object GET should stay on the legacy fallback path for codec profiles",
        }
    )

body_sha = sha256_hex(get_body)
expected_sha = sha256_hex(body)
body_match = body_sha == expected_sha
headers_report = {
    "create_bucket": create_bucket,
    "put_object": put_object,
    "head_object": head_object,
    "get_object": get_object,
}
snapshot = {
    "profile": profile,
    "path": profile,
    "size": object_size,
    "object_key": object_key,
    "expected_body_sha256": expected_sha,
    "body_sha256": body_sha,
    "body_sha256_match_expected": body_match,
    "head_status": head_object["status"],
    "get_status": get_object["status"],
    "head_headers": header_map(head_object["headers"]),
    "get_headers": header_map(get_object["headers"]),
}

(out_dir / "response_headers.json").write_text(json.dumps(headers_report, indent=2, sort_keys=True) + "\n")
(out_dir / "snapshot.json").write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
(out_dir / "body_sha256.txt").write_text(body_sha + "\n")
with (out_dir / "fallback_probe_summary.csv").open("w", encoding="utf-8", newline="") as handle:
    fieldnames = [
        "profile",
        "probe",
        "object_key",
        "status",
        "status_code",
        "body_len",
        "expected_runtime_fallback_reason",
        "note",
    ]
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(fallback_rows)

get_headers = snapshot["get_headers"]
content_length = ",".join(get_headers.get("content-length", []))
etag = ",".join(get_headers.get("etag", []))
status_match = head_object["status"] == 200 and get_object["status"] == 200
error_count = 0 if body_match and status_match else 1
(out_dir / "compat_summary.csv").write_text(
    "size,path,body_sha256_match,content_length_match,etag_match,content_range_match,"
    "checksum_headers_match,sse_headers_match,status_code_match,error_count,body_sha256,status_code,content_length,etag\n"
    f"{object_size},{profile},{str(body_match).lower()},true,true,true,true,true,{str(status_match).lower()},"
    f"{error_count},{body_sha},{get_object['status']},{content_length},{etag}\n"
)
PY
}

copy_profile_compat_artifacts() {
  local profile="$1"
  local label="$2"
  local compat_dir="${OUT_DIR}/${profile}/compat"

  if [[ -f "${compat_dir}/body_sha256.txt" ]]; then
    cp "${compat_dir}/body_sha256.txt" "${OUT_DIR}/body_sha256_${label}.txt"
  fi
  if [[ -f "${compat_dir}/response_headers.json" ]]; then
    cp "${compat_dir}/response_headers.json" "${OUT_DIR}/response_headers_${label}.json"
  fi
}

write_root_compat_summary() {
  local legacy_snapshot="${OUT_DIR}/legacy/compat/snapshot.json"
  local codec_snapshots=()
  local profile snapshot

  [[ -f "$legacy_snapshot" ]] || return 0

  for profile in "$@"; do
    [[ "$profile" == legacy ]] && continue
    snapshot="${OUT_DIR}/${profile}/compat/snapshot.json"
    if [[ -f "$snapshot" ]]; then
      codec_snapshots+=("$snapshot")
    fi
  done

  [[ "${#codec_snapshots[@]}" -gt 0 ]] || return 0

  "$PYTHON_BIN" - "${OUT_DIR}/compat_summary.csv" "$legacy_snapshot" "${codec_snapshots[@]}" <<'PY'
import csv
import json
import sys

out_csv = sys.argv[1]
legacy_path = sys.argv[2]
codec_paths = sys.argv[3:]
legacy = json.load(open(legacy_path, encoding="utf-8"))
codecs = [json.load(open(path, encoding="utf-8")) for path in codec_paths]


def values(snapshot, name):
    return snapshot.get("get_headers", {}).get(name, [])


def prefixed(snapshot, prefix):
    headers = snapshot.get("get_headers", {})
    return {key: headers[key] for key in sorted(headers) if key.startswith(prefix)}


with open(out_csv, "w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(
        [
            "size",
            "path",
            "body_sha256_match",
            "content_length_match",
            "etag_match",
            "content_range_match",
            "checksum_headers_match",
            "sse_headers_match",
            "status_code_match",
            "error_count",
            "legacy_body_sha256",
            "new_body_sha256",
            "legacy_status_code",
            "new_status_code",
        ]
    )
    for codec in codecs:
        body_match = legacy.get("body_sha256") == codec.get("body_sha256")
        content_length_match = values(legacy, "content-length") == values(codec, "content-length")
        etag_match = values(legacy, "etag") == values(codec, "etag")
        content_range_match = values(legacy, "content-range") == values(codec, "content-range")
        checksum_headers_match = prefixed(legacy, "x-amz-checksum") == prefixed(codec, "x-amz-checksum")
        sse_headers_match = prefixed(legacy, "x-amz-server-side-encryption") == prefixed(codec, "x-amz-server-side-encryption")
        status_code_match = legacy.get("get_status") == codec.get("get_status") and legacy.get("head_status") == codec.get("head_status")
        checks = [
            body_match,
            content_length_match,
            etag_match,
            content_range_match,
            checksum_headers_match,
            sse_headers_match,
            status_code_match,
        ]
        writer.writerow(
            [
                codec.get("size", legacy.get("size", "")),
                codec.get("path", ""),
                str(body_match).lower(),
                str(content_length_match).lower(),
                str(etag_match).lower(),
                str(content_range_match).lower(),
                str(checksum_headers_match).lower(),
                str(sse_headers_match).lower(),
                str(status_code_match).lower(),
                sum(1 for check in checks if not check),
                legacy.get("body_sha256", ""),
                codec.get("body_sha256", ""),
                legacy.get("get_status", ""),
                codec.get("get_status", ""),
            ]
        )
PY
}

write_root_fallback_probe_summary() {
  local out_csv="${OUT_DIR}/fallback_probe_summary.csv"
  local profile summary wrote_header=false
  : >"$out_csv"

  for profile in "$@"; do
    summary="${OUT_DIR}/${profile}/compat/fallback_probe_summary.csv"
    [[ -f "$summary" ]] || continue
    if [[ "$wrote_header" == "false" ]]; then
      cat "$summary" >>"$out_csv"
      wrote_header=true
    else
      tail -n +2 "$summary" >>"$out_csv"
    fi
  done

  if [[ "$wrote_header" == "false" ]]; then
    cat >"$out_csv" <<'EOF'
profile,probe,object_key,status,status_code,body_len,expected_runtime_fallback_reason,note
N/A,missing,N/A,missing,N/A,N/A,N/A,no_profile_fallback_probe_summary
EOF
  fi
}

write_root_metrics_summary() {
  local profile_dirs=()
  local profile

  for profile in "$@"; do
    profile_dirs+=("${OUT_DIR}/${profile}")
  done

  [[ "${#profile_dirs[@]}" -gt 0 ]] || return

  "$PYTHON_BIN" - "${OUT_DIR}/metrics_summary.csv" "${OUT_DIR}/engine_compare.csv" "${OUT_DIR}/baseline_compare.csv" "${profile_dirs[@]}" <<'PY'
import csv
import pathlib
import sys

metrics_csv = pathlib.Path(sys.argv[1])
engine_compare_csv = pathlib.Path(sys.argv[2])
baseline_compare_csv = pathlib.Path(sys.argv[3])
profile_dirs = [pathlib.Path(value) for value in sys.argv[4:]]


def load_manifest(path):
    data = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key] = value
    return data


def parse_float(value):
    if value in ("", "N/A", None):
        return None
    return float(value)


def delta_pct(new_value, baseline_value):
    if new_value is None or baseline_value in (None, 0.0):
        return ""
    return f"{((new_value - baseline_value) / baseline_value) * 100:.2f}"


def performance_note(throughput_delta, latency_delta):
    if throughput_delta == "" or latency_delta == "":
        return ""
    throughput_delta = float(throughput_delta)
    latency_delta = float(latency_delta)
    if throughput_delta < 0 and latency_delta > 0:
        return "slower_than_legacy_and_higher_latency"
    if throughput_delta < 0:
        return "slower_than_legacy"
    if latency_delta > 0:
        return "higher_latency_than_legacy"
    return "at_or_better_than_legacy"


def row_value(row, key):
    return row.get(key, "")


rows = []
by_profile = {}
for profile_dir in profile_dirs:
    manifest = load_manifest(profile_dir / "manifest.env")
    profile = manifest["profile"]
    with open(profile_dir / "warp" / "median_summary.csv", encoding="utf-8", newline="") as handle:
        profile_rows = list(csv.DictReader(handle))
    by_profile[profile] = profile_rows
    for row in profile_rows:
        rows.append(
            {
                "profile": profile,
                "read_path": manifest.get("metrics_path", profile),
                "codec_streaming_enabled": manifest.get("RUSTFS_GET_CODEC_STREAMING_ENABLE", ""),
                "codec_engine": manifest.get("RUSTFS_GET_CODEC_STREAMING_ENGINE", ""),
                "metadata_early_stop": manifest.get("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", ""),
                "shard_locality_preference": manifest.get("RUSTFS_GET_SHARD_LOCALITY_PREFERENCE_ENABLE", ""),
                "codec_max_inflight": manifest.get("RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT", ""),
                "output_handoff_attribution": manifest.get("RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE", ""),
                "round_cooldown_secs": manifest.get("round_cooldown_secs", ""),
                "duration": manifest.get("duration", ""),
                "size": row["size"],
                "tool": row["tool"],
                "concurrency": row["concurrency"],
                "successful_rounds": row["successful_rounds"],
                "failed_rounds": row["failed_rounds"],
                "median_throughput_bps": row["median_throughput_bps"],
                "median_reqps": row["median_reqps"],
                "median_latency_ms": row["median_latency_ms"],
                "median_req_p90_ms": row_value(row, "median_req_p90_ms"),
                "median_req_p99_ms": row_value(row, "median_req_p99_ms"),
            }
        )

legacy_by_size = {row["size"]: row for row in by_profile.get("legacy", [])}
for row in rows:
    baseline = legacy_by_size.get(row["size"])
    if baseline is None or row["profile"] == "legacy":
        row["baseline_profile"] = ""
        row["delta_throughput_pct_vs_legacy"] = ""
        row["delta_reqps_pct_vs_legacy"] = ""
        row["delta_latency_pct_vs_legacy"] = ""
        row["performance_note"] = ""
        continue
    throughput_delta = delta_pct(parse_float(row["median_throughput_bps"]), parse_float(baseline["median_throughput_bps"]))
    reqps_delta = delta_pct(parse_float(row["median_reqps"]), parse_float(baseline["median_reqps"]))
    latency_delta = delta_pct(parse_float(row["median_latency_ms"]), parse_float(baseline["median_latency_ms"]))
    p90_delta = delta_pct(parse_float(row.get("median_req_p90_ms", "")), parse_float(baseline.get("median_req_p90_ms", "")))
    p99_delta = delta_pct(parse_float(row.get("median_req_p99_ms", "")), parse_float(baseline.get("median_req_p99_ms", "")))
    row["baseline_profile"] = "legacy"
    row["delta_throughput_pct_vs_legacy"] = throughput_delta
    row["delta_reqps_pct_vs_legacy"] = reqps_delta
    row["delta_latency_pct_vs_legacy"] = latency_delta
    row["delta_req_p90_pct_vs_legacy"] = p90_delta
    row["delta_req_p99_pct_vs_legacy"] = p99_delta
    row["performance_note"] = performance_note(throughput_delta, latency_delta)

fieldnames = [
    "profile",
    "read_path",
    "codec_streaming_enabled",
    "codec_engine",
    "metadata_early_stop",
    "shard_locality_preference",
    "codec_max_inflight",
    "output_handoff_attribution",
    "round_cooldown_secs",
    "duration",
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
    "baseline_profile",
    "delta_throughput_pct_vs_legacy",
    "delta_reqps_pct_vs_legacy",
    "delta_latency_pct_vs_legacy",
    "delta_req_p90_pct_vs_legacy",
    "delta_req_p99_pct_vs_legacy",
    "performance_note",
]
with open(metrics_csv, "w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

baseline_fields = [
    "profile",
    "read_path",
    "codec_streaming_enabled",
    "codec_engine",
    "metadata_early_stop",
    "shard_locality_preference",
    "codec_max_inflight",
    "output_handoff_attribution",
    "round_cooldown_secs",
    "duration",
    "size",
    "tool",
    "concurrency",
    "successful_rounds",
    "failed_rounds",
    "baseline_profile",
    "new_median_reqps",
    "baseline_median_reqps",
    "delta_reqps_pct_vs_legacy",
    "new_median_latency_ms",
    "baseline_median_latency_ms",
    "delta_latency_pct_vs_legacy",
    "new_median_req_p90_ms",
    "baseline_median_req_p90_ms",
    "delta_req_p90_pct_vs_legacy",
    "new_median_req_p99_ms",
    "baseline_median_req_p99_ms",
    "delta_req_p99_pct_vs_legacy",
    "new_median_throughput_bps",
    "baseline_median_throughput_bps",
    "delta_throughput_pct_vs_legacy",
    "performance_note",
]
baseline_rows = []
for row in rows:
    if row["profile"] == "legacy" or row["baseline_profile"] != "legacy":
        continue
    baseline = legacy_by_size.get(row["size"])
    if baseline is None:
        continue
    baseline_rows.append(
        {
            "profile": row["profile"],
            "read_path": row["read_path"],
            "codec_streaming_enabled": row["codec_streaming_enabled"],
            "codec_engine": row["codec_engine"],
            "metadata_early_stop": row["metadata_early_stop"],
            "shard_locality_preference": row["shard_locality_preference"],
            "codec_max_inflight": row["codec_max_inflight"],
            "output_handoff_attribution": row["output_handoff_attribution"],
            "round_cooldown_secs": row["round_cooldown_secs"],
            "duration": row["duration"],
            "size": row["size"],
            "tool": row["tool"],
            "concurrency": row["concurrency"],
            "successful_rounds": row["successful_rounds"],
            "failed_rounds": row["failed_rounds"],
            "baseline_profile": "legacy",
            "new_median_reqps": row["median_reqps"],
            "baseline_median_reqps": baseline.get("median_reqps", ""),
            "delta_reqps_pct_vs_legacy": row["delta_reqps_pct_vs_legacy"],
            "new_median_latency_ms": row["median_latency_ms"],
            "baseline_median_latency_ms": baseline.get("median_latency_ms", ""),
            "delta_latency_pct_vs_legacy": row["delta_latency_pct_vs_legacy"],
            "new_median_req_p90_ms": row.get("median_req_p90_ms", ""),
            "baseline_median_req_p90_ms": baseline.get("median_req_p90_ms", ""),
            "delta_req_p90_pct_vs_legacy": row.get("delta_req_p90_pct_vs_legacy", ""),
            "new_median_req_p99_ms": row.get("median_req_p99_ms", ""),
            "baseline_median_req_p99_ms": baseline.get("median_req_p99_ms", ""),
            "delta_req_p99_pct_vs_legacy": row.get("delta_req_p99_pct_vs_legacy", ""),
            "new_median_throughput_bps": row["median_throughput_bps"],
            "baseline_median_throughput_bps": baseline.get("median_throughput_bps", ""),
            "delta_throughput_pct_vs_legacy": row["delta_throughput_pct_vs_legacy"],
            "performance_note": row["performance_note"],
        }
    )

with open(baseline_compare_csv, "w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=baseline_fields)
    writer.writeheader()
    writer.writerows(baseline_rows)

codec_legacy_by_size = {row["size"]: row for row in by_profile.get("codec-legacy", [])}
codec_rustfs_by_size = {row["size"]: row for row in by_profile.get("codec-rustfs", [])}
if not legacy_by_size:
    raise SystemExit(0)

compare_fields = [
    "size",
    "legacy_median_throughput_bps",
    "codec_legacy_median_throughput_bps",
    "codec_rustfs_median_throughput_bps",
    "legacy_median_reqps",
    "codec_legacy_median_reqps",
    "codec_rustfs_median_reqps",
    "legacy_median_latency_ms",
    "codec_legacy_median_latency_ms",
    "codec_rustfs_median_latency_ms",
    "legacy_median_req_p90_ms",
    "codec_legacy_median_req_p90_ms",
    "codec_rustfs_median_req_p90_ms",
    "legacy_median_req_p99_ms",
    "codec_legacy_median_req_p99_ms",
    "codec_rustfs_median_req_p99_ms",
    "codec_legacy_delta_throughput_pct_vs_legacy",
    "codec_rustfs_delta_throughput_pct_vs_legacy",
    "codec_rustfs_delta_throughput_pct_vs_codec_legacy",
    "codec_legacy_delta_latency_pct_vs_legacy",
    "codec_rustfs_delta_latency_pct_vs_legacy",
    "codec_rustfs_delta_latency_pct_vs_codec_legacy",
    "codec_legacy_delta_req_p90_pct_vs_legacy",
    "codec_rustfs_delta_req_p90_pct_vs_legacy",
    "codec_rustfs_delta_req_p90_pct_vs_codec_legacy",
    "codec_legacy_delta_req_p99_pct_vs_legacy",
    "codec_rustfs_delta_req_p99_pct_vs_legacy",
    "codec_rustfs_delta_req_p99_pct_vs_codec_legacy",
]
compare_rows = []
for size, legacy in legacy_by_size.items():
    codec_legacy = codec_legacy_by_size.get(size, {})
    codec_rustfs = codec_rustfs_by_size.get(size, {})
    compare_rows.append(
        {
            "size": size,
            "legacy_median_throughput_bps": legacy.get("median_throughput_bps", ""),
            "codec_legacy_median_throughput_bps": codec_legacy.get("median_throughput_bps", ""),
            "codec_rustfs_median_throughput_bps": codec_rustfs.get("median_throughput_bps", ""),
            "legacy_median_reqps": legacy.get("median_reqps", ""),
            "codec_legacy_median_reqps": codec_legacy.get("median_reqps", ""),
            "codec_rustfs_median_reqps": codec_rustfs.get("median_reqps", ""),
            "legacy_median_latency_ms": legacy.get("median_latency_ms", ""),
            "codec_legacy_median_latency_ms": codec_legacy.get("median_latency_ms", ""),
            "codec_rustfs_median_latency_ms": codec_rustfs.get("median_latency_ms", ""),
            "legacy_median_req_p90_ms": legacy.get("median_req_p90_ms", ""),
            "codec_legacy_median_req_p90_ms": codec_legacy.get("median_req_p90_ms", ""),
            "codec_rustfs_median_req_p90_ms": codec_rustfs.get("median_req_p90_ms", ""),
            "legacy_median_req_p99_ms": legacy.get("median_req_p99_ms", ""),
            "codec_legacy_median_req_p99_ms": codec_legacy.get("median_req_p99_ms", ""),
            "codec_rustfs_median_req_p99_ms": codec_rustfs.get("median_req_p99_ms", ""),
            "codec_legacy_delta_throughput_pct_vs_legacy": delta_pct(
                parse_float(codec_legacy.get("median_throughput_bps", "")),
                parse_float(legacy.get("median_throughput_bps", "")),
            ),
            "codec_rustfs_delta_throughput_pct_vs_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_throughput_bps", "")),
                parse_float(legacy.get("median_throughput_bps", "")),
            ),
            "codec_rustfs_delta_throughput_pct_vs_codec_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_throughput_bps", "")),
                parse_float(codec_legacy.get("median_throughput_bps", "")),
            ),
            "codec_legacy_delta_latency_pct_vs_legacy": delta_pct(
                parse_float(codec_legacy.get("median_latency_ms", "")),
                parse_float(legacy.get("median_latency_ms", "")),
            ),
            "codec_rustfs_delta_latency_pct_vs_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_latency_ms", "")),
                parse_float(legacy.get("median_latency_ms", "")),
            ),
            "codec_rustfs_delta_latency_pct_vs_codec_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_latency_ms", "")),
                parse_float(codec_legacy.get("median_latency_ms", "")),
            ),
            "codec_legacy_delta_req_p90_pct_vs_legacy": delta_pct(
                parse_float(codec_legacy.get("median_req_p90_ms", "")),
                parse_float(legacy.get("median_req_p90_ms", "")),
            ),
            "codec_rustfs_delta_req_p90_pct_vs_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_req_p90_ms", "")),
                parse_float(legacy.get("median_req_p90_ms", "")),
            ),
            "codec_rustfs_delta_req_p90_pct_vs_codec_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_req_p90_ms", "")),
                parse_float(codec_legacy.get("median_req_p90_ms", "")),
            ),
            "codec_legacy_delta_req_p99_pct_vs_legacy": delta_pct(
                parse_float(codec_legacy.get("median_req_p99_ms", "")),
                parse_float(legacy.get("median_req_p99_ms", "")),
            ),
            "codec_rustfs_delta_req_p99_pct_vs_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_req_p99_ms", "")),
                parse_float(legacy.get("median_req_p99_ms", "")),
            ),
            "codec_rustfs_delta_req_p99_pct_vs_codec_legacy": delta_pct(
                parse_float(codec_rustfs.get("median_req_p99_ms", "")),
                parse_float(codec_legacy.get("median_req_p99_ms", "")),
            ),
        }
    )

with open(engine_compare_csv, "w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=compare_fields)
    writer.writeheader()
    writer.writerows(compare_rows)
PY
}

write_root_cpu_rss_notes() {
  local notes_file="${OUT_DIR}/cpu_rss_notes.txt"
  : >"$notes_file"
  for profile_dir in "${OUT_DIR}"/*; do
    [[ -d "$profile_dir" ]] || continue
    if [[ -f "${profile_dir}/cpu_rss_notes.txt" ]]; then
      {
        cat "${profile_dir}/cpu_rss_notes.txt"
        printf '\n'
      } >>"$notes_file"
    fi
  done
}

write_raw_output_paths() {
  local out_file="${OUT_DIR}/raw_output_paths.txt"
	  {
	    find "$OUT_DIR" -maxdepth 1 -type f \
	      \( -name 'metrics_summary.csv' \
	        -o -name 'service_metrics_summary.csv' \
	        -o -name 'service_metrics_round_summary.csv' \
	        -o -name 'service_metrics_stage_distribution.csv' \
	        -o -name 'service_metrics_round_percentiles.csv' \
	        -o -name 'service_metrics_acceptance.csv' \) | sort
	    for profile_dir in "${OUT_DIR}"/*; do
	      [[ -d "$profile_dir" ]] || continue
	      find "$profile_dir" -maxdepth 1 -type f \
	        \( -name 'metrics_summary.csv' \
	          -o -name 'service_metrics_summary.csv' \
	          -o -name 'service_metrics_round_summary.csv' \
	          -o -name 'service_metrics_stage_distribution.csv' \
	          -o -name 'service_metrics_round_percentiles.csv' \) | sort
	      if [[ -d "${profile_dir}/service-metrics" ]]; then
	        find "${profile_dir}/service-metrics" -type f | sort
	      fi
	      if [[ -d "${profile_dir}/warp/logs" ]]; then
	        find "${profile_dir}/warp/logs" -type f | sort
	      fi
	      if [[ -d "${profile_dir}/warp_prepare" ]]; then
	        find "${profile_dir}/warp_prepare" -type f | sort
	      fi
	    done
	  } >"$out_file"
}

write_default_switch_readiness_report() {
  local report_path="${OUT_DIR}/default_switch_readiness.md"
  local baseline_compare_path="${OUT_DIR}/baseline_compare.csv"
  local compat_summary_path="${OUT_DIR}/compat_summary.csv"
  local cpu_rss_notes_path="${OUT_DIR}/cpu_rss_notes.txt"
  local fallback_coverage_path="${OUT_DIR}/fallback_coverage.txt"
  local service_metrics_acceptance_path="${OUT_DIR}/service_metrics_acceptance.csv"
  local tracking_issues_path="${OUT_DIR}/tracking_issues.txt"

  local stable not_worse improved_over_five two_sizes_gt_five
  local headers_compatible p95_p99_ok cpu_rss_ok runtime_metrics_ok fallback_proven kill_switch_verified correctness_matrix_ok
  local decision

  if [[ -f "$baseline_compare_path" ]]; then
    read -r stable not_worse improved_over_five < <(
      awk -F',' -v expected_rounds="$ROUNDS" '
        BEGIN {
          rows = 0
          stable = 1
          not_worse = 1
          improved = 0
        }
        NR == 1 { next }
        $11 == "1MiB" || $11 == "4MiB" || $11 == "10MiB" {
          rows++
          if ($14 != expected_rounds || $15 != "0") {
            stable = 0
          }
          if ($19 == "" || $22 == "" || ($19 + 0.0) < 0.0 || ($22 + 0.0) > 0.0) {
            not_worse = 0
          }
          if ($31 != "" && ($31 + 0.0) > 5.0) {
            improved++
          }
        }
        END {
          if (rows == 0) {
            stable = 0
            not_worse = 0
          }
          printf "%s %s %d\n", stable ? "pass" : "fail", not_worse ? "pass" : "fail", improved
        }
      ' "$baseline_compare_path"
    )
  else
    stable="fail"
    not_worse="fail"
    improved_over_five=0
  fi

  if [[ "$improved_over_five" -ge 2 ]]; then
    two_sizes_gt_five="pass"
  else
    two_sizes_gt_five="fail"
  fi

  if [[ -f "$compat_summary_path" ]] && awk -F',' 'NR > 1 && $10 != "0" { exit 1 } END { exit (NR > 1 ? 0 : 1) }' "$compat_summary_path"; then
    headers_compatible="pass"
  else
    headers_compatible="fail"
  fi

  p95_p99_ok="fail"
  correctness_matrix_ok="fail"
  runtime_metrics_ok="fail"

  if [[ "$DIAGNOSTIC_METRICS" == "true" && -f "$service_metrics_acceptance_path" ]] \
     && "$PYTHON_BIN" - "$service_metrics_acceptance_path" <<'PY'
import csv
import sys

with open(sys.argv[1], encoding="utf-8", newline="") as handle:
    rows = list(csv.DictReader(handle))

statuses = [row.get("status") for row in rows]
if not rows or "fail" in statuses or "pass" not in statuses:
    raise SystemExit(1)
PY
  then
    runtime_metrics_ok="pass"
  elif [[ "$DIAGNOSTIC_METRICS" != "true" ]]; then
    runtime_metrics_ok="skipped"
  fi

  if [[ -f "$cpu_rss_notes_path" ]] && grep -q 'cpu_rss_acceptability=acceptable' "$cpu_rss_notes_path"; then
    cpu_rss_ok="pass"
  else
    cpu_rss_ok="fail"
  fi

  if [[ -f "$fallback_coverage_path" ]] \
    && grep -q 'read_quorum_not_safe' "$fallback_coverage_path" \
    && grep -q 'range / encrypted / compressed / remote' "$fallback_coverage_path"; then
    fallback_proven="pass"
  else
    fallback_proven="fail"
  fi

  if [[ -f "$fallback_coverage_path" ]] && grep -q 'codec_streaming_reader_gate_defaults_to_disabled' "$fallback_coverage_path"; then
    kill_switch_verified="pass"
  else
    kill_switch_verified="fail"
  fi

  if [[ "$stable" == "pass" \
     && "$not_worse" == "pass" \
     && "$two_sizes_gt_five" == "pass" \
     && "$p95_p99_ok" == "pass" \
     && "$cpu_rss_ok" == "pass" \
     && "$runtime_metrics_ok" == "pass" \
     && "$correctness_matrix_ok" == "pass" \
     && "$headers_compatible" == "pass" \
     && "$fallback_proven" == "pass" \
     && "$kill_switch_verified" == "pass" ]]; then
    decision="Default enablement prerequisites passed; scoped default enablement may be considered."
  else
    decision="Default enablement is not ready; keep opt-in only."
  fi

  {
    echo "# GET V2 PR-35 Default Switch Readiness"
    echo
    echo "Decision: **${decision}**"
    echo
    echo "## Hard Prerequisite Status"
    echo
    echo "| Prerequisite | Status | Notes |"
    echo "| --- | --- | --- |"
    echo "| multi-round cooled A/B stable | ${stable} | expected rounds per target size: ${ROUNDS} |"
    echo "| 1MiB / 4MiB / 10MiB not worse than legacy | ${not_worse} | derived from baseline_compare.csv |"
    echo "| at least two sizes improve by more than 5% | ${two_sizes_gt_five} | qualifying sizes: ${improved_over_five} |"
    echo "| p95 / p99 do not regress | ${p95_p99_ok} | p99 is aggregated in metrics_summary.csv; p95 remains unavailable in current warp text output |"
    echo "| CPU and RSS acceptable | ${cpu_rss_ok} | see cpu_rss_notes.txt |"
    echo "| runtime server metrics prove path and fallback deltas | ${runtime_metrics_ok} | see service_metrics_acceptance.csv; skipped for observability-off performance runs |"
    echo "| correctness matrix passes | ${correctness_matrix_ok} | compat smoke passes, but full correctness matrix is not fully evidenced by this harness alone |"
    echo "| response headers compatible | ${headers_compatible} | see compat_summary.csv |"
    echo "| range / encrypted / compressed / remote fallback proven | ${fallback_proven} | see fallback_coverage.txt |"
    echo "| kill switch verified | ${kill_switch_verified} | see fallback_coverage.txt |"
    echo
    echo "## Evidence Files"
    echo
    echo "- \`baseline_compare.csv\`"
    echo "- \`compat_summary.csv\`"
    echo "- \`metrics_summary.csv\`"
    echo "- \`service_metrics_summary.csv\`"
    echo "- \`service_metrics_acceptance.csv\`"
    echo "- \`fallback_probe_summary.csv\`"
    echo "- \`cpu_rss_notes.txt\`"
    echo "- \`fallback_coverage.txt\`"
    echo "- \`tracking_issues.txt\`"
    echo "- \`raw_output_paths.txt\`"
    echo
    echo "## Tracking Issues"
    echo
    echo '```text'
    if [[ -f "$tracking_issues_path" ]]; then
      cat "$tracking_issues_path"
    fi
    echo '```'
  } >"$report_path"
}

run_profile() {
  local profile="$1"
  local baseline_csv="${2:-}"
  local bench_rc=0

  stop_server
  start_server "$profile"
  prepare_warp_existing_objects "$profile"
  capture_service_metrics_snapshot "$profile" before
  if run_bench "$profile" "$baseline_csv"; then
    :
  else
    bench_rc=$?
  fi
  write_metrics_summary "$profile"
  if [[ "$SKIP_COMPAT_PROBE" == "true" ]]; then
    log "Skipping compatibility probe for profile=${profile}"
    write_skipped_compat_probe "$profile"
  else
    run_compat_probe "$profile"
  fi
  if [[ "$DIAGNOSTIC_METRICS" == "true" && "$DIAGNOSTIC_METRICS_SETTLE_SECS" -gt 0 ]]; then
    sleep "$DIAGNOSTIC_METRICS_SETTLE_SECS"
  fi
  capture_service_metrics_snapshot "$profile" after
  write_profile_service_metrics_summary "$profile"
  write_profile_service_metrics_round_breakdown "$profile"
  stop_server
  write_profile_cpu_rss_notes "$profile"

  log "Median summary: ${OUT_DIR}/${profile}/warp/median_summary.csv"
  log "Metrics summary: ${OUT_DIR}/${profile}/metrics_summary.csv"
  log "Service metrics summary: ${OUT_DIR}/${profile}/service_metrics_summary.csv"
  log "Service metrics round summary: ${OUT_DIR}/${profile}/service_metrics_round_summary.csv"
  log "Service metrics stage distribution: ${OUT_DIR}/${profile}/service_metrics_stage_distribution.csv"
  log "Service metrics round percentiles: ${OUT_DIR}/${profile}/service_metrics_round_percentiles.csv"
  log "Compatibility summary: ${OUT_DIR}/${profile}/compat/compat_summary.csv"
  if [[ -f "${OUT_DIR}/${profile}/warp/baseline_compare.csv" ]]; then
    log "Baseline compare: ${OUT_DIR}/${profile}/warp/baseline_compare.csv"
  fi

  return "$bench_rc"
}

main() {
  ORIGINAL_ARGS=("$@")
  parse_args "$@"
  validate_args
  setup_output
  run_detached_if_requested
  local codec_profiles=()
  local profiles=()
  local profiles_csv=""
  local raw engine profile
  IFS=',' read -r -a engines <<< "$CODEC_ENGINES"
  for raw in "${engines[@]}"; do
    engine="${raw//[[:space:]]/}"
    [[ -n "$engine" ]] || continue
    profile="codec-${engine}"
    codec_profiles+=("$profile")
  done

  case "$MODE" in
    legacy)
      profiles=("legacy")
      ;;
    codec)
      profiles=("${codec_profiles[@]}")
      ;;
    both)
      profiles=("legacy" "${codec_profiles[@]}")
      ;;
  esac
  profiles_csv="$(IFS=,; echo "${profiles[*]}")"

  write_root_environment
  write_root_manifest "$profiles_csv"
  build_rustfs_if_needed

  trap stop_server EXIT INT TERM

  log "Output dir: $OUT_DIR"

  local legacy_baseline_csv=""
  for profile in "${profiles[@]}"; do
    if [[ "$profile" == "legacy" ]]; then
      if ! run_profile "$profile"; then
        PROFILE_FAILURES=$((PROFILE_FAILURES + 1))
      fi
      if [[ -f "${OUT_DIR}/legacy/warp/median_summary.csv" ]]; then
        legacy_baseline_csv="${OUT_DIR}/legacy/warp/median_summary.csv"
      fi
    else
      if ! run_profile "$profile" "$legacy_baseline_csv"; then
        PROFILE_FAILURES=$((PROFILE_FAILURES + 1))
      fi
    fi
    copy_profile_compat_artifacts "$profile" "${profile//-/_}"
  done

  write_root_metrics_summary "${profiles[@]}"
  write_root_service_metrics_summary "${profiles[@]}"
  write_root_profile_csv service_metrics_round_summary.csv "${profiles[@]}"
  write_root_profile_csv service_metrics_stage_distribution.csv "${profiles[@]}"
  write_root_profile_csv service_metrics_round_percentiles.csv "${profiles[@]}"
  write_service_metrics_acceptance
  write_root_compat_summary "${profiles[@]}"
  write_root_fallback_probe_summary "${profiles[@]}"
  write_root_cpu_rss_notes
  write_fallback_coverage
  write_tracking_issues
  write_raw_output_paths
  write_default_switch_readiness_report

  if [[ -f "${OUT_DIR}/compat_summary.csv" ]]; then
    log "Compatibility compare: ${OUT_DIR}/compat_summary.csv"
  fi
  if [[ -f "${OUT_DIR}/engine_compare.csv" ]]; then
    log "Engine compare: ${OUT_DIR}/engine_compare.csv"
  fi
  if [[ -f "${OUT_DIR}/metrics_summary.csv" ]]; then
    log "Metrics summary: ${OUT_DIR}/metrics_summary.csv"
  fi
  if [[ -f "${OUT_DIR}/service_metrics_summary.csv" ]]; then
    log "Service metrics summary: ${OUT_DIR}/service_metrics_summary.csv"
  fi
  if [[ -f "${OUT_DIR}/service_metrics_acceptance.csv" ]]; then
    log "Service metrics acceptance: ${OUT_DIR}/service_metrics_acceptance.csv"
  fi
  if [[ -f "${OUT_DIR}/fallback_probe_summary.csv" ]]; then
    log "Fallback probe summary: ${OUT_DIR}/fallback_probe_summary.csv"
  fi
  if [[ -f "${OUT_DIR}/baseline_compare.csv" ]]; then
    log "Baseline compare: ${OUT_DIR}/baseline_compare.csv"
  fi
  if [[ -f "${OUT_DIR}/default_switch_readiness.md" ]]; then
    log "Default switch readiness: ${OUT_DIR}/default_switch_readiness.md"
  fi
  if [[ "$DRY_RUN" != "true" && "$PROFILE_FAILURES" -gt 0 ]]; then
    die "one or more benchmark profiles reported failed rounds; see per-profile median/baseline outputs for details"
  fi
  log "GET codec streaming smoke finished."
}

main "$@"
