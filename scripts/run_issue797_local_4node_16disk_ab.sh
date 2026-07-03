#!/usr/bin/env bash
set -euo pipefail

# Local 4-node / 16-disk RustFS runner for rustfs/backlog#797.
# It starts four local RustFS processes, runs warp workloads, and captures
# health, logs, benchmark summaries, and optional signed admin metrics.

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${RUSTFS_BIN:-${PROJECT_ROOT}/target/debug/rustfs}"
BUILD_BIN="${BUILD_BIN:-true}"
BASE_PORT="${BASE_PORT:-19100}"
NODE_COUNT="${NODE_COUNT:-4}"
DISKS_PER_NODE="${DISKS_PER_NODE:-4}"
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
REGION="${REGION:-us-east-1}"
SIZES="${SIZES:-4KiB,1MiB}"
CONCURRENCY="${CONCURRENCY:-8}"
DURATION="${DURATION:-60s}"
WARP_BIN="${WARP_BIN:-warp}"
WARP_MODE="${WARP_MODE:-mixed}"
WARP_EXTRA_ARGS="${WARP_EXTRA_ARGS:---noclear}"
PROFILES="${PROFILES:-baseline,metrics_logging}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/bench/issue-797-local-4node-16disk-ab-$(date -u +%Y%m%dT%H%M%SZ)}"
DATA_ROOT="${DATA_ROOT:-/tmp/issue797-local-4node-16disk-ab-$(date -u +%Y%m%dT%H%M%SZ)}"
KEEP_DATA="${KEEP_DATA:-false}"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
HEALTH_POLL_SECS="${HEALTH_POLL_SECS:-2}"
CAPTURE_ADMIN_METRICS="${CAPTURE_ADMIN_METRICS:-true}"
AWSCURL_BIN="${AWSCURL_BIN:-awscurl}"
CURL_BIN="${CURL_BIN:-curl}"
RG_BIN="${RG_BIN:-rg}"
DRY_RUN=false

PIDS=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_issue797_local_4node_16disk_ab.sh [options]

Options:
  --rustfs-bin <path>          RustFS binary (default: target/debug/rustfs)
  --skip-build                 Do not build rustfs before running
  --base-port <port>           First node port; uses port..port+3
  --sizes <csv>                Object sizes for warp (default: 4KiB,1MiB)
  --concurrency <n>            Warp concurrency (default: 8)
  --duration <dur>             Warp duration per size/profile (default: 60s)
  --profiles <csv>             baseline,metrics_logging,locality_on
  --out-dir <dir>              Output directory
  --data-root <dir>            Temporary disk root
  --keep-data                  Keep data root after exit
  --warp-extra-args <args>     Extra args appended to warp (default: --noclear)
  --skip-admin-metrics         Do not attempt signed admin metrics capture
  --dry-run                    Print planned layout without running
  -h, --help                   Show help

Profiles:
  baseline         Metrics exports, file logging, shard locality, and batch
                   processor observation disabled.
  metrics_logging  Enables metrics export gate, bounded warn-level file
                   logging, shard locality observe mode, and batch processor
                   observe mode.
  locality_on      Same as metrics_logging, but uses shard locality on mode.

Notes:
  Admin metrics are captured from /rustfs/admin/v3/metrics when awscurl or
  curl --aws-sigv4 is available. If neither is available, benchmark still runs
  and records a skipped metrics status file.
USAGE
}

log() {
  printf '[INFO] %s\n' "$*"
}

warn() {
  printf '[WARN] %s\n' "$*" >&2
}

die() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    die "missing value for ${flag}"
  fi
  printf '%s\n' "$value"
}

# Like arg_value, but accepts values that themselves start with `--`
# (e.g. `--warp-extra-args --noclear`).
arg_value_allow_dashes() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" ]]; then
    die "missing value for ${flag}"
  fi
  printf '%s\n' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --rustfs-bin) RUSTFS_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --skip-build) BUILD_BIN=false; shift ;;
      --base-port) BASE_PORT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sizes) SIZES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrency) CONCURRENCY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration) DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --profiles) PROFILES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --data-root) DATA_ROOT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --keep-data) KEEP_DATA=true; shift ;;
      --warp-extra-args) WARP_EXTRA_ARGS="$(arg_value_allow_dashes "$1" "${2:-}")"; shift 2 ;;
      --skip-admin-metrics) CAPTURE_ADMIN_METRICS=false; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

validate_args() {
  is_positive_integer "$BASE_PORT" || die "--base-port must be a positive integer"
  is_positive_integer "$CONCURRENCY" || die "--concurrency must be a positive integer"
  [[ "$NODE_COUNT" == "4" ]] || die "NODE_COUNT is fixed to 4 for this runner"
  [[ "$DISKS_PER_NODE" == "4" ]] || die "DISKS_PER_NODE is fixed to 4 for this runner"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "command not found: $1"
  fi
}

print_redacted_command() {
  local redact_next=false
  local arg

  printf 'Command:'
  for arg in "$@"; do
    if [[ "$redact_next" == "true" ]]; then
      printf ' %q' "REDACTED"
      redact_next=false
      continue
    fi

    case "$arg" in
      --access-key|--secret-key)
        printf ' %q' "$arg"
        redact_next=true
        ;;
      -accessKey=*|-secretKey=*)
        printf ' %q' "${arg%%=*}=REDACTED"
        ;;
      *)
        printf ' %q' "$arg"
        ;;
    esac
  done
  printf '\n'
}

endpoint_for_node() {
  local endpoint_node_id="$1"
  printf 'http://127.0.0.1:%s' "$((BASE_PORT + endpoint_node_id - 1))"
}

endpoint_label() {
  local endpoint="$1"
  local label
  label="${endpoint#*://}"
  label="${label%%/*}"
  printf '%s' "$label" | tr -c 'A-Za-z0-9._-' '_'
}

all_endpoints_csv() {
  local endpoint_loop_id
  local endpoints=()
  for ((endpoint_loop_id = 1; endpoint_loop_id <= NODE_COUNT; endpoint_loop_id++)); do
    endpoints+=("$(endpoint_for_node "$endpoint_loop_id")")
  done
  local IFS=','
  printf '%s' "${endpoints[*]}"
}

metrics_url() {
  # SCANNER(1) + NET(32) + RPC(256): enough for readiness context plus
  # internode aggregate traffic without collecting the heavier all-metrics set.
  printf '%s/rustfs/admin/v3/metrics?types=289&by-host=true&n=1\n' "${1%/}"
}

curl_supports_aws_sigv4() {
  "$CURL_BIN" --help all 2>/dev/null | grep -q -- '--aws-sigv4'
}

profile_is_supported() {
  case "$1" in
    baseline|metrics_logging|locality_on) return 0 ;;
    *) return 1 ;;
  esac
}

profile_env_file() {
  local profile="$1"
  local env_file="$2"
  case "$profile" in
    baseline)
      cat >"$env_file" <<'EOF'
RUSTFS_OBS_LOGGER_LEVEL=off
RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
RUSTFS_OBS_METRICS_EXPORT_ENABLED=false
RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
RUSTFS_OBS_USE_STDOUT=false
RUSTFS_OBS_LOG_STDOUT_ENABLED=false
RUSTFS_BATCH_PROCESSOR_ADAPTIVE=off
RUSTFS_SHARD_LOCALITY_SCHEDULING=off
EOF
      ;;
    metrics_logging)
      cat >"$env_file" <<'EOF'
RUSTFS_OBS_LOGGER_LEVEL=warn
RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318
RUSTFS_OBS_ENDPOINT_TIMEOUT_MILLIS=500
RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
RUSTFS_OBS_METRICS_EXPORT_ENABLED=true
RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
RUSTFS_OBS_USE_STDOUT=false
RUSTFS_OBS_LOG_STDOUT_ENABLED=false
RUSTFS_OBS_METER_INTERVAL=5
RUSTFS_OBS_LOG_KEEP_FILES=2
RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES=268435456
RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES=134217728
RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS=0
RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS=30
RUSTFS_BATCH_PROCESSOR_ADAPTIVE=observe
RUSTFS_SHARD_LOCALITY_SCHEDULING=observe
EOF
      ;;
    locality_on)
      cat >"$env_file" <<'EOF'
RUSTFS_OBS_LOGGER_LEVEL=warn
RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318
RUSTFS_OBS_ENDPOINT_TIMEOUT_MILLIS=500
RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
RUSTFS_OBS_METRICS_EXPORT_ENABLED=true
RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
RUSTFS_OBS_USE_STDOUT=false
RUSTFS_OBS_LOG_STDOUT_ENABLED=false
RUSTFS_OBS_METER_INTERVAL=5
RUSTFS_OBS_LOG_KEEP_FILES=2
RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES=268435456
RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES=134217728
RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS=0
RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS=30
RUSTFS_BATCH_PROCESSOR_ADAPTIVE=observe
RUSTFS_SHARD_LOCALITY_SCHEDULING=on
EOF
      ;;
  esac
}

load_profile_env() {
  local env_file="$1"
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

prepare_profile_layout() {
  local profile="$1"
  local profile_dir="$OUT_DIR/$profile"
  mkdir -p "$profile_dir"/{bench,health,logs,metrics,pids}
  profile_env_file "$profile" "$profile_dir/profile.env"
}

build_volumes() {
  local profile="$1"
  local node_index disk_index
  local volumes=()
  for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
    for ((disk_index = 1; disk_index <= DISKS_PER_NODE; disk_index++)); do
      volumes+=("http://127.0.0.1:$((BASE_PORT + node_index - 1))${DATA_ROOT}/${profile}/node${node_index}/disk${disk_index}")
    done
  done
  printf '%s ' "${volumes[@]}"
}

prepare_data_dirs() {
  local profile="$1"
  local node_index disk_index
  for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
    for ((disk_index = 1; disk_index <= DISKS_PER_NODE; disk_index++)); do
      mkdir -p "${DATA_ROOT}/${profile}/node${node_index}/disk${disk_index}"
    done
  done
}

stop_nodes() {
  local pid
  if [[ ${#PIDS[@]} -eq 0 ]]; then
    return
  fi
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for ((_ = 1; _ <= 5; _++)); do
    local remaining=0
    for pid in "${PIDS[@]}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        remaining=$((remaining + 1))
      fi
    done
    [[ "$remaining" -eq 0 ]] && break
    sleep 1
  done
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    wait "$pid" >/dev/null 2>&1 || true
  done
  PIDS=()
}

cleanup() {
  stop_nodes
  if [[ "$KEEP_DATA" != "true" && "$DRY_RUN" != "true" && -n "$DATA_ROOT" && -d "$DATA_ROOT" ]]; then
    rm -rf "$DATA_ROOT"
  fi
}

trap cleanup EXIT

start_nodes() {
  local profile="$1"
  local profile_dir="$OUT_DIR/$profile"
  local volumes
  volumes="$(build_volumes "$profile")"

  prepare_data_dirs "$profile"
  load_profile_env "$profile_dir/profile.env"

  local node_index endpoint log_file pid_file
  for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
    endpoint="$(endpoint_for_node "$node_index")"
    log_file="$profile_dir/logs/node${node_index}.log"
    pid_file="$profile_dir/pids/node${node_index}.pid"

    (
      export RUSTFS_ACCESS_KEY="$ACCESS_KEY"
      export RUSTFS_SECRET_KEY="$SECRET_KEY"
      export RUSTFS_ADDRESS="127.0.0.1:$((BASE_PORT + node_index - 1))"
      export RUSTFS_CONSOLE_ENABLE=false
      export RUSTFS_SCANNER_ENABLED=false
      export RUSTFS_SCANNER_START_DELAY_SECS=3600
      export RUSTFS_SCANNER_CYCLE=3600
      export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
      export RUSTFS_VOLUMES="$volumes"
      export RUSTFS_OBS_SERVICE_NAME="rustfs-issue797-${profile}-node${node_index}"
      export RUSTFS_OBS_LOG_DIRECTORY="$profile_dir/logs/node${node_index}"
      mkdir -p "$RUSTFS_OBS_LOG_DIRECTORY"
      # Keep both env and positional volumes: current CLI still requires the
      # positional server volumes, while env makes the layout visible in logs.
      # shellcheck disable=SC2086
      exec "$RUSTFS_BIN" server $volumes
    ) >"$log_file" 2>&1 &
    PIDS+=("$!")
    printf '%s\n' "$!" >"$pid_file"
    log "started ${profile} node${node_index} ${endpoint} pid=$!"
  done
}

wait_for_health() {
  local profile="$1"
  local profile_dir="$OUT_DIR/$profile"
  local deadline node_index endpoint status_file
  deadline=$((SECONDS + WAIT_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    local ready=0
    for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
      endpoint="$(endpoint_for_node "$node_index")"
      status_file="$profile_dir/health/node${node_index}.live"
      if "$CURL_BIN" -fsS --max-time 2 "${endpoint}/health/live" >"$status_file" 2>"$status_file.err"; then
        ready=$((ready + 1))
      fi
    done
    if [[ "$ready" -eq "$NODE_COUNT" ]]; then
      log "${profile}: all nodes are live"
      return 0
    fi
    sleep "$HEALTH_POLL_SECS"
  done

  for ((node_index = 1; node_index <= NODE_COUNT; node_index++)); do
    warn "${profile} node${node_index} log tail:"
    tail -n 40 "$profile_dir/logs/node${node_index}.log" >&2 || true
  done
  die "${profile}: timed out waiting for health"
}

capture_admin_metrics() {
  local profile="$1"
  local phase="$2"
  local profile_dir="$OUT_DIR/$profile"
  local status_file="$profile_dir/metrics/${phase}.status"
  local endpoint label metrics_file endpoints

  if [[ "$CAPTURE_ADMIN_METRICS" != "true" ]]; then
    echo "skipped: disabled" >"$status_file"
    return 0
  fi
  local capture_method=""
  if command -v "$AWSCURL_BIN" >/dev/null 2>&1; then
    capture_method="awscurl"
  elif curl_supports_aws_sigv4; then
    capture_method="curl-sigv4"
  else
    echo "skipped: awscurl not found and curl lacks --aws-sigv4" >"$status_file"
    return 0
  fi

  echo "capturing: ${capture_method}" >"$status_file"
  IFS=',' read -r -a endpoints <<<"$(all_endpoints_csv)"
  for endpoint in "${endpoints[@]}"; do
    label="$(endpoint_label "$endpoint")"
    metrics_file="$profile_dir/metrics/${phase}.${label}.ndjson"
    if [[ "$capture_method" == "awscurl" ]]; then
      AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
      AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
      AWS_DEFAULT_REGION="$REGION" \
      "$AWSCURL_BIN" \
        --service s3 \
        --region "$REGION" \
        --request GET \
        "$(metrics_url "$endpoint")" \
        >"$metrics_file" 2>"$metrics_file.err" || true
    else
      local curl_config="$profile_dir/metrics/.curl-sigv4-${phase}.${label}.conf"
      {
        printf 'aws-sigv4 = "aws:amz:%s:s3"\n' "$REGION"
        printf 'user = "%s:%s"\n' "$ACCESS_KEY" "$SECRET_KEY"
        printf 'request = "GET"\n'
        printf 'max-time = 10\n'
        printf 'fail\n'
        printf 'silent\n'
        printf 'show-error\n'
      } >"$curl_config"
      chmod 600 "$curl_config"
      "$CURL_BIN" --config "$curl_config" "$(metrics_url "$endpoint")" >"$metrics_file" 2>"$metrics_file.err" || true
      rm -f "$curl_config"
    fi
  done
  echo "done: ${capture_method}" >"$status_file"
}

run_bench() {
  local profile="$1"
  local profile_dir="$OUT_DIR/$profile"
  local bucket_profile="${profile//_/-}"
  local -a cmd=(
    "${PROJECT_ROOT}/scripts/run_object_batch_bench.sh"
    --tool warp
    --endpoint "$(all_endpoints_csv)"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --region "$REGION"
    --auto-new-bucket
    --bucket-prefix "issue797-${bucket_profile}"
    --sizes "$SIZES"
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --warp-bin "$WARP_BIN"
    --warp-mode "$WARP_MODE"
    --out-dir "$profile_dir/bench"
  )

  if [[ -n "$WARP_EXTRA_ARGS" ]]; then
    cmd+=(--extra-args "$WARP_EXTRA_ARGS")
  fi

  print_redacted_command "${cmd[@]}" >"$profile_dir/bench/command.txt"
  log "${profile}: running warp benchmark"
  "${cmd[@]}" 2>&1 | tee "$profile_dir/bench/run.log"
}

extract_tail_summary() {
  local profile="$1"
  local profile_dir="$OUT_DIR/$profile"
  local output="$profile_dir/bench/tail_latency_summary.txt"
  : >"$output"
  if ! command -v "$RG_BIN" >/dev/null 2>&1; then
    echo "rg not found; skipped" >"$output"
    return 0
  fi
  "$RG_BIN" -n 'Average:|Median:|90th:|99th:|Fastest:|Slowest:|StdDev:|Total:|Throughput by host|warp: <ERROR>' \
    "$profile_dir/bench"/*.log >"$output" || true
}

write_run_meta() {
  local meta="$OUT_DIR/run-meta.txt"
  mkdir -p "$OUT_DIR"
  {
    echo "created_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "rustfs_bin=$RUSTFS_BIN"
    echo "base_port=$BASE_PORT"
    echo "node_count=$NODE_COUNT"
    echo "disks_per_node=$DISKS_PER_NODE"
    echo "sizes=$SIZES"
    echo "concurrency=$CONCURRENCY"
    echo "duration=$DURATION"
    echo "profiles=$PROFILES"
    echo "warp_extra_args=$WARP_EXTRA_ARGS"
    echo "endpoints=$(all_endpoints_csv)"
    echo "data_root=$DATA_ROOT"
    echo "keep_data=$KEEP_DATA"
    echo "capture_admin_metrics=$CAPTURE_ADMIN_METRICS"
  } >"$meta"
}

run_profile() {
  local profile="$1"
  profile_is_supported "$profile" || die "unsupported profile: $profile"
  prepare_profile_layout "$profile"
  start_nodes "$profile"
  wait_for_health "$profile"
  capture_admin_metrics "$profile" before
  run_bench "$profile"
  capture_admin_metrics "$profile" after
  extract_tail_summary "$profile"
  stop_nodes
}

main() {
  parse_args "$@"
  validate_args

  write_run_meta

  if [[ "$DRY_RUN" == "true" ]]; then
    log "dry run only"
    cat "$OUT_DIR/run-meta.txt"
    return 0
  fi

  require_cmd "$CURL_BIN"
  require_cmd "$WARP_BIN"

  if [[ "$BUILD_BIN" == "true" ]]; then
    log "building rustfs binary"
    cargo build -p rustfs --bin rustfs
  fi
  [[ -x "$RUSTFS_BIN" ]] || die "rustfs binary is not executable: $RUSTFS_BIN"

  local profile
  IFS=',' read -r -a profile_arr <<<"$PROFILES"
  for profile in "${profile_arr[@]}"; do
    profile="${profile//[[:space:]]/}"
    [[ -z "$profile" ]] && continue
    run_profile "$profile"
  done

  log "done. Output dir: $OUT_DIR"
}

main "$@"
