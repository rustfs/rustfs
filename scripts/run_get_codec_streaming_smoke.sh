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
CONCURRENCY=32
DURATION="15s"
ROUNDS=3
RETRY_PER_ROUND=1
MODE="both"
OUT_DIR=""
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
CODEC_MIN_SIZE=1
RUST_LOG="warn"
HEALTH_TIMEOUT_SECS=60
DRY_RUN=false
SKIP_BUILD=false

SERVER_PID=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_get_codec_streaming_smoke.sh [options]

Purpose:
  Start a local RustFS server and run GET warp benchmarks for the legacy read
  path, the experimental codec streaming read path, or both.

Core options:
  --mode <legacy|codec|both>     Which profile(s) to run (default: both)
  --address <host:port>          RustFS listen address (default: 127.0.0.1:19030)
  --bucket <name>                Benchmark bucket (default: rustfs-get-codec-smoke)
  --sizes <csv>                  Object sizes (default: 1MiB,4MiB,10MiB)
  --concurrency <n>              warp concurrency (default: 32)
  --duration <duration>          warp duration per round (default: 15s)
  --rounds <n>                   rounds per size (default: 3)
  --retry-per-round <n>          failed-attempt retries per round (default: 1)
  --out-dir <path>               output directory (default: target/bench/get-codec-streaming-<timestamp>)

Binary/options:
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --codec-min-size <bytes>       RUSTFS_GET_CODEC_STREAMING_MIN_SIZE (default: 1)
  --skip-build                   do not run cargo build --release -p rustfs
  --dry-run                      print benchmark commands without starting RustFS

Credentials:
  --access-key <value>           RustFS access key (default: rustfsadmin)
  --secret-key <value>           RustFS secret key (default: rustfsadmin)
  --region <value>               S3 region (default: us-east-1)

Output:
  <out-dir>/legacy/warp/median_summary.csv
  <out-dir>/codec/warp/median_summary.csv
  <out-dir>/codec/warp/baseline_compare.csv    when --mode both
  <out-dir>/<profile>/manifest.env
  <out-dir>/<profile>/rustfs.log

Example:
  scripts/run_get_codec_streaming_smoke.sh \
    --mode both --sizes 1MiB,4MiB,10MiB --concurrency 64 --duration 30s
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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode) MODE="$2"; shift 2 ;;
      --address) ADDRESS="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --codec-min-size) CODEC_MIN_SIZE="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
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

validate_args() {
  case "$MODE" in
    legacy|codec|both) ;;
    *) die "--mode must be legacy, codec, or both" ;;
  esac

  [[ -n "$ADDRESS" ]] || die "--address must not be empty"
  [[ -n "$ACCESS_KEY" ]] || die "--access-key must not be empty"
  [[ -n "$SECRET_KEY" ]] || die "--secret-key must not be empty"
  [[ -n "$BUCKET" ]] || die "--bucket must not be empty"
  [[ -n "$SIZES" ]] || die "--sizes must not be empty"
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_positive_int "$CODEC_MIN_SIZE" "--codec-min-size"
  validate_positive_int "$HEALTH_TIMEOUT_SECS" "--health-timeout-secs"

  [[ -x "$ENHANCED_BENCH" ]] || die "enhanced benchmark script is not executable: $ENHANCED_BENCH"
  require_cmd curl
  require_cmd git

  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd cargo
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/get-codec-streaming-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
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
    codec) echo "true" ;;
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

write_manifest() {
  local profile="$1"
  local profile_dir="$2"
  local codec_enabled="$3"
  local volumes="$4"
  local git_head git_dirty_count

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
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
rust_log=${RUST_LOG}
rustfs_volumes=${volumes}
RUSTFS_GET_CODEC_STREAMING_ENABLE=${codec_enabled}
RUSTFS_GET_CODEC_STREAMING_MIN_SIZE=${CODEC_MIN_SIZE}
RUSTFS_SCANNER_ENABLED=false
RUSTFS_SCANNER_START_DELAY_SECS=3600
RUSTFS_SCANNER_CYCLE=3600
RUSTFS_CONSOLE_ENABLE=false
RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
EOF
}

stop_server() {
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
  local rustfs_log

  data_root="$(profile_data_root "$profile")"
  volumes="$(profile_volumes "$data_root")"
  codec_enabled="$(profile_codec_enabled "$profile")"
  rustfs_log="${profile_dir}/rustfs.log"

  mkdir -p "${data_root}/disk1" "${data_root}/disk2" "${data_root}/disk3" "${data_root}/disk4"
  write_manifest "$profile" "$profile_dir" "$codec_enabled" "$volumes"

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
    export RUSTFS_GET_CODEC_STREAMING_MIN_SIZE="$CODEC_MIN_SIZE"
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
  wait_for_health "$profile" "$rustfs_log"
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
    --out-dir "$bench_dir"
  )

  if [[ -n "$baseline_csv" ]]; then
    cmd+=(--baseline-csv "$baseline_csv")
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  log "Running ${profile} GET benchmark..."
  "${cmd[@]}"
}

run_profile() {
  local profile="$1"
  local baseline_csv="${2:-}"

  stop_server
  start_server "$profile"
  run_bench "$profile" "$baseline_csv"
  stop_server

  log "Median summary: ${OUT_DIR}/${profile}/warp/median_summary.csv"
  if [[ -f "${OUT_DIR}/${profile}/warp/baseline_compare.csv" ]]; then
    log "Baseline compare: ${OUT_DIR}/${profile}/warp/baseline_compare.csv"
  fi
}

main() {
  parse_args "$@"
  validate_args
  setup_output
  build_rustfs_if_needed

  trap stop_server EXIT INT TERM

  log "Output dir: $OUT_DIR"
  case "$MODE" in
    legacy)
      run_profile legacy
      ;;
    codec)
      run_profile codec
      ;;
    both)
      run_profile legacy
      run_profile codec "${OUT_DIR}/legacy/warp/median_summary.csv"
      ;;
  esac

  log "GET codec streaming smoke finished."
}

main "$@"
