#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

ADDRESS="127.0.0.1:19031"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
REGION="us-east-1"
SIZE="10MiB"
CONCURRENCY=32
DURATION="10s"
ROUNDS=3
ROUND_COOLDOWN_SECS=10
RETRY_PER_ROUND=1
SEED_DURATION="5s"
SEED_CONCURRENCY=8
HEALTH_TIMEOUT_SECS=60
OUT_DIR=""
DATA_ROOT=""
BUCKET=""
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
BASELINE_CSV=""
DRY_RUN=false
SKIP_BUILD=false

SERVER_PID=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_get_metrics_gate_smoke.sh [options]

Purpose:
  Start one local single-node multi-disk RustFS server with observability
  export disabled, seed 10MiB objects, and run a focused warp GET benchmark.

Options:
  --address <host:port>          RustFS listen address (default: 127.0.0.1:19031)
  --access-key <value>           Access key (default: rustfsadmin)
  --secret-key <value>           Secret key (default: rustfsadmin)
  --region <value>               Region (default: us-east-1)
  --bucket <name>                Benchmark bucket (default: auto-generated)
  --size <label>                 Object size label (default: 10MiB)
  --concurrency <n>              warp GET concurrency (default: 32)
  --duration <duration>          warp GET duration per round (default: 10s)
  --rounds <n>                   Benchmark rounds (default: 3)
  --round-cooldown-secs <n>      Cooldown after each round (default: 10)
  --retry-per-round <n>          Failed-round retries (default: 1)
  --seed-duration <duration>     warp PUT duration for object seeding (default: 5s)
  --seed-concurrency <n>         warp PUT concurrency for object seeding (default: 8)
  --health-timeout-secs <n>      Health wait timeout (default: 60)
  --out-dir <path>               Output directory (default: target/bench/get-metrics-gate-<timestamp>)
  --data-root <path>             Data root for d1..d4 (default: /private/tmp/get-metrics-gate-<timestamp>)
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --baseline-csv <path>          Optional baseline median_summary.csv for delta output
  --skip-build                   Skip cargo build --release -p rustfs --bin rustfs
  --dry-run                      Print commands only
  -h, --help                     Show help
USAGE
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

endpoint_url() {
  echo "http://${ADDRESS}"
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
      --address) ADDRESS="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --size) SIZE="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --round-cooldown-secs) ROUND_COOLDOWN_SECS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --seed-duration) SEED_DURATION="$2"; shift 2 ;;
      --seed-concurrency) SEED_CONCURRENCY="$2"; shift 2 ;;
      --health-timeout-secs) HEALTH_TIMEOUT_SECS="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --data-root) DATA_ROOT="$2"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --baseline-csv) BASELINE_CSV="$2"; shift 2 ;;
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
  [[ -n "$ADDRESS" ]] || die "--address must not be empty"
  [[ -n "$ACCESS_KEY" ]] || die "--access-key must not be empty"
  [[ -n "$SECRET_KEY" ]] || die "--secret-key must not be empty"
  [[ -n "$REGION" ]] || die "--region must not be empty"
  [[ -n "$SIZE" ]] || die "--size must not be empty"
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_positive_int "$SEED_CONCURRENCY" "--seed-concurrency"
  validate_positive_int "$HEALTH_TIMEOUT_SECS" "--health-timeout-secs"
  validate_non_negative_int "$ROUND_COOLDOWN_SECS" "--round-cooldown-secs"

  [[ -x "$ENHANCED_BENCH" ]] || die "benchmark script is not executable: $ENHANCED_BENCH"
  require_cmd curl
  require_cmd git
  require_cmd "$WARP_BIN"
  if [[ "$DRY_RUN" != "true" && "$SKIP_BUILD" != "true" ]]; then
    require_cmd cargo
  fi
  if [[ -n "$BASELINE_CSV" && ! -f "$BASELINE_CSV" ]]; then
    die "--baseline-csv does not exist: $BASELINE_CSV"
  fi
}

setup_paths() {
  local timestamp
  timestamp="$(date +%Y%m%d-%H%M%S)"
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/get-metrics-gate-${timestamp}"
  fi
  if [[ -z "$DATA_ROOT" ]]; then
    DATA_ROOT="/private/tmp/get-metrics-gate-${timestamp}"
  fi
  if [[ -z "$BUCKET" ]]; then
    BUCKET="rustfs-get-metrics-${timestamp}"
  fi

  mkdir -p "$OUT_DIR" "$DATA_ROOT"/d1 "$DATA_ROOT"/d2 "$DATA_ROOT"/d3 "$DATA_ROOT"/d4
}

build_rustfs_if_needed() {
  if [[ "$DRY_RUN" == "true" || "$SKIP_BUILD" == "true" ]]; then
    return
  fi
  cargo build --release -p rustfs --bin rustfs
}

write_manifest() {
  local git_head
  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"

  cat >"${OUT_DIR}/manifest.env" <<EOF
git_head=${git_head}
endpoint=$(endpoint_url)
address=${ADDRESS}
bucket=${BUCKET}
region=${REGION}
size=${SIZE}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
retry_per_round=${RETRY_PER_ROUND}
seed_duration=${SEED_DURATION}
seed_concurrency=${SEED_CONCURRENCY}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
data_root=${DATA_ROOT}
RUST_LOG=off
RUSTFS_OBS_LOGGER_LEVEL=off
RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
RUSTFS_OBS_METRICS_EXPORT_ENABLED=false
RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
RUSTFS_OBS_USE_STDOUT=false
RUSTFS_OBS_LOG_STDOUT_ENABLED=false
RUSTFS_SCANNER_ENABLED=false
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
  local health_url
  health_url="$(endpoint_url)/health"

  for ((attempt = 1; attempt <= HEALTH_TIMEOUT_SECS; attempt++)); do
    if curl -fsS --noproxy '*' --connect-timeout 2 --max-time 3 "$health_url" >/dev/null 2>&1; then
      return
    fi
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      tail -n 80 "${OUT_DIR}/rustfs.log" >&2 || true
      die "RustFS exited before health check passed"
    fi
    sleep 1
  done

  tail -n 80 "${OUT_DIR}/rustfs.log" >&2 || true
  die "RustFS health check timed out"
}

start_server() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] start RustFS at $(endpoint_url)"
    return
  fi

  [[ -x "$RUSTFS_BIN" ]] || die "RustFS binary is not executable: $RUSTFS_BIN"

  (
    export RUST_LOG=off
    export RUSTFS_OBS_LOGGER_LEVEL=off
    export RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
    export RUSTFS_OBS_METRICS_EXPORT_ENABLED=false
    export RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
    export RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
    export RUSTFS_OBS_USE_STDOUT=false
    export RUSTFS_OBS_LOG_STDOUT_ENABLED=false
    export RUSTFS_SCANNER_ENABLED=false
    export RUSTFS_CONSOLE_ENABLE=false
    export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
    export RUSTFS_ADDRESS="$ADDRESS"
    export RUSTFS_ACCESS_KEY="$ACCESS_KEY"
    export RUSTFS_SECRET_KEY="$SECRET_KEY"
    export RUSTFS_RPC_SECRET="rustfs-get-metrics-gate-rpc-secret"
    export RUSTFS_REGION="$REGION"
    exec "$RUSTFS_BIN" server \
      "${DATA_ROOT}/d1" \
      "${DATA_ROOT}/d2" \
      "${DATA_ROOT}/d3" \
      "${DATA_ROOT}/d4"
  ) >"${OUT_DIR}/rustfs.log" 2>&1 &

  SERVER_PID="$!"
  wait_for_health
}

run_bench() {
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
    --sizes "$SIZE"
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --round-cooldown-secs "$ROUND_COOLDOWN_SECS"
    --out-dir "${OUT_DIR}/warp"
  )

  if [[ -n "$BASELINE_CSV" ]]; then
    cmd+=(--baseline-csv "$BASELINE_CSV")
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  "${cmd[@]}"
}

to_bps() {
  local human="$1"
  local number unit factor
  if [[ "$human" == "N/A" || -z "$human" ]]; then
    echo "N/A"
    return
  fi

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

extract_final_get_metrics() {
  local log_file="$1"
  local avg_line reqs_line throughput_human reqps latency_human

  avg_line="$(awk '/^[[:space:]]*\* Average:/ { line=$0 } END { print line }' "$log_file")"
  reqs_line="$(awk '/^[[:space:]]*\* Reqs: Avg:/ { line=$0 } END { print line }' "$log_file")"

  throughput_human="$(
    echo "$avg_line" \
      | sed -nE 's/^[[:space:]]*\* Average: ([0-9]+(\.[0-9]+)? [A-Za-z\/]+), ([0-9]+(\.[0-9]+)?) obj\/s$/\1/p'
  )"
  reqps="$(
    echo "$avg_line" \
      | sed -nE 's/^[[:space:]]*\* Average: ([0-9]+(\.[0-9]+)? [A-Za-z\/]+), ([0-9]+(\.[0-9]+)?) obj\/s$/\3/p'
  )"
  latency_human="$(
    echo "$reqs_line" \
      | sed -nE 's/^[[:space:]]*\* Reqs: Avg: ([0-9]+(\.[0-9]+)?)(ms|us|µs|s),.*$/\1 \3/p'
  )"

  echo "${throughput_human:-N/A},${reqps:-N/A},${latency_human:-N/A}"
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

rebuild_round_results() {
  local round_csv="${OUT_DIR}/warp/round_results.csv"
  local tmp_csv
  tmp_csv="$(mktemp)"

  while IFS=',' read -r size tool round attempt concurrency status throughput_human throughput_bps reqps latency_human latency_ms log_file; do
    if [[ "$size" == "size" ]]; then
      echo "$size,$tool,$round,$attempt,$concurrency,$status,$throughput_human,$throughput_bps,$reqps,$latency_human,$latency_ms,$log_file" >> "$tmp_csv"
      continue
    fi

    if [[ "$status" == "ok" ]]; then
      local metrics
      metrics="$(extract_final_get_metrics "$log_file")"
      throughput_human="$(echo "$metrics" | cut -d',' -f1)"
      reqps="$(echo "$metrics" | cut -d',' -f2)"
      latency_human="$(echo "$metrics" | cut -d',' -f3)"
      throughput_bps="$(to_bps "$throughput_human")"
      latency_ms="$(to_ms "$latency_human")"
    fi

    echo "$size,$tool,$round,$attempt,$concurrency,$status,$throughput_human,$throughput_bps,$reqps,$latency_human,$latency_ms,$log_file" >> "$tmp_csv"
  done < "$round_csv"

  mv "$tmp_csv" "$round_csv"
}

rebuild_median_summary() {
  local round_csv="${OUT_DIR}/warp/round_results.csv"
  local median_csv="${OUT_DIR}/warp/median_summary.csv"
  local ok_rounds fail_rounds t_vals r_vals l_vals m_t m_r m_l

  ok_rounds="$(awk -F',' 'NR>1 && $6=="ok" {c++} END{print c+0}' "$round_csv")"
  fail_rounds="$(awk -F',' 'NR>1 && $6!="ok" {c++} END{print c+0}' "$round_csv")"
  t_vals="$(awk -F',' 'NR>1 && $6=="ok" && $8!="N/A" {print $8}' "$round_csv")"
  r_vals="$(awk -F',' 'NR>1 && $6=="ok" && $9!="N/A" {print $9}' "$round_csv")"
  l_vals="$(awk -F',' 'NR>1 && $6=="ok" && $11!="N/A" {print $11}' "$round_csv")"

  m_t="$(median_from_numbers "$t_vals")"
  m_r="$(median_from_numbers "$r_vals")"
  m_l="$(median_from_numbers "$l_vals")"

  {
    echo "size,tool,concurrency,successful_rounds,failed_rounds,median_throughput_bps,median_reqps,median_latency_ms"
    echo "$SIZE,warp,$CONCURRENCY,$ok_rounds,$fail_rounds,$m_t,$m_r,$m_l"
  } > "$median_csv"
}

rebuild_baseline_compare() {
  local compare_csv="${OUT_DIR}/warp/baseline_compare.csv"
  if [[ -z "$BASELINE_CSV" ]]; then
    return
  fi

  echo "size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct,new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct" > "$compare_csv"

  awk -F',' '
    NR==FNR {
      if (FNR==1) next
      b_req=$7
      b_lat=$8
      b_thr=$6
      next
    }
    FNR==2 {
      n_thr=$6
      n_req=$7
      n_lat=$8
      dr="N/A"; dl="N/A"; dt="N/A"
      if (b_req!="N/A" && n_req!="N/A" && b_req+0!=0) dr=sprintf("%.2f", ((n_req-b_req)/b_req)*100)
      if (b_lat!="N/A" && n_lat!="N/A" && b_lat+0!=0) dl=sprintf("%.2f", ((n_lat-b_lat)/b_lat)*100)
      if (b_thr!="N/A" && n_thr!="N/A" && b_thr+0!=0) dt=sprintf("%.2f", ((n_thr-b_thr)/b_thr)*100)
      print $1 "," $2 "," $3 "," n_req "," b_req "," dr "," n_lat "," b_lat "," dl "," n_thr "," b_thr "," dt
    }
  ' "$BASELINE_CSV" "${OUT_DIR}/warp/median_summary.csv" >> "$compare_csv"
}

postprocess_results() {
  rebuild_round_results
  rebuild_median_summary
  rebuild_baseline_compare
}

main() {
  parse_args "$@"
  validate_args
  setup_paths
  write_manifest
  build_rustfs_if_needed

  trap stop_server EXIT INT TERM

  echo "Output dir: $OUT_DIR"
  start_server
  run_bench
  postprocess_results
  stop_server
}

main "$@"
