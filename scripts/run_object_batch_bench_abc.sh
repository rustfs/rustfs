#!/usr/bin/env bash
set -euo pipefail

# One-click controller:
# - Switches RUSTFS_CAPACITY_* and RUSTFS_OBJECT_* by profile A/B/C
# - Calls scripts/run_object_batch_bench_enhanced.sh for each profile
# - Supports optional "apply command" hook to reload/restart RustFS per profile

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENHANCED_SCRIPT="$SCRIPT_DIR/run_object_batch_bench_enhanced.sh"

GROUP="all" # all|A|B|C
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
BUCKET="rustfs-bench"
REGION="us-east-1"
TOOL="warp"
CONCURRENCY=128
ROUNDS=3
RETRY_PER_ROUND=2
RETRY_SLEEP_SECS=2
INSECURE=false
DRY_RUN=false
OUT_ROOT=""
BASELINE_ROOT=""

# tool-specific
WARP_BIN="warp"
WARP_MODE="mixed"
DURATION="60s"
S3BENCH_BIN="s3bench"
SAMPLES=20000

# optional hooks
APPLY_CMD=""
APPLY_CMD_ARR=()
APPLY_WAIT_SECS=20

EXTRA_ARGS=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_object_batch_bench_abc.sh \
    --tool <warp|s3bench> --endpoint <url> --access-key <ak> --secret-key <sk> [options]

Required:
  --tool                      warp | s3bench
  --endpoint                  S3 endpoint
  --access-key                S3 access key
  --secret-key                S3 secret key

Core options:
  --group                     all|A|B|C (default: all)
  --bucket                    Bucket name (default: rustfs-bench)
  --region                    Region (default: us-east-1)
  --concurrency               Default 128
  --rounds                    Default 3
  --retry-per-round           Default 2
  --retry-sleep-secs          Default 2
  --out-root                  Default target/bench/object-batch-abc-<timestamp>
  --baseline-root             If set, use <baseline-root>/<group>/median_summary.csv
  --insecure                  Allow insecure TLS
  --dry-run                   Print commands without execution

Warp options:
  --warp-bin                  Default: warp
  --warp-mode                 get|put|mixed (default: mixed)
  --duration                  Default: 60s

s3bench options:
  --s3bench-bin               Default: s3bench
  --samples                   Default: 20000

Hooks:
  --apply-cmd                 Optional command to apply/restart RustFS after profile env switch.
                              Executed directly (no shell eval), e.g. "bash scripts/restart.sh"
  --apply-wait-secs           Wait time after apply cmd (default: 20)

Extra:
  --extra-args                Extra args passed to enhanced script, quoted as one string
  -h, --help                  Show this help

Examples:
  scripts/run_object_batch_bench_abc.sh \
    --tool warp --endpoint http://127.0.0.1:9000 \
    --access-key minioadmin --secret-key minioadmin \
    --bucket bench-obj --group all --duration 90s

  scripts/run_object_batch_bench_abc.sh \
    --tool s3bench --endpoint http://127.0.0.1:9000 \
    --access-key minioadmin --secret-key minioadmin \
    --group B --samples 50000 --apply-cmd "bash scripts/run.capacity-object.lab.sh"
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

validate_positive_int() {
  local v="$1"
  local n="$2"
  if ! [[ "$v" =~ ^[0-9]+$ ]] || [[ "$v" -le 0 ]]; then
    echo "ERROR: $n must be a positive integer, got: $v" >&2
    exit 1
  fi
}

parse_apply_cmd() {
  local raw="$1"

  if [[ "$raw" == *';'* || "$raw" == *'&&'* || "$raw" == *'||'* || "$raw" == *'|'* || "$raw" == *'<'* || "$raw" == *'>'* || "$raw" == *'`'* || "$raw" == *'$'* ]]; then
    echo "ERROR: --apply-cmd does not allow shell operators or expansions; pass a plain command and args only" >&2
    exit 1
  fi

  IFS=$' \t\n' read -r -a APPLY_CMD_ARR <<< "$raw"
  if [[ "${#APPLY_CMD_ARR[@]}" -eq 0 ]]; then
    echo "ERROR: --apply-cmd must not be empty" >&2
    exit 1
  fi

  require_cmd "${APPLY_CMD_ARR[0]}"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --tool) TOOL="$2"; shift 2 ;;
      --endpoint) ENDPOINT="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --group) GROUP="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --retry-sleep-secs) RETRY_SLEEP_SECS="$2"; shift 2 ;;
      --out-root) OUT_ROOT="$2"; shift 2 ;;
      --baseline-root) BASELINE_ROOT="$2"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --warp-mode) WARP_MODE="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --s3bench-bin) S3BENCH_BIN="$2"; shift 2 ;;
      --samples) SAMPLES="$2"; shift 2 ;;
      --apply-cmd) APPLY_CMD="$2"; shift 2 ;;
      --apply-wait-secs) APPLY_WAIT_SECS="$2"; shift 2 ;;
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

validate_args() {
  if [[ "$TOOL" != "warp" && "$TOOL" != "s3bench" ]]; then
    echo "ERROR: --tool must be warp or s3bench" >&2
    exit 1
  fi
  case "$GROUP" in
    all|A|B|C) ;;
    *) echo "ERROR: --group must be all|A|B|C" >&2; exit 1 ;;
  esac
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint/--access-key/--secret-key are required" >&2
    exit 1
  fi
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_positive_int "$RETRY_SLEEP_SECS" "--retry-sleep-secs"
  validate_positive_int "$APPLY_WAIT_SECS" "--apply-wait-secs"
  if [[ "$TOOL" == "s3bench" ]]; then
    validate_positive_int "$SAMPLES" "--samples"
  fi
  if [[ -n "$APPLY_CMD" ]]; then
    parse_apply_cmd "$APPLY_CMD"
  fi
}

setup_out_root() {
  if [[ -z "$OUT_ROOT" ]]; then
    OUT_ROOT="target/bench/object-batch-abc-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_ROOT"
}

apply_capacity_common() {
  export RUSTFS_CAPACITY_SCHEDULED_INTERVAL=300
  export RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=8
  export RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=14
  export RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=45
  export RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=1000000
  export RUSTFS_CAPACITY_STAT_TIMEOUT=5
  export RUSTFS_CAPACITY_SAMPLE_RATE=100
  export RUSTFS_CAPACITY_METRICS_INTERVAL=120
}

apply_object_profile_A() {
  export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=128
  export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=2097152
  export RUSTFS_OBJECT_GET_TIMEOUT=18
  export RUSTFS_OBJECT_DISK_READ_TIMEOUT=6
  export RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT=4
  export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true
  export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
  export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=12
  export RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD=6
}

apply_object_profile_B() {
  export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=112
  export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=4194304
  export RUSTFS_OBJECT_GET_TIMEOUT=30
  export RUSTFS_OBJECT_DISK_READ_TIMEOUT=10
  export RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT=5
  export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true
  export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
  export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=12
  export RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD=6
}

apply_object_profile_C() {
  export RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=72
  export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=8388608
  export RUSTFS_OBJECT_GET_TIMEOUT=50
  export RUSTFS_OBJECT_DISK_READ_TIMEOUT=14
  export RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT=6
  export RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=true
  export RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=true
  export RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=12
  export RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD=6
}

sizes_for_group() {
  case "$1" in
    A) echo "1KiB,4KiB,8KiB,16KiB,32KiB,100KiB" ;;
    B) echo "100KiB,512KiB,1MiB,2MiB" ;;
    C) echo "2MiB,5MiB,10MiB" ;;
    *) echo "" ;;
  esac
}

run_apply_hook_if_needed() {
  local group="$1"
  if [[ "${#APPLY_CMD_ARR[@]}" -eq 0 ]]; then
    return
  fi
  echo "[${group}] running apply command..."
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] '
    printf '%q ' "${APPLY_CMD_ARR[@]}"
    printf '\n'
    echo "[DRY-RUN] sleep $APPLY_WAIT_SECS"
  else
    "${APPLY_CMD_ARR[@]}"
    echo "[${group}] waiting ${APPLY_WAIT_SECS}s for service readiness..."
    sleep "$APPLY_WAIT_SECS"
  fi
}

write_env_snapshot() {
  local out_file="$1"
  cat > "$out_file" <<EOF
RUSTFS_CAPACITY_SCHEDULED_INTERVAL=${RUSTFS_CAPACITY_SCHEDULED_INTERVAL}
RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY=${RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY}
RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD=${RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD}
RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD=${RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD}
RUSTFS_CAPACITY_MAX_FILES_THRESHOLD=${RUSTFS_CAPACITY_MAX_FILES_THRESHOLD}
RUSTFS_CAPACITY_STAT_TIMEOUT=${RUSTFS_CAPACITY_STAT_TIMEOUT}
RUSTFS_CAPACITY_SAMPLE_RATE=${RUSTFS_CAPACITY_SAMPLE_RATE}
RUSTFS_CAPACITY_METRICS_INTERVAL=${RUSTFS_CAPACITY_METRICS_INTERVAL}
RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS=${RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS}
RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=${RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE}
RUSTFS_OBJECT_GET_TIMEOUT=${RUSTFS_OBJECT_GET_TIMEOUT}
RUSTFS_OBJECT_DISK_READ_TIMEOUT=${RUSTFS_OBJECT_DISK_READ_TIMEOUT}
RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT=${RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT}
RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE=${RUSTFS_OBJECT_PRIORITY_SCHEDULING_ENABLE}
RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE=${RUSTFS_OBJECT_LOCK_OPTIMIZATION_ENABLE}
RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD=${RUSTFS_OBJECT_HIGH_CONCURRENCY_THRESHOLD}
RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD=${RUSTFS_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD}
EOF
}

run_group() {
  local g="$1"
  local sizes out_dir baseline_csv

  apply_capacity_common
  case "$g" in
    A) apply_object_profile_A ;;
    B) apply_object_profile_B ;;
    C) apply_object_profile_C ;;
    *) echo "ERROR: unsupported group $g" >&2; exit 1 ;;
  esac

  sizes="$(sizes_for_group "$g")"
  out_dir="$OUT_ROOT/$g"
  mkdir -p "$out_dir"
  write_env_snapshot "$out_dir/env_snapshot.env"

  run_apply_hook_if_needed "$g"

  baseline_csv=""
  if [[ -n "$BASELINE_ROOT" && -f "$BASELINE_ROOT/$g/median_summary.csv" ]]; then
    baseline_csv="$BASELINE_ROOT/$g/median_summary.csv"
  fi

  local cmd=(
    "$ENHANCED_SCRIPT"
    "--tool" "$TOOL"
    "--endpoint" "$ENDPOINT"
    "--access-key" "$ACCESS_KEY"
    "--secret-key" "$SECRET_KEY"
    "--bucket" "$BUCKET"
    "--region" "$REGION"
    "--concurrency" "$CONCURRENCY"
    "--sizes" "$sizes"
    "--rounds" "$ROUNDS"
    "--retry-per-round" "$RETRY_PER_ROUND"
    "--retry-sleep-secs" "$RETRY_SLEEP_SECS"
    "--out-dir" "$out_dir"
  )

  if [[ -n "$baseline_csv" ]]; then
    cmd+=("--baseline-csv" "$baseline_csv")
  fi
  if [[ "$INSECURE" == "true" ]]; then
    cmd+=("--insecure")
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=("--dry-run")
  fi

  if [[ "$TOOL" == "warp" ]]; then
    cmd+=("--warp-bin" "$WARP_BIN" "--warp-mode" "$WARP_MODE" "--duration" "$DURATION")
  else
    cmd+=("--s3bench-bin" "$S3BENCH_BIN" "--samples" "$SAMPLES")
  fi
  if [[ "${#EXTRA_ARGS[@]}" -gt 0 ]]; then
    local joined
    joined="$(printf '%s ' "${EXTRA_ARGS[@]}" | sed 's/[[:space:]]*$//')"
    cmd+=("--extra-args" "$joined")
  fi

  echo
  echo "===== Running group ${g} ====="
  echo "Sizes: $sizes"
  echo "Output: $out_dir"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] %q ' "${cmd[@]}"
    printf '\n'
  else
    "${cmd[@]}"
  fi
}

main() {
  parse_args "$@"
  validate_args
  require_cmd awk
  require_cmd sed
  if [[ ! -x "$ENHANCED_SCRIPT" ]]; then
    echo "ERROR: enhanced script missing or not executable: $ENHANCED_SCRIPT" >&2
    exit 1
  fi
  setup_out_root

  echo "Controller output root: $OUT_ROOT"
  echo "Tool=$TOOL Group=$GROUP Concurrency=$CONCURRENCY Rounds=$ROUNDS"

  case "$GROUP" in
    all)
      run_group A
      run_group B
      run_group C
      ;;
    A|B|C)
      run_group "$GROUP"
      ;;
  esac

  echo
  echo "Done. Group outputs are under: $OUT_ROOT"
}

main "$@"
