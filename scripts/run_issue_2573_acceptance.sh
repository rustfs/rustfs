#!/usr/bin/env bash
set -euo pipefail

# Issue 2573 acceptance runner
# Runs the key workload profiles discussed in docs/tasks/issue-2573/05-benchmark-and-acceptance.md
# and samples the RustFS process RSS during load and cooldown.

WARP_BIN="${WARP_BIN:-warp}"
HOST="${HOST:-http://127.0.0.1:9000}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
BUCKET="${BUCKET:-rustfs-issue-2573}"
REGION="${REGION:-us-east-1}"
CONCURRENCY="${CONCURRENCY:-30}"
DURATION="${DURATION:-60s}"
COOLDOWN_SECS="${COOLDOWN_SECS:-180}"
SAMPLE_SECS="${SAMPLE_SECS:-1}"
RUSTFS_PID="${RUSTFS_PID:-}"
OUT_DIR="${OUT_DIR:-target/bench/issue-2573-acceptance-$(date +%Y%m%d-%H%M%S)}"
INSECURE="${INSECURE:-false}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_issue_2573_acceptance.sh [options]

Options:
  --warp-bin <path>        warp binary (default: warp)
  --host <url>             S3 endpoint (default: http://127.0.0.1:9000)
  --access-key <ak>        access key (default: minioadmin)
  --secret-key <sk>        secret key (default: minioadmin)
  --bucket <name>          bucket name (default: rustfs-issue-2573)
  --region <name>          region (default: us-east-1)
  --concurrency <n>        warp concurrency (default: 30)
  --duration <dur>         warp duration per profile (default: 60s)
  --cooldown-secs <n>      cooldown sampling after each profile (default: 180)
  --sample-secs <n>        RSS sample interval seconds (default: 1)
  --pid <pid>              rustfs process pid (default: auto-detect by pgrep)
  --out-dir <dir>          output directory
  --insecure               pass --insecure to warp
  -h, --help               show help

Profiles executed:
  1. 4KiB mixed
  2. 11MiB mixed
  3. 11MiB delete
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --host) HOST="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$2"; shift 2 ;;
      --sample-secs) SAMPLE_SECS="$2"; shift 2 ;;
      --pid) RUSTFS_PID="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

resolve_pid() {
  if [[ -n "$RUSTFS_PID" ]]; then
    echo "$RUSTFS_PID"
    return
  fi

  local pid
  pid="$(pgrep -n rustfs || true)"
  if [[ -z "$pid" ]]; then
    echo "ERROR: failed to auto-detect rustfs pid; pass --pid" >&2
    exit 1
  fi
  echo "$pid"
}

sample_rss_loop() {
  local pid="$1"
  local out_file="$2"
  local started_at
  started_at="$(date +%s)"

  echo "timestamp,elapsed_seconds,rss_kib,vsz_kib" > "$out_file"
  while kill -0 "$pid" >/dev/null 2>&1; do
    local now elapsed sample
    now="$(date +%s)"
    elapsed="$((now - started_at))"
    sample="$(ps -o rss=,vsz= -p "$pid" | awk 'NF>=2 {print $1","$2}')"
    if [[ -n "$sample" ]]; then
      echo "$(date +%Y-%m-%dT%H:%M:%S),${elapsed},${sample}" >> "$out_file"
    fi
    sleep "$SAMPLE_SECS"
  done
}

sample_rss_window() {
  local pid="$1"
  local seconds="$2"
  local out_file="$3"
  local started_at deadline
  started_at="$(date +%s)"
  deadline="$((started_at + seconds))"

  echo "timestamp,elapsed_seconds,rss_kib,vsz_kib" > "$out_file"
  while true; do
    local now elapsed sample
    now="$(date +%s)"
    if (( now > deadline )); then
      break
    fi
    elapsed="$((now - started_at))"
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sample="$(ps -o rss=,vsz= -p "$pid" | awk 'NF>=2 {print $1","$2}')"
    if [[ -n "$sample" ]]; then
      echo "$(date +%Y-%m-%dT%H:%M:%S),${elapsed},${sample}" >> "$out_file"
    fi
    sleep "$SAMPLE_SECS"
  done
}

run_profile() {
  local profile_name="$1"
  local mode="$2"
  local obj_size="$3"
  local pid="$4"
  local benchdata="$OUT_DIR/${profile_name// /-}"
  local warp_log="$OUT_DIR/${profile_name// /-}.warp.log"
  local rss_during="$OUT_DIR/${profile_name// /-}.rss_during.csv"
  local rss_cooldown="$OUT_DIR/${profile_name// /-}.rss_cooldown.csv"

  local -a cmd=(
    "$WARP_BIN" "$mode"
    "--host" "$HOST"
    "--access-key" "$ACCESS_KEY"
    "--secret-key" "$SECRET_KEY"
    "--bucket" "$BUCKET"
    "--region" "$REGION"
    "--obj.size" "$obj_size"
    "--concurrent" "$CONCURRENCY"
    "--duration" "$DURATION"
    "--benchdata" "$benchdata"
  )
  if [[ "$INSECURE" == "true" ]]; then
    cmd+=("--insecure")
  fi

  echo "==== Running profile: $profile_name ===="
  printf 'Command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  sample_rss_window "$pid" "$((COOLDOWN_SECS + 1))" /dev/null >/dev/null 2>&1 || true
  sample_rss_loop "$pid" "$rss_during" &
  local sampler_pid=$!
  if ! "${cmd[@]}" 2>&1 | tee "$warp_log"; then
    echo "ERROR: profile failed: $profile_name" >&2
    kill "$sampler_pid" >/dev/null 2>&1 || true
    wait "$sampler_pid" >/dev/null 2>&1 || true
    exit 1
  fi
  kill "$sampler_pid" >/dev/null 2>&1 || true
  wait "$sampler_pid" >/dev/null 2>&1 || true

  echo "==== Cooldown sampling: $profile_name ($COOLDOWN_SECS s) ===="
  sample_rss_window "$pid" "$COOLDOWN_SECS" "$rss_cooldown"
}

main() {
  parse_args "$@"
  require_cmd "$WARP_BIN"
  require_cmd ps
  require_cmd pgrep
  mkdir -p "$OUT_DIR"

  local pid
  pid="$(resolve_pid)"

  echo "Output dir: $OUT_DIR"
  echo "RustFS pid: $pid"
  echo "Host: $HOST"
  echo "Bucket: $BUCKET"
  echo "Profiles:"
  echo "  - 4KiB mixed"
  echo "  - 11MiB mixed"
  echo "  - 11MiB delete"

  run_profile "4KiB mixed" "mixed" "4KiB" "$pid"
  run_profile "11MiB mixed" "mixed" "11MiB" "$pid"
  run_profile "11MiB delete" "delete" "11MiB" "$pid"

  echo
  echo "Acceptance run finished."
  echo "Artifacts:"
  find "$OUT_DIR" -maxdepth 1 -type f | sort
}

main "$@"
