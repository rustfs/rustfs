#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

HOST="${HOST:-127.0.0.1:9000}"
ACCESS_KEY="${ACCESS_KEY:-}"
SECRET_KEY="${SECRET_KEY:-}"
BUCKET_PREFIX="${BUCKET_PREFIX:-issue712-gt1g-multipart-focus}"
LOOKUP="${LOOKUP:-path}"
DURATION="${DURATION:-10m}"
WARP_BIN="${WARP_BIN:-warp}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/bench/gt1g-multipart-server-path-$(date +%Y%m%d-%H%M%S)}"
DRY_RUN=false

DEFAULT_PROFILES="1g-64m-pc4,2g-128m-pc4"
PROFILES="${PROFILES:-$DEFAULT_PROFILES}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_gt1g_multipart_put_server_path_focus.sh --access-key <ak> --secret-key <sk> [options]

Required:
  --access-key <ak>
  --secret-key <sk>

Options:
  --host <host:port>              Default: 127.0.0.1:9000
  --bucket-prefix <prefix>        Default: issue712-gt1g-multipart-focus
  --lookup <path|dns>             Default: path
  --duration <dur>                Default: 10m
  --profiles <csv>                Default: 1g-64m-pc4,2g-128m-pc4
  --warp-bin <path>               Default: warp
  --out-dir <dir>                 Default: target/bench/gt1g-multipart-server-path-<timestamp>
  --dry-run
  -h, --help

Profiles:
  1g-64m-pc4      concurrent=4  parts=16  part.size=64MiB   part.concurrent=4
  2g-128m-pc4     concurrent=4  parts=16  part.size=128MiB  part.concurrent=4
  2g-256m-pc4     concurrent=4  parts=8   part.size=256MiB  part.concurrent=4
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --host) HOST="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$2"; shift 2 ;;
      --lookup) LOOKUP="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --profiles) PROFILES="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
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
  if [[ -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --access-key and --secret-key are required" >&2
    exit 1
  fi
}

setup_output() {
  mkdir -p "$OUT_DIR/logs" "$OUT_DIR/benchdata"
  SUMMARY_CSV="$OUT_DIR/summary.csv"
  COMMANDS_TXT="$OUT_DIR/commands.txt"
  MANIFEST_TXT="$OUT_DIR/run_manifest.txt"

  echo "profile,host,bucket,duration,concurrent,parts,part_size,part_concurrent,throughput_human,reqps,latency_human,log_file,benchdata_file,status" > "$SUMMARY_CSV"
  : > "$COMMANDS_TXT"
}

write_manifest() {
  {
    echo "created_at=$(date +%Y-%m-%dT%H:%M:%S%z)"
    echo "host=${HOST}"
    echo "bucket_prefix=${BUCKET_PREFIX}"
    echo "lookup=${LOOKUP}"
    echo "duration=${DURATION}"
    echo "profiles=${PROFILES}"
    echo "warp_bin=${WARP_BIN}"
    echo "dry_run=${DRY_RUN}"
    echo "access_key=REDACTED"
    echo "secret_key=REDACTED"
  } > "$MANIFEST_TXT"
}

profile_spec() {
  case "$1" in
    1g-64m-pc4) echo "4|16|64MiB|4" ;;
    2g-128m-pc4) echo "4|16|128MiB|4" ;;
    2g-256m-pc4) echo "4|8|256MiB|4" ;;
    *)
      echo "ERROR: unknown profile: $1" >&2
      exit 1
      ;;
  esac
}

extract_first() {
  local regex="$1"
  local file="$2"
  rg -o "$regex" "$file" | head -n1 || true
}

extract_metrics() {
  local log_file="$1"
  local throughput reqps latency

  throughput="$(extract_first '[0-9]+(\.[0-9]+)?[[:space:]]*(GiB/s|MiB/s|KiB/s|GB/s|MB/s|KB/s|B/s)' "$log_file")"
  reqps="$(extract_first '[0-9]+(\.[0-9]+)?[[:space:]]*(obj/s|req/s|ops/s|requests/s)' "$log_file")"
  latency="$(rg -o 'Reqs:[[:space:]]+Avg:[[:space:]]+[0-9]+(\.[0-9]+)?(ms|us|µs|s)' "$log_file" | head -n1 | sed -E 's/^Reqs:[[:space:]]+Avg:[[:space:]]+//')"

  throughput="$(trim "${throughput:-N/A}")"
  reqps="$(trim "${reqps:-N/A}")"
  latency="$(trim "${latency:-N/A}")"
  reqps="$(echo "$reqps" | awk '{print $1}')"

  echo "${throughput},${reqps:-N/A},${latency}"
}

run_profile() {
  local profile="$1"
  local spec concurrent parts part_size part_concurrent
  local bucket log_file benchdata_file status metrics throughput reqps latency

  spec="$(profile_spec "$profile")"
  IFS='|' read -r concurrent parts part_size part_concurrent <<< "$spec"

  bucket="$(echo "${BUCKET_PREFIX}-${profile}-$(date +%Y%m%d%H%M%S)" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g; s/-{2,}/-/g; s/^-+//; s/-+$//')"
  bucket="${bucket:0:63}"
  log_file="$OUT_DIR/logs/${profile}.log"
  benchdata_file="$OUT_DIR/benchdata/${profile}.csv.zst"

  local -a cmd=(
    "$WARP_BIN" multipart-put
    --host "$HOST"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket "$bucket"
    --lookup "$LOOKUP"
    --duration "$DURATION"
    --concurrent "$concurrent"
    --parts "$parts"
    --part.size "$part_size"
    --part.concurrent "$part_concurrent"
    --benchdata "$benchdata_file"
    --analyze.v
    --no-color
  )

  printf '%q ' "${cmd[@]}" >> "$COMMANDS_TXT"
  printf '\n' >> "$COMMANDS_TXT"

  status="ok"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] %q ' "${cmd[@]}"
    printf '\n'
    : > "$log_file"
  else
    if ! "${cmd[@]}" >"$log_file" 2>&1; then
      status="failed"
    fi
  fi

  metrics="$(extract_metrics "$log_file")"
  throughput="$(echo "$metrics" | cut -d',' -f1)"
  reqps="$(echo "$metrics" | cut -d',' -f2)"
  latency="$(echo "$metrics" | cut -d',' -f3)"

  echo "${profile},${HOST},${bucket},${DURATION},${concurrent},${parts},${part_size},${part_concurrent},${throughput},${reqps},${latency},${log_file},${benchdata_file},${status}" >> "$SUMMARY_CSV"
}

main() {
  parse_args "$@"
  validate_args
  require_cmd "$WARP_BIN"
  require_cmd rg
  setup_output
  write_manifest

  echo "Output dir: $OUT_DIR"
  echo "Profiles: $PROFILES"

  IFS=',' read -r -a profiles <<< "$PROFILES"
  for raw in "${profiles[@]}"; do
    profile="$(trim "$raw")"
    [[ -z "$profile" ]] && continue
    run_profile "$profile"
  done

  echo
  echo "Summary:"
  cat "$SUMMARY_CSV"
}

main "$@"
