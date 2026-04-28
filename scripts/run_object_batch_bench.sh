#!/usr/bin/env bash
set -euo pipefail

# Batch object benchmark runner for warp/s3bench.
# Runs a fixed size matrix under the same concurrency and exports per-size logs + summary CSV.

DEFAULT_SIZES="1KiB,4KiB,8KiB,16KiB,32KiB,100KiB,512KiB,1MiB,2MiB,5MiB,10MiB"

TOOL="warp"
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
BUCKET="rustfs-bench"
REGION="us-east-1"
CONCURRENCY=128
DURATION="60s"
SAMPLES=20000
SIZES="$DEFAULT_SIZES"
OUT_DIR=""
WARP_BIN="warp"
WARP_MODE="mixed"
S3BENCH_BIN="s3bench"
INSECURE=false
DRY_RUN=false
EXTRA_ARGS=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_object_batch_bench.sh --tool <warp|s3bench> --endpoint <host:port|url> \
    --access-key <ak> --secret-key <sk> [options]

Required:
  --tool                warp | s3bench
  --endpoint            S3 endpoint
  --access-key          S3 access key
  --secret-key          S3 secret key

Optional:
  --bucket              Bucket name (default: rustfs-bench)
  --region              Region (default: us-east-1)
  --concurrency         Concurrency for all sizes (default: 128)
  --duration            warp duration, e.g. 60s/2m (default: 60s)
  --samples             s3bench numSamples (default: 20000)
  --sizes               Comma-separated sizes (default: 1KiB..10MiB matrix)
  --out-dir             Output directory (default: target/bench/object-batch-<timestamp>)
  --warp-bin            warp binary (default: warp)
  --warp-mode           warp mode: get|put|mixed (default: mixed)
  --s3bench-bin         s3bench binary (default: s3bench)
  --extra-args          Extra args appended to tool command, quoted as one string
  --insecure            For TLS endpoints with self-signed certs
  --dry-run             Print commands only
  -h, --help            Show help

Examples:
  # warp
  scripts/run_object_batch_bench.sh \
    --tool warp --endpoint http://127.0.0.1:9000 \
    --access-key minioadmin --secret-key minioadmin \
    --bucket bench-obj --concurrency 128 --duration 90s --warp-mode get

  # s3bench
  scripts/run_object_batch_bench.sh \
    --tool s3bench --endpoint http://127.0.0.1:9000 \
    --access-key minioadmin --secret-key minioadmin \
    --bucket bench-obj --concurrency 128 --samples 50000
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
      --region) REGION="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --samples) SAMPLES="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --warp-mode) WARP_MODE="$2"; shift 2 ;;
      --s3bench-bin) S3BENCH_BIN="$2"; shift 2 ;;
      --extra-args)
        # shellcheck disable=SC2206
        EXTRA_ARGS=($2)
        shift 2
        ;;
      --insecure) INSECURE=true; shift ;;
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
  if [[ "$TOOL" != "warp" && "$TOOL" != "s3bench" ]]; then
    echo "ERROR: --tool must be warp or s3bench" >&2
    exit 1
  fi
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint/--access-key/--secret-key are required" >&2
    exit 1
  fi
  if ! [[ "$CONCURRENCY" =~ ^[0-9]+$ ]] || [[ "$CONCURRENCY" -le 0 ]]; then
    echo "ERROR: --concurrency must be a positive integer" >&2
    exit 1
  fi
  if [[ "$TOOL" == "s3bench" ]]; then
    if ! [[ "$SAMPLES" =~ ^[0-9]+$ ]] || [[ "$SAMPLES" -le 0 ]]; then
      echo "ERROR: --samples must be a positive integer" >&2
      exit 1
    fi
  fi
  if [[ "$TOOL" == "warp" ]]; then
    local warp_host
    warp_host="$(normalize_warp_host "$ENDPOINT")"
    if [[ -z "$warp_host" ]]; then
      echo "ERROR: invalid --endpoint for warp: $ENDPOINT" >&2
      exit 1
    fi
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/object-batch-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
  SUMMARY_CSV="$OUT_DIR/summary.csv"
  echo "size,tool,concurrency,status,throughput,requests_per_sec,avg_latency,log_file" > "$SUMMARY_CSV"
}

extract_value() {
  local pattern="$1"
  local file="$2"
  rg -o "$pattern" "$file" | head -n1 || true
}

collect_metrics() {
  local log_file="$1"
  local throughput reqps latency
  throughput="$(extract_value '([0-9]+(\\.[0-9]+)?\\s*(GiB/s|MiB/s|MB/s|KB/s))' "$log_file")"
  reqps="$(extract_value '([0-9]+(\\.[0-9]+)?\\s*(req/s|ops/s|requests/s))' "$log_file")"
  latency="$(extract_value '([0-9]+(\\.[0-9]+)?\\s*(ms|us|µs|s))(\\s*(avg|mean))?' "$log_file")"
  echo "${throughput:-N/A},${reqps:-N/A},${latency:-N/A}"
}

run_one() {
  local size="$1"
  local log_file="$OUT_DIR/${TOOL}_${size}.log"
  local status="ok"

  echo "==== [$TOOL] size=$size concurrency=$CONCURRENCY ===="

  if [[ "$TOOL" == "warp" ]]; then
    local warp_host
    warp_host="$(normalize_warp_host "$ENDPOINT")"
    local cmd=(
      "$WARP_BIN" "$WARP_MODE"
      "--host" "$warp_host"
      "--access-key" "$ACCESS_KEY"
      "--secret-key" "$SECRET_KEY"
      "--bucket" "$BUCKET"
      "--obj.size" "$size"
      "--concurrent" "$CONCURRENCY"
      "--duration" "$DURATION"
      "--region" "$REGION"
    )
    if [[ "$INSECURE" == "true" ]]; then
      cmd+=("--insecure")
    fi
    if [[ ${EXTRA_ARGS[@]+_} ]]; then
      cmd+=("${EXTRA_ARGS[@]}")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "size=$size tool=$TOOL dry_run" > "$log_file"
    else
      if ! "${cmd[@]}" 2>&1 | tee "$log_file"; then
        status="failed"
      fi
    fi
  else
    local cmd=(
      "$S3BENCH_BIN"
      "-accessKey=$ACCESS_KEY"
      "-secretKey=$SECRET_KEY"
      "-bucket=$BUCKET"
      "-endpoint=$ENDPOINT"
      "-region=$REGION"
      "-numClients=$CONCURRENCY"
      "-numSamples=$SAMPLES"
      "-objectSize=$size"
    )
    if [[ "$INSECURE" == "true" ]]; then
      cmd+=("-insecure")
    fi
    if [[ ${EXTRA_ARGS[@]+_} ]]; then
      cmd+=("${EXTRA_ARGS[@]}")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "size=$size tool=$TOOL dry_run" > "$log_file"
    else
      if ! "${cmd[@]}" 2>&1 | tee "$log_file"; then
        status="failed"
      fi
    fi
  fi

  local metrics throughput reqps latency
  metrics="$(collect_metrics "$log_file")"
  throughput="$(echo "$metrics" | cut -d',' -f1)"
  reqps="$(echo "$metrics" | cut -d',' -f2)"
  latency="$(echo "$metrics" | cut -d',' -f3)"

  echo "$size,$TOOL,$CONCURRENCY,$status,$throughput,$reqps,$latency,$log_file" >> "$SUMMARY_CSV"
}

main() {
  parse_args "$@"
  validate_args
  require_cmd rg
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

  IFS=',' read -r -a size_arr <<< "$SIZES"
  for raw_size in "${size_arr[@]}"; do
    size="$(echo "$raw_size" | xargs)"
    if [[ -z "$size" ]]; then
      continue
    fi
    run_one "$size"
  done

  echo
  echo "Done. Summary:"
  cat "$SUMMARY_CSV"
}

main "$@"
