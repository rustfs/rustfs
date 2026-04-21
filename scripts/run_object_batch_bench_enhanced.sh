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
REGION="us-east-1"
CONCURRENCY=128
SIZES="$DEFAULT_SIZES"
OUT_DIR=""
INSECURE=false
DRY_RUN=false

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
BASELINE_CSV=""
EXTRA_ARGS=()

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
  --region                     Region (default: us-east-1)
  --concurrency                Concurrency for all sizes (default: 128)
  --sizes                      Comma-separated sizes (default: 1KiB..10MiB matrix)
  --out-dir                    Output directory (default: target/bench/object-batch-enhanced-<timestamp>)
  --insecure                   Allow insecure TLS
  --dry-run                    Print commands only, do not execute

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
  --baseline-csv               Baseline median CSV to compare
  --extra-args                 Extra args appended to tool command, quoted as one string

Output files:
  round_results.csv            One row per round attempt (with retry trace)
  median_summary.csv           Median metrics per object size
  baseline_compare.csv         Delta vs baseline (if --baseline-csv is set)

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
      --sizes) SIZES="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --warp-mode) WARP_MODE="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --s3bench-bin) S3BENCH_BIN="$2"; shift 2 ;;
      --samples) SAMPLES="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --retry-sleep-secs) RETRY_SLEEP_SECS="$2"; shift 2 ;;
      --baseline-csv) BASELINE_CSV="$2"; shift 2 ;;
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
  if [[ "$TOOL" == "s3bench" ]]; then
    validate_positive_int "$SAMPLES" "--samples"
  fi
  if [[ -n "$BASELINE_CSV" && ! -f "$BASELINE_CSV" ]]; then
    echo "ERROR: --baseline-csv does not exist: $BASELINE_CSV" >&2
    exit 1
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/object-batch-enhanced-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR/logs"

  ROUND_CSV="$OUT_DIR/round_results.csv"
  MEDIAN_CSV="$OUT_DIR/median_summary.csv"
  COMPARE_CSV="$OUT_DIR/baseline_compare.csv"

  echo "size,tool,round,attempt,concurrency,status,throughput_human,throughput_bps,reqps,latency_human,latency_ms,log_file" > "$ROUND_CSV"
  echo "size,tool,concurrency,successful_rounds,failed_rounds,median_throughput_bps,median_reqps,median_latency_ms" > "$MEDIAN_CSV"
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

to_bps() {
  local human="$1"
  if [[ "$human" == "N/A" || -z "$human" ]]; then
    echo "N/A"
    return
  fi
  awk -v v="$human" '
    function abs(x){return x<0?-x:x}
    BEGIN{
      if (match(v, /^([0-9]+(\.[0-9]+)?)\s*(GiB\/s|MiB\/s|KiB\/s|GB\/s|MB\/s|KB\/s|B\/s)$/, m)) {
        n=m[1]; u=m[3];
        if (u=="GiB/s") f=1024*1024*1024;
        else if (u=="MiB/s") f=1024*1024;
        else if (u=="KiB/s") f=1024;
        else if (u=="GB/s") f=1000*1000*1000;
        else if (u=="MB/s") f=1000*1000;
        else if (u=="KB/s") f=1000;
        else f=1;
        printf "%.6f\n", n*f;
      } else {
        print "N/A";
      }
    }'
}

to_ms() {
  local human="$1"
  if [[ "$human" == "N/A" || -z "$human" ]]; then
    echo "N/A"
    return
  fi
  awk -v v="$human" '
    BEGIN{
      if (match(v, /^([0-9]+(\.[0-9]+)?)\s*(ms|us|µs|s)$/, m)) {
        n=m[1]; u=m[3];
        if (u=="s") f=1000;
        else if (u=="ms") f=1;
        else f=0.001;
        printf "%.6f\n", n*f;
      } else {
        print "N/A";
      }
    }'
}

extract_first() {
  local regex="$1"
  local file="$2"
  rg -o "$regex" "$file" | head -n1 || true
}

extract_metrics() {
  local log_file="$1"

  local throughput reqps latency
  throughput="$(extract_first '[0-9]+(\.[0-9]+)?\s*(GiB/s|MiB/s|KiB/s|GB/s|MB/s|KB/s|B/s)' "$log_file")"
  reqps="$(extract_first '[0-9]+(\.[0-9]+)?\s*(req/s|ops/s|requests/s)' "$log_file")"
  latency="$(extract_first '[0-9]+(\.[0-9]+)?\s*(ms|us|µs|s)\s*(avg|mean)' "$log_file")"

  if [[ -z "$latency" ]]; then
    latency="$(extract_first '[0-9]+(\.[0-9]+)?\s*(ms|us|µs|s)' "$log_file")"
  fi

  throughput="$(trim "${throughput:-N/A}")"
  reqps="$(trim "${reqps:-N/A}")"
  latency="$(trim "${latency:-N/A}")"

  # Keep only "<num> <unit>" for latency if suffix avg/mean exists.
  latency="$(echo "$latency" | awk '{print $1" "$2}')"
  reqps_num="$(echo "$reqps" | awk '{print $1}')"

  echo "$throughput,${reqps_num:-N/A},$latency"
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

  if [[ "$TOOL" == "warp" ]]; then
    local cmd=(
      "$WARP_BIN" "$WARP_MODE"
      "--host" "$ENDPOINT"
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
    cmd+=("${EXTRA_ARGS[@]}")

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "dry run" > "$log_file"
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
    cmd+=("${EXTRA_ARGS[@]}")

    if [[ "$DRY_RUN" == "true" ]]; then
      printf '[DRY-RUN] %q ' "${cmd[@]}"
      printf '\n'
      echo "dry run" > "$log_file"
    else
      if ! "${cmd[@]}" 2>&1 | tee "$log_file"; then
        status="failed"
      fi
    fi
  fi

  local metrics throughput_human reqps latency_human throughput_bps latency_ms
  metrics="$(extract_metrics "$log_file")"
  throughput_human="$(echo "$metrics" | cut -d',' -f1)"
  reqps="$(echo "$metrics" | cut -d',' -f2)"
  latency_human="$(echo "$metrics" | cut -d',' -f3)"
  throughput_bps="$(to_bps "$throughput_human")"
  latency_ms="$(to_ms "$latency_human")"

  if [[ "$DRY_RUN" != "true" && "$status" == "ok" ]]; then
    if [[ "$throughput_bps" == "N/A" && "$reqps" == "N/A" ]]; then
      status="failed"
    fi
  fi

  echo "$size,$TOOL,$round,$attempt,$CONCURRENCY,$status,$throughput_human,$throughput_bps,$reqps,$latency_human,$latency_ms,$log_file" >> "$ROUND_CSV"
  echo "$status"
}

run_size() {
  local size="$1"
  local round success attempt rc

  for ((round=1; round<=ROUNDS; round++)); do
    success="no"
    for ((attempt=1; attempt<=RETRY_PER_ROUND+1; attempt++)); do
      echo "==== size=$size round=$round attempt=$attempt/${RETRY_PER_ROUND+1} ===="
      rc="$(run_one_attempt "$size" "$round" "$attempt")"
      if [[ "$rc" == "ok" || "$DRY_RUN" == "true" ]]; then
        success="yes"
        break
      fi
      if (( attempt < RETRY_PER_ROUND+1 )); then
        echo "Round failed, retry in ${RETRY_SLEEP_SECS}s..."
        sleep "$RETRY_SLEEP_SECS"
      fi
    done

    if [[ "$success" == "no" ]]; then
      echo "WARN: size=$size round=$round failed after retries."
    fi
  done
}

build_median_summary() {
  local sizes_arr size
  IFS=',' read -r -a sizes_arr <<< "$SIZES"

  for raw in "${sizes_arr[@]}"; do
    size="$(trim "$raw")"
    [[ -z "$size" ]] && continue

    local ok_rounds fail_rounds t_vals r_vals l_vals
    ok_rounds="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" {c++} END{print c+0}' "$ROUND_CSV")"
    fail_rounds="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6!="ok" {c++} END{print c+0}' "$ROUND_CSV")"

    t_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $8!="N/A" {print $8}' "$ROUND_CSV")"
    r_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $9!="N/A" {print $9}' "$ROUND_CSV")"
    l_vals="$(awk -F',' -v s="$size" 'NR>1 && $1==s && $6=="ok" && $11!="N/A" {print $11}' "$ROUND_CSV")"

    local m_t m_r m_l
    m_t="$(median_from_numbers "$t_vals")"
    m_r="$(median_from_numbers "$r_vals")"
    m_l="$(median_from_numbers "$l_vals")"

    echo "$size,$TOOL,$CONCURRENCY,$ok_rounds,$fail_rounds,$m_t,$m_r,$m_l" >> "$MEDIAN_CSV"
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

  IFS=',' read -r -a size_arr <<< "$SIZES"
  for raw in "${size_arr[@]}"; do
    size="$(trim "$raw")"
    [[ -z "$size" ]] && continue
    run_size "$size"
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
}

main "$@"

