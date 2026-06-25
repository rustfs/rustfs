#!/usr/bin/env bash
set -euo pipefail

ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
BUCKET=""
OBJECTS=""
REGION="us-east-1"
MODES="sequential,ranged_parallel"
CONCURRENCIES="1,4,8"
RANGE_WORKERS="4"
ROUNDS=3
COOLDOWN_SECS=15
PRESIGN_EXPIRY="2h"
OUT_DIR=""
MC_BIN="${MC_BIN:-mc}"
CURL_BIN="${CURL_BIN:-curl}"
JQ_BIN="${JQ_BIN:-jq}"
INSECURE=false
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_gt1g_get_http_matrix.sh \
    --endpoint <url> --access-key <ak> --secret-key <sk> \
    --bucket <bucket> --objects <csv> [options]

Required:
  --endpoint <url>
  --access-key <ak>
  --secret-key <sk>
  --bucket <bucket>
  --objects <csv>               Format: label=object-key,label=object-key

Options:
  --region <name>               Default: us-east-1
  --modes <csv>                 Default: sequential,ranged_parallel
  --concurrencies <csv>         Default: 1,4,8
  --range-workers <csv>         Default: 4
                                 Used only by ranged_parallel mode
  --rounds <n>                  Default: 3
  --cooldown-secs <n>           Default: 15
  --presign-expiry <dur>        Default: 2h
  --out-dir <dir>               Default: target/bench/gt1g-get-http-<timestamp>
  --mc-bin <path>               Default: mc
  --curl-bin <path>             Default: curl
  --jq-bin <path>               Default: jq
  --insecure                    Allow insecure TLS
  --dry-run
  -h, --help

Examples:
  scripts/run_gt1g_get_http_matrix.sh \
    --endpoint http://127.0.0.1:9000 \
    --access-key rustfsadmin \
    --secret-key rustfsadmin \
    --bucket rustfs-bench \
    --objects '1GiB=bench/issue713/plain-1g.bin,2GiB=bench/issue713/plain-2g.bin'
USAGE
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "ERROR: missing value for $flag" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

run_rust_helper_fallback() {
  local helper_out_dir="$OUT_DIR"
  if [[ -n "$helper_out_dir" && "$helper_out_dir" != /* ]]; then
    helper_out_dir="$PWD/$helper_out_dir"
  fi

  echo "mc not found; falling back to rustfs/tests/gt1g_get_benchmark_tool.rs"

  GT1G_GET_ACTION=bench \
  GT1G_GET_ENDPOINT="$ENDPOINT" \
  GT1G_GET_ACCESS_KEY="$ACCESS_KEY" \
  GT1G_GET_SECRET_KEY="$SECRET_KEY" \
  GT1G_GET_BUCKET="$BUCKET" \
  GT1G_GET_REGION="$REGION" \
  GT1G_GET_OBJECTS="$OBJECTS" \
  GT1G_GET_MODES="$MODES" \
  GT1G_GET_CONCURRENCIES="$CONCURRENCIES" \
  GT1G_GET_RANGE_WORKERS="$RANGE_WORKERS" \
  GT1G_GET_ROUNDS="$ROUNDS" \
  GT1G_GET_COOLDOWN_SECS="$COOLDOWN_SECS" \
  GT1G_GET_OUT_DIR="$helper_out_dir" \
  cargo test -p rustfs --test gt1g_get_benchmark_tool gt1g_get_benchmark_tool -- --ignored --nocapture
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key) SECRET_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket) BUCKET="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --objects) OBJECTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --modes) MODES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --range-workers) RANGE_WORKERS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rounds) ROUNDS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --presign-expiry) PRESIGN_EXPIRY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-bin) MC_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --curl-bin) CURL_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --jq-bin) JQ_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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

validate_positive_int() {
  local value="$1"
  local label="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value <= 0 )); then
    echo "ERROR: $label must be a positive integer, got: $value" >&2
    exit 1
  fi
}

validate_nonnegative_int() {
  local value="$1"
  local label="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "ERROR: $label must be a nonnegative integer, got: $value" >&2
    exit 1
  fi
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/gt1g-get-http-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  LOG_DIR="${OUT_DIR}/logs"
  mkdir -p "$LOG_DIR"
  ROUND_CSV="${OUT_DIR}/round_results.csv"
  MEDIAN_CSV="${OUT_DIR}/median_summary.csv"
  cat > "$ROUND_CSV" <<'EOF'
object_label,object_key,mode,range_workers,concurrency,round,status,total_bytes,elapsed_ms,throughput_bps,avg_client_latency_ms,log_file
EOF
  cat > "$MEDIAN_CSV" <<'EOF'
object_label,object_key,mode,range_workers,concurrency,successful_rounds,failed_rounds,median_total_bytes,median_elapsed_ms,median_throughput_bps,median_avg_client_latency_ms
EOF
  cat > "${OUT_DIR}/artifact_layout.txt" <<'EOF'
round_results.csv
median_summary.csv
artifact_layout.txt
logs/
- <label>_<mode>_rw<workers>_c<concurrency>_r<round>.log
EOF
}

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%.0f\n", time()*1000'
}

csv_to_lines() {
  local csv="$1"
  local raw item
  IFS=',' read -r -a parts <<< "$csv"
  for raw in "${parts[@]}"; do
    item="$(trim "$raw")"
    [[ -z "$item" ]] && continue
    echo "$item"
  done
}

cooldown_sleep() {
  local secs="$1"
  if (( secs <= 0 )); then
    return 0
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] sleep ${secs}s"
  else
    sleep "$secs"
  fi
}

mc_cmd() {
  if [[ "$INSECURE" == "true" ]]; then
    "$MC_BIN" --config-dir "$MC_CONFIG_DIR" --insecure "$@"
  else
    "$MC_BIN" --config-dir "$MC_CONFIG_DIR" "$@"
  fi
}

setup_mc_alias() {
  MC_CONFIG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/issue713-gt1g-get-mc.XXXXXX")"
  trap 'rm -rf "$MC_CONFIG_DIR"' EXIT
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] mc alias set issue713 ${ENDPOINT} REDACTED REDACTED"
  else
    mc_cmd alias set issue713 "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
  fi
}

get_object_size_bytes() {
  local object_key="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo 1073741824
    return
  fi
  mc_cmd stat --json "issue713/${BUCKET}/${object_key}" | "$JQ_BIN" -r '.size'
}

presign_object_url() {
  local object_key="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "${ENDPOINT%/}/${BUCKET}/${object_key}"
    return
  fi
  mc_cmd share download --json --expire "$PRESIGN_EXPIRY" "issue713/${BUCKET}/${object_key}" \
    | "$JQ_BIN" -r '.share // .url // .download // empty'
}

sum_metric_files() {
  local total=0 value
  for metric_file in "$@"; do
    [[ -f "$metric_file" ]] || continue
    value="$(tr -d '\r\n' < "$metric_file")"
    [[ -z "$value" ]] && continue
    total="$(awk -v a="$total" -v b="$value" 'BEGIN { printf "%.0f\n", a + b }')"
  done
  echo "$total"
}

run_sequential_round() {
  local url="$1"
  local object_size="$2"
  local concurrency="$3"
  local log_file="$4"
  local object_label="$5"
  local round="$6"

  local -a pids=()
  local -a metric_files=()
  local task_index metric_file
  local curl_rc=0
  local start_ms end_ms elapsed_ms total_bytes avg_latency_ms throughput_bps expected_total_bytes

  start_ms="$(now_ms)"
  for ((task_index = 1; task_index <= concurrency; task_index++)); do
    metric_file="${LOG_DIR}/${object_label}_sequential_c${concurrency}_r${round}_t${task_index}.bytes"
    metric_files+=("$metric_file")
    (
      if [[ "$INSECURE" == "true" ]]; then
        "$CURL_BIN" -fsSL -k "$url" -o /dev/null -w '%{size_download}\n' > "$metric_file"
      else
        "$CURL_BIN" -fsSL "$url" -o /dev/null -w '%{size_download}\n' > "$metric_file"
      fi
    ) >>"$log_file" 2>&1 &
    pids+=("$!")
  done

  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      curl_rc=1
    fi
  done

  end_ms="$(now_ms)"
  elapsed_ms=$((end_ms - start_ms))
  total_bytes="$(sum_metric_files "${metric_files[@]}")"
  expected_total_bytes=$((object_size * concurrency))
  avg_latency_ms="$(awk -v elapsed="$elapsed_ms" -v conc="$concurrency" 'BEGIN { printf "%.3f\n", elapsed / conc }')"
  throughput_bps="$(awk -v bytes="$total_bytes" -v elapsed="$elapsed_ms" 'BEGIN { if (elapsed <= 0) printf "0.000000\n"; else printf "%.6f\n", bytes / (elapsed / 1000.0) }')"

  if [[ "$curl_rc" -ne 0 || "$total_bytes" -ne "$expected_total_bytes" ]]; then
    echo "failed,${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms}"
  else
    echo "ok,${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms}"
  fi

  rm -f "${metric_files[@]}"
}

run_ranged_parallel_round() {
  local url="$1"
  local object_size="$2"
  local concurrency="$3"
  local range_workers="$4"
  local log_file="$5"
  local object_label="$6"
  local round="$7"

  local chunk_size=$(( (object_size + range_workers - 1) / range_workers ))
  local -a pids=()
  local -a metric_files=()
  local curl_rc=0
  local start_ms end_ms elapsed_ms total_bytes avg_latency_ms throughput_bps expected_total_bytes
  local client_index worker_index range_start range_end metric_file

  start_ms="$(now_ms)"
  for ((client_index = 1; client_index <= concurrency; client_index++)); do
    for ((worker_index = 0; worker_index < range_workers; worker_index++)); do
      range_start=$((worker_index * chunk_size))
      if (( range_start >= object_size )); then
        continue
      fi
      range_end=$((range_start + chunk_size - 1))
      if (( range_end >= object_size )); then
        range_end=$((object_size - 1))
      fi
      metric_file="${LOG_DIR}/${object_label}_ranged_rw${range_workers}_c${concurrency}_r${round}_t${client_index}_w${worker_index}.bytes"
      metric_files+=("$metric_file")
      (
        if [[ "$INSECURE" == "true" ]]; then
          "$CURL_BIN" -fsSL -k -H "Range: bytes=${range_start}-${range_end}" "$url" -o /dev/null -w '%{size_download}\n' > "$metric_file"
        else
          "$CURL_BIN" -fsSL -H "Range: bytes=${range_start}-${range_end}" "$url" -o /dev/null -w '%{size_download}\n' > "$metric_file"
        fi
      ) >>"$log_file" 2>&1 &
      pids+=("$!")
    done
  done

  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      curl_rc=1
    fi
  done

  end_ms="$(now_ms)"
  elapsed_ms=$((end_ms - start_ms))
  total_bytes="$(sum_metric_files "${metric_files[@]}")"
  expected_total_bytes=$((object_size * concurrency))
  avg_latency_ms="$(awk -v elapsed="$elapsed_ms" -v conc="$concurrency" 'BEGIN { printf "%.3f\n", elapsed / conc }')"
  throughput_bps="$(awk -v bytes="$total_bytes" -v elapsed="$elapsed_ms" 'BEGIN { if (elapsed <= 0) printf "0.000000\n"; else printf "%.6f\n", bytes / (elapsed / 1000.0) }')"

  if [[ "$curl_rc" -ne 0 || "$total_bytes" -ne "$expected_total_bytes" ]]; then
    echo "failed,${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms}"
  else
    echo "ok,${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms}"
  fi

  rm -f "${metric_files[@]}"
}

median_from_numbers() {
  local values="$1"
  local count
  count="$(printf '%s\n' "$values" | awk 'NF { c++ } END { print c + 0 }')"
  if [[ "$count" -eq 0 ]]; then
    echo "N/A"
    return
  fi

  printf '%s\n' "$values" | awk 'NF' | sort -n | awk '
    { a[NR] = $1 }
    END {
      n = NR
      if (n == 0) { print "N/A"; exit }
      if (n % 2 == 1) {
        printf "%.6f\n", a[(n + 1) / 2]
      } else {
        printf "%.6f\n", (a[n / 2] + a[n / 2 + 1]) / 2
      }
    }'
}

build_median_summary() {
  local object_label object_key mode range_workers concurrency
  while IFS=',' read -r object_label object_key mode range_workers concurrency; do
    local ok_rounds fail_rounds bytes_vals elapsed_vals throughput_vals latency_vals
    ok_rounds="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7=="ok" {count++} END{print count+0}' "$ROUND_CSV")"
    fail_rounds="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7!="ok" {count++} END{print count+0}' "$ROUND_CSV")"

    bytes_vals="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7=="ok" {print $8}' "$ROUND_CSV")"
    elapsed_vals="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7=="ok" {print $9}' "$ROUND_CSV")"
    throughput_vals="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7=="ok" {print $10}' "$ROUND_CSV")"
    latency_vals="$(awk -F',' -v l="$object_label" -v o="$object_key" -v m="$mode" -v rw="$range_workers" -v c="$concurrency" 'NR>1 && $1==l && $2==o && $3==m && $4==rw && $5==c && $7=="ok" {print $11}' "$ROUND_CSV")"

    echo "${object_label},${object_key},${mode},${range_workers},${concurrency},${ok_rounds},${fail_rounds},$(median_from_numbers "$bytes_vals"),$(median_from_numbers "$elapsed_vals"),$(median_from_numbers "$throughput_vals"),$(median_from_numbers "$latency_vals")" \
      >> "$MEDIAN_CSV"
  done < <(
    awk -F',' 'NR>1 { print $1 "," $2 "," $3 "," $4 "," $5 }' "$ROUND_CSV" | sort -u
  )
}

main() {
  parse_args "$@"
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" || -z "$BUCKET" || -z "$OBJECTS" ]]; then
    echo "ERROR: --endpoint, --access-key, --secret-key, --bucket, and --objects are required" >&2
    exit 1
  fi
  validate_positive_int "$ROUNDS" "--rounds"
  validate_nonnegative_int "$COOLDOWN_SECS" "--cooldown-secs"
  if ! command -v "$MC_BIN" >/dev/null 2>&1; then
    setup_output
    run_rust_helper_fallback
    exit 0
  fi

  require_cmd "$MC_BIN"
  require_cmd "$CURL_BIN"
  require_cmd "$JQ_BIN"
  require_cmd awk
  require_cmd sort
  require_cmd perl

  setup_output
  setup_mc_alias

  IFS=',' read -r -a object_specs <<< "$OBJECTS"
  for raw_object_spec in "${object_specs[@]}"; do
    local object_spec object_label object_key object_size presigned_url
    object_spec="$(trim "$raw_object_spec")"
    [[ -z "$object_spec" ]] && continue
    object_label="${object_spec%%=*}"
    object_key="${object_spec#*=}"
    if [[ -z "$object_label" || -z "$object_key" || "$object_label" == "$object_key" ]]; then
      echo "ERROR: invalid object spec: $object_spec" >&2
      exit 1
    fi

    object_size="$(get_object_size_bytes "$object_key")"
    presigned_url="$(presign_object_url "$object_key")"
    if [[ -z "$presigned_url" ]]; then
      echo "ERROR: failed to generate presigned URL for ${object_key}" >&2
      exit 1
    fi

    for mode in $(csv_to_lines "$MODES"); do
      for concurrency in $(csv_to_lines "$CONCURRENCIES"); do
        validate_positive_int "$concurrency" "--concurrencies item"

        if [[ "$mode" == "sequential" ]]; then
          local round result status total_bytes elapsed_ms throughput_bps avg_latency_ms log_file
          for ((round = 1; round <= ROUNDS; round++)); do
            log_file="${LOG_DIR}/${object_label}_${mode}_rw1_c${concurrency}_r${round}.log"
            result="$(run_sequential_round "$presigned_url" "$object_size" "$concurrency" "$log_file" "$object_label" "$round")"
            IFS=',' read -r status total_bytes elapsed_ms throughput_bps avg_latency_ms <<< "$result"
            echo "${object_label},${object_key},${mode},1,${concurrency},${round},${status},${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms},${log_file}" >> "$ROUND_CSV"
            if (( COOLDOWN_SECS > 0 && round < ROUNDS )); then
              cooldown_sleep "$COOLDOWN_SECS"
            fi
          done
          continue
        fi

        if [[ "$mode" == "ranged_parallel" ]]; then
          for range_workers in $(csv_to_lines "$RANGE_WORKERS"); do
            validate_positive_int "$range_workers" "--range-workers item"
            local round result status total_bytes elapsed_ms throughput_bps avg_latency_ms log_file
            for ((round = 1; round <= ROUNDS; round++)); do
              log_file="${LOG_DIR}/${object_label}_${mode}_rw${range_workers}_c${concurrency}_r${round}.log"
              result="$(run_ranged_parallel_round "$presigned_url" "$object_size" "$concurrency" "$range_workers" "$log_file" "$object_label" "$round")"
              IFS=',' read -r status total_bytes elapsed_ms throughput_bps avg_latency_ms <<< "$result"
              echo "${object_label},${object_key},${mode},${range_workers},${concurrency},${round},${status},${total_bytes},${elapsed_ms},${throughput_bps},${avg_latency_ms},${log_file}" >> "$ROUND_CSV"
              if (( COOLDOWN_SECS > 0 && round < ROUNDS )); then
                cooldown_sleep "$COOLDOWN_SECS"
              fi
            done
          done
          continue
        fi

        echo "ERROR: unsupported mode: $mode" >&2
        exit 1
      done
    done
  done

  build_median_summary

  echo "Artifacts written to: $OUT_DIR"
  echo "Summaries:"
  echo "  - $ROUND_CSV"
  echo "  - $MEDIAN_CSV"
}

main "$@"
