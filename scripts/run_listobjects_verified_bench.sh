#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/bench/listobjects-verified-$(date +%Y%m%d-%H%M%S)}"
DATA_ROOT="${DATA_ROOT:-/private/tmp/listobjects-verified-$(date +%Y%m%d-%H%M%S)}"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"
RUSTFS_BIN_USER_SET="false"
if [[ -n "${RUSTFS_BIN:-}" ]]; then
  RUSTFS_BIN_USER_SET="true"
else
  RUSTFS_BIN="${PROJECT_ROOT}/target/${BUILD_PROFILE}/rustfs"
fi
WARP_BIN="${WARP_BIN:-warp}"
ADDRESS="${ADDRESS:-127.0.0.1:9000}"
ENDPOINT="${ENDPOINT:-http://${ADDRESS}}"
ACCESS_KEY="${ACCESS_KEY:-minioadmin}"
SECRET_KEY="${SECRET_KEY:-minioadmin}"
REGION="${REGION:-us-east-1}"
OBJECTS="${OBJECTS:-10000}"
OBJECT_SIZE="${OBJECT_SIZE:-1KB}"
CONCURRENCY="${CONCURRENCY:-16}"
MAX_KEYS="${MAX_KEYS:-1000}"
DURATION="${DURATION:-20s}"
WARMUP_DURATION="${WARMUP_DURATION:-5s}"
METER_INTERVAL="${METER_INTERVAL:-1}"
RESOURCE_SAMPLE_INTERVAL="${RESOURCE_SAMPLE_INTERVAL:-1}"
SKIP_BUILD="${SKIP_BUILD:-false}"
INDEX_PROVIDER="${INDEX_PROVIDER:-persistent_key_only}"
INDEX_GENERATION="${INDEX_GENERATION:-bench-$(date +%Y%m%d%H%M%S)}"
KEY_INDEX_PATH="${KEY_INDEX_PATH:-${OUT_DIR}/persistent-key-only.index}"
PROMETHEUS_QUERY_URL="${PROMETHEUS_QUERY_URL:-http://localhost:9090/api/v1/query}"
PROMETHEUS_JOB="${PROMETHEUS_JOB:-rustfs-app-metrics}"

SERVER_PID=""
RESOURCE_SAMPLER_PID=""

log_info() { printf '[INFO] %s\n' "$*"; }
log_warn() { printf '[WARN] %s\n' "$*" >&2; }
die() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_listobjects_verified_bench.sh [options]

Options:
  --out-dir <path>       Output directory.
  --data-root <path>     RustFS data root for d1..d4.
  --rustfs-bin <path>    RustFS binary path.
  --warp-bin <path>      warp binary path.
  --objects <n>          Objects prepared by warp list (default: 10000).
  --duration <dur>       Timed benchmark duration (default: 20s).
  --warmup-duration <d>  Warmup duration before timed run (default: 5s).
  --concurrency <n>      warp --concurrent value (default: 16).
  --max-keys <n>         warp --max-keys value (default: 1000).
  --release              Build and run target/release/rustfs.
  --skip-build           Use existing RustFS binary.
  --index-provider <v>   Opt-in provider (default: persistent_key_only).
  --key-index-path <p>   Persistent key-only index path.
  --prometheus-query-url <url>
                         Prometheus API query URL or UI /query URL.
  -h, --help             Show this help.

Environment:
  OUT_DIR, DATA_ROOT, RUSTFS_BIN, WARP_BIN, ADDRESS, ACCESS_KEY, SECRET_KEY,
  REGION, OBJECTS, OBJECT_SIZE, CONCURRENCY, MAX_KEYS, DURATION,
  WARMUP_DURATION, METER_INTERVAL, RESOURCE_SAMPLE_INTERVAL, SKIP_BUILD,
  BUILD_PROFILE, INDEX_PROVIDER, INDEX_GENERATION, KEY_INDEX_PATH,
  PROMETHEUS_QUERY_URL, PROMETHEUS_JOB.
USAGE
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "missing value for $flag"
  printf '%s\n' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --data-root) DATA_ROOT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$(arg_value "$1" "${2:-}")"; RUSTFS_BIN_USER_SET="true"; shift 2 ;;
      --warp-bin) WARP_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --objects) OBJECTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration) DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --warmup-duration) WARMUP_DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrency) CONCURRENCY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --max-keys) MAX_KEYS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --release)
        BUILD_PROFILE="release"
        if [[ "$RUSTFS_BIN_USER_SET" == "false" ]]; then
          RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
        fi
        shift
        ;;
      --skip-build) SKIP_BUILD="true"; shift ;;
      --index-provider) INDEX_PROVIDER="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --key-index-path) KEY_INDEX_PATH="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --prometheus-query-url) PROMETHEUS_QUERY_URL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

require_cmds() {
  command -v "$WARP_BIN" >/dev/null 2>&1 || die "warp binary not found: $WARP_BIN"
  command -v curl >/dev/null 2>&1 || die "curl not found"
  command -v python3 >/dev/null 2>&1 || die "python3 not found"
  command -v awk >/dev/null 2>&1 || die "awk not found"
  command -v sed >/dev/null 2>&1 || die "sed not found"
}

build_rustfs() {
  if [[ "$SKIP_BUILD" == "true" ]]; then
    [[ -x "$RUSTFS_BIN" ]] || die "RustFS binary is not executable: $RUSTFS_BIN"
    return
  fi
  if [[ "$BUILD_PROFILE" == "release" ]]; then
    (cd "$PROJECT_ROOT" && cargo build -p rustfs --release)
  else
    (cd "$PROJECT_ROOT" && cargo build -p rustfs)
  fi
  [[ -x "$RUSTFS_BIN" ]] || die "RustFS binary is not executable after build: $RUSTFS_BIN"
}

stop_server() {
  stop_resource_sampler
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  SERVER_PID=""
}

stop_resource_sampler() {
  if [[ -n "$RESOURCE_SAMPLER_PID" ]] && kill -0 "$RESOURCE_SAMPLER_PID" >/dev/null 2>&1; then
    kill "$RESOURCE_SAMPLER_PID" >/dev/null 2>&1 || true
    wait "$RESOURCE_SAMPLER_PID" >/dev/null 2>&1 || true
  fi
  RESOURCE_SAMPLER_PID=""
}

start_resource_sampler() {
  local out_file="$1"
  local pid="$SERVER_PID"
  echo "unix_ts,pid,cpu_percent,rss_kib" >"$out_file"
  (
    while kill -0 "$pid" >/dev/null 2>&1; do
      ps -p "$pid" -o pid=,%cpu=,rss= | awk -v ts="$(date +%s)" '{printf "%s,%s,%s,%s\n", ts, $1, $2, $3}'
      sleep "$RESOURCE_SAMPLE_INTERVAL"
    done
  ) >>"$out_file" 2>/dev/null &
  RESOURCE_SAMPLER_PID="$!"
}

wait_for_health() {
  local log_file="$1"
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if curl -fsS "${ENDPOINT}/health" >/dev/null 2>&1 || curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      tail -n 80 "$log_file" >&2 || true
      die "RustFS exited before health check passed"
    fi
    sleep 1
  done
  tail -n 80 "$log_file" >&2 || true
  die "RustFS health check timed out"
}

start_server() {
  local mode="$1"
  local log_file="$2"
  local reset_data="${3:-false}"

  stop_server
  if [[ "$reset_data" == "true" ]]; then
    rm -rf "$DATA_ROOT"
  fi
  mkdir -p "$DATA_ROOT"/d{1,2,3,4}

  (
    export RUST_LOG=off
    export RUSTFS_OBS_LOGGER_LEVEL=off
    export RUSTFS_OBS_TRACES_EXPORT_ENABLED=false
    export RUSTFS_OBS_METRICS_EXPORT_ENABLED=true
    export RUSTFS_OBS_LOGS_EXPORT_ENABLED=false
    export RUSTFS_OBS_PROFILING_EXPORT_ENABLED=false
    export RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318
    export RUSTFS_OBS_METRIC_ENDPOINT=http://127.0.0.1:4318/v1/metrics
    export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:4318/v1/metrics
    export RUSTFS_OBS_ENDPOINT_TIMEOUT_MILLIS=200
    export RUSTFS_OBS_USE_STDOUT=true
    export RUSTFS_OBS_METER_INTERVAL="$METER_INTERVAL"
    export RUSTFS_OBS_LOG_STDOUT_ENABLED=false
    export RUSTFS_SCANNER_ENABLED=false
    export RUSTFS_CONSOLE_ENABLE=false
    export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
    export RUSTFS_ADDRESS="$ADDRESS"
    export RUSTFS_ACCESS_KEY="$ACCESS_KEY"
    export RUSTFS_SECRET_KEY="$SECRET_KEY"
    export RUSTFS_RPC_SECRET="rustfs-listobjects-verified-rpc-secret"
    export RUSTFS_REGION="$REGION"
    unset RUSTFS_LIST_OBJECTS_INDEX_MODE
    unset RUSTFS_LIST_OBJECTS_INDEX_PROVIDER
    unset RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_PATH
    unset RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_GENERATION
    if [[ "$mode" == "opt_in_verified" ]]; then
      export RUSTFS_LIST_OBJECTS_INDEX_MODE=index_key_only
      export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER="$INDEX_PROVIDER"
      if [[ "$INDEX_PROVIDER" == "persistent_key_only" || "$INDEX_PROVIDER" == "persisted_key_only" ]]; then
        export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_PATH="$KEY_INDEX_PATH"
        export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_GENERATION="$INDEX_GENERATION"
      fi
    fi
    exec "$RUSTFS_BIN" server \
      "${DATA_ROOT}/d1" \
      "${DATA_ROOT}/d2" \
      "${DATA_ROOT}/d3" \
      "${DATA_ROOT}/d4"
  ) >"$log_file" 2>&1 &

  SERVER_PID="$!"
  wait_for_health "$log_file"
}

prepare_bucket() {
  local duration="$2"
  local log_file="$3"
  local bucket="$1"

  "$WARP_BIN" list \
    --host "$ADDRESS" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --region "$REGION" \
    --bucket "$bucket" \
    --objects "$OBJECTS" \
    --obj.size "$OBJECT_SIZE" \
    --concurrent "$CONCURRENCY" \
    --max-keys "$MAX_KEYS" \
    --duration "$duration" \
    --noclear \
    --no-color \
    >"$log_file" 2>&1
}

write_key_index() {
  local bucket="$1"
  local index_path="$2"
  local index_dir tmp_keys tmp_index token page response_file next_token
  index_dir="$(dirname "$index_path")"
  tmp_keys="${index_path}.keys.tmp"
  tmp_index="${index_path}.tmp"
  mkdir -p "$index_dir"
  : >"$tmp_keys"

  token=""
  page=0
  while true; do
    local url="${ENDPOINT}/${bucket}?list-type=2&max-keys=${MAX_KEYS}"
    if [[ -n "$token" ]]; then
      local encoded_token
      encoded_token="$(printf '%s' "$token" | urlencode)"
      url="${url}&continuation-token=${encoded_token}"
    fi

    response_file="${index_path}.page-${page}.xml"
    curl -fsS \
      --user "${ACCESS_KEY}:${SECRET_KEY}" \
      --aws-sigv4 "aws:amz:${REGION}:s3" \
      -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
      -o "$response_file" \
      "$url"
    extract_xml_keys <"$response_file" >>"$tmp_keys" || true
    next_token="$(extract_next_token <"$response_file" || true)"
    rm -f "$response_file"
    if [[ -z "$next_token" ]]; then
      break
    fi
    token="$next_token"
    page=$((page + 1))
  done

  {
    echo "# rustfs-listobjects-key-only-v1"
    echo "# generation=${INDEX_GENERATION}"
    LC_ALL=C sort -u "$tmp_keys"
  } >"$tmp_index"
  mv "$tmp_index" "$index_path"
  rm -f "$tmp_keys"
}

extract_xml_keys() {
  grep -o '<Key>[^<]*</Key>' | sed 's:<Key>::g; s:</Key>::g'
}

extract_next_token() {
  grep -o '<NextContinuationToken>[^<]*</NextContinuationToken>' | sed 's:<NextContinuationToken>::g; s:</NextContinuationToken>::g' | head -n 1
}

urlencode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))'
}

percentile_summary() {
  local values_file="$1"
  python3 - "$values_file" <<'PY'
import math
import statistics
import sys

path = sys.argv[1]
values = []
with open(path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line:
            values.append(float(line))

if not values:
    print("0,N/A,N/A,N/A,N/A")
    raise SystemExit(0)

values.sort()
def percentile(p: float) -> float:
    if len(values) == 1:
        return values[0]
    rank = math.ceil((p / 100.0) * len(values)) - 1
    rank = min(max(rank, 0), len(values) - 1)
    return values[rank]

print(
    f"{len(values)},"
    f"{statistics.fmean(values):.6f},"
    f"{percentile(50):.6f},"
    f"{percentile(95):.6f},"
    f"{percentile(99):.6f}"
)
PY
}

run_signed_list_bench() {
  local mode="$1"
  local bucket="$2"
  local duration="$3"
  local out_dir="$4"
  local deadline=$((SECONDS + ${duration%s}))
  local latencies_file="$out_dir/list-latencies-ms.txt"
  local request_log="$out_dir/list-requests.csv"
  local total_keys=0
  local total_requests=0
  local iterations=0

  : >"$latencies_file"
  echo "iteration,request,keys,latency_ms,truncated" >"$request_log"

  while (( SECONDS < deadline )); do
    local token=""
    local request_in_iteration=0
    iterations=$((iterations + 1))

    while (( SECONDS < deadline )); do
      local url="${ENDPOINT}/${bucket}?list-type=2&max-keys=${MAX_KEYS}"
      if [[ -n "$token" ]]; then
        local encoded_token
        encoded_token="$(printf '%s' "$token" | urlencode)"
        url="${url}&continuation-token=${encoded_token}"
      fi

      local response_file time_total status key_count next_token truncated
      response_file="$out_dir/response-${mode}-${iterations}-${request_in_iteration}.xml"
      time_total="$(curl -fsS \
        --user "${ACCESS_KEY}:${SECRET_KEY}" \
        --aws-sigv4 "aws:amz:${REGION}:s3" \
        -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
        -w '%{time_total}' \
        -o "$response_file" \
        "$url")"
      status=$?
      if [[ "$status" -ne 0 ]]; then
        log_warn "curl list failed for mode=$mode status=$status url=$url"
        return 1
      fi

      key_count="$(extract_xml_keys <"$response_file" | grep -cv '^$' || true)"
      next_token="$(extract_next_token <"$response_file" || true)"
      truncated="false"
      [[ -n "$next_token" ]] && truncated="true"

      awk -v sec="$time_total" 'BEGIN{printf "%.6f\n", sec * 1000.0}' >>"$latencies_file"
      echo "$iterations,$request_in_iteration,$key_count,$(awk -v sec="$time_total" 'BEGIN{printf "%.6f", sec * 1000.0}'),$truncated" >>"$request_log"

      total_keys=$((total_keys + key_count))
      total_requests=$((total_requests + 1))
      request_in_iteration=$((request_in_iteration + 1))

      if [[ -z "$next_token" ]]; then
        break
      fi
      token="$next_token"
    done
  done

  local percentile_line req_count avg_ms p50_ms p95_ms p99_ms objps
  percentile_line="$(percentile_summary "$latencies_file")"
  IFS=',' read -r req_count avg_ms p50_ms p95_ms p99_ms <<<"$percentile_line"
  objps="$(awk -v keys="$total_keys" -v duration="${duration%s}" 'BEGIN{if(duration<=0) print "N/A"; else printf "%.6f", keys/duration}')"

  {
    echo "mode,objects,object_size,max_keys,duration,requests,scan_iterations,total_keys,objps,latency_avg_ms,p50_ms,p95_ms,p99_ms"
    echo "$mode,$OBJECTS,$OBJECT_SIZE,$MAX_KEYS,$duration,$req_count,$iterations,$total_keys,$objps,$avg_ms,$p50_ms,$p95_ms,$p99_ms"
  } >"$out_dir/list-summary.csv"
}

last_metric_field_sum() {
  local metric="$1"
  local field="$2"
  local log_file="$3"
  awk -v metric="$metric" -v field="$field" '
    function finish_block() {
      if (found) {
        value=sum
        found=0
        sum=0
      }
    }
    /^Metric #[0-9]+/ {
      finish_block()
    }
    $0 ~ "Name[[:space:]]*:[[:space:]]*" metric "$" { found=1; sum=0; next }
    found && $0 ~ "^[[:space:]]*" field "[[:space:]]*:" {
      line=$0
      sub("^[[:space:]]*" field "[[:space:]]*:[[:space:]]*", "", line)
      gsub(/[^0-9.eE+-]/, "", line)
      sum += line + 0
    }
    END {
      finish_block()
      if (value == "") print "0"; else print value
    }
  ' "$log_file"
}

last_metric_value() {
  local metric="$1"
  local log_file="$2"
  last_metric_field_sum "$metric" "Value" "$log_file"
}

last_metric_sum() {
  local metric="$1"
  local log_file="$2"
  last_metric_field_sum "$metric" "Sum" "$log_file"
}

last_metric_count() {
  local metric="$1"
  local log_file="$2"
  last_metric_field_sum "$metric" "Count" "$log_file"
}

write_ratio_summary() {
  local mode="$1"
  local log_file="$2"
  local csv_file="$3"
  local attempt fallback served verify_attempts verify_misses returned_objects amplification_sum amplification_count amplification stale_ratio fallback_ratio

  attempt="$(last_metric_value rustfs_s3_list_objects_index_attempt_total "$log_file")"
  fallback="$(last_metric_value rustfs_s3_list_objects_index_fallback_total "$log_file")"
  served="$(last_metric_value rustfs_s3_list_objects_index_served_total "$log_file")"
  verify_attempts="$(last_metric_sum rustfs_s3_list_objects_index_live_verify_attempts "$log_file")"
  verify_misses="$(last_metric_sum rustfs_s3_list_objects_index_live_verify_misses "$log_file")"
  returned_objects="$(last_metric_sum rustfs_s3_list_objects_index_returned_objects "$log_file")"
  amplification_sum="$(last_metric_sum rustfs_s3_list_objects_index_verification_io_amplification "$log_file")"
  amplification_count="$(last_metric_count rustfs_s3_list_objects_index_verification_io_amplification "$log_file")"
  amplification="$(awk -v sum="$amplification_sum" -v count="$amplification_count" 'BEGIN{if(count+0==0) print "0"; else printf "%.6f", (sum+0)/(count+0)}')"

  fallback_ratio="$(awk -v f="$fallback" -v a="$attempt" 'BEGIN{if(a+0==0) print "N/A"; else printf "%.6f", (f+0)/(a+0)}')"
  stale_ratio="$(awk -v m="$verify_misses" -v a="$verify_attempts" 'BEGIN{if(a+0==0) print "N/A"; else printf "%.6f", (m+0)/(a+0)}')"

  {
    echo "mode,index_attempt_total,index_fallback_total,index_served_total,live_verify_attempts,live_verify_misses,returned_objects,verification_io_amplification,fallback_ratio,stale_missing_candidate_ratio"
    echo "$mode,$attempt,$fallback,$served,$verify_attempts,$verify_misses,$returned_objects,$amplification,$fallback_ratio,$stale_ratio"
  } >"$csv_file"
}

prometheus_api_query_url() {
  local url="$PROMETHEUS_QUERY_URL"
  if [[ "$url" == */api/v1/query ]]; then
    printf '%s\n' "$url"
  elif [[ "$url" == */query ]]; then
    printf '%s/api/v1/query\n' "${url%/query}"
  else
    printf '%s\n' "$url"
  fi
}

query_prometheus_expr() {
  local expr="$1"
  local out_file="$2"
  local api_url
  api_url="$(prometheus_api_query_url)"
  curl -fsS --get --data-urlencode "query=${expr}" "$api_url" -o "$out_file"
}

write_prometheus_snapshot() {
  local mode="$1"
  local out_dir="$2"
  local prom_dir="$out_dir/prometheus"
  mkdir -p "$prom_dir"

  query_prometheus_expr "sum(rustfs_s3_list_objects_index_attempt_total)" "$prom_dir/index_attempt_total.json" || {
    log_warn "Prometheus query failed; skipping Prometheus snapshot for mode=$mode"
    return 0
  }
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_fallback_total)" "$prom_dir/index_fallback_total.json" || true
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_served_total)" "$prom_dir/index_served_total.json" || true
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_live_verify_attempts_sum)" "$prom_dir/live_verify_attempts_sum.json" || true
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_live_verify_misses_sum)" "$prom_dir/live_verify_misses_sum.json" || true
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_returned_objects_sum)" "$prom_dir/returned_objects_sum.json" || true
  query_prometheus_expr "sum(rustfs_s3_list_objects_index_verification_io_amplification_sum) / sum(rustfs_s3_list_objects_index_verification_io_amplification_count)" "$prom_dir/verification_io_amplification_avg.json" || true
  query_prometheus_expr "rate(process_cpu_seconds_total{job=\"${PROMETHEUS_JOB}\"}[1m])" "$prom_dir/process_cpu_rate_1m.json" || true
  query_prometheus_expr "process_resident_memory_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/process_resident_memory_bytes.json" || true
}

write_resource_summary() {
  local samples_file="$1"
  local summary_file="$2"
  awk -F',' '
    NR == 1 { next }
    NF >= 4 {
      cpu += $3
      rss += $4
      if ($3 > max_cpu) max_cpu = $3
      if ($4 > max_rss) max_rss = $4
      count += 1
    }
    END {
      print "samples,avg_cpu_percent,max_cpu_percent,avg_rss_mib,max_rss_mib"
      if (count == 0) {
        print "0,N/A,N/A,N/A,N/A"
      } else {
        printf "%d,%.6f,%.6f,%.6f,%.6f\n", count, cpu / count, max_cpu, (rss / count) / 1024.0, max_rss / 1024.0
      }
    }
  ' "$samples_file" >"$summary_file"
}

run_mode() {
  local mode="$1"
  local bucket="$2"
  local reset_data="${3:-false}"
  local mode_dir="$OUT_DIR/$mode"
  mkdir -p "$mode_dir"

  log_info "Starting RustFS for mode=$mode"
  start_server "$mode" "$mode_dir/rustfs.log" "$reset_data"
  start_resource_sampler "$mode_dir/resource-samples.csv"

  log_info "Signed ListObjectsV2 bench for mode=$mode duration=$DURATION"
  run_signed_list_bench "$mode" "$bucket" "$DURATION" "$mode_dir"
  stop_resource_sampler
  sleep "$((METER_INTERVAL + 2))"
  write_ratio_summary "$mode" "$mode_dir/rustfs.log" "$mode_dir/index-ratios.csv"
  write_resource_summary "$mode_dir/resource-samples.csv" "$mode_dir/resource-summary.csv"
  write_prometheus_snapshot "$mode" "$mode_dir"
  stop_server
}

write_manifest() {
  mkdir -p "$OUT_DIR"
  {
    echo "out_dir=$OUT_DIR"
    echo "data_root=$DATA_ROOT"
    echo "build_profile=$BUILD_PROFILE"
    echo "rustfs_bin=$RUSTFS_BIN"
    echo "warp_bin=$WARP_BIN"
    echo "address=$ADDRESS"
    echo "objects=$OBJECTS"
    echo "object_size=$OBJECT_SIZE"
    echo "concurrency=$CONCURRENCY"
    echo "max_keys=$MAX_KEYS"
    echo "duration=$DURATION"
    echo "warmup_duration=$WARMUP_DURATION"
    echo "meter_interval=$METER_INTERVAL"
    echo "resource_sample_interval=$RESOURCE_SAMPLE_INTERVAL"
    echo "index_provider=$INDEX_PROVIDER"
    echo "index_generation=$INDEX_GENERATION"
    echo "key_index_path=$KEY_INDEX_PATH"
    echo "prometheus_query_url=$PROMETHEUS_QUERY_URL"
    echo "prometheus_job=$PROMETHEUS_JOB"
  } >"$OUT_DIR/manifest.env"
}

write_combined_summary() {
  local summary="$OUT_DIR/summary.md"
  {
    echo "# ListObjects Verified Benchmark Summary"
    echo
    echo "## Manifest"
    echo
    sed 's/^/- /' "$OUT_DIR/manifest.env"
    echo
    echo "## Warp"
    echo
    echo "### Walker"
    echo
    cat "$OUT_DIR/walker/list-summary.csv"
    echo
    echo "### Opt-in verified"
    echo
    cat "$OUT_DIR/opt_in_verified/list-summary.csv"
    echo
    echo "## Index Ratios"
    echo
    echo "### Walker"
    echo
    cat "$OUT_DIR/walker/index-ratios.csv"
    echo
    echo "### Opt-in verified"
    echo
    cat "$OUT_DIR/opt_in_verified/index-ratios.csv"
    echo
    echo "## Resource Samples"
    echo
    echo "### Walker"
    echo
    cat "$OUT_DIR/walker/resource-summary.csv"
    echo
    echo "### Opt-in verified"
    echo
    cat "$OUT_DIR/opt_in_verified/resource-summary.csv"
  } >"$summary"
}

main() {
  parse_args "$@"
  require_cmds
  write_manifest
  build_rustfs
  trap stop_server EXIT INT TERM

  local bucket="rustfs-listobjects-bench"
  local prep_dir="$OUT_DIR/prepare"
  mkdir -p "$prep_dir"

  log_info "Starting RustFS for data preparation"
  start_server walker "$prep_dir/rustfs.log" true
  log_info "Preparing bucket=$bucket objects=$OBJECTS with warp list"
  prepare_bucket "$bucket" "$WARMUP_DURATION" "$prep_dir/warp-prepare.log" || {
    tail -n 80 "$prep_dir/warp-prepare.log" >&2 || true
    return 1
  }
  log_info "Writing persistent key-only index: $KEY_INDEX_PATH"
  write_key_index "$bucket" "$KEY_INDEX_PATH"
  stop_server

  run_mode walker "$bucket" false
  run_mode opt_in_verified "$bucket" false
  write_combined_summary

  log_info "Summary: $OUT_DIR/summary.md"
}

main "$@"
