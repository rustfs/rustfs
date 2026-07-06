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
NAMESPACE_JOURNAL_PATH="${NAMESPACE_JOURNAL_PATH:-}"
PREBUILD_KEY_INDEX="${PREBUILD_KEY_INDEX:-false}"
PREPARE_ONLY="${PREPARE_ONLY:-false}"
REUSE_PREPARED_DATA="${REUSE_PREPARED_DATA:-false}"
RUN_METADATA_FAST="${RUN_METADATA_FAST:-true}"
RUN_CHAOS="${RUN_CHAOS:-true}"
CHAOS_MAX_KEYS="${CHAOS_MAX_KEYS:-3}"
METADATA_FAST_STALENESS_MS="${METADATA_FAST_STALENESS_MS:-5000}"
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
  --journal-path <p>     Optional local namespace mutation journal override path.
  --prebuild-key-index   Build persistent key-only index from the bench script.
  --prepare-only         Prepare bucket data and exit without running modes.
  --reuse-prepared-data  Reuse --data-root and skip warp data preparation.
  --skip-metadata-fast   Skip metadata-fast benchmark mode.
  --skip-chaos           Skip metadata-fast stale/fallback chaos probes.
  --chaos-max-keys <n>   Metadata-fast chaos page size (default: 3).
  --metadata-fast-staleness-ms <n>
                         Metadata-fast staleness SLA in milliseconds (default: 5000).
  --prometheus-query-url <url>
                         Prometheus API query URL or UI /query URL.
  -h, --help             Show this help.

Environment:
  OUT_DIR, DATA_ROOT, RUSTFS_BIN, WARP_BIN, ADDRESS, ACCESS_KEY, SECRET_KEY,
  REGION, OBJECTS, OBJECT_SIZE, CONCURRENCY, MAX_KEYS, DURATION,
  WARMUP_DURATION, METER_INTERVAL, RESOURCE_SAMPLE_INTERVAL, SKIP_BUILD,
  BUILD_PROFILE, INDEX_PROVIDER, INDEX_GENERATION, KEY_INDEX_PATH,
  NAMESPACE_JOURNAL_PATH, PREBUILD_KEY_INDEX, PREPARE_ONLY, REUSE_PREPARED_DATA,
  RUN_METADATA_FAST, RUN_CHAOS, CHAOS_MAX_KEYS, METADATA_FAST_STALENESS_MS,
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
      --journal-path) NAMESPACE_JOURNAL_PATH="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --prebuild-key-index) PREBUILD_KEY_INDEX="true"; shift ;;
      --prepare-only) PREPARE_ONLY="true"; shift ;;
      --reuse-prepared-data) REUSE_PREPARED_DATA="true"; shift ;;
      --skip-metadata-fast) RUN_METADATA_FAST="false"; shift ;;
      --skip-chaos) RUN_CHAOS="false"; shift ;;
      --chaos-max-keys) CHAOS_MAX_KEYS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --metadata-fast-staleness-ms) METADATA_FAST_STALENESS_MS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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
    unset RUSTFS_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH
    unset RUSTFS_LIST_OBJECTS_METADATA_FAST_ENABLED
    unset RUSTFS_LIST_OBJECTS_METADATA_FAST_STALENESS_MS
    if [[ "$mode" == "opt_in_verified" || "$mode" == "metadata_fast" ]]; then
      export RUSTFS_LIST_OBJECTS_INDEX_MODE=index_key_only
      if [[ "$mode" == "metadata_fast" ]]; then
        export RUSTFS_LIST_OBJECTS_INDEX_MODE=index_metadata_fast
        export RUSTFS_LIST_OBJECTS_METADATA_FAST_ENABLED=true
        export RUSTFS_LIST_OBJECTS_METADATA_FAST_STALENESS_MS="$METADATA_FAST_STALENESS_MS"
      fi
      export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER="$INDEX_PROVIDER"
      if [[ "$INDEX_PROVIDER" == "persistent_key_only" || "$INDEX_PROVIDER" == "persisted_key_only" ]]; then
        export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_PATH="$KEY_INDEX_PATH"
        export RUSTFS_LIST_OBJECTS_INDEX_PROVIDER_GENERATION="$INDEX_GENERATION"
        if [[ -n "$NAMESPACE_JOURNAL_PATH" ]]; then
          export RUSTFS_LIST_OBJECTS_NAMESPACE_JOURNAL_PATH="$NAMESPACE_JOURNAL_PATH"
        fi
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
  local index_dir tmp_keys tmp_objects sorted_keys tmp_index token page response_file next_token key_count
  index_dir="$(dirname "$index_path")"
  tmp_keys="${index_path}.keys.tmp"
  tmp_objects="${index_path}.objects.tmp"
  sorted_keys="${index_path}.keys.sorted.tmp"
  tmp_index="${index_path}.tmp"
  mkdir -p "$index_dir"
  : >"$tmp_keys"
  : >"$tmp_objects"

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
    extract_metadata_object_rows <"$response_file" >>"$tmp_objects" || true
    next_token="$(extract_next_token <"$response_file" || true)"
    rm -f "$response_file"
    if [[ -z "$next_token" ]]; then
      break
    fi
    token="$next_token"
    page=$((page + 1))
  done

  LC_ALL=C sort -u "$tmp_keys" >"$sorted_keys"
  key_count="$(awk 'END{print NR + 0}' "$sorted_keys")"
  {
    echo "# rustfs-listobjects-key-only-v1"
    echo "# bucket=${bucket}"
    echo "# generation=${INDEX_GENERATION}"
    echo "# checkpoint_high_water_mark=${key_count}"
    cat "$sorted_keys"
    cat "$tmp_objects"
  } >"$tmp_index"
  mv "$tmp_index" "$index_path"
  rm -f "$tmp_keys" "$tmp_objects" "$sorted_keys"
}

extract_xml_keys() {
  python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
for elem in root.iter():
    if elem.tag.rsplit("}", 1)[-1] == "Key" and elem.text:
        print(elem.text)'
}

extract_next_token() {
  python3 -c 'import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
for elem in root.iter():
    if elem.tag.rsplit("}", 1)[-1] == "NextContinuationToken" and elem.text:
        print(elem.text)
        break'
}

xml_tag_text() {
  local tag="$1"
  python3 -c 'import sys, xml.etree.ElementTree as ET
tag = sys.argv[1]
root = ET.fromstring(sys.stdin.read())
for elem in root.iter():
    if elem.tag.rsplit("}", 1)[-1] == tag and elem.text:
        print(elem.text)
        break' "$tag"
}

extract_metadata_object_rows() {
  python3 -c 'import base64, datetime, sys, xml.etree.ElementTree as ET
def local_name(tag):
    return tag.rsplit("}", 1)[-1]
def text(parent, name):
    for child in parent:
        if local_name(child.tag) == name:
            return child.text or ""
    return ""
def enc(value):
    if not value:
        return "-"
    return base64.b64encode(value.encode("utf-8")).decode("ascii")
def nanos(value):
    if not value:
        return "-"
    try:
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return str(int(dt.timestamp() * 1_000_000_000))
    except ValueError:
        return "-"
root = ET.fromstring(sys.stdin.read())
for contents in root.iter():
    if local_name(contents.tag) != "Contents":
        continue
    key = text(contents, "Key")
    if not key:
        continue
    size = text(contents, "Size") or "0"
    etag = text(contents, "ETag").strip("\"")
    storage_class = text(contents, "StorageClass")
    print("# object\t{}\t{}\t{}\t{}\t{}".format(
        enc(key),
        size,
        nanos(text(contents, "LastModified")),
        enc(etag),
        enc(storage_class),
    ))'
}

signed_list_once() {
  local bucket="$1"
  local response_file="$2"
  local max_keys="${3:-1}"
  signed_list_page "$bucket" "$response_file" "$max_keys" ""
}

signed_list_page() {
  local bucket="$1"
  local response_file="$2"
  local max_keys="${3:-1}"
  local token="${4:-}"
  local url="${ENDPOINT}/${bucket}?list-type=2&max-keys=${max_keys}"
  if [[ -n "$token" ]]; then
    local encoded_token
    encoded_token="$(printf '%s' "$token" | urlencode)"
    url="${url}&continuation-token=${encoded_token}"
  fi
  curl -fsS \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -o "$response_file" \
    "$url"
}

urlencode_path() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe="/-_.~"))' "$1"
}

signed_put_object() {
  local bucket="$1"
  local key="$2"
  local body_file="$3"
  local out_file="$4"
  local encoded_key
  encoded_key="$(urlencode_path "$key")"
  curl -fsS \
    --request PUT \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    --upload-file "$body_file" \
    -o "$out_file" \
    "${ENDPOINT}/${bucket}/${encoded_key}"
}

signed_delete_object() {
  local bucket="$1"
  local key="$2"
  local out_file="$3"
  local encoded_key
  encoded_key="$(urlencode_path "$key")"
  curl -fsS \
    --request DELETE \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -o "$out_file" \
    "${ENDPOINT}/${bucket}/${encoded_key}"
}

response_etag() {
  awk 'BEGIN{IGNORECASE=1} /^etag:/ {gsub("\r", "", $0); sub("^[^:]*:[[:space:]]*", "", $0); print $0; exit}' "$1"
}

signed_multipart_complete() {
  local bucket="$1"
  local key="$2"
  local work_dir="$3"
  local encoded_key upload_xml upload_id encoded_upload_id part1_file part2_file part1_headers part2_headers etag1 etag2 complete_xml
  encoded_key="$(urlencode_path "$key")"
  upload_xml="$work_dir/multipart-init.xml"

  curl -fsS \
    --request POST \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -o "$upload_xml" \
    "${ENDPOINT}/${bucket}/${encoded_key}?uploads"
  upload_id="$(xml_tag_text UploadId <"$upload_xml")"
  [[ -n "$upload_id" ]] || die "multipart initiate did not return UploadId"
  encoded_upload_id="$(printf '%s' "$upload_id" | urlencode)"

  part1_file="$work_dir/multipart-part-1.bin"
  part2_file="$work_dir/multipart-part-2.bin"
  part1_headers="$work_dir/multipart-part-1.headers"
  part2_headers="$work_dir/multipart-part-2.headers"
  python3 -c 'from pathlib import Path; Path(__import__("sys").argv[1]).write_bytes(b"a" * (5 * 1024 * 1024)); Path(__import__("sys").argv[2]).write_bytes(b"b")' "$part1_file" "$part2_file"

  curl -fsS \
    --request PUT \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    --upload-file "$part1_file" \
    -D "$part1_headers" \
    -o "$work_dir/multipart-part-1.out" \
    "${ENDPOINT}/${bucket}/${encoded_key}?partNumber=1&uploadId=${encoded_upload_id}"
  curl -fsS \
    --request PUT \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    --upload-file "$part2_file" \
    -D "$part2_headers" \
    -o "$work_dir/multipart-part-2.out" \
    "${ENDPOINT}/${bucket}/${encoded_key}?partNumber=2&uploadId=${encoded_upload_id}"

  etag1="$(response_etag "$part1_headers")"
  etag2="$(response_etag "$part2_headers")"
  [[ -n "$etag1" && -n "$etag2" ]] || die "multipart part upload did not return ETags"
  complete_xml="$work_dir/multipart-complete.xml"
  {
    echo "<CompleteMultipartUpload>"
    echo "  <Part><PartNumber>1</PartNumber><ETag>${etag1}</ETag></Part>"
    echo "  <Part><PartNumber>2</PartNumber><ETag>${etag2}</ETag></Part>"
    echo "</CompleteMultipartUpload>"
  } >"$complete_xml"

  curl -fsS \
    --request POST \
    --user "${ACCESS_KEY}:${SECRET_KEY}" \
    --aws-sigv4 "aws:amz:${REGION}:s3" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "Content-Type: application/xml" \
    --data-binary "@${complete_xml}" \
    -o "$work_dir/multipart-complete.out" \
    "${ENDPOINT}/${bucket}/${encoded_key}?uploadId=${encoded_upload_id}"
}

urlencode() {
  python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))'
}

base64_token() {
  python3 -c 'import base64, sys; print(base64.b64encode(sys.stdin.read().encode("utf-8")).decode("ascii"))'
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
        snapshot_total += sum
        snapshot_seen = 1
        found=0
        sum=0
      }
    }
    function finish_snapshot() {
      finish_block()
      if (snapshot_seen) {
        value=snapshot_total
      }
      snapshot_total=0
      snapshot_seen=0
    }
    /^Metric #0$/ {
      finish_snapshot()
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
      finish_snapshot()
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

write_fallback_reason_summary() {
  local mode="$1"
  local log_file="$2"
  local csv_file="$3"

  awk -v mode="$mode" '
    BEGIN {
      print "mode,reason,index_fallback_total"
    }
    /Name[[:space:]]*: rustfs_s3_list_objects_index_fallback_total/ {
      in_metric = 1
      value = ""
      next
    }
    in_metric && /Name[[:space:]]*:/ && $0 !~ /rustfs_s3_list_objects_index_fallback_total/ {
      in_metric = 0
      value = ""
      next
    }
    in_metric && /Value[[:space:]]*:/ {
      line = $0
      sub(/^.*Value[[:space:]]*:[[:space:]]*/, "", line)
      split(line, parts, /[[:space:]]+/)
      value = parts[1]
      next
    }
    in_metric && /->[[:space:]]*reason:/ {
      line = $0
      sub(/^.*->[[:space:]]*reason:[[:space:]]*/, "", line)
      split(line, parts, /[[:space:]]+/)
      reason = parts[1]
      last[reason] = value + 0
      seen[reason] = 1
      value = ""
      next
    }
    END {
      count = 0
      for (reason in seen) {
        printf "%s,%s,%.0f\n", mode, reason, last[reason]
        count += 1
      }
      if (count == 0) {
        printf "%s,N/A,0\n", mode
      }
    }
  ' "$log_file" >"$csv_file"
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
  query_prometheus_expr "rustfs_memory_allocator_reserved_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_reserved_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_committed_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_committed_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_page_committed_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_page_committed_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_malloc_requested_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_malloc_requested_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_malloc_requested_peak_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_malloc_requested_peak_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_malloc_requested_total_bytes{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_malloc_requested_total_bytes.json" || true
  query_prometheus_expr "rustfs_memory_allocator_heap_count{job=\"${PROMETHEUS_JOB}\"}" "$prom_dir/allocator_heap_count.json" || true
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

run_metadata_fast_chaos() {
  local bucket="$1"
  local out_dir="$2"
  mkdir -p "$out_dir"

  local summary="$out_dir/chaos-summary.csv"
  local fallback_summary="$out_dir/fallback-probes.csv"
  local page1="$out_dir/page1-before-mutation.xml"
  local page2="$out_dir/page2-after-mutation.xml"
  local fresh_after="$out_dir/fresh-after-mutation.xml"
  local generation_mismatch="$out_dir/generation-mismatch.xml"
  local degraded_response="$out_dir/degraded-fallback.xml"
  local page1_keys="$out_dir/page1-before-mutation.keys"
  local page2_keys="$out_dir/page2-after-mutation.keys"
  local fresh_after_keys="$out_dir/fresh-after-mutation.keys"
  local expected_page2_keys="$out_dir/expected-page2-after-mutation.keys"
  local body_file="$out_dir/overwrite-body.bin"
  local delete_out="$out_dir/delete.out"
  local overwrite_out="$out_dir/overwrite.out"
  local token first_key second_key multipart_key overlap last_key monotonic_status no_skip_status page1_count page2_count fresh_max_keys forged_token journal_sequence

  echo "probe,status,detail" >"$summary"
  echo "probe,status,detail" >"$fallback_summary"

  signed_list_page "$bucket" "$page1" "$CHAOS_MAX_KEYS" ""
  extract_xml_keys <"$page1" >"$page1_keys"
  token="$(extract_next_token <"$page1" || true)"
  page1_count="$(awk 'END{print NR + 0}' "$page1_keys")"
  if [[ -z "$token" || "$page1_count" -lt 2 ]]; then
    echo "initial_page,skipped,requires at least two keys and a continuation token" >>"$summary"
    return 0
  fi
  echo "initial_page,ok,keys=${page1_count}; continuation_token=yes" >>"$summary"

  first_key="$(sed -n '1p' "$page1_keys")"
  second_key="$(sed -n '2p' "$page1_keys")"
  printf 'metadata-fast overwrite probe %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$body_file"

  signed_put_object "$bucket" "$first_key" "$body_file" "$overwrite_out"
  echo "overwrite,ok,key=${first_key}" >>"$summary"

  signed_delete_object "$bucket" "$second_key" "$delete_out"
  echo "delete,ok,key=${second_key}" >>"$summary"

  multipart_key="zzzz-metadata-fast-chaos-multipart-$(date +%s).bin"
  signed_multipart_complete "$bucket" "$multipart_key" "$out_dir"
  echo "multipart_complete,ok,key=${multipart_key}" >>"$summary"

  signed_list_page "$bucket" "$page2" "$CHAOS_MAX_KEYS" "$token"
  extract_xml_keys <"$page2" >"$page2_keys"
  page2_count="$(awk 'END{print NR + 0}' "$page2_keys")"

  overlap="$(comm -12 <(LC_ALL=C sort "$page1_keys") <(LC_ALL=C sort "$page2_keys") | awk 'END{print NR + 0}')"
  if [[ "$overlap" -eq 0 ]]; then
    echo "continuation_no_duplicate,ok,overlap=0" >>"$summary"
  else
    echo "continuation_no_duplicate,failed,overlap=${overlap}" >>"$summary"
    return 1
  fi

  last_key="$(tail -n 1 "$page1_keys")"
  monotonic_status="ok"
  if [[ -n "$last_key" ]] && ! awk -v last="$last_key" 'NF && $0 <= last { bad = 1 } END { exit bad }' "$page2_keys"; then
    monotonic_status="failed"
  fi
  echo "continuation_monotonic,${monotonic_status},page2_keys=${page2_count}; last_page1=${last_key}" >>"$summary"
  [[ "$monotonic_status" == "ok" ]] || return 1

  fresh_max_keys=$((page1_count + page2_count + CHAOS_MAX_KEYS))
  signed_list_page "$bucket" "$fresh_after" "$fresh_max_keys" ""
  extract_xml_keys <"$fresh_after" >"$fresh_after_keys"
  awk -v last="$last_key" 'NF && $0 > last { print }' "$fresh_after_keys" | head -n "$page2_count" >"$expected_page2_keys"
  no_skip_status="ok"
  if ! cmp -s "$expected_page2_keys" "$page2_keys"; then
    no_skip_status="failed"
  fi
  echo "continuation_no_skip,${no_skip_status},expected_next_keys=$(awk 'END{print NR + 0}' "$expected_page2_keys"); actual_page2_keys=${page2_count}" >>"$summary"
  [[ "$no_skip_status" == "ok" ]] || return 1

  forged_token="$(printf '%s[rustfs_cache:v2,return:,src:index_metadata_fast,gen:%s-mismatch]' "$last_key" "$INDEX_GENERATION" | base64_token)"
  if signed_list_page "$bucket" "$generation_mismatch" "$CHAOS_MAX_KEYS" "$forged_token"; then
    echo "generation_mismatch,ok,forged_generation=${INDEX_GENERATION}-mismatch" >>"$fallback_summary"
  else
    echo "generation_mismatch,failed,forged_generation=${INDEX_GENERATION}-mismatch" >>"$fallback_summary"
  fi

  if [[ -z "$NAMESPACE_JOURNAL_PATH" ]]; then
    echo "degraded,skipped,requires --journal-path to avoid mutating quorum-backed journal state" >>"$fallback_summary"
    return 0
  fi

  journal_sequence="$(journal_high_water_mark "$NAMESPACE_JOURNAL_PATH" "$bucket")"
  write_local_journal_state "$NAMESPACE_JOURNAL_PATH" "$bucket" "$journal_sequence" "degraded"
  if signed_list_page "$bucket" "$degraded_response" "$CHAOS_MAX_KEYS" ""; then
    echo "degraded,ok,journal_path=${NAMESPACE_JOURNAL_PATH}; high_water_mark=${journal_sequence}" >>"$fallback_summary"
  else
    echo "degraded,failed,journal_path=${NAMESPACE_JOURNAL_PATH}; high_water_mark=${journal_sequence}" >>"$fallback_summary"
  fi
  write_local_journal_state "$NAMESPACE_JOURNAL_PATH" "$bucket" "$journal_sequence" "healthy"
  echo "degraded_restore,ok,journal_path=${NAMESPACE_JOURNAL_PATH}; high_water_mark=${journal_sequence}" >>"$fallback_summary"
}

journal_high_water_mark() {
  local root="$1"
  local bucket="$2"
  local state_file="${root}/${bucket}.state"
  if [[ -f "$state_file" ]]; then
    awk -F= '/^# high_water_mark=/ {print $2; found=1; exit} END {if(!found) print "0"}' "$state_file"
  else
    echo "0"
  fi
}

write_local_journal_state() {
  local root="$1"
  local bucket="$2"
  local high_water_mark="$3"
  local status="$4"
  local state_file="${root}/${bucket}.state"
  local tmp_file="${state_file}.tmp"
  mkdir -p "$root"
  {
    echo "# rustfs-listobjects-namespace-journal-v1"
    echo "# bucket=${bucket}"
    echo "# high_water_mark=${high_water_mark}"
    echo "# status=${status}"
  } >"$tmp_file"
  mv "$tmp_file" "$state_file"
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

  if [[ "$mode" == "opt_in_verified" && "$PREBUILD_KEY_INDEX" != "true" ]] \
    && [[ "$INDEX_PROVIDER" == "persistent_key_only" || "$INDEX_PROVIDER" == "persisted_key_only" ]]; then
    log_info "Warming persistent key-only provider through production rebuild path"
    signed_list_once "$bucket" "$mode_dir/provider-warmup.xml" 1
  fi

  log_info "Signed ListObjectsV2 bench for mode=$mode duration=$DURATION"
  run_signed_list_bench "$mode" "$bucket" "$DURATION" "$mode_dir"
  stop_resource_sampler
  sleep "$((METER_INTERVAL + 2))"
  write_ratio_summary "$mode" "$mode_dir/rustfs.log" "$mode_dir/index-ratios.csv"
  write_fallback_reason_summary "$mode" "$mode_dir/rustfs.log" "$mode_dir/index-fallback-reasons.csv"
  write_resource_summary "$mode_dir/resource-samples.csv" "$mode_dir/resource-summary.csv"
  write_prometheus_snapshot "$mode" "$mode_dir"

  if [[ "$mode" == "metadata_fast" && "$RUN_CHAOS" == "true" ]]; then
    log_info "Running metadata-fast chaos probes"
    run_metadata_fast_chaos "$bucket" "$mode_dir/chaos"
    sleep "$((METER_INTERVAL + 2))"
    write_ratio_summary "${mode}_after_chaos" "$mode_dir/rustfs.log" "$mode_dir/chaos/index-ratios-after-chaos.csv"
    write_fallback_reason_summary "${mode}_after_chaos" "$mode_dir/rustfs.log" "$mode_dir/chaos/index-fallback-reasons-after-chaos.csv"
  fi

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
    echo "namespace_journal_path=${NAMESPACE_JOURNAL_PATH:-.rustfs.sys/listobjects/ns-journal/v1}"
    echo "prebuild_key_index=$PREBUILD_KEY_INDEX"
    echo "prepare_only=$PREPARE_ONLY"
    echo "reuse_prepared_data=$REUSE_PREPARED_DATA"
    echo "run_metadata_fast=$RUN_METADATA_FAST"
    echo "run_chaos=$RUN_CHAOS"
    echo "chaos_max_keys=$CHAOS_MAX_KEYS"
    echo "metadata_fast_staleness_ms=$METADATA_FAST_STALENESS_MS"
    echo "prometheus_query_url=$PROMETHEUS_QUERY_URL"
    echo "prometheus_job=$PROMETHEUS_JOB"
  } >"$OUT_DIR/manifest.env"
}

summary_csv_section() {
  local title="$1"
  local file="$2"
  if [[ -f "$file" ]]; then
    echo "### $title"
    echo
    cat "$file"
    echo
  fi
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
    summary_csv_section "Walker" "$OUT_DIR/walker/list-summary.csv"
    summary_csv_section "Opt-in verified" "$OUT_DIR/opt_in_verified/list-summary.csv"
    summary_csv_section "Metadata-fast" "$OUT_DIR/metadata_fast/list-summary.csv"
    echo
    echo "## Index Ratios"
    echo
    summary_csv_section "Walker" "$OUT_DIR/walker/index-ratios.csv"
    summary_csv_section "Opt-in verified" "$OUT_DIR/opt_in_verified/index-ratios.csv"
    summary_csv_section "Metadata-fast" "$OUT_DIR/metadata_fast/index-ratios.csv"
    echo
    echo "## Fallback Reasons"
    echo
    summary_csv_section "Walker" "$OUT_DIR/walker/index-fallback-reasons.csv"
    summary_csv_section "Opt-in verified" "$OUT_DIR/opt_in_verified/index-fallback-reasons.csv"
    summary_csv_section "Metadata-fast" "$OUT_DIR/metadata_fast/index-fallback-reasons.csv"
    echo
    echo "## Resource Samples"
    echo
    summary_csv_section "Walker" "$OUT_DIR/walker/resource-summary.csv"
    summary_csv_section "Opt-in verified" "$OUT_DIR/opt_in_verified/resource-summary.csv"
    summary_csv_section "Metadata-fast" "$OUT_DIR/metadata_fast/resource-summary.csv"
    if [[ -f "$OUT_DIR/metadata_fast/chaos/chaos-summary.csv" ]]; then
      echo
      echo "## Metadata-fast Chaos"
      echo
      summary_csv_section "Chaos probes" "$OUT_DIR/metadata_fast/chaos/chaos-summary.csv"
      summary_csv_section "Fallback probes" "$OUT_DIR/metadata_fast/chaos/fallback-probes.csv"
      summary_csv_section "Ratios after chaos" "$OUT_DIR/metadata_fast/chaos/index-ratios-after-chaos.csv"
      summary_csv_section "Fallback reasons after chaos" "$OUT_DIR/metadata_fast/chaos/index-fallback-reasons-after-chaos.csv"
    fi
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

  if [[ "$REUSE_PREPARED_DATA" == "true" ]]; then
    log_info "Reusing prepared data root: $DATA_ROOT"
  else
    log_info "Starting RustFS for data preparation"
    start_server walker "$prep_dir/rustfs.log" true
    log_info "Preparing bucket=$bucket objects=$OBJECTS with warp list"
    prepare_bucket "$bucket" "$WARMUP_DURATION" "$prep_dir/warp-prepare.log" || {
      tail -n 80 "$prep_dir/warp-prepare.log" >&2 || true
      return 1
    }
  fi

  if [[ "$PREBUILD_KEY_INDEX" == "true" ]]; then
    if [[ "$REUSE_PREPARED_DATA" == "true" ]]; then
      log_info "Starting RustFS to build persistent key-only index from reused data"
      start_server walker "$prep_dir/rustfs.log" false
    fi
    log_info "Writing persistent key-only index from bench script: $KEY_INDEX_PATH"
    write_key_index "$bucket" "$KEY_INDEX_PATH"
    stop_server
  else
    log_info "Skipping bench-side key index generation; opt-in mode will rebuild through RustFS lifecycle"
    stop_server
  fi

  if [[ "$PREPARE_ONLY" == "true" ]]; then
    log_info "Prepare-only mode complete: $DATA_ROOT"
    return 0
  fi

  run_mode walker "$bucket" false
  run_mode opt_in_verified "$bucket" false
  if [[ "$RUN_METADATA_FAST" == "true" ]]; then
    run_mode metadata_fast "$bucket" false
  fi
  write_combined_summary

  log_info "Summary: $OUT_DIR/summary.md"
}

main "$@"
