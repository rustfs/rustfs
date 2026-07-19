#!/usr/bin/env bash

# Manual/nightly validation for cold object-data-cache stampedes.
#
# This gate deliberately requires a dedicated RustFS instance, an exact 28 GiB
# cgroup v2 limit, pre-provisioned objects, and authoritative Prometheus queries.
# It never substitutes process I/O counters or synthetic values for server
# metrics. With no --run flag (or with missing prerequisites outside --strict),
# it reports SKIP and exits successfully.

set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

readonly REQUESTS=2000
readonly KEY_MATRIX="1 4 32"
readonly REQUIRED_MEMORY_BYTES=$((28 * 1024 * 1024 * 1024))
readonly OBJECT_SIZE=$((384 * 1024 * 1024))
readonly READER_BYTES_QUERY='sum(rustfs_io_get_object_reader_bytes_total) or vector(0)'
readonly FOLLOWER_PERMIT_QUERY='sum(rustfs_object_data_cache_cold_fill_follower_disk_permits)'

RUN=0
SELF_TEST=0
STRICT=0
ACK_ISOLATED=0
ROUNDS=3
ENDPOINT=""
BUCKET=""
KEY_PREFIX=""
EXPECTED_SHA256=""
REGION="us-east-1"
PROMETHEUS_QUERY_URL=""
PROMETHEUS_SCRAPE_SECONDS=""
RESET_COMMAND=""
CACHE_OFF_COMMAND=""
CACHE_ON_COMMAND=""
SERVER_PID=""
CGROUP_PATH=""
SAMPLE_INTERVAL="0.25"
METRICS_SETTLE_SECONDS="5"
LOAD_TIMEOUT_SECONDS="1800"
OUT_DIR=""

usage() {
  cat <<'USAGE'
Usage:
  validate_object_data_cache_cold_stampede.sh --run --ack-isolated \
    --endpoint URL --bucket NAME --key-prefix PREFIX \
    --expected-sha256 HEX --prometheus-query-url URL \
    --prometheus-scrape-seconds SECONDS \
    --reset-command COMMAND --cache-off-command COMMAND \
    --cache-on-command COMMAND --server-pid PID [--cgroup-path PATH] [options]

Required run contract:
  * N is fixed at 2000; K is fixed at 1, 4, and 32; each K runs >= 3 rounds.
  * PREFIX/object-00 through PREFIX/object-31 must already exist, each exactly
    384 MiB with identical SHA-256 content.
  * COMMAND must synchronously make the selected objects cold before returning.
  * The RustFS process must be isolated from unrelated traffic and run in a
    dedicated cgroup v2 with memory.max exactly 28 GiB.
  * The built-in first-party reader-byte counter and follower-permit gauge must
    each return exactly one Prometheus vector sample.
  * Cache switch commands must return only after the mode is effective and must
    keep the same RustFS PID inside the dedicated cgroup.
  * Every response must match SHA-256, ETag, Content-Length, and its per-key
    stable header contract. Date, x-amz-id-2, x-amz-request-id,
    x-minio-request-id, x-rustfs-request-id, Connection, Keep-Alive, and
    Transfer-Encoding are explicitly excluded as volatile/transport headers.

Built-in PromQL:
  reader bytes:     sum(rustfs_io_get_object_reader_bytes_total) or vector(0)
  follower permits: sum(rustfs_object_data_cache_cold_fill_follower_disk_permits)

Options:
  --self-test                  Validate the follower sample append/check chain locally
  --rounds N                   Rounds per K (default: 3; minimum: 3)
  --region REGION              SigV4 region (default: us-east-1)
  --sample-interval SECONDS    Follower gauge polling interval (default: 0.25)
  --prometheus-scrape-seconds N Configured scrape interval; required
  --metrics-settle-seconds N   Wait for final Prometheus scrape (default: 5)
  --load-timeout-seconds N     Per-round load timeout (default: 1800)
  --out-dir PATH               Artifact directory (default: temporary)
  --strict                     Missing prerequisite is FAIL instead of SKIP
  -h, --help                   Show this help

Credentials are read only from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
optional AWS_SESSION_TOKEN. They are never written to the result artifacts.
USAGE
}

skip_or_fail() {
  local message=$1
  if ((STRICT)); then
    printf 'FAIL: %s\n' "$message" >&2
    exit 1
  fi
  printf 'SKIP: %s\n' "$message"
  exit 0
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

need_value() {
  (($# >= 2)) || fail "option $1 requires a value"
}

append_follower_sample() {
  local samples_file=$1
  local sample=$2
  local sample_epoch sample_value trailing
  read -r sample_epoch sample_value trailing <<<"$sample"
  [[ -n $sample_epoch && -n $sample_value && -z $trailing ]] || return 1
  printf '%s %s\n' "$sample_epoch" "$sample_value" >>"$samples_file"
}

follower_samples_check() {
  local samples_file=$1
  local ready_file=$2
  local scrape_seconds=${3:-$PROMETHEUS_SCRAPE_SECONDS}
  python3 "$SCRIPT_DIR/check_object_data_cache_follower_samples.py" \
    --samples "$samples_file" --ready "$ready_file" \
    --scrape-seconds "$scrape_seconds"
}

run_operator_command() {
  local mode=$1
  local round=$2
  local key_count=$3
  local command=$4
  env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
    RUSTFS_STAMPEDE_MODE="$mode" RUSTFS_STAMPEDE_ROUND="$round" RUSTFS_STAMPEDE_K="$key_count" \
    bash -c "$command"
}

follower_samples_self_test() (
  command -v python3 >/dev/null 2>&1 || fail "python3 is required for --self-test"
  local temp_dir samples_file ready_file result sample
  temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/rustfs-follower-samples.XXXXXX")
  trap 'rm -rf -- "$temp_dir"' EXIT
  samples_file="$temp_dir/follower.samples"
  ready_file="$temp_dir/ready"
  printf '999\n' >"$ready_file"

  : >"$samples_file"
  for sample in '1000 0' '1000 0' '1000 0'; do
    append_follower_sample "$samples_file" "$sample" || fail "self-test could not append duplicate follower sample"
  done
  [[ $(<"$samples_file") == $'1000 0\n1000 0\n1000 0' ]] || fail "duplicate Prometheus timestamps were not preserved by the append chain"
  if follower_samples_check "$samples_file" "$ready_file" 5 >/dev/null 2>&1; then
    fail "duplicate Prometheus timestamps unexpectedly passed the follower sample checker"
  fi

  : >"$samples_file"
  for sample in '1000 0' '1000 1' '1005 0' '1010 0'; do
    append_follower_sample "$samples_file" "$sample" || fail "self-test could not append conflicting follower sample"
  done
  if follower_samples_check "$samples_file" "$ready_file" 5 >/dev/null 2>&1; then
    fail "conflicting values for one Prometheus timestamp unexpectedly passed the follower sample checker"
  fi

  : >"$samples_file"
  for sample in '1000 0' '1005 0' '1010 0'; do
    append_follower_sample "$samples_file" "$sample" || fail "self-test could not append fresh follower sample"
  done
  [[ $(<"$samples_file") == $'1000 0\n1005 0\n1010 0' ]] || fail "fresh Prometheus timestamps were not preserved by the append chain"
  result=$(follower_samples_check "$samples_file" "$ready_file" 5) || fail "fresh Prometheus timestamps unexpectedly failed the follower sample checker"
  [[ $result == '3,0' ]] || fail "fresh Prometheus timestamps returned unexpected result: $result"

  result=$(
    AWS_ACCESS_KEY_ID=self-test-access \
      AWS_SECRET_ACCESS_KEY=self-test-secret \
      AWS_SESSION_TOKEN=self-test-session \
      run_operator_command self-test 7 11 \
        'printf "%s|%s|%s|%s|%s|%s" "$RUSTFS_STAMPEDE_MODE" "$RUSTFS_STAMPEDE_ROUND" "$RUSTFS_STAMPEDE_K" "${AWS_ACCESS_KEY_ID-unset}" "${AWS_SECRET_ACCESS_KEY-unset}" "${AWS_SESSION_TOKEN-unset}"'
  ) || fail "operator command self-test failed"
  [[ $result == 'self-test|7|11|unset|unset|unset' ]] || fail "operator command inherited AWS credentials: $result"
  printf 'PASS: cold stampede script self-test\n'
)

while (($#)); do
  case "$1" in
    --run) RUN=1 ;;
    --self-test) SELF_TEST=1 ;;
    --strict) STRICT=1 ;;
    --ack-isolated) ACK_ISOLATED=1 ;;
    --endpoint) need_value "$@"; ENDPOINT=$2; shift ;;
    --bucket) need_value "$@"; BUCKET=$2; shift ;;
    --key-prefix) need_value "$@"; KEY_PREFIX=$2; shift ;;
    --expected-sha256) need_value "$@"; EXPECTED_SHA256=$2; shift ;;
    --region) need_value "$@"; REGION=$2; shift ;;
    --prometheus-query-url) need_value "$@"; PROMETHEUS_QUERY_URL=$2; shift ;;
    --prometheus-scrape-seconds) need_value "$@"; PROMETHEUS_SCRAPE_SECONDS=$2; shift ;;
    --reset-command) need_value "$@"; RESET_COMMAND=$2; shift ;;
    --cache-off-command) need_value "$@"; CACHE_OFF_COMMAND=$2; shift ;;
    --cache-on-command) need_value "$@"; CACHE_ON_COMMAND=$2; shift ;;
    --server-pid) need_value "$@"; SERVER_PID=$2; shift ;;
    --cgroup-path) need_value "$@"; CGROUP_PATH=$2; shift ;;
    --rounds) need_value "$@"; ROUNDS=$2; shift ;;
    --sample-interval) need_value "$@"; SAMPLE_INTERVAL=$2; shift ;;
    --metrics-settle-seconds) need_value "$@"; METRICS_SETTLE_SECONDS=$2; shift ;;
    --load-timeout-seconds) need_value "$@"; LOAD_TIMEOUT_SECONDS=$2; shift ;;
    --out-dir) need_value "$@"; OUT_DIR=$2; shift ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown option: $1" ;;
  esac
  shift
done

if ((SELF_TEST)); then
  follower_samples_self_test
  exit 0
fi

((RUN)) || skip_or_fail "not started; pass --run after reading --help"
((ACK_ISOLATED)) || skip_or_fail "--ack-isolated is required because global counters must have no unrelated traffic"

[[ $(uname -s) == Linux ]] || skip_or_fail "Linux with cgroup v2 is required"
for command in bash curl python3; do
  command -v "$command" >/dev/null 2>&1 || skip_or_fail "required command is unavailable: $command"
done

[[ -n $ENDPOINT ]] || skip_or_fail "--endpoint is required"
[[ -n $BUCKET ]] || skip_or_fail "--bucket is required"
[[ -n $KEY_PREFIX ]] || skip_or_fail "--key-prefix is required"
[[ $EXPECTED_SHA256 =~ ^[[:xdigit:]]{64}$ ]] || skip_or_fail "--expected-sha256 must be exactly 64 hexadecimal characters"
[[ -n $PROMETHEUS_QUERY_URL ]] || skip_or_fail "--prometheus-query-url is required"
[[ -n $PROMETHEUS_SCRAPE_SECONDS ]] || skip_or_fail "--prometheus-scrape-seconds is required"
[[ -n $RESET_COMMAND ]] || skip_or_fail "--reset-command is required to prove each round starts cold"
[[ -n $CACHE_OFF_COMMAND ]] || skip_or_fail "--cache-off-command is required"
[[ -n $CACHE_ON_COMMAND ]] || skip_or_fail "--cache-on-command is required"
[[ -n ${AWS_ACCESS_KEY_ID:-} ]] || skip_or_fail "AWS_ACCESS_KEY_ID is required"
[[ -n ${AWS_SECRET_ACCESS_KEY:-} ]] || skip_or_fail "AWS_SECRET_ACCESS_KEY is required"
if ! [[ $ROUNDS =~ ^[0-9]+$ ]] || ((ROUNDS < 3)); then
  fail "--rounds must be an integer >= 3"
fi
[[ $METRICS_SETTLE_SECONDS =~ ^[0-9]+$ ]] || fail "--metrics-settle-seconds must be a non-negative integer"
if ! [[ $LOAD_TIMEOUT_SECONDS =~ ^[0-9]+$ ]] || ((LOAD_TIMEOUT_SECONDS == 0)); then
  fail "--load-timeout-seconds must be positive"
fi
python3 - "$SAMPLE_INTERVAL" "$PROMETHEUS_SCRAPE_SECONDS" <<'PY' || fail "sample and scrape intervals must be finite positive numbers"
import math, sys
values = [float(value) for value in sys.argv[1:]]
raise SystemExit(0 if all(math.isfinite(value) and value > 0 for value in values) else 1)
PY
python3 - "$METRICS_SETTLE_SECONDS" "$PROMETHEUS_SCRAPE_SECONDS" <<'PY' \
  || fail "--metrics-settle-seconds must be at least one configured Prometheus scrape interval"
import sys
raise SystemExit(0 if float(sys.argv[1]) >= float(sys.argv[2]) else 1)
PY

[[ $SERVER_PID =~ ^[0-9]+$ ]] || skip_or_fail "--server-pid is required to bind OOM evidence to RustFS"
[[ -r /proc/$SERVER_PID/cgroup ]] || skip_or_fail "cannot read /proc/$SERVER_PID/cgroup"
if [[ -z $CGROUP_PATH ]]; then
  cgroup_relative=$(awk -F: '$1 == "0" { print $3; exit }' "/proc/$SERVER_PID/cgroup")
  [[ -n $cgroup_relative ]] || skip_or_fail "server process is not in a cgroup v2 hierarchy"
  CGROUP_PATH="/sys/fs/cgroup${cgroup_relative}"
fi

[[ -r $CGROUP_PATH/memory.max ]] || skip_or_fail "cannot read $CGROUP_PATH/memory.max"
[[ -r $CGROUP_PATH/memory.events ]] || skip_or_fail "cannot read $CGROUP_PATH/memory.events"
[[ -r $CGROUP_PATH/cgroup.procs ]] || skip_or_fail "cannot read $CGROUP_PATH/cgroup.procs"
memory_max=$(<"$CGROUP_PATH/memory.max")
[[ $memory_max =~ ^[0-9]+$ ]] || skip_or_fail "memory.max must be finite, not '$memory_max'"
((memory_max == REQUIRED_MEMORY_BYTES)) || skip_or_fail "memory.max must equal 28 GiB ($REQUIRED_MEMORY_BYTES), got $memory_max"

server_in_cgroup() {
  awk -v pid="$SERVER_PID" '$1 == pid { found = 1; exit } END { exit !found }' \
    "$CGROUP_PATH/cgroup.procs"
}
server_in_cgroup || skip_or_fail "RustFS PID $SERVER_PID is not in $CGROUP_PATH"

nofile_limit=$(ulimit -n)
[[ $nofile_limit =~ ^[0-9]+$ ]] || skip_or_fail "unable to determine the open-file limit"
((nofile_limit >= REQUESTS + 256)) || skip_or_fail "open-file limit must be at least $((REQUESTS + 256)), got $nofile_limit"

if [[ -z $OUT_DIR ]]; then
  OUT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rustfs-cold-stampede.XXXXXX")
else
  mkdir -p "$OUT_DIR"
fi
readonly OUT_DIR
readonly RESULTS_CSV="$OUT_DIR/results.csv"
LOAD_PID=""

cleanup_load() {
  if [[ -n $LOAD_PID ]] && kill -0 "$LOAD_PID" 2>/dev/null; then
    kill "$LOAD_PID" 2>/dev/null || true
    wait "$LOAD_PID" 2>/dev/null || true
  fi
}
trap cleanup_load EXIT

prometheus_sample() {
  local query=$1
  local response
  response=$(curl --fail --silent --show-error --get \
    --data-urlencode "query=$query" "$PROMETHEUS_QUERY_URL") || return 1
  python3 -c '
import json, math, sys
doc = json.load(sys.stdin)
if doc.get("status") != "success":
    raise SystemExit("Prometheus query was not successful")
result = doc.get("data", {}).get("result", [])
if len(result) != 1 or "value" not in result[0]:
    raise SystemExit(f"expected exactly one vector sample, got {len(result)}")
timestamp = float(result[0]["value"][0])
value = float(result[0]["value"][1])
if not math.isfinite(timestamp) or not math.isfinite(value) or value < 0:
    raise SystemExit(f"invalid metric value: {value}")
print(format(timestamp, ".17g"), format(value, ".17g"))
' <<<"$response"
}

prometheus_value() {
  local timestamp value
  read -r timestamp value < <(prometheus_sample "$1") || return 1
  [[ -n $timestamp && -n $value ]] || return 1
  printf '%s\n' "$value"
}

cgroup_event() {
  local name=$1
  awk -v name="$name" '$1 == name { print $2; found = 1; exit } END { if (!found) exit 1 }' \
    "$CGROUP_PATH/memory.events"
}

numeric_delta_check() {
  local before=$1
  local after=$2
  local expected_min=$3
  local expected_max=$4
  python3 - "$before" "$after" "$expected_min" "$expected_max" <<'PY'
import math, sys
before, after, lower, upper = map(float, sys.argv[1:])
delta = after - before
if not math.isfinite(delta) or delta < lower or delta > upper:
    raise SystemExit(f"reader byte delta {delta:.0f} outside [{lower:.0f}, {upper:.0f}]")
print(f"{delta:.0f}")
PY
}

numeric_positive_delta() {
  local before=$1
  local after=$2
  python3 - "$before" "$after" <<'PY'
import math, sys
before, after = map(float, sys.argv[1:])
delta = after - before
if not math.isfinite(delta) or delta <= 0:
    raise SystemExit(f"reader byte baseline delta must be positive, got {delta:.0f}")
print(f"{delta:.0f}")
PY
}

run_load() {
  local key_count=$1
  local request_count=$2
  local summary_file=$3
  local ready_file=$4
  local reference_file=${5:-}
  python3 - "$ENDPOINT" "$BUCKET" "$KEY_PREFIX" "$EXPECTED_SHA256" "$REGION" \
    "$OBJECT_SIZE" "$request_count" "$key_count" "$LOAD_TIMEOUT_SECONDS" "$ready_file" "$reference_file" \
    >"$summary_file" <<'PY'
import asyncio
import datetime
import hashlib
import hmac
import json
import os
import ssl
import sys
import time
import urllib.parse

(
    endpoint, bucket, key_prefix, expected_sha, region, object_size,
    request_count, key_count, timeout_seconds, ready_file, reference_file,
) = sys.argv[1:]
object_size = int(object_size)
request_count = int(request_count)
key_count = int(key_count)
timeout_seconds = int(timeout_seconds)
expected_sha = expected_sha.lower()
access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
session_token = os.environ.get("AWS_SESSION_TOKEN")

parsed = urllib.parse.urlsplit(endpoint)
if parsed.scheme not in ("http", "https") or not parsed.hostname:
    raise SystemExit("endpoint must be an absolute http(s) URL")
port = parsed.port or (443 if parsed.scheme == "https" else 80)
host_header = parsed.netloc
base_path = parsed.path.rstrip("/")
payload_hash = hashlib.sha256(b"").hexdigest()
tls_context = ssl.create_default_context() if parsed.scheme == "https" else None
volatile_headers = {
    "connection",
    "date",
    "keep-alive",
    "transfer-encoding",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-minio-request-id",
    "x-rustfs-request-id",
}

def sign_headers(method, canonical_uri):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    headers = {
        "host": host_header,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if session_token:
        headers["x-amz-security-token"] = session_token
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in sorted(headers))
    canonical_request = "\n".join((
        method, canonical_uri, "", canonical_headers, signed_headers, payload_hash,
    ))
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join((
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode()).hexdigest(),
    ))
    def digest(key, message):
        return hmac.new(key, message.encode(), hashlib.sha256).digest()
    signing_key = digest(digest(digest(digest(
        ("AWS4" + secret_key).encode(), date_stamp), region), "s3"), "aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers

async def read_one(index, release, connected, connected_lock, all_connected):
    key = f"{key_prefix.rstrip('/')}/object-{index % key_count:02d}"
    raw_path = f"{base_path}/{bucket}/{key}"
    canonical_uri = urllib.parse.quote(raw_path, safe="/-_.~")
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(parsed.hostname, port, ssl=tls_context), timeout=60,
    )
    async with connected_lock:
        connected[0] += 1
        if connected[0] == request_count:
            all_connected.set()
    await release.wait()
    headers = sign_headers("GET", canonical_uri)
    request = [f"GET {canonical_uri} HTTP/1.1", "Connection: close"]
    request.extend(f"{name}: {value}" for name, value in headers.items())
    writer.write(("\r\n".join(request) + "\r\n\r\n").encode())
    await writer.drain()
    status = (await reader.readline()).decode("latin1").rstrip("\r\n")
    parts = status.split(" ", 2)
    if len(parts) < 2 or parts[1] != "200":
        raise RuntimeError(f"request {index}: unexpected status {status!r}")
    response_headers = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, value = line.decode("latin1").split(":", 1)
        response_headers[name.strip().lower()] = value.strip()
    if "transfer-encoding" in response_headers:
        raise RuntimeError(f"request {index}: chunked responses are not accepted")
    length = int(response_headers.get("content-length", "-1"))
    if length != object_size:
        raise RuntimeError(f"request {index}: content-length {length}, expected {object_size}")
    digest = hashlib.sha256()
    remaining = length
    while remaining:
        chunk = await reader.read(min(64 * 1024, remaining))
        if not chunk:
            raise RuntimeError(f"request {index}: body ended with {remaining} bytes missing")
        digest.update(chunk)
        remaining -= len(chunk)
    writer.close()
    await writer.wait_closed()
    actual = digest.hexdigest()
    if actual != expected_sha:
        raise RuntimeError(f"request {index}: SHA-256 {actual}, expected {expected_sha}")
    etag = response_headers.get("etag")
    if not etag:
        raise RuntimeError(f"request {index}: missing ETag")
    semantic_headers = {
        name: value
        for name, value in response_headers.items()
        if name not in volatile_headers and name not in {"content-length", "etag"}
    }
    return {
        "bytes": length,
        "etag": etag,
        "key_index": index % key_count,
        "semantic_headers": semantic_headers,
        "sha256": actual,
    }

async def main():
    release = asyncio.Event()
    all_connected = asyncio.Event()
    connected = [0]
    connected_lock = asyncio.Lock()
    tasks = [asyncio.create_task(read_one(
        index, release, connected, connected_lock, all_connected,
    )) for index in range(request_count)]
    started = time.monotonic()
    await asyncio.wait_for(all_connected.wait(), timeout=300)
    with open(ready_file, "x", encoding="utf-8") as handle:
        handle.write(str(time.time()))
    release.set()
    responses = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout_seconds)
    reference_contracts = {}
    if reference_file:
        with open(reference_file, encoding="utf-8") as handle:
            reference_contracts = json.load(handle).get("contracts", {})
    contracts = {}
    for index, response in enumerate(responses):
        key = str(response["key_index"])
        contract = {
            "etag": response["etag"],
            "semantic_headers": response["semantic_headers"],
        }
        expected = reference_contracts.get(key) or contracts.setdefault(key, contract)
        if contract != expected:
            raise RuntimeError(
                f"request {index}: stable response contract differs: actual={contract!r}, expected={expected!r}"
            )
    print(json.dumps({
        "requests": len(responses),
        "keys": key_count,
        "bytes": sum(response["bytes"] for response in responses),
        "contracts": contracts or reference_contracts,
        "sha256_failures": 0,
        "duration_seconds": time.monotonic() - started,
    }, sort_keys=True))

asyncio.run(main())
PY
}

summary_fields() {
  local summary_file=$1
  python3 - "$summary_file" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    doc = json.load(handle)
if doc.get("sha256_failures") != 0:
    raise SystemExit("SHA-256 failures were reported")
if not doc.get("contracts"):
    raise SystemExit("no ETag/stable-header response contract was recorded")
print(doc["requests"], doc["duration_seconds"])
PY
}

switch_cache_mode() {
  local mode=$1
  local command=$2
  local round=$3
  local key_count=$4
  run_operator_command "$mode" "$round" "$key_count" "$command" \
    || fail "cache-$mode command failed for round=$round K=$key_count"
  server_in_cgroup || fail "cache-$mode command moved RustFS PID $SERVER_PID out of $CGROUP_PATH"
}

reset_cache() {
  local mode=$1
  local round=$2
  local key_count=$3
  run_operator_command "$mode" "$round" "$key_count" "$RESET_COMMAND" \
    || fail "reset command failed for mode=$mode round=$round K=$key_count"
  server_in_cgroup || fail "reset command moved RustFS PID $SERVER_PID out of $CGROUP_PATH"
}

printf 'mode,k,round,requests,object_size,baseline_reader_bytes,reader_bytes_delta,reader_bytes_limit,follower_samples,follower_max,oom_delta,oom_kill_delta,memory_peak_before,memory_peak_after,duration_seconds\n' >"$RESULTS_CSV"
overall_oom_before=$(cgroup_event oom) || fail "memory.events has no oom field"
overall_oom_kill_before=$(cgroup_event oom_kill) || fail "memory.events has no oom_kill field"

printf 'INFO: artifacts: %s\n' "$OUT_DIR"
printf 'INFO: fixed matrix N=%d, K={1,4,32}, rounds=%d, object_size=%d\n' \
  "$REQUESTS" "$ROUNDS" "$OBJECT_SIZE"

for ((round = 1; round <= ROUNDS; round++)); do
  for key_count in $KEY_MATRIX; do
    baseline_prefix="cache-off-k${key_count}-round${round}"
    baseline_summary="$OUT_DIR/$baseline_prefix-load.json"
    baseline_ready="$OUT_DIR/$baseline_prefix-ready"

    printf 'INFO: K=%d round=%d: switching cache off for one request per key baseline\n' "$key_count" "$round"
    switch_cache_mode off "$CACHE_OFF_COMMAND" "$round" "$key_count"
    reset_cache off "$round" "$key_count"
    baseline_reader_before=$(prometheus_value "$READER_BYTES_QUERY") || fail "reader query failed before cache-off K=$key_count round=$round"
    baseline_oom_before=$(cgroup_event oom) || fail "cannot read oom before baseline"
    baseline_oom_kill_before=$(cgroup_event oom_kill) || fail "cannot read oom_kill before baseline"
    baseline_peak_before=$(cat "$CGROUP_PATH/memory.peak" 2>/dev/null || printf 'NA')

    run_load "$key_count" "$key_count" "$baseline_summary" "$baseline_ready"
    sleep "$METRICS_SETTLE_SECONDS"
    baseline_reader_after=$(prometheus_value "$READER_BYTES_QUERY") || fail "reader query failed after cache-off K=$key_count round=$round"
    baseline_reader_delta=$(numeric_positive_delta "$baseline_reader_before" "$baseline_reader_after") \
      || fail "cache-off reader baseline is invalid for K=$key_count round=$round"
    baseline_oom_after=$(cgroup_event oom) || fail "cannot read oom after baseline"
    baseline_oom_kill_after=$(cgroup_event oom_kill) || fail "cannot read oom_kill after baseline"
    baseline_oom_delta=$((baseline_oom_after - baseline_oom_before))
    baseline_oom_kill_delta=$((baseline_oom_kill_after - baseline_oom_kill_before))
    ((baseline_oom_delta == 0 && baseline_oom_kill_delta == 0)) \
      || fail "OOM event delta is non-zero for cache-off K=$key_count round=$round"
    baseline_peak_after=$(cat "$CGROUP_PATH/memory.peak" 2>/dev/null || printf 'NA')
    read -r baseline_requests baseline_duration < <(summary_fields "$baseline_summary")
    ((baseline_requests == key_count)) \
      || fail "cache-off baseline completed $baseline_requests requests, expected $key_count"
    printf 'cache_off,%d,%d,%d,%d,%s,%s,NA,NA,NA,%d,%d,%s,%s,%s\n' \
      "$key_count" "$round" "$key_count" "$OBJECT_SIZE" "$baseline_reader_delta" "$baseline_reader_delta" \
      "$baseline_oom_delta" "$baseline_oom_kill_delta" "$baseline_peak_before" "$baseline_peak_after" "$baseline_duration" \
      >>"$RESULTS_CSV"

    prefix="k${key_count}-round${round}"
    summary_file="$OUT_DIR/$prefix-load.json"
    ready_file="$OUT_DIR/$prefix-ready"
    follower_file="$OUT_DIR/$prefix-follower.samples"
    : >"$follower_file"

    printf 'INFO: K=%d round=%d: switching cache on and resetting\n' "$key_count" "$round"
    switch_cache_mode on "$CACHE_ON_COMMAND" "$round" "$key_count"
    reset_cache on "$round" "$key_count"

    reader_before=$(prometheus_value "$READER_BYTES_QUERY") || fail "reader query failed before K=$key_count round=$round"
    oom_before=$(cgroup_event oom) || fail "cannot read oom before round"
    oom_kill_before=$(cgroup_event oom_kill) || fail "cannot read oom_kill before round"
    memory_peak_before=$(cat "$CGROUP_PATH/memory.peak" 2>/dev/null || printf 'NA')

    run_load "$key_count" "$REQUESTS" "$summary_file" "$ready_file" "$baseline_summary" &
    load_pid=$!
    LOAD_PID=$load_pid
    while kill -0 "$load_pid" 2>/dev/null; do
      if [[ -e $ready_file ]]; then
        if sample=$(prometheus_sample "$FOLLOWER_PERMIT_QUERY" 2>/dev/null); then
          append_follower_sample "$follower_file" "$sample" || fail "invalid follower sample for K=$key_count round=$round"
        fi
      fi
      sleep "$SAMPLE_INTERVAL"
    done
    wait "$load_pid" || fail "load or SHA-256 validation failed for K=$key_count round=$round (see $summary_file)"
    LOAD_PID=""

    sleep "$METRICS_SETTLE_SECONDS"
    reader_after=$(prometheus_value "$READER_BYTES_QUERY") || fail "reader query failed after K=$key_count round=$round"
    expected_reader_bytes=$(((baseline_reader_delta * 90) / 100))
    reader_limit=$(((baseline_reader_delta * 110 + 99) / 100))
    reader_delta=$(numeric_delta_check "$reader_before" "$reader_after" \
      "$expected_reader_bytes" "$reader_limit") || fail "reader byte invariant failed for K=$key_count round=$round"
    follower_result=$(follower_samples_check "$follower_file" "$ready_file") || fail "follower permit invariant failed for K=$key_count round=$round"
    IFS=, read -r follower_samples follower_max <<<"$follower_result"

    oom_after=$(cgroup_event oom) || fail "cannot read oom after round"
    oom_kill_after=$(cgroup_event oom_kill) || fail "cannot read oom_kill after round"
    server_in_cgroup || fail "RustFS PID $SERVER_PID left $CGROUP_PATH during the run"
    oom_delta=$((oom_after - oom_before))
    oom_kill_delta=$((oom_kill_after - oom_kill_before))
    ((oom_delta == 0 && oom_kill_delta == 0)) || fail "OOM event delta is non-zero for K=$key_count round=$round"
    memory_peak_after=$(cat "$CGROUP_PATH/memory.peak" 2>/dev/null || printf 'NA')

    read -r completed_requests duration_seconds < <(summary_fields "$summary_file")
    ((completed_requests == REQUESTS)) || fail "completed request count is $completed_requests, expected $REQUESTS"

    printf 'cache_on,%d,%d,%d,%d,%s,%s,%d,%s,%s,%d,%d,%s,%s,%s\n' \
      "$key_count" "$round" "$completed_requests" "$OBJECT_SIZE" "$baseline_reader_delta" "$reader_delta" \
      "$reader_limit" "$follower_samples" "$follower_max" "$oom_delta" \
      "$oom_kill_delta" "$memory_peak_before" "$memory_peak_after" "$duration_seconds" \
      >>"$RESULTS_CSV"
    printf 'PASS: K=%d round=%d SHA-256=%s reader_delta=%s follower_max=0 OOM_delta=0\n' \
      "$key_count" "$round" "$EXPECTED_SHA256" "$reader_delta"
  done
done

overall_oom_after=$(cgroup_event oom) || fail "cannot read final oom count"
overall_oom_kill_after=$(cgroup_event oom_kill) || fail "cannot read final oom_kill count"
server_in_cgroup || fail "RustFS PID $SERVER_PID left $CGROUP_PATH during the run"
((overall_oom_after == overall_oom_before)) || fail "overall oom delta is non-zero"
((overall_oom_kill_after == overall_oom_kill_before)) || fail "overall oom_kill delta is non-zero"

printf 'PASS: cold stampede matrix completed; results: %s\n' "$RESULTS_CSV"
