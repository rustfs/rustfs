#!/usr/bin/env bash
set -euo pipefail

# Local GET benchmark harness for the experimental codec streaming read path.
#
# This script intentionally keeps the benchmark orchestration thin:
# - start one local RustFS server with controlled GET/codec/scanner env
# - run the existing enhanced object benchmark in GET mode
# - optionally run legacy and codec profiles back-to-back for A/B comparison

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

ADDRESS="127.0.0.1:19030"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
BUCKET="rustfs-get-codec-smoke"
REGION="us-east-1"
SIZES="1MiB,4MiB,10MiB"
CONCURRENCY=32
DURATION="15s"
ROUNDS=3
RETRY_PER_ROUND=1
ROUND_COOLDOWN_SECS=0
MODE="both"
OUT_DIR=""
RUSTFS_BIN="${PROJECT_ROOT}/target/release/rustfs"
WARP_BIN="warp"
PYTHON_BIN="python3"
CODEC_MIN_SIZE=1
RUST_LOG="warn"
HEALTH_TIMEOUT_SECS=60
COMPAT_OBJECT_KEY="__rustfs_get_v2_pr24_compat/object.bin"
COMPAT_OBJECT_SIZE=65536
DRY_RUN=false
SKIP_BUILD=false

SERVER_PID=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_get_codec_streaming_smoke.sh [options]

Purpose:
  Start a local RustFS server and run GET warp benchmarks for the legacy read
  path, the experimental codec streaming read path, or both.

Core options:
  --mode <legacy|codec|both>     Which profile(s) to run (default: both)
  --address <host:port>          RustFS listen address (default: 127.0.0.1:19030)
  --bucket <name>                Benchmark bucket (default: rustfs-get-codec-smoke)
  --sizes <csv>                  Object sizes (default: 1MiB,4MiB,10MiB)
  --concurrency <n>              warp concurrency (default: 32)
  --duration <duration>          warp duration per round (default: 15s)
  --rounds <n>                   rounds per size (default: 3)
  --retry-per-round <n>          failed-attempt retries per round (default: 1)
  --round-cooldown-secs <n>      cooldown seconds after each completed round (default: 0)
  --out-dir <path>               output directory (default: target/bench/get-codec-streaming-<timestamp>)

Binary/options:
  --rustfs-bin <path>            RustFS binary (default: target/release/rustfs)
  --warp-bin <path>              warp binary (default: warp)
  --python-bin <path>            Python binary for SigV4 compatibility probe (default: python3)
  --codec-min-size <bytes>       RUSTFS_GET_CODEC_STREAMING_MIN_SIZE (default: 1)
  --compat-object-key <key>      Object key used by the compatibility probe
  --compat-object-size <bytes>   Object size used by the compatibility probe (default: 65536)
  --skip-build                   do not run cargo build --release -p rustfs
  --dry-run                      print benchmark commands without starting RustFS

Credentials:
  --access-key <value>           RustFS access key (default: rustfsadmin)
  --secret-key <value>           RustFS secret key (default: rustfsadmin)
  --region <value>               S3 region (default: us-east-1)

Output:
  <out-dir>/legacy/warp/median_summary.csv
  <out-dir>/codec/warp/median_summary.csv
  <out-dir>/codec/warp/baseline_compare.csv    when --mode both
  <out-dir>/compat_summary.csv                  when --mode both
  <out-dir>/body_sha256_legacy.txt              when legacy profile runs
  <out-dir>/body_sha256_new.txt                 when codec profile runs
  <out-dir>/response_headers_legacy.json        when legacy profile runs
  <out-dir>/response_headers_new.json           when codec profile runs
  <out-dir>/<profile>/manifest.env
  <out-dir>/<profile>/metrics_summary.csv
  <out-dir>/<profile>/compat/compat_summary.csv
  <out-dir>/<profile>/compat/response_headers.json
  <out-dir>/<profile>/compat/body_sha256.txt
  <out-dir>/<profile>/rustfs.log

Example:
  scripts/run_get_codec_streaming_smoke.sh \
    --mode both --sizes 1MiB,4MiB,10MiB --concurrency 64 --duration 30s
USAGE
}

log() {
  printf '%s\n' "$*"
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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode) MODE="$2"; shift 2 ;;
      --address) ADDRESS="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --rounds) ROUNDS="$2"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$2"; shift 2 ;;
      --round-cooldown-secs) ROUND_COOLDOWN_SECS="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --rustfs-bin) RUSTFS_BIN="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --python-bin) PYTHON_BIN="$2"; shift 2 ;;
      --codec-min-size) CODEC_MIN_SIZE="$2"; shift 2 ;;
      --compat-object-key) COMPAT_OBJECT_KEY="$2"; shift 2 ;;
      --compat-object-size) COMPAT_OBJECT_SIZE="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
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
  case "$MODE" in
    legacy|codec|both) ;;
    *) die "--mode must be legacy, codec, or both" ;;
  esac

  [[ -n "$ADDRESS" ]] || die "--address must not be empty"
  [[ -n "$ACCESS_KEY" ]] || die "--access-key must not be empty"
  [[ -n "$SECRET_KEY" ]] || die "--secret-key must not be empty"
  [[ -n "$BUCKET" ]] || die "--bucket must not be empty"
  [[ -n "$SIZES" ]] || die "--sizes must not be empty"
  validate_positive_int "$CONCURRENCY" "--concurrency"
  validate_positive_int "$ROUNDS" "--rounds"
  validate_positive_int "$RETRY_PER_ROUND" "--retry-per-round"
  validate_non_negative_int "$ROUND_COOLDOWN_SECS" "--round-cooldown-secs"
  validate_positive_int "$CODEC_MIN_SIZE" "--codec-min-size"
  validate_positive_int "$COMPAT_OBJECT_SIZE" "--compat-object-size"
  validate_positive_int "$HEALTH_TIMEOUT_SECS" "--health-timeout-secs"
  [[ -n "$COMPAT_OBJECT_KEY" ]] || die "--compat-object-key must not be empty"

  [[ -x "$ENHANCED_BENCH" ]] || die "enhanced benchmark script is not executable: $ENHANCED_BENCH"
  require_cmd curl
  require_cmd git

  if [[ "$DRY_RUN" != "true" ]]; then
    require_cmd cargo
    require_cmd "$PYTHON_BIN"
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/get-codec-streaming-$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p "$OUT_DIR"
}

build_rustfs_if_needed() {
  if [[ "$DRY_RUN" == "true" || "$SKIP_BUILD" == "true" ]]; then
    return
  fi

  log "Building RustFS release binary..."
  cargo build --release -p rustfs
}

profile_codec_enabled() {
  local profile="$1"
  case "$profile" in
    legacy) echo "false" ;;
    codec) echo "true" ;;
    *) die "unknown profile: $profile" ;;
  esac
}

profile_data_root() {
  local profile="$1"
  echo "${OUT_DIR}/${profile}/data"
}

profile_volumes() {
  local data_root="$1"
  printf '%s/disk1 %s/disk2 %s/disk3 %s/disk4' "$data_root" "$data_root" "$data_root" "$data_root"
}

endpoint_url() {
  echo "http://${ADDRESS}"
}

write_manifest() {
  local profile="$1"
  local profile_dir="$2"
  local codec_enabled="$3"
  local volumes="$4"
  local git_head git_dirty_count

  git_head="$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
  git_dirty_count="$(git -C "$PROJECT_ROOT" status --porcelain | awk 'END { print NR + 0 }')"

  cat >"${profile_dir}/manifest.env" <<EOF
profile=${profile}
git_head=${git_head}
git_dirty_count=${git_dirty_count}
endpoint=$(endpoint_url)
address=${ADDRESS}
bucket=${BUCKET}
region=${REGION}
sizes=${SIZES}
concurrency=${CONCURRENCY}
duration=${DURATION}
rounds=${ROUNDS}
retry_per_round=${RETRY_PER_ROUND}
round_cooldown_secs=${ROUND_COOLDOWN_SECS}
rustfs_bin=${RUSTFS_BIN}
warp_bin=${WARP_BIN}
python_bin=${PYTHON_BIN}
rust_log=${RUST_LOG}
compat_object_key=${COMPAT_OBJECT_KEY}
compat_object_size=${COMPAT_OBJECT_SIZE}
rustfs_volumes=${volumes}
RUSTFS_GET_CODEC_STREAMING_ENABLE=${codec_enabled}
RUSTFS_GET_CODEC_STREAMING_MIN_SIZE=${CODEC_MIN_SIZE}
RUSTFS_SCANNER_ENABLED=false
RUSTFS_SCANNER_START_DELAY_SECS=3600
RUSTFS_SCANNER_CYCLE=3600
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
  local profile="$1"
  local log_file="$2"
  local health_url
  health_url="$(endpoint_url)/health"

  for ((attempt = 1; attempt <= HEALTH_TIMEOUT_SECS; attempt++)); do
    if curl -fsS --noproxy '*' --connect-timeout 2 --max-time 3 "$health_url" >/dev/null 2>&1; then
      return
    fi
    if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      tail -n 80 "$log_file" >&2 || true
      die "RustFS exited before health check passed for profile: $profile"
    fi
    sleep 1
  done

  tail -n 80 "$log_file" >&2 || true
  die "RustFS health check timed out for profile: $profile"
}

start_server() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local data_root
  local volumes
  local codec_enabled
  local rustfs_log

  data_root="$(profile_data_root "$profile")"
  volumes="$(profile_volumes "$data_root")"
  codec_enabled="$(profile_codec_enabled "$profile")"
  rustfs_log="${profile_dir}/rustfs.log"

  mkdir -p "${data_root}/disk1" "${data_root}/disk2" "${data_root}/disk3" "${data_root}/disk4"
  write_manifest "$profile" "$profile_dir" "$codec_enabled" "$volumes"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] start RustFS profile=${profile} endpoint=$(endpoint_url)"
    log "[DRY-RUN] RUSTFS_VOLUMES=${volumes}"
    return
  fi

  [[ -x "$RUSTFS_BIN" ]] || die "RustFS binary is not executable: $RUSTFS_BIN"

  (
    export RUSTFS_ADDRESS="$ADDRESS"
    export RUSTFS_ACCESS_KEY="$ACCESS_KEY"
    export RUSTFS_SECRET_KEY="$SECRET_KEY"
    export RUSTFS_VOLUMES="$volumes"
    export RUSTFS_CONSOLE_ENABLE=false
    export RUSTFS_GET_CODEC_STREAMING_ENABLE="$codec_enabled"
    export RUSTFS_GET_CODEC_STREAMING_MIN_SIZE="$CODEC_MIN_SIZE"
    export RUSTFS_REGION="$REGION"
    export RUSTFS_RPC_SECRET="rustfs-get-codec-smoke-rpc-secret"
    export RUSTFS_SCANNER_ENABLED=false
    export RUSTFS_SCANNER_START_DELAY_SECS=3600
    export RUSTFS_SCANNER_CYCLE=3600
    export RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
    export RUST_LOG
    exec "$RUSTFS_BIN"
  ) >"$rustfs_log" 2>&1 &

  SERVER_PID="$!"
  wait_for_health "$profile" "$rustfs_log"
}

run_bench() {
  local profile="$1"
  local baseline_csv="${2:-}"
  local profile_dir="${OUT_DIR}/${profile}"
  local bench_dir="${profile_dir}/warp"
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
    --sizes "$SIZES"
    --concurrency "$CONCURRENCY"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --round-cooldown-secs "$ROUND_COOLDOWN_SECS"
    --out-dir "$bench_dir"
  )

  if [[ -n "$baseline_csv" ]]; then
    cmd+=(--baseline-csv "$baseline_csv")
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  log "Running ${profile} GET benchmark..."
  "${cmd[@]}"
}

write_metrics_summary() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local median_csv="${profile_dir}/warp/median_summary.csv"

  if [[ -f "$median_csv" ]]; then
    cp "$median_csv" "${profile_dir}/metrics_summary.csv"
  fi
}

run_compat_probe() {
  local profile="$1"
  local profile_dir="${OUT_DIR}/${profile}"
  local compat_dir="${profile_dir}/compat"

  mkdir -p "$compat_dir"

  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] run compatibility probe profile=${profile} key=${COMPAT_OBJECT_KEY}"
    cat >"${compat_dir}/compat_summary.csv" <<EOF
size,path,body_sha256_match,content_length_match,etag_match,content_range_match,checksum_headers_match,sse_headers_match,status_code_match,error_count,body_sha256,status_code,content_length,etag
${COMPAT_OBJECT_SIZE},${profile},true,true,true,true,true,true,true,0,DRY_RUN,200,${COMPAT_OBJECT_SIZE},DRY_RUN
EOF
    printf '%s\n' "DRY_RUN" >"${compat_dir}/body_sha256.txt"
    printf '{}\n' >"${compat_dir}/response_headers.json"
    cat >"${compat_dir}/snapshot.json" <<EOF
{
  "profile": "${profile}",
  "path": "${profile}",
  "size": ${COMPAT_OBJECT_SIZE},
  "object_key": "${COMPAT_OBJECT_KEY}",
  "expected_body_sha256": "DRY_RUN",
  "body_sha256": "DRY_RUN",
  "body_sha256_match_expected": true,
  "head_status": 200,
  "get_status": 200,
  "head_headers": {
    "content-length": ["${COMPAT_OBJECT_SIZE}"],
    "etag": ["DRY_RUN"]
  },
  "get_headers": {
    "content-length": ["${COMPAT_OBJECT_SIZE}"],
    "etag": ["DRY_RUN"]
  }
}
EOF
    return
  fi

  "$PYTHON_BIN" - "$(endpoint_url)" "$ACCESS_KEY" "$SECRET_KEY" "$REGION" "$BUCKET" \
    "$COMPAT_OBJECT_KEY" "$COMPAT_OBJECT_SIZE" "$compat_dir" "$profile" <<'PY'
import datetime as dt
import hashlib
import hmac
import http.client
import json
import pathlib
import sys
import urllib.parse

endpoint, access_key, secret_key, region, bucket, object_key, object_size_raw, out_dir_raw, profile = sys.argv[1:]
object_size = int(object_size_raw)
out_dir = pathlib.Path(out_dir_raw)
out_dir.mkdir(parents=True, exist_ok=True)


def payload(size: int) -> bytes:
    return bytes(((index * 31 + 7) % 251 for index in range(size)))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sign(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def signing_key(secret: str, date_stamp: str) -> bytes:
    key_date = sign(("AWS4" + secret).encode(), date_stamp)
    key_region = sign(key_date, region)
    key_service = sign(key_region, "s3")
    return sign(key_service, "aws4_request")


def canonical_query(query: str) -> str:
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = [
        (urllib.parse.quote(key, safe="-_.~"), urllib.parse.quote(value, safe="-_.~"))
        for key, value in pairs
    ]
    encoded.sort()
    return "&".join(f"{key}={value}" for key, value in encoded)


def canonical_uri(path: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="-_.~/") for part in path.split("/")) or "/"


def signed_headers(method: str, path: str, body: bytes, extra_headers: list[tuple[str, str]]) -> dict[str, str]:
    parsed = urllib.parse.urlsplit(endpoint)
    amz_date = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    headers = [("host", parsed.netloc), ("x-amz-content-sha256", sha256_hex(body)), ("x-amz-date", amz_date)]
    headers.extend((name.lower(), " ".join(value.strip().split())) for name, value in extra_headers)
    headers.sort()
    canonical_headers = "".join(f"{name}:{value}\n" for name, value in headers)
    signed_names = ";".join(name for name, _ in headers)
    raw_path, _, raw_query = path.partition("?")
    canonical_request = "\n".join(
        [method, canonical_uri(raw_path), canonical_query(raw_query), canonical_headers, signed_names, sha256_hex(body)]
    )
    scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
    )
    signature = hmac.new(signing_key(secret_key, date_stamp), string_to_sign.encode(), hashlib.sha256).hexdigest()
    result = {name: value for name, value in headers}
    result["Authorization"] = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_names}, "
        f"Signature={signature}"
    )
    return result


def request(method: str, path: str, body: bytes = b"", extra_headers: list[tuple[str, str]] | None = None):
    parsed = urllib.parse.urlsplit(endpoint)
    headers = signed_headers(method, path, body, extra_headers or [])
    if body and "content-length" not in headers:
        headers["content-length"] = str(len(body))
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    response_body = response.read()
    snapshot = {
        "status": response.status,
        "reason": response.reason,
        "headers": response.getheaders(),
        "body_sha256": sha256_hex(response_body),
        "body_len": len(response_body),
    }
    conn.close()
    return snapshot, response_body


def header_map(headers: list[list[str] | tuple[str, str]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for name, value in headers:
        result.setdefault(name.lower(), []).append(value)
    for value in result.values():
        value.sort()
    return dict(sorted(result.items()))


bucket_path = "/" + urllib.parse.quote(bucket, safe="")
object_path = bucket_path + "/" + urllib.parse.quote(object_key, safe="/-_.~")
body = payload(object_size)

create_bucket, _ = request("PUT", bucket_path)
if create_bucket["status"] not in (200, 409):
    raise SystemExit(f"create bucket failed: {create_bucket['status']} {create_bucket['reason']}")

put_object, _ = request("PUT", object_path, body, [("content-type", "application/octet-stream")])
if put_object["status"] not in (200, 204):
    raise SystemExit(f"put object failed: {put_object['status']} {put_object['reason']}")

head_object, _ = request("HEAD", object_path)
get_object, get_body = request("GET", object_path)
if get_object["status"] != 200:
    raise SystemExit(f"get object failed: {get_object['status']} {get_object['reason']}")

body_sha = sha256_hex(get_body)
expected_sha = sha256_hex(body)
body_match = body_sha == expected_sha
headers_report = {
    "create_bucket": create_bucket,
    "put_object": put_object,
    "head_object": head_object,
    "get_object": get_object,
}
snapshot = {
    "profile": profile,
    "path": profile,
    "size": object_size,
    "object_key": object_key,
    "expected_body_sha256": expected_sha,
    "body_sha256": body_sha,
    "body_sha256_match_expected": body_match,
    "head_status": head_object["status"],
    "get_status": get_object["status"],
    "head_headers": header_map(head_object["headers"]),
    "get_headers": header_map(get_object["headers"]),
}

(out_dir / "response_headers.json").write_text(json.dumps(headers_report, indent=2, sort_keys=True) + "\n")
(out_dir / "snapshot.json").write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
(out_dir / "body_sha256.txt").write_text(body_sha + "\n")

get_headers = snapshot["get_headers"]
content_length = ",".join(get_headers.get("content-length", []))
etag = ",".join(get_headers.get("etag", []))
status_match = head_object["status"] == 200 and get_object["status"] == 200
error_count = 0 if body_match and status_match else 1
(out_dir / "compat_summary.csv").write_text(
    "size,path,body_sha256_match,content_length_match,etag_match,content_range_match,"
    "checksum_headers_match,sse_headers_match,status_code_match,error_count,body_sha256,status_code,content_length,etag\n"
    f"{object_size},{profile},{str(body_match).lower()},true,true,true,true,true,{str(status_match).lower()},"
    f"{error_count},{body_sha},{get_object['status']},{content_length},{etag}\n"
)
PY
}

copy_profile_compat_artifacts() {
  local profile="$1"
  local label="$2"
  local compat_dir="${OUT_DIR}/${profile}/compat"

  if [[ -f "${compat_dir}/body_sha256.txt" ]]; then
    cp "${compat_dir}/body_sha256.txt" "${OUT_DIR}/body_sha256_${label}.txt"
  fi
  if [[ -f "${compat_dir}/response_headers.json" ]]; then
    cp "${compat_dir}/response_headers.json" "${OUT_DIR}/response_headers_${label}.json"
  fi
}

write_ab_compat_summary() {
  local legacy_snapshot="${OUT_DIR}/legacy/compat/snapshot.json"
  local codec_snapshot="${OUT_DIR}/codec/compat/snapshot.json"

  if [[ ! -f "$legacy_snapshot" || ! -f "$codec_snapshot" ]]; then
    return
  fi

  "$PYTHON_BIN" - "$legacy_snapshot" "$codec_snapshot" "${OUT_DIR}/compat_summary.csv" <<'PY'
import csv
import json
import sys

legacy_path, codec_path, out_csv = sys.argv[1:]
legacy = json.load(open(legacy_path, encoding="utf-8"))
codec = json.load(open(codec_path, encoding="utf-8"))


def values(snapshot, name):
    return snapshot.get("get_headers", {}).get(name, [])


def prefixed(snapshot, prefix):
    headers = snapshot.get("get_headers", {})
    return {key: headers[key] for key in sorted(headers) if key.startswith(prefix)}


body_match = legacy.get("body_sha256") == codec.get("body_sha256")
content_length_match = values(legacy, "content-length") == values(codec, "content-length")
etag_match = values(legacy, "etag") == values(codec, "etag")
content_range_match = values(legacy, "content-range") == values(codec, "content-range")
checksum_headers_match = prefixed(legacy, "x-amz-checksum") == prefixed(codec, "x-amz-checksum")
sse_headers_match = prefixed(legacy, "x-amz-server-side-encryption") == prefixed(codec, "x-amz-server-side-encryption")
status_code_match = legacy.get("get_status") == codec.get("get_status") and legacy.get("head_status") == codec.get("head_status")
checks = [
    body_match,
    content_length_match,
    etag_match,
    content_range_match,
    checksum_headers_match,
    sse_headers_match,
    status_code_match,
]

with open(out_csv, "w", encoding="utf-8", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(
        [
            "size",
            "path",
            "body_sha256_match",
            "content_length_match",
            "etag_match",
            "content_range_match",
            "checksum_headers_match",
            "sse_headers_match",
            "status_code_match",
            "error_count",
            "legacy_body_sha256",
            "new_body_sha256",
            "legacy_status_code",
            "new_status_code",
        ]
    )
    writer.writerow(
        [
            codec.get("size", legacy.get("size", "")),
            "codec_streaming",
            str(body_match).lower(),
            str(content_length_match).lower(),
            str(etag_match).lower(),
            str(content_range_match).lower(),
            str(checksum_headers_match).lower(),
            str(sse_headers_match).lower(),
            str(status_code_match).lower(),
            sum(1 for check in checks if not check),
            legacy.get("body_sha256", ""),
            codec.get("body_sha256", ""),
            legacy.get("get_status", ""),
            codec.get("get_status", ""),
        ]
    )
PY
}

run_profile() {
  local profile="$1"
  local baseline_csv="${2:-}"

  stop_server
  start_server "$profile"
  run_bench "$profile" "$baseline_csv"
  write_metrics_summary "$profile"
  run_compat_probe "$profile"
  stop_server

  log "Median summary: ${OUT_DIR}/${profile}/warp/median_summary.csv"
  log "Metrics summary: ${OUT_DIR}/${profile}/metrics_summary.csv"
  log "Compatibility summary: ${OUT_DIR}/${profile}/compat/compat_summary.csv"
  if [[ -f "${OUT_DIR}/${profile}/warp/baseline_compare.csv" ]]; then
    log "Baseline compare: ${OUT_DIR}/${profile}/warp/baseline_compare.csv"
  fi
}

main() {
  parse_args "$@"
  validate_args
  setup_output
  build_rustfs_if_needed

  trap stop_server EXIT INT TERM

  log "Output dir: $OUT_DIR"
  case "$MODE" in
    legacy)
      run_profile legacy
      copy_profile_compat_artifacts legacy legacy
      ;;
    codec)
      run_profile codec
      copy_profile_compat_artifacts codec new
      ;;
    both)
      run_profile legacy
      copy_profile_compat_artifacts legacy legacy
      run_profile codec "${OUT_DIR}/legacy/warp/median_summary.csv"
      copy_profile_compat_artifacts codec new
      write_ab_compat_summary
      ;;
  esac

  if [[ -f "${OUT_DIR}/compat_summary.csv" ]]; then
    log "Compatibility compare: ${OUT_DIR}/compat_summary.csv"
  fi
  log "GET codec streaming smoke finished."
}

main "$@"
