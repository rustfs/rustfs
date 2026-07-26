#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

OUT_DIR=""
ACCESS_KEY=""
SECRET_KEY=""
REGION="us-east-1"
TOOL="warp"
WARP_BIN="warp"
WARP_MODE="get"
DURATION="60s"
CONCURRENCIES="1,64"
SIZES="0B,16383B,16384B,16385B,32767B,32768B,32769B,131071B,131072B,131073B,262143B,262144B,262145B,1048575B,1048576B,1048577B,4194304B"
ROUNDS_PER_LEG=5
COOLDOWN_SECS=20
BUCKET_PREFIX="rustfs-1432-paired"
DRY_RUN=false

RUSTFS_ENDPOINT=""
RUSTFS_IMAGE_REF=""
RUSTFS_IMAGE_DIGEST=""
RUSTFS_REVISION=""
RUSTFS_ACK_CONTRACT=""

MINIO_ENDPOINT=""
MINIO_IMAGE_REF=""
MINIO_IMAGE_DIGEST=""
MINIO_RELEASE=""
MINIO_ACK_CONTRACT=""

COMMON_LABELS=()
RUSTFS_NODE_ARGS=()
MINIO_NODE_ARGS=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_pinned_paired_abba_bench.sh --access-key <ak> --secret-key <sk> \
    --rustfs-endpoint <url> --rustfs-image-ref <ref> --rustfs-image-digest <sha256:...> --rustfs-revision <rev> --rustfs-ack-contract <strict|relaxed> \
    --minio-endpoint <url> --minio-image-ref <ref> --minio-image-digest <sha256:...> --minio-release <release> --minio-ack-contract <strict|relaxed> [options]

Options:
  --out-dir <dir>                         Default: target/bench/pinned-paired-abba-<timestamp>
  --tool <warp|s3bench>                   Default: warp
  --warp-bin <path>                       Default: warp
  --warp-mode <get|put|mixed>             Default: get
  --duration <dur>                        Default: 60s
  --concurrencies <csv>                   Default: 1,64
  --sizes <csv>                           Default: #1432 boundary matrix
  --rounds-per-leg <n>                    Default: 5
  --cooldown-secs <n>                     Default: 20
  --bucket-prefix <prefix>                Default: rustfs-1432-paired
  --label <key=value>                     Repeatable common label passed to every leg
  --rustfs-node-metrics-url <node=url>    Repeatable RustFS node metrics URL
  --rustfs-node-docker-container <node=id>
  --minio-node-metrics-url <node=url>     Repeatable MinIO node metrics URL
  --minio-node-docker-container <node=id>
  --dry-run                               Materialize manifest and commands only
  -h, --help

This runner only orchestrates comparable paired evidence. It does not claim performance causality by itself; every emitted leg delegates to run_object_batch_bench_enhanced.sh with --require-server-provenance.
USAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

validate_nonempty() {
  local value="$1"
  local name="$2"
  [[ -n "$value" ]] || die "$name is required"
}

validate_csv_nonempty() {
  local value="$1"
  local name="$2"
  [[ -n "$value" && "$value" != *",,"* ]] || die "$name must be a non-empty CSV"
}

validate_positive_int() {
  local value="$1"
  local name="$2"
  [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]] || die "$name must be a positive integer, got: $value"
}

validate_nonnegative_int() {
  local value="$1"
  local name="$2"
  [[ "$value" =~ ^[0-9]+$ ]] || die "$name must be a nonnegative integer, got: $value"
}

validate_digest() {
  local value="$1"
  local name="$2"
  [[ "$value" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || die "$name must be an immutable sha256 digest"
}

validate_ack_contract() {
  local value="$1"
  local name="$2"
  case "$value" in
    strict|relaxed) ;;
    *) die "$name must be strict or relaxed" ;;
  esac
}

validate_label() {
  local value="$1"
  local option="$2"
  [[ "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*=.+$ && "$value" != *$'\n'* && "$value" != *,* ]] || die "$option must be key=value without commas/newlines and with a shell-safe key"
}

append_node_arg() {
  local target="$1"
  local option="$2"
  local value="$3"
  validate_label "$value" "$option"
  if [[ "$target" == "rustfs" ]]; then
    RUSTFS_NODE_ARGS+=("$option" "$value")
  else
    MINIO_NODE_ARGS+=("$option" "$value")
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --tool) TOOL="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --warp-mode) WARP_MODE="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --rounds-per-leg) ROUNDS_PER_LEG="$2"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$2"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$2"; shift 2 ;;
      --label) validate_label "$2" "--label"; COMMON_LABELS+=("--label" "$2"); shift 2 ;;
      --rustfs-endpoint) RUSTFS_ENDPOINT="$2"; shift 2 ;;
      --rustfs-image-ref) RUSTFS_IMAGE_REF="$2"; shift 2 ;;
      --rustfs-image-digest) RUSTFS_IMAGE_DIGEST="$2"; shift 2 ;;
      --rustfs-revision) RUSTFS_REVISION="$2"; shift 2 ;;
      --rustfs-ack-contract) RUSTFS_ACK_CONTRACT="$2"; shift 2 ;;
      --rustfs-node-metrics-url) append_node_arg rustfs "--node-metrics-url" "$2"; shift 2 ;;
      --rustfs-node-docker-container) append_node_arg rustfs "--node-docker-container" "$2"; shift 2 ;;
      --minio-endpoint) MINIO_ENDPOINT="$2"; shift 2 ;;
      --minio-image-ref) MINIO_IMAGE_REF="$2"; shift 2 ;;
      --minio-image-digest) MINIO_IMAGE_DIGEST="$2"; shift 2 ;;
      --minio-release) MINIO_RELEASE="$2"; shift 2 ;;
      --minio-ack-contract) MINIO_ACK_CONTRACT="$2"; shift 2 ;;
      --minio-node-metrics-url) append_node_arg minio "--node-metrics-url" "$2"; shift 2 ;;
      --minio-node-docker-container) append_node_arg minio "--node-docker-container" "$2"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

validate_args() {
  validate_nonempty "$ACCESS_KEY" "--access-key"
  validate_nonempty "$SECRET_KEY" "--secret-key"
  validate_nonempty "$RUSTFS_ENDPOINT" "--rustfs-endpoint"
  validate_nonempty "$RUSTFS_IMAGE_REF" "--rustfs-image-ref"
  validate_nonempty "$RUSTFS_IMAGE_DIGEST" "--rustfs-image-digest"
  validate_nonempty "$RUSTFS_REVISION" "--rustfs-revision"
  validate_nonempty "$RUSTFS_ACK_CONTRACT" "--rustfs-ack-contract"
  validate_nonempty "$MINIO_ENDPOINT" "--minio-endpoint"
  validate_nonempty "$MINIO_IMAGE_REF" "--minio-image-ref"
  validate_nonempty "$MINIO_IMAGE_DIGEST" "--minio-image-digest"
  validate_nonempty "$MINIO_RELEASE" "--minio-release"
  validate_nonempty "$MINIO_ACK_CONTRACT" "--minio-ack-contract"
  validate_digest "$RUSTFS_IMAGE_DIGEST" "--rustfs-image-digest"
  validate_digest "$MINIO_IMAGE_DIGEST" "--minio-image-digest"
  validate_ack_contract "$RUSTFS_ACK_CONTRACT" "--rustfs-ack-contract"
  validate_ack_contract "$MINIO_ACK_CONTRACT" "--minio-ack-contract"
  validate_csv_nonempty "$CONCURRENCIES" "--concurrencies"
  validate_csv_nonempty "$SIZES" "--sizes"
  validate_positive_int "$ROUNDS_PER_LEG" "--rounds-per-leg"
  validate_nonnegative_int "$COOLDOWN_SECS" "--cooldown-secs"
  [[ "$TOOL" == "warp" || "$TOOL" == "s3bench" ]] || die "--tool must be warp or s3bench"
  [[ "$WARP_MODE" == "get" || "$WARP_MODE" == "put" || "$WARP_MODE" == "mixed" ]] || die "--warp-mode must be get, put, or mixed"
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/pinned-paired-abba-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  mkdir -p "$OUT_DIR"
}

write_manifest() {
  local manifest="$OUT_DIR/paired_manifest.env"
  {
    printf 'created_utc=%q\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'benchmark_issue=%q\n' "rustfs/backlog#1432"
    printf 'schedule=%q\n' "ABBA"
    printf 'tool=%q\n' "$TOOL"
    printf 'warp_mode=%q\n' "$WARP_MODE"
    printf 'duration=%q\n' "$DURATION"
    printf 'concurrencies=%q\n' "$CONCURRENCIES"
    printf 'sizes=%q\n' "$SIZES"
    printf 'rounds_per_leg=%q\n' "$ROUNDS_PER_LEG"
    printf 'cooldown_secs=%q\n' "$COOLDOWN_SECS"
    printf 'rustfs_endpoint=%q\n' "$RUSTFS_ENDPOINT"
    printf 'rustfs_image_ref=%q\n' "$RUSTFS_IMAGE_REF"
    printf 'rustfs_image_digest=%q\n' "$RUSTFS_IMAGE_DIGEST"
    printf 'rustfs_revision=%q\n' "$RUSTFS_REVISION"
    printf 'rustfs_ack_contract=%q\n' "$RUSTFS_ACK_CONTRACT"
    printf 'minio_endpoint=%q\n' "$MINIO_ENDPOINT"
    printf 'minio_image_ref=%q\n' "$MINIO_IMAGE_REF"
    printf 'minio_image_digest=%q\n' "$MINIO_IMAGE_DIGEST"
    printf 'minio_release=%q\n' "$MINIO_RELEASE"
    printf 'minio_ack_contract=%q\n' "$MINIO_ACK_CONTRACT"
    printf 'access_key=%q\n' "REDACTED"
    printf 'secret_key=%q\n' "REDACTED"
  } > "$manifest"
}

write_schedule() {
  local schedule="$OUT_DIR/abba_schedule.csv"
  echo "leg,product,endpoint,server_image_ref,server_image_digest,server_revision_or_release,ack_contract,leg_out_dir" > "$schedule"
  echo "A1,minio,$MINIO_ENDPOINT,$MINIO_IMAGE_REF,$MINIO_IMAGE_DIGEST,$MINIO_RELEASE,$MINIO_ACK_CONTRACT,$OUT_DIR/A1-minio" >> "$schedule"
  echo "B1,rustfs,$RUSTFS_ENDPOINT,$RUSTFS_IMAGE_REF,$RUSTFS_IMAGE_DIGEST,$RUSTFS_REVISION,$RUSTFS_ACK_CONTRACT,$OUT_DIR/B1-rustfs" >> "$schedule"
  echo "B2,rustfs,$RUSTFS_ENDPOINT,$RUSTFS_IMAGE_REF,$RUSTFS_IMAGE_DIGEST,$RUSTFS_REVISION,$RUSTFS_ACK_CONTRACT,$OUT_DIR/B2-rustfs" >> "$schedule"
  echo "A2,minio,$MINIO_ENDPOINT,$MINIO_IMAGE_REF,$MINIO_IMAGE_DIGEST,$MINIO_RELEASE,$MINIO_ACK_CONTRACT,$OUT_DIR/A2-minio" >> "$schedule"
}

RUNNER_CMD=()

build_runner_cmd_for_product() {
  local product="$1"
  local concurrency="$2"
  local bucket="$3"
  local leg_out_dir="$4"
  local endpoint image_ref image_digest revision_or_release ack_contract
  local -a node_args
  if [[ "$product" == "rustfs" ]]; then
    endpoint="$RUSTFS_ENDPOINT"
    image_ref="$RUSTFS_IMAGE_REF"
    image_digest="$RUSTFS_IMAGE_DIGEST"
    revision_or_release="$RUSTFS_REVISION"
    ack_contract="$RUSTFS_ACK_CONTRACT"
    node_args=("${RUSTFS_NODE_ARGS[@]}")
  else
    endpoint="$MINIO_ENDPOINT"
    image_ref="$MINIO_IMAGE_REF"
    image_digest="$MINIO_IMAGE_DIGEST"
    revision_or_release="$MINIO_RELEASE"
    ack_contract="$MINIO_ACK_CONTRACT"
    node_args=("${MINIO_NODE_ARGS[@]}")
  fi

  RUNNER_CMD=(
    "$ENHANCED_BENCH" \
    --tool "$TOOL" \
    --endpoint "$endpoint" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --region "$REGION" \
    --bucket-size-suffix \
    --concurrency "$concurrency" \
    --sizes "$SIZES" \
    --rounds "$ROUNDS_PER_LEG" \
    --cooldown-secs "$COOLDOWN_SECS" \
    --server-image-ref "$image_ref" \
    --server-image-digest "$image_digest" \
    --server-revision "$revision_or_release" \
    --require-server-provenance \
    --label "bench_issue=1432" \
    --label "product=$product" \
    --label "ack_contract=$ack_contract" \
    --label "schedule=ABBA" \
    --warp-bin "$WARP_BIN" \
    --warp-mode "$WARP_MODE" \
    --duration "$DURATION" \
    --bucket "$bucket" \
    --out-dir "$leg_out_dir"
  )
  RUNNER_CMD+=("${COMMON_LABELS[@]}" "${node_args[@]}")
  if [[ "$DRY_RUN" == "true" ]]; then
    RUNNER_CMD+=(--dry-run)
  fi
}

quote_cmd() {
  printf '%q ' "$@"
}

run_leg() {
  local leg="$1"
  local product="$2"
  local leg_out_dir="$3"
  local conc="$4"
  local leg_lower
  leg_lower="$(printf '%s' "$leg" | tr '[:upper:]' '[:lower:]')"
  local bucket="${BUCKET_PREFIX}-${leg_lower}-c${conc}"
  build_runner_cmd_for_product "$product" "$conc" "$bucket" "$leg_out_dir/c${conc}"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "-- dry-run leg=$leg product=$product concurrency=$conc"
    echo "   $(quote_cmd "${RUNNER_CMD[@]}")"
  else
    "${RUNNER_CMD[@]}"
  fi
}

run_schedule() {
  local conc leg product leg_out_dir
  IFS=',' read -r -a conc_values <<< "$CONCURRENCIES"
  for conc in "${conc_values[@]}"; do
    validate_positive_int "$conc" "concurrency cell"
    while IFS=',' read -r leg product _ _ _ _ _ leg_out_dir; do
      [[ "$leg" != "leg" ]] || continue
      run_leg "$leg" "$product" "$leg_out_dir" "$conc"
    done < "$OUT_DIR/abba_schedule.csv"
  done
}

parse_args "$@"
validate_args
setup_output
write_manifest
write_schedule
run_schedule

echo "paired ABBA artifact root: $OUT_DIR"
