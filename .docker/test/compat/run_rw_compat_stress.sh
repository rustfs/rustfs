#!/usr/bin/env bash
set -euo pipefail

# High-concurrency S3 read/write stress runner for rio/rio-v2 format compatibility.
# Write a manifest on an old endpoint, then verify the same manifest through rio-v2.

MODE="write"
ENDPOINT=""
ACCESS_KEY="${AWS_ACCESS_KEY_ID:-}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-}"
BUCKET="compat-rw-stress"
REGION="us-east-1"
CONCURRENCY=64
OUT_DIR=""
WORK_DIR=""
MANIFEST=""
PROFILE="200g"
OBJECT_SPEC=""
DATA_PATTERN="compressible"
ENCRYPTION="none"
SSE_KMS_KEY_ID=""
SSE_C_KEY_FILE=""
CLIENT="mc"
AWS_BIN="${AWS_BIN:-aws}"
MC_BIN="${MC_BIN:-}"
KEEP_PAYLOADS=false
DRY_RUN=false
RESUME=false

usage() {
  cat <<'USAGE'
Usage:
  .docker/compat/run_rw_compat_stress.sh --mode <write|verify|mixed> \
    --endpoint <url> --access-key <ak> --secret-key <sk> [options]

Modes:
  write     Create bucket, upload objects concurrently, and write manifest.csv.
  verify    Read objects concurrently from --endpoint and verify against manifest.csv.
  mixed     Write and verify against the same endpoint.

Required:
  --endpoint              S3 endpoint URL, for example http://127.0.0.1:9100
  --access-key            S3 access key
  --secret-key            S3 secret key

Core options:
  --bucket                Bucket name (default: compat-rw-stress)
  --region                Region (default: us-east-1)
  --concurrency           Parallel object operations (default: 64)
  --out-dir               Output directory (default: target/compat/rw-stress-<timestamp>)
  --work-dir              Payload scratch directory (default: <out-dir>/payloads)
  --manifest              Manifest path (default: <out-dir>/manifest.csv for write/mixed)
  --profile               compact | 5g | 200g (default: 200g)
  --object-spec           Override profile. Format: size:count,size:count
                         Example: 1KiB:1024,1MiB:1024,64MiB:512,1GiB:64
  --data-pattern          compressible | random | mixed (default: compressible)
  --keep-payloads         Do not delete local payload files after upload
  --resume                Skip write tasks that already have rows in <out-dir>/tasks/write-rows
  --dry-run               Print planned tasks and commands without executing S3 operations
  --client                mc | aws (default: mc)
  --mc-bin                Path to mc binary (default: first tmp/mc.* or mc in PATH)
  --aws-bin               Path to aws binary (used with --client aws)

Encryption options:
  --encryption            none | sse-s3 | sse-kms | sse-c (default: none)
  --sse-kms-key-id        KMS key id for --encryption sse-kms
  --sse-c-key-file        Raw 32-byte SSE-C key file for --encryption sse-c

Examples:
  # Generate the old RustFS beta5 dataset.
  .docker/compat/run_rw_compat_stress.sh \
    --mode write --endpoint http://127.0.0.1:9100 \
    --access-key rustfsadmin --secret-key rustfsadmin \
    --bucket compat-beta5 --concurrency 96

  # Verify that dataset after mounting beta5 disks into the rio-v2 compose.
  .docker/compat/run_rw_compat_stress.sh \
    --mode verify --endpoint http://127.0.0.1:9300 \
    --access-key rustfsadmin --secret-key rustfsadmin \
    --bucket compat-beta5 --concurrency 96 \
    --manifest target/compat/rw-stress-YYYYmmdd-HHMMSS/manifest.csv

  # Faster smoke run.
  .docker/compat/run_rw_compat_stress.sh \
    --mode mixed --endpoint http://127.0.0.1:9300 \
    --access-key rustfsadmin --secret-key rustfsadmin \
    --profile compact --concurrency 16
USAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "command not found: $1"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode) MODE="$2"; shift 2 ;;
      --endpoint) ENDPOINT="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --concurrency) CONCURRENCY="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --work-dir) WORK_DIR="$2"; shift 2 ;;
      --manifest) MANIFEST="$2"; shift 2 ;;
      --profile) PROFILE="$2"; shift 2 ;;
      --object-spec) OBJECT_SPEC="$2"; shift 2 ;;
      --data-pattern) DATA_PATTERN="$2"; shift 2 ;;
      --encryption) ENCRYPTION="$2"; shift 2 ;;
      --sse-kms-key-id) SSE_KMS_KEY_ID="$2"; shift 2 ;;
      --sse-c-key-file) SSE_C_KEY_FILE="$2"; shift 2 ;;
      --client) CLIENT="$2"; shift 2 ;;
      --mc-bin) MC_BIN="$2"; shift 2 ;;
      --aws-bin) AWS_BIN="$2"; shift 2 ;;
      --keep-payloads) KEEP_PAYLOADS=true; shift ;;
      --resume) RESUME=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

validate_args() {
  [[ "$MODE" =~ ^(write|verify|mixed)$ ]] || die "--mode must be write, verify, or mixed"
  [[ "$CLIENT" =~ ^(mc|aws)$ ]] || die "--client must be mc or aws"
  [[ "$PROFILE" =~ ^(compact|5g|200g)$ ]] || die "--profile must be compact, 5g, or 200g"
  [[ "$DATA_PATTERN" =~ ^(compressible|random|mixed)$ ]] || die "--data-pattern must be compressible, random, or mixed"
  [[ "$ENCRYPTION" =~ ^(none|sse-s3|sse-kms|sse-c)$ ]] || die "--encryption must be none, sse-s3, sse-kms, or sse-c"
  [[ -n "$ENDPOINT" && -n "$ACCESS_KEY" && -n "$SECRET_KEY" ]] || die "--endpoint/--access-key/--secret-key are required"
  [[ "$CONCURRENCY" =~ ^[0-9]+$ && "$CONCURRENCY" -gt 0 ]] || die "--concurrency must be a positive integer"
  if [[ "$ENCRYPTION" == "sse-kms" && -z "$SSE_KMS_KEY_ID" ]]; then
    die "--sse-kms-key-id is required for --encryption sse-kms"
  fi
  if [[ "$ENCRYPTION" == "sse-c" ]]; then
    [[ -n "$SSE_C_KEY_FILE" && -f "$SSE_C_KEY_FILE" ]] || die "--sse-c-key-file must point to an existing key file"
  fi
  if [[ "$MODE" == "verify" && -z "$MANIFEST" ]]; then
    die "--manifest is required for --mode verify"
  fi
}

setup_paths() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/compat/rw-stress-$(date +%Y%m%d-%H%M%S)"
  fi
  if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$OUT_DIR/payloads"
  fi
  if [[ -z "$MANIFEST" ]]; then
    MANIFEST="$OUT_DIR/manifest.csv"
  fi

  mkdir -p "$OUT_DIR" "$WORK_DIR" "$OUT_DIR/tasks" "$OUT_DIR/logs"
  TASKS_FILE="$OUT_DIR/tasks/tasks.tsv"
  WRITE_ROWS_DIR="$OUT_DIR/tasks/write-rows"
  VERIFY_ROWS_DIR="$OUT_DIR/tasks/verify-rows"
  MC_CONFIG_DIR_LOCAL="$OUT_DIR/mc-config"
  MC_ALIAS="compat"
  mkdir -p "$WRITE_ROWS_DIR" "$VERIFY_ROWS_DIR"
}

resolve_mc_bin() {
  if [[ -n "$MC_BIN" ]]; then
    echo "$MC_BIN"
    return
  fi

  local candidate
  candidate="$(find tmp -maxdepth 1 -type f -name 'mc.*' -perm -111 2>/dev/null | sort | tail -n 1 || true)"
  if [[ -n "$candidate" ]]; then
    echo "$candidate"
    return
  fi

  command -v mc 2>/dev/null || true
}

size_to_bytes() {
  local raw="$1"
  local num unit
  if [[ "$raw" =~ ^([0-9]+)(B|KiB|MiB|GiB|KB|MB|GB)?$ ]]; then
    num="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]:-B}"
  else
    die "invalid size: $raw"
  fi

  case "$unit" in
    B) echo "$num" ;;
    KiB) echo $((num * 1024)) ;;
    MiB) echo $((num * 1024 * 1024)) ;;
    GiB) echo $((num * 1024 * 1024 * 1024)) ;;
    KB) echo $((num * 1000)) ;;
    MB) echo $((num * 1000 * 1000)) ;;
    GB) echo $((num * 1000 * 1000 * 1000)) ;;
    *) die "invalid size unit: $unit" ;;
  esac
}

profile_spec() {
  if [[ -n "$OBJECT_SPEC" ]]; then
    echo "$OBJECT_SPEC"
    return
  fi

  case "$PROFILE" in
    compact)
      echo "1KiB:64,1MiB:64,16MiB:16,128MiB:4"
      ;;
    5g)
      echo "1KiB:1024,1MiB:255,16MiB:64,64MiB:28,1GiB:2"
      ;;
    200g)
      echo "1KiB:1024,1MiB:1024,64MiB:512,1GiB:64,8GiB:12,6GiB:1"
      ;;
  esac
}

content_for_index() {
  local index="$1"
  case $((index % 3)) in
    0) echo "txt|text/plain" ;;
    1) echo "json|application/json" ;;
    2) echo "bin|binary/octet-stream" ;;
  esac
}

generate_tasks() {
  local spec item size count bytes i content ext mime key seed index=0
  spec="$(profile_spec)"
  : > "$TASKS_FILE"

  IFS=',' read -r -a items <<< "$spec"
  for item in "${items[@]}"; do
    item="${item//[[:space:]]/}"
    [[ -n "$item" ]] || continue
    [[ "$item" =~ ^([^:]+):([0-9]+)$ ]] || die "invalid object spec item: $item"
    size="${BASH_REMATCH[1]}"
    count="${BASH_REMATCH[2]}"
    bytes="$(size_to_bytes "$size")"

    for ((i = 1; i <= count; i++)); do
      content="$(content_for_index "$index")"
      ext="${content%%|*}"
      mime="${content#*|}"
      key="rw-stress/${size}/obj-$(printf '%06d' "$i").${ext}"
      seed="${BUCKET}:${key}:${bytes}"
      printf '%s\t%s\t%s\t%s\t%s\n' "$key" "$size" "$bytes" "$mime" "$seed" >> "$TASKS_FILE"
      index=$((index + 1))
    done
  done
}

write_row_file_for_key() {
  local key="$1"
  echo "$WRITE_ROWS_DIR/${key//\//_}.csv"
}

prepare_write_input() {
  WRITE_INPUT="$TASKS_FILE"
  if [[ "$RESUME" != "true" ]]; then
    return
  fi

  WRITE_INPUT="$OUT_DIR/tasks/write-input.tsv"
  : > "$WRITE_INPUT"

  local line key _size _bytes _mime _seed row_file
  while IFS= read -r line || [[ -n "$line" ]]; do
    IFS=$'\t' read -r key _size _bytes _mime _seed <<< "$line"
    row_file="$(write_row_file_for_key "$key")"
    [[ -s "$row_file" ]] && continue
    printf '%s\n' "$line" >> "$WRITE_INPUT"
  done < "$TASKS_FILE"
}

print_plan() {
  local total_objects total_bytes profile_label
  if [[ -f "$TASKS_FILE" ]]; then
    total_objects="$(wc -l < "$TASKS_FILE" | tr -d ' ')"
    total_bytes="$(awk -F '\t' '{sum += $3} END {print sum + 0}' "$TASKS_FILE")"
    profile_label="$(profile_spec)"
  else
    total_objects="$(awk 'END {count = NR - 1; if (count < 0) count = 0; print count}' "$MANIFEST")"
    total_bytes="$(awk -F ',' 'NR > 1 {sum += $3} END {print sum + 0}' "$MANIFEST")"
    profile_label="from manifest"
  fi

  cat <<PLAN
Mode:        $MODE
Endpoint:    $ENDPOINT
Bucket:      $BUCKET
Profile:     $profile_label
Objects:     $total_objects
Bytes:       $total_bytes
Concurrency: $CONCURRENCY
Encryption:  $ENCRYPTION
Client:      $CLIENT
Manifest:    $MANIFEST
Out dir:     $OUT_DIR
Work dir:    $WORK_DIR
PLAN
}

aws_base() {
  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_DEFAULT_REGION="$REGION" \
    "$AWS_BIN" --endpoint-url "$ENDPOINT" "$@"
}

mc_base() {
  "$MC_BIN" --config-dir "$MC_CONFIG_DIR_LOCAL" "$@"
}

aws_cp_args() {
  case "$ENCRYPTION" in
    none) ;;
    sse-s3) printf '%s\n' "--sse" "AES256" ;;
    sse-kms) printf '%s\n' "--sse" "aws:kms" "--sse-kms-key-id" "$SSE_KMS_KEY_ID" ;;
    sse-c) printf '%s\n' "--sse-c" "AES256" "--sse-c-key" "fileb://$SSE_C_KEY_FILE" ;;
  esac
}

mc_enc_target() {
  printf '%s/%s/rw-stress/' "$MC_ALIAS" "$BUCKET"
}

mc_cp_args() {
  local target
  target="$(mc_enc_target)"
  case "$ENCRYPTION" in
    none) ;;
    sse-s3) printf '%s\n' "--enc-s3" "$target" ;;
    sse-kms) printf '%s\n' "--enc-kms" "${target}=${SSE_KMS_KEY_ID}" ;;
    sse-c)
      local key_b64
      key_b64="$(base64 < "$SSE_C_KEY_FILE" | tr -d '\n')"
      printf '%s\n' "--enc-c" "${target}=${key_b64}"
      ;;
  esac
}

aws_get_args() {
  if [[ "$ENCRYPTION" == "sse-c" ]]; then
    printf '%s\n' "--sse-c" "AES256" "--sse-c-key" "fileb://$SSE_C_KEY_FILE"
  fi
}

mc_get_args() {
  if [[ "$ENCRYPTION" == "sse-c" ]]; then
    local target key_b64
    target="$(mc_enc_target)"
    key_b64="$(base64 < "$SSE_C_KEY_FILE" | tr -d '\n')"
    printf '%s\n' "--enc-c" "${target}=${key_b64}"
  fi
}

setup_mc_alias() {
  if [[ "$CLIENT" != "mc" || "$DRY_RUN" == "true" ]]; then
    return
  fi
  mkdir -p "$MC_CONFIG_DIR_LOCAL"
  mc_base alias set "$MC_ALIAS" "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" --api S3v4 --path auto >/dev/null
}

create_bucket_if_needed() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] create bucket if missing: $BUCKET"
    return
  fi

  if [[ "$CLIENT" == "mc" ]]; then
    mc_base mb --ignore-existing --region "$REGION" "$MC_ALIAS/$BUCKET" >/dev/null
    return
  fi

  if aws_base s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    return
  fi

  aws_base s3api create-bucket --bucket "$BUCKET" >/dev/null
}

payload_path_for_key() {
  local key="$1"
  echo "$WORK_DIR/${key//\//_}"
}

generate_payload() {
  local file="$1"
  local bytes="$2"
  local seed="$3"
  local pattern="$DATA_PATTERN"
  mkdir -p "$(dirname "$file")"

  if [[ "$pattern" == "mixed" ]]; then
    if [[ $((bytes % 2)) -eq 0 ]]; then
      pattern="compressible"
    else
      pattern="random"
    fi
  fi

  if [[ "$pattern" == "random" ]]; then
    head -c "$bytes" /dev/zero | openssl enc -aes-256-ctr -nosalt -pass "pass:$seed" -out "$file"
  else
    yes "$seed payload-for-rio-compatibility" | tr '\n' ' ' | head -c "$bytes" > "$file"
  fi
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

sha256_object() {
  local key="$1"
  shift
  if [[ "$CLIENT" == "mc" ]]; then
    mc_base cat "$@" "$MC_ALIAS/$BUCKET/$key" | shasum -a 256 | awk '{print $1}'
  else
    aws_base s3 cp "s3://$BUCKET/$key" - --no-progress "$@" | shasum -a 256 | awk '{print $1}'
  fi
}

collect_args() {
  local generator="$1"
  extra_args=()
  while IFS= read -r arg; do
    extra_args+=("$arg")
  done < <("$generator")
}

write_one() {
  local line="$1"
  local key size bytes mime seed file sha row_file
  local -a extra_args
  IFS=$'\t' read -r key size bytes mime seed <<< "$line"
  file="$(payload_path_for_key "$key")"
  row_file="$(write_row_file_for_key "$key")"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] upload $bytes bytes to s3://$BUCKET/$key content-type=$mime"
    printf '%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "DRY_RUN" "$ENCRYPTION" > "$row_file"
    return
  fi

  generate_payload "$file" "$bytes" "$seed"
  sha="$(sha256_file "$file")"

  if [[ "$CLIENT" == "mc" ]]; then
    collect_args mc_cp_args
    mc_base cp --quiet --attr "Content-Type=$mime" ${extra_args[@]+"${extra_args[@]}"} "$file" "$MC_ALIAS/$BUCKET/$key" \
      > "$OUT_DIR/logs/${key//\//_}.put.log" 2>&1
  else
    collect_args aws_cp_args
    aws_base s3 cp "$file" "s3://$BUCKET/$key" --no-progress --content-type "$mime" ${extra_args[@]+"${extra_args[@]}"} \
      > "$OUT_DIR/logs/${key//\//_}.put.log" 2>&1
  fi

  printf '%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "$sha" "$ENCRYPTION" > "$row_file"
  if [[ "$KEEP_PAYLOADS" != "true" ]]; then
    rm -f "$file"
  fi
}

verify_one() {
  local line="$1"
  local key size bytes mime expected encryption actual row_file
  local -a extra_args
  IFS=',' read -r key size bytes mime expected encryption <<< "$line"
  row_file="$VERIFY_ROWS_DIR/${key//\//_}.csv"

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] verify s3://$BUCKET/$key expected=$expected"
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "$expected" "DRY_RUN" "dry-run" > "$row_file"
    return
  fi

  if [[ "$CLIENT" == "mc" ]]; then
    collect_args mc_get_args
  else
    collect_args aws_get_args
  fi
  if ! actual="$(sha256_object "$key" ${extra_args[@]+"${extra_args[@]}"})"; then
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "$expected" "ERROR" "download failed" > "$row_file"
    return 1
  fi

  if [[ "$actual" == "$expected" ]]; then
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "$expected" "$actual" "ok" > "$row_file"
  else
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$key" "$size" "$bytes" "$mime" "$expected" "$actual" "sha256-mismatch" > "$row_file"
    return 1
  fi
}

run_parallel_tasks() {
  local action="$1"
  local input_file="$2"
  local failure_file="$OUT_DIR/tasks/${action}.failed"
  local line
  rm -f "$failure_file"

  while IFS= read -r line || [[ -n "$line" ]]; do
    while [[ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$CONCURRENCY" ]]; do
      sleep 0.2
    done

    ({
      if [[ "$action" == "write" ]]; then
        write_one "$line"
      else
        verify_one "$line"
      fi
    } || touch "$failure_file") &
  done < "$input_file"

  wait
  [[ ! -f "$failure_file" ]]
}

combine_write_manifest() {
  echo "key,size,bytes,content_type,sha256,encryption" > "$MANIFEST"
  find "$WRITE_ROWS_DIR" -type f -name '*.csv' -print0 | sort -z | xargs -0 cat >> "$MANIFEST"

  local expected actual
  expected="$(wc -l < "$TASKS_FILE" | tr -d ' ')"
  actual="$(( $(wc -l < "$MANIFEST" | tr -d ' ') - 1 ))"
  if [[ "$actual" -ne "$expected" ]]; then
    die "manifest row count mismatch: expected $expected, got $actual"
  fi
}

prepare_verify_input() {
  VERIFY_INPUT="$OUT_DIR/tasks/verify-input.csv"
  tail -n +2 "$MANIFEST" > "$VERIFY_INPUT"
}

combine_verify_summary() {
  VERIFY_SUMMARY="$OUT_DIR/verify-summary.csv"
  echo "key,size,bytes,content_type,expected_sha256,actual_sha256,status" > "$VERIFY_SUMMARY"
  find "$VERIFY_ROWS_DIR" -type f -name '*.csv' -print0 | sort -z | xargs -0 cat >> "$VERIFY_SUMMARY"

  local failed
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "Verification dry run complete. Summary: $VERIFY_SUMMARY"
    return
  fi

  failed="$(awk -F ',' 'NR > 1 && $7 != "ok" {count++} END {print count + 0}' "$VERIFY_SUMMARY")"
  local expected actual
  expected="$(wc -l < "$VERIFY_INPUT" | tr -d ' ')"
  actual="$(( $(wc -l < "$VERIFY_SUMMARY" | tr -d ' ') - 1 ))"
  if [[ "$actual" -ne "$expected" ]]; then
    echo "Verification row count mismatch: expected $expected, got $actual" >&2
    return 1
  fi
  if [[ "$failed" -ne 0 ]]; then
    echo "Verification failed: $failed object(s). See $VERIFY_SUMMARY" >&2
    return 1
  fi
  echo "Verification passed. Summary: $VERIFY_SUMMARY"
}

export_functions() {
  export ENDPOINT ACCESS_KEY SECRET_KEY BUCKET REGION ENCRYPTION SSE_KMS_KEY_ID SSE_C_KEY_FILE AWS_BIN
  export CLIENT MC_BIN MC_CONFIG_DIR_LOCAL MC_ALIAS
  export WORK_DIR OUT_DIR WRITE_ROWS_DIR VERIFY_ROWS_DIR DATA_PATTERN KEEP_PAYLOADS DRY_RUN
  export -f aws_base mc_base aws_cp_args aws_get_args mc_enc_target mc_cp_args mc_get_args payload_path_for_key generate_payload sha256_file sha256_object write_one verify_one
}

main() {
  parse_args "$@"
  validate_args
  setup_paths

  if [[ "$DRY_RUN" != "true" ]]; then
    if [[ "$CLIENT" == "mc" ]]; then
      MC_BIN="$(resolve_mc_bin)"
      [[ -n "$MC_BIN" ]] || die "mc binary not found; pass --mc-bin or put mc in PATH"
      require_cmd "$MC_BIN"
    else
      require_cmd "$AWS_BIN"
    fi
    require_cmd shasum
    require_cmd head
    require_cmd yes
    require_cmd openssl
  elif [[ "$CLIENT" == "mc" ]]; then
    MC_BIN="$(resolve_mc_bin)"
  fi

  setup_mc_alias

  if [[ "$MODE" == "write" || "$MODE" == "mixed" ]]; then
    local write_failed=false
    generate_tasks
    prepare_write_input
    print_plan
    create_bucket_if_needed
    export_functions
    if ! run_parallel_tasks write "$WRITE_INPUT"; then
      write_failed=true
    fi
    combine_write_manifest
    echo "Write manifest: $MANIFEST"
    if [[ "$write_failed" == "true" ]]; then
      die "one or more write tasks failed; see $OUT_DIR/logs"
    fi
  fi

  if [[ "$MODE" == "verify" || "$MODE" == "mixed" ]]; then
    local verify_failed=false
    [[ -f "$MANIFEST" ]] || die "manifest not found: $MANIFEST"
    if [[ "$MODE" == "verify" ]]; then
      cp "$MANIFEST" "$OUT_DIR/manifest.csv"
      MANIFEST="$OUT_DIR/manifest.csv"
    fi
    prepare_verify_input
    print_plan
    export_functions
    if ! run_parallel_tasks verify "$VERIFY_INPUT"; then
      verify_failed=true
    fi
    combine_verify_summary
    if [[ "$verify_failed" == "true" ]]; then
      die "one or more verify tasks failed; see $VERIFY_SUMMARY"
    fi
  fi
}

main "$@"
