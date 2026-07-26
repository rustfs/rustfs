#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

OUT_DIR=""
ACCESS_KEY=""
SECRET_KEY=""
WARP_BIN="warp"
DURATION="60s"
CONCURRENCIES="64,128"
SIZES="524288B,1048575B,1048576B,1048577B,4194304B"
ROUNDS_PER_LEG=5
COOLDOWN_SECS=20
BUCKET_PREFIX="rustfs-1434-handoff"
DRY_RUN=false

ARM_A=""
ARM_B=""
ARM_C=""
ARM_D=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_exact_1mib_handoff_abba.sh --access-key <ak> --secret-key <sk> \
    --arm-a <spec> --arm-b <spec> --arm-c <spec> --arm-d <spec> [options]

Arm spec:
  name|endpoint|server_image_ref|server_image_digest|server_revision|inner_capacity|outer_capacity|outer_source

Required arms:
  A = current adaptive inner, outer auto
  B = exact-1MiB inner candidate, outer auto
  C = current adaptive inner, outer 1MiB override
  D = exact-1MiB inner candidate, outer 1MiB override

Options:
  --out-dir <dir>              Default: target/bench/exact-1mib-handoff-abba-<timestamp>
  --concurrencies <csv>        Default: 64,128
  --sizes <csv>                Default: 512KiB, 1MiB-1, 1MiB, 1MiB+1, 4MiB
  --rounds-per-leg <n>         Default: 5
  --cooldown-secs <n>          Default: 20
  --bucket-prefix <prefix>     Default: rustfs-1434-handoff
  --warp-bin <path>            Default: warp
  --duration <dur>             Default: 60s
  --dry-run                    Materialize manifest and commands only
  -h, --help

This runner records isolated-host ABBA evidence for rustfs/backlog#1434 only. It does not change production defaults and does not claim an optimization unless the raw artifact gate is analyzed separately.
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

validate_csv_nonempty() {
  local value="$1"
  local name="$2"
  [[ -n "$value" && "$value" != *",,"* ]] || die "$name must be a non-empty CSV"
}

validate_digest() {
  local value="$1"
  local name="$2"
  [[ "$value" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || die "$name must be an immutable sha256 digest"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --warp-bin) WARP_BIN="$2"; shift 2 ;;
      --duration) DURATION="$2"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$2"; shift 2 ;;
      --sizes) SIZES="$2"; shift 2 ;;
      --rounds-per-leg) ROUNDS_PER_LEG="$2"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$2"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$2"; shift 2 ;;
      --arm-a) ARM_A="$2"; shift 2 ;;
      --arm-b) ARM_B="$2"; shift 2 ;;
      --arm-c) ARM_C="$2"; shift 2 ;;
      --arm-d) ARM_D="$2"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

split_arm() {
  local spec="$1"
  local prefix="$2"
  local -a fields
  IFS='|' read -r -a fields <<< "$spec"
  [[ "${#fields[@]}" -eq 8 ]] || die "$prefix must have 8 pipe-separated fields"
  printf -v "${prefix}_name" '%s' "${fields[0]}"
  printf -v "${prefix}_endpoint" '%s' "${fields[1]}"
  printf -v "${prefix}_image_ref" '%s' "${fields[2]}"
  printf -v "${prefix}_image_digest" '%s' "${fields[3]}"
  printf -v "${prefix}_revision" '%s' "${fields[4]}"
  printf -v "${prefix}_inner" '%s' "${fields[5]}"
  printf -v "${prefix}_outer" '%s' "${fields[6]}"
  printf -v "${prefix}_outer_source" '%s' "${fields[7]}"
}

validate_arm() {
  local prefix="$1"
  local expected_name="$2"
  local name_var="${prefix}_name"
  local endpoint_var="${prefix}_endpoint"
  local digest_var="${prefix}_image_digest"
  local inner_var="${prefix}_inner"
  local outer_var="${prefix}_outer"
  local source_var="${prefix}_outer_source"
  [[ "${!name_var}" == "$expected_name" ]] || die "$prefix name must be $expected_name"
  validate_nonempty "${!endpoint_var}" "$prefix endpoint"
  validate_digest "${!digest_var}" "$prefix image digest"
  [[ "${!inner_var}" =~ ^[0-9]+$ ]] || die "$prefix inner capacity must be numeric bytes"
  [[ "${!outer_var}" == "auto" || "${!outer_var}" =~ ^[0-9]+$ ]] || die "$prefix outer capacity must be auto or numeric bytes"
  case "${!source_var}" in
    selected|env_override) ;;
    *) die "$prefix outer source must be selected or env_override" ;;
  esac
}

validate_args() {
  validate_nonempty "$ACCESS_KEY" "--access-key"
  validate_nonempty "$SECRET_KEY" "--secret-key"
  validate_nonempty "$ARM_A" "--arm-a"
  validate_nonempty "$ARM_B" "--arm-b"
  validate_nonempty "$ARM_C" "--arm-c"
  validate_nonempty "$ARM_D" "--arm-d"
  validate_csv_nonempty "$CONCURRENCIES" "--concurrencies"
  validate_csv_nonempty "$SIZES" "--sizes"
  validate_positive_int "$ROUNDS_PER_LEG" "--rounds-per-leg"
  validate_nonnegative_int "$COOLDOWN_SECS" "--cooldown-secs"
  split_arm "$ARM_A" "A"
  split_arm "$ARM_B" "B"
  split_arm "$ARM_C" "C"
  split_arm "$ARM_D" "D"
  validate_arm "A" "A"
  validate_arm "B" "B"
  validate_arm "C" "C"
  validate_arm "D" "D"
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="${PROJECT_ROOT}/target/bench/exact-1mib-handoff-abba-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  mkdir -p "$OUT_DIR"
}

arm_value() {
  local arm="$1"
  local field="$2"
  local var="${arm}_${field}"
  printf '%s' "${!var}"
}

write_manifest() {
  local manifest="$OUT_DIR/handoff_manifest.env"
  {
    printf 'created_utc=%q\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'benchmark_issue=%q\n' "rustfs/backlog#1434"
    printf 'schedule=%q\n' "ABBA_then_CDDC"
    printf 'sizes=%q\n' "$SIZES"
    printf 'concurrencies=%q\n' "$CONCURRENCIES"
    printf 'rounds_per_leg=%q\n' "$ROUNDS_PER_LEG"
    printf 'cooldown_secs=%q\n' "$COOLDOWN_SECS"
    printf 'access_key=%q\n' "REDACTED"
    printf 'secret_key=%q\n' "REDACTED"
  } > "$manifest"
}

write_schedule() {
  local schedule="$OUT_DIR/handoff_abba_schedule.csv"
  echo "leg,arm,endpoint,server_image_ref,server_image_digest,server_revision,inner_capacity,outer_capacity,outer_source,leg_out_dir" > "$schedule"
  for leg in A1 B1 B2 A2 C1 D1 D2 C2; do
    local arm="${leg:0:1}"
    echo "$leg,$arm,$(arm_value "$arm" endpoint),$(arm_value "$arm" image_ref),$(arm_value "$arm" image_digest),$(arm_value "$arm" revision),$(arm_value "$arm" inner),$(arm_value "$arm" outer),$(arm_value "$arm" outer_source),$OUT_DIR/$leg-arm-$arm" >> "$schedule"
  done
}

quote_cmd() {
  printf '%q ' "$@"
}

run_leg() {
  local leg="$1"
  local arm="${leg:0:1}"
  local conc="$2"
  local leg_lower
  leg_lower="$(printf '%s' "$leg" | tr '[:upper:]' '[:lower:]')"
  local bucket="${BUCKET_PREFIX}-${leg_lower}-c${conc}"
  local leg_out_dir="$OUT_DIR/$leg-arm-$arm/c${conc}"
  local -a cmd=(
    "$ENHANCED_BENCH"
    --tool warp
    --endpoint "$(arm_value "$arm" endpoint)"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket "$bucket"
    --bucket-size-suffix
    --concurrency "$conc"
    --sizes "$SIZES"
    --rounds "$ROUNDS_PER_LEG"
    --cooldown-secs "$COOLDOWN_SECS"
    --warp-bin "$WARP_BIN"
    --warp-mode get
    --duration "$DURATION"
    --service-metrics-dir "$leg_out_dir/service-metrics"
    --service-metrics-filter-regex 'rustfs_io_get_object_|rustfs_s3_get_object_'
    --server-image-ref "$(arm_value "$arm" image_ref)"
    --server-image-digest "$(arm_value "$arm" image_digest)"
    --server-revision "$(arm_value "$arm" revision)"
    --require-server-provenance
    --label bench_issue=1434
    --label handoff_arm="$arm"
    --label expected_reader_path=legacy_duplex
    --label expected_inner_capacity="$(arm_value "$arm" inner)"
    --label expected_outer_capacity="$(arm_value "$arm" outer)"
    --label expected_outer_source="$(arm_value "$arm" outer_source)"
    --out-dir "$leg_out_dir"
  )
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "-- dry-run leg=$leg arm=$arm concurrency=$conc"
    echo "   $(quote_cmd "${cmd[@]}" --dry-run)"
  else
    "${cmd[@]}"
  fi
}

run_schedule() {
  local conc leg
  IFS=',' read -r -a conc_values <<< "$CONCURRENCIES"
  for conc in "${conc_values[@]}"; do
    validate_positive_int "$conc" "concurrency cell"
    for leg in A1 B1 B2 A2 C1 D1 D2 C2; do
      run_leg "$leg" "$conc"
    done
  done
}

parse_args "$@"
validate_args
setup_output
write_manifest
write_schedule
run_schedule

echo "exact 1MiB handoff ABBA artifact root: $OUT_DIR"
