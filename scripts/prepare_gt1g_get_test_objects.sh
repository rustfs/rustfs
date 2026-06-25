#!/usr/bin/env bash
set -euo pipefail

ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
BUCKET=""
PREFIX="bench/issue713"
OBJECTS="1GiB=plain-1g.bin,2GiB=plain-2g.bin"
REGION="us-east-1"
MC_BIN="${MC_BIN:-mc}"
FORCE=false
INSECURE=false
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/prepare_gt1g_get_test_objects.sh \
    --endpoint <url> --access-key <ak> --secret-key <sk> --bucket <bucket> [options]

Required:
  --endpoint <url>
  --access-key <ak>
  --secret-key <sk>
  --bucket <bucket>

Options:
  --prefix <path>              Default: bench/issue713
  --objects <csv>              Default: 1GiB=plain-1g.bin,2GiB=plain-2g.bin
                               Format: size=object-name,size=object-name
  --region <name>              Default: us-east-1
  --mc-bin <path>              Default: mc
  --force                      Re-upload even if object already exists
  --insecure                   Allow insecure TLS
  --dry-run
  -h, --help

Examples:
  scripts/prepare_gt1g_get_test_objects.sh \
    --endpoint http://127.0.0.1:9000 \
    --access-key rustfsadmin \
    --secret-key rustfsadmin \
    --bucket rustfs-bench \
    --prefix bench/issue713/plain
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

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key) SECRET_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket) BUCKET="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --prefix) PREFIX="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --objects) OBJECTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-bin) MC_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --force) FORCE=true; shift ;;
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
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" || -z "$BUCKET" ]]; then
    echo "ERROR: --endpoint, --access-key, --secret-key, and --bucket are required" >&2
    exit 1
  fi
}

size_to_bytes() {
  local size="$1"
  case "$size" in
    *GiB)
      local n="${size%GiB}"
      echo $((n * 1024 * 1024 * 1024))
      ;;
    *MiB)
      local n="${size%MiB}"
      echo $((n * 1024 * 1024))
      ;;
    *KiB)
      local n="${size%KiB}"
      echo $((n * 1024))
      ;;
    *B)
      echo "${size%B}"
      ;;
    *)
      echo "ERROR"
      ;;
  esac
}

create_sparse_file() {
  local file_path="$1"
  local bytes="$2"
  if command -v mkfile >/dev/null 2>&1; then
    mkfile -n "$bytes" "$file_path"
  else
    truncate -s "$bytes" "$file_path"
  fi
}

object_exists() {
  local alias_path="$1"
  local -a cmd=("$MC_BIN" stat "$alias_path")
  if [[ "$INSECURE" == "true" ]]; then
    cmd=("$MC_BIN" --insecure stat "$alias_path")
  fi

  "${cmd[@]}" >/dev/null 2>&1
}

main() {
  parse_args "$@"
  validate_args
  require_cmd "$MC_BIN"

  local tmp_root
  tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/issue713-gt1g-get.XXXXXX")"
  trap 'rm -rf "$tmp_root"' EXIT

  local mc_config_dir="$tmp_root/mc"
  mkdir -p "$mc_config_dir"

  local -a mc_alias_cmd=("$MC_BIN" --config-dir "$mc_config_dir")
  if [[ "$INSECURE" == "true" ]]; then
    mc_alias_cmd+=("--insecure")
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] ${mc_alias_cmd[*]} alias set issue713 ${ENDPOINT} REDACTED REDACTED"
  else
    "${mc_alias_cmd[@]}" alias set issue713 "$ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
  fi

  IFS=',' read -r -a object_specs <<< "$OBJECTS"
  for raw_spec in "${object_specs[@]}"; do
    local spec size object_name bytes local_file object_key alias_path
    spec="$(echo "$raw_spec" | awk '{$1=$1;print}')"
    [[ -z "$spec" ]] && continue

    size="${spec%%=*}"
    object_name="${spec#*=}"
    if [[ -z "$size" || -z "$object_name" || "$size" == "$object_name" ]]; then
      echo "ERROR: invalid object spec: $spec" >&2
      exit 1
    fi

    bytes="$(size_to_bytes "$size")"
    if [[ "$bytes" == "ERROR" ]]; then
      echo "ERROR: unsupported size label: $size" >&2
      exit 1
    fi

    object_key="${PREFIX%/}/${object_name}"
    alias_path="issue713/${BUCKET}/${object_key}"

    if [[ "$FORCE" != "true" && "$DRY_RUN" != "true" ]] && object_exists "$alias_path"; then
      echo "skip existing: s3://${BUCKET}/${object_key}"
      continue
    fi

    local_file="${tmp_root}/${object_name}"
    create_sparse_file "$local_file" "$bytes"

    if [[ "$DRY_RUN" == "true" ]]; then
      echo "[DRY-RUN] create sparse file ${local_file} (${bytes} bytes)"
      echo "[DRY-RUN] ${mc_alias_cmd[*]} cp ${local_file} ${alias_path}"
    else
      echo "uploading: s3://${BUCKET}/${object_key} (${size})"
      "${mc_alias_cmd[@]}" cp "$local_file" "$alias_path" >/dev/null
    fi
  done

  echo "prepared objects:"
  for raw_spec in "${object_specs[@]}"; do
    local spec object_name
    spec="$(echo "$raw_spec" | awk '{$1=$1;print}')"
    [[ -z "$spec" ]] && continue
    object_name="${spec#*=}"
    echo "  s3://${BUCKET}/${PREFIX%/}/${object_name}"
  done
}

main "$@"
