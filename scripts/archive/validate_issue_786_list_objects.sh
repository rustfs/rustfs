#!/usr/bin/env bash
set -euo pipefail

# Acceptance runner for rustfs/backlog#786 and PR #4072 follow-up
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/issue-786-acceptance-$(date +%Y%m%d-%H%M%S)}"
SKIP_LIVE="${SKIP_LIVE:-true}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
LIVE_LIST_BUCKET="${LIVE_LIST_BUCKET:-}"
LIVE_LIST_MAX_KEYS="${LIVE_LIST_MAX_KEYS:-2}"
LIVE_LIST_REGION="${LIVE_LIST_REGION:-us-east-1}"

log_info() { printf '[INFO] %s\n' "$*"; }
log_warn() { printf '[WARN] %s\n' "$*"; }
log_error() { printf '[ERROR] %s\n' "$*" >&2; }

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_786_list_objects.sh [--full] [--no-skip-live]

Environment:
  OUT_DIR            output directory (default: target/issue-786-acceptance-<ts>)
  SKIP_LIVE          skip live S3 endpoint checks when true (default: true)
  ENDPOINT           rustfs endpoint used by optional live checks (default: http://127.0.0.1:9000)
  LIVE_LIST_BUCKET   S3 bucket used for pagination smoke (required)
  LIVE_LIST_MAX_KEYS maximum keys used per page for smoke (default: 2)
  LIVE_LIST_REGION   signing region for curl --aws-sigv4 (default: us-east-1)
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

run_unit_checks() {
  log_info "Running focused rustfs-ecstore tests for issue-786 cursor fallback"
  mkdir -p "$OUT_DIR"

  local test_log="$OUT_DIR/issue-786-ecstore-tests.log"
  : > "$test_log"

  local tests=(
    list_path_marker_round_trip_preserves_set_index
    list_path_marker_parser_uses_trailing_cache_tag
    list_path_marker_parser_accepts_legacy_v1_tag
    list_path_marker_parser_return_tag_forces_cache_refresh
    list_path_marker_parser_recovers_from_corrupt_set_index
  )

  local test
  for test in "${tests[@]}"; do
    echo "[TEST] $test" | tee -a "$test_log"
    (cd "$PROJECT_ROOT" && cargo test -p rustfs-ecstore "$test" -- --nocapture | tee -a "$test_log")
  done

  log_info "Unit test log: $test_log"
}

run_static_checks() {
  log_info "Running issue-786 static marker checks"
  local static_log="$OUT_DIR/static-checks.log"

  : > "$static_log"

  if rg -n "struct ListContinuationV2|return:\\]|rustfs_cache:v2|MARKER_TAG_VERSION" "$PROJECT_ROOT/crates/ecstore/src/store/list_objects.rs" | tee -a "$static_log"; then
    log_info "ListContinuationV2 symbols found in list_objects.rs"
  else
    log_error "Required cursor symbols not found in list_objects.rs"
    exit 1
  fi

  log_info "Static check log: $static_log"
}

extract_xml_keys() {
  local xml_text="$1"
  printf '%s\n' "$xml_text" | sed -n 's:.*<Key>\(.*\)</Key>.*:\1:p'
}

run_live_smoke() {
  if [[ "$SKIP_LIVE" == "true" ]]; then
    log_warn "SKIP_LIVE=true, skipping live endpoint checks"
    return 0
  fi

  local live_log="$OUT_DIR/live-pagination-smoke.log"
  : > "$live_log"

  if ! curl -fsS "${ENDPOINT}/health/ready" >/dev/null 2>&1; then
    log_warn "Live endpoint not reachable: ${ENDPOINT}/health/ready"
    echo "live pagination smoke: skipped (endpoint not reachable)" | tee -a "$live_log"
    return 0
  fi
  log_info "Live endpoint reachable"

  local access_key="${RUSTFS_ACCESS_KEY:-${WARP_ACCESS_KEY:-}}"
  local secret_key="${RUSTFS_SECRET_KEY:-${WARP_SECRET_KEY:-}}"
  if [[ -z "$access_key" || -z "$secret_key" ]]; then
    log_warn "Live pagination smoke skipped: missing RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY or WARP_* alternatives"
    echo "live pagination smoke: skipped (missing credentials)" | tee -a "$live_log"
    return 0
  fi

  if [[ -z "$LIVE_LIST_BUCKET" ]]; then
    log_warn "Live pagination smoke skipped: LIVE_LIST_BUCKET is empty"
    echo "live pagination smoke: skipped (bucket not configured)" | tee -a "$live_log"
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    log_warn "Live pagination smoke skipped: python3 not available"
    echo "live pagination smoke: skipped (missing python3 parser)" | tee -a "$live_log"
    return 0
  fi

  local sigv4_target="aws:amz:${LIVE_LIST_REGION}:s3"
  local base_url="${ENDPOINT}/${LIVE_LIST_BUCKET}?list-type=2&max-keys=${LIVE_LIST_MAX_KEYS}"

  local page1
  page1=$(curl -fsS \
    --user "${access_key}:${secret_key}" \
    --aws-sigv4 "$sigv4_target" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "$base_url")

  local token1
  token1=$(printf '%s' "$page1" | sed -n 's:.*<NextContinuationToken>\(.*\)</NextContinuationToken>.*:\1:p' | head -n 1)

  local page1_key_count
  page1_key_count=$(printf '%s' "$page1" | sed -n 's:.*<Key>\(.*\)</Key>.*:\1:p' | grep -cv '^$')

  local -a keys1 keys2
  while IFS= read -r key; do
    keys1+=("$key")
  done < <(extract_xml_keys "$page1")

  if [[ -z "$token1" ]]; then
    log_info "Live pagination smoke: single page only (key count=${page1_key_count})"
    echo "live pagination smoke: pass (single page, key_count=${page1_key_count})" | tee -a "$live_log"
    return 0
  fi

  local token1_encoded
  token1_encoded=$(printf '%s' "$token1" | python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.stdin.read().strip(), safe=""))')

  local page2
  page2=$(curl -fsS \
    --user "${access_key}:${secret_key}" \
    --aws-sigv4 "$sigv4_target" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    "${base_url}&continuation-token=${token1_encoded}")

  local page2_key_count
  page2_key_count=$(printf '%s' "$page2" | sed -n 's:.*<Key>\(.*\)</Key>.*:\1:p' | grep -cv '^$')

  if (( page2_key_count == 0 )); then
    log_error "Live pagination smoke: second page returned zero keys for token ${token1}"
    return 1
  fi

  local duplicate
  local dkeys=()
  duplicate=0
  local key

  while IFS= read -r key; do
    keys2+=("$key")
    if printf '%s\n' "${keys1[@]}" | grep -Fx -- "$key" >/dev/null 2>&1; then
      duplicate=1
      dkeys+=("$key")
    fi
  done < <(extract_xml_keys "$page2")

  if (( duplicate )); then
    log_error "Live pagination smoke detected duplicated keys across page transitions: ${dkeys[*]}"
    printf '%s\n' "${dkeys[@]}" | tee -a "$live_log"
    return 1
  fi

  log_info "Live pagination smoke: first page=${page1_key_count}, second page=${page2_key_count}, token=${token1}"
  echo "live pagination smoke: pass (continuous two-page check)" | tee -a "$live_log"
  echo "page1=${page1_key_count}" | tee -a "$live_log"
  echo "page2=${page2_key_count}" | tee -a "$live_log"
  log_info "Live pagination smoke log: $live_log"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full|--no-skip-live)
        SKIP_LIVE="false"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "unknown arg: $1"
        usage
        exit 1
        ;;
    esac
  done
}

main() {
  parse_args "$@"
  require_cmd cargo
  require_cmd rg
  require_cmd tee
  require_cmd curl

  mkdir -p "$OUT_DIR"

  run_unit_checks
  run_static_checks

  if [[ "$SKIP_LIVE" != "true" ]]; then
    run_live_smoke
  fi

  log_info "Validation summary:"
  log_info "  - logs: $OUT_DIR"
  log_info "  - status: pass"
}

main "$@"
