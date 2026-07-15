#!/usr/bin/env bash
set -euo pipefail

# Issue 2723 verification runner
# Validates site-replication behavior against Task 07 matrix/evidence.

SITE_A_ENDPOINT="${SITE_A_ENDPOINT:-}"
SITE_B_ENDPOINT="${SITE_B_ENDPOINT:-}"
ACCESS_KEY="${ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${SECRET_KEY:-rustfsadmin}"
REGION="${REGION:-us-east-1}"
CA_CERT="${CA_CERT:-}"
OUT_DIR="${OUT_DIR:-target/verify/issue-2723-$(date +%Y%m%d-%H%M%S)}"
SITE_A_RESTART_CMD="${SITE_A_RESTART_CMD:-}"
SITE_B_RESTART_CMD="${SITE_B_RESTART_CMD:-}"
BUCKET="${BUCKET:-}"
REPL_OBJECT_KEY="${REPL_OBJECT_KEY:-issue-2723-e2e-object.txt}"
REPL_OBJECT_BODY="${REPL_OBJECT_BODY:-issue-2723-replication-check}"
AWS_PROFILE="${AWS_PROFILE:-}"

AWSCURL_BIN="${AWSCURL_BIN:-awscurl}"
AWS_BIN="${AWS_BIN:-aws}"
HEALTHCHECK_FORCE_LOOPBACK_RESOLVE="${HEALTHCHECK_FORCE_LOOPBACK_RESOLVE:-false}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_2723_site_replication.sh [options]

Required:
  --site-a-endpoint <url>      Site A admin endpoint, e.g. https://site-a.example.com:9000
  --site-b-endpoint <url>      Site B admin endpoint, e.g. https://site-b.example.com:9000
  --access-key <ak>
  --secret-key <sk>

Optional:
  --region <name>              AWS region for signing (default: us-east-1)
  --ca-cert <path>             CA cert for strict HTTPS health checks
  --out-dir <dir>              Artifact output directory
  --site-a-restart-cmd <cmd>   Restart command for site A (optional)
  --site-b-restart-cmd <cmd>   Restart command for site B (optional)
  --bucket <name>              Replication validation bucket (optional)
  --repl-object-key <key>      Replication validation object key
  --repl-object-body <text>    Replication validation object body
  --awscurl-bin <path>         awscurl binary (default: awscurl)
  --aws-bin <path>             aws cli binary (default: aws)
  --aws-profile <profile>      AWS CLI profile for object-flow checks
  --healthcheck-force-loopback-resolve
                               Force HTTPS healthcheck `--resolve host:port:127.0.0.1`
                               (default: false; intended for single-host local Docker)
  -h, --help                   Show help

Notes:
  1) If --bucket is provided and aws cli is available, script will run optional
     object-flow checks on both sites.
  2) Restart verification is skipped unless both restart commands are provided.
USAGE
}

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

fail() {
  log "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --site-a-endpoint) SITE_A_ENDPOINT="$2"; shift 2 ;;
      --site-b-endpoint) SITE_B_ENDPOINT="$2"; shift 2 ;;
      --access-key) ACCESS_KEY="$2"; shift 2 ;;
      --secret-key) SECRET_KEY="$2"; shift 2 ;;
      --region) REGION="$2"; shift 2 ;;
      --ca-cert) CA_CERT="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --site-a-restart-cmd) SITE_A_RESTART_CMD="$2"; shift 2 ;;
      --site-b-restart-cmd) SITE_B_RESTART_CMD="$2"; shift 2 ;;
      --bucket) BUCKET="$2"; shift 2 ;;
      --repl-object-key) REPL_OBJECT_KEY="$2"; shift 2 ;;
      --repl-object-body) REPL_OBJECT_BODY="$2"; shift 2 ;;
      --awscurl-bin) AWSCURL_BIN="$2"; shift 2 ;;
      --aws-bin) AWS_BIN="$2"; shift 2 ;;
      --aws-profile) AWS_PROFILE="$2"; shift 2 ;;
      --healthcheck-force-loopback-resolve) HEALTHCHECK_FORCE_LOOPBACK_RESOLVE="true"; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        fail "unknown argument: $1"
        ;;
    esac
  done
}

endpoint_scheme() {
  local endpoint="$1"
  if [[ "$endpoint" == https://* ]]; then
    echo "https"
  elif [[ "$endpoint" == http://* ]]; then
    echo "http"
  else
    fail "endpoint must include scheme http:// or https:// : $endpoint"
  fi
}

endpoint_hostport() {
  local endpoint="$1"
  local hostport
  hostport="${endpoint#http://}"
  hostport="${hostport#https://}"
  hostport="${hostport%%/*}"
  echo "$hostport"
}

admin_get() {
  local endpoint="$1"
  local path="$2"
  local out_file="$3"
  local url="${endpoint%/}${path}"
  if [[ -n "$CA_CERT" ]]; then
    REQUESTS_CA_BUNDLE="$CA_CERT" SSL_CERT_FILE="$CA_CERT" \
      "$AWSCURL_BIN" --service s3 --region "$REGION" --access_key "$ACCESS_KEY" --secret_key "$SECRET_KEY" "$url" >"$out_file"
  else
    "$AWSCURL_BIN" --service s3 --region "$REGION" --access_key "$ACCESS_KEY" --secret_key "$SECRET_KEY" "$url" >"$out_file"
  fi
}

strict_healthcheck() {
  local endpoint="$1"
  local label="$2"
  local out_file="$3"
  local scheme hostport host port
  scheme="$(endpoint_scheme "$endpoint")"
  hostport="$(endpoint_hostport "$endpoint")"
  host="${hostport%:*}"
  port="${hostport##*:}"
  if [[ "$port" == "$hostport" ]]; then
    port=$([[ "$scheme" == "https" ]] && echo "443" || echo "80")
  fi
  local url="${scheme}://${host}:${port}/health"

  if [[ "$scheme" == "https" ]]; then
    if [[ -z "$CA_CERT" ]]; then
      fail "HTTPS endpoint requires --ca-cert for strict validation: $endpoint"
    fi
    if [[ "$HEALTHCHECK_FORCE_LOOPBACK_RESOLVE" == "true" ]]; then
      curl -fsS --cacert "$CA_CERT" --resolve "${host}:${port}:127.0.0.1" "$url" >"$out_file"
    else
      curl -fsS --cacert "$CA_CERT" "$url" >"$out_file"
    fi
  else
    curl -fsS "$url" >"$out_file"
  fi
  log "health check passed for ${label}: $url"
}

analyze_duplicates() {
  local status_json="$1"
  local out_file="$2"
  jq -r '
    def identity_key($e):
      ($e | sub("^https?://";"") | sub("/$";"") | ascii_downcase);
    (.sites // {})
    | to_entries
    | map(.value.endpoint // "")
    | map(select(. != ""))
    | map(identity_key(.))
    | group_by(.)
    | map({identity: .[0], count: length})
    | map(select(.count > 1))
  ' "$status_json" >"$out_file"
}

optional_object_flow_check() {
  local endpoint="$1"
  local label="$2"
  local put_out="$3"
  local get_out="$4"

  if [[ -z "$BUCKET" ]]; then
    log "skip object-flow check for ${label}: --bucket not provided"
    return 0
  fi
  if ! command -v "$AWS_BIN" >/dev/null 2>&1; then
    log "skip object-flow check for ${label}: aws cli not found"
    return 0
  fi

  local common=(
    --endpoint-url "$endpoint"
    --region "$REGION"
    --no-cli-pager
  )
  if [[ -n "$AWS_PROFILE" ]]; then
    common+=(--profile "$AWS_PROFILE")
  fi
  if [[ -n "$CA_CERT" ]]; then
    common+=(--ca-bundle "$CA_CERT")
  fi

  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    "$AWS_BIN" "${common[@]}" s3api put-object \
    --bucket "$BUCKET" --key "$REPL_OBJECT_KEY" --body <(printf '%s' "$REPL_OBJECT_BODY") >"$put_out"

  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    "$AWS_BIN" "${common[@]}" s3api head-object \
    --bucket "$BUCKET" --key "$REPL_OBJECT_KEY" >"$get_out"

  log "object-flow check passed for ${label} on bucket=${BUCKET}, key=${REPL_OBJECT_KEY}"
}

restart_if_configured() {
  if [[ -z "$SITE_A_RESTART_CMD" || -z "$SITE_B_RESTART_CMD" ]]; then
    log "skip restart verification: restart commands not fully provided"
    return 0
  fi
  log "running restart command for site A"
  bash -lc "$SITE_A_RESTART_CMD"
  log "running restart command for site B"
  bash -lc "$SITE_B_RESTART_CMD"
}

main() {
  parse_args "$@"
  [[ -n "$SITE_A_ENDPOINT" ]] || fail "--site-a-endpoint is required"
  [[ -n "$SITE_B_ENDPOINT" ]] || fail "--site-b-endpoint is required"
  [[ -n "$ACCESS_KEY" ]] || fail "--access-key is required"
  [[ -n "$SECRET_KEY" ]] || fail "--secret-key is required"

  require_cmd "$AWSCURL_BIN"
  require_cmd curl
  require_cmd jq

  mkdir -p "$OUT_DIR"
  log "output directory: $OUT_DIR"

  local a_status="$OUT_DIR/site-a.status.json"
  local a_info="$OUT_DIR/site-a.info.json"
  local b_status="$OUT_DIR/site-b.status.json"
  local b_info="$OUT_DIR/site-b.info.json"
  local a_health="$OUT_DIR/site-a.health.txt"
  local b_health="$OUT_DIR/site-b.health.txt"
  local a_dupes="$OUT_DIR/site-a.duplicates.json"
  local b_dupes="$OUT_DIR/site-b.duplicates.json"
  local summary="$OUT_DIR/summary.txt"

  log "step 1/6: strict health checks"
  strict_healthcheck "$SITE_A_ENDPOINT" "site-a" "$a_health"
  strict_healthcheck "$SITE_B_ENDPOINT" "site-b" "$b_health"

  log "step 2/6: collect site-replication status/info"
  admin_get "$SITE_A_ENDPOINT" "/rustfs/admin/v3/site-replication/status" "$a_status"
  admin_get "$SITE_A_ENDPOINT" "/rustfs/admin/v3/site-replication/info" "$a_info"
  admin_get "$SITE_B_ENDPOINT" "/rustfs/admin/v3/site-replication/status" "$b_status"
  admin_get "$SITE_B_ENDPOINT" "/rustfs/admin/v3/site-replication/info" "$b_info"

  log "step 3/6: duplicate identity analysis"
  analyze_duplicates "$a_status" "$a_dupes"
  analyze_duplicates "$b_status" "$b_dupes"

  log "step 4/6: optional object-flow checks"
  optional_object_flow_check "$SITE_A_ENDPOINT" "site-a" "$OUT_DIR/site-a.put.json" "$OUT_DIR/site-a.head.json"
  optional_object_flow_check "$SITE_B_ENDPOINT" "site-b" "$OUT_DIR/site-b.put.json" "$OUT_DIR/site-b.head.json"

  log "step 5/6: optional restart verification"
  restart_if_configured
  if [[ -n "$SITE_A_RESTART_CMD" && -n "$SITE_B_RESTART_CMD" ]]; then
    admin_get "$SITE_A_ENDPOINT" "/rustfs/admin/v3/site-replication/status" "$OUT_DIR/site-a.status.after-restart.json"
    admin_get "$SITE_B_ENDPOINT" "/rustfs/admin/v3/site-replication/status" "$OUT_DIR/site-b.status.after-restart.json"
  fi

  log "step 6/6: write summary"
  {
    echo "Issue 2723 verification summary"
    echo "site-a endpoint: $SITE_A_ENDPOINT"
    echo "site-b endpoint: $SITE_B_ENDPOINT"
    echo "region: $REGION"
    echo "ca-cert: ${CA_CERT:-<none>}"
    echo
    echo "Duplicate identities (site-a): $(jq 'length' "$a_dupes")"
    echo "Duplicate identities (site-b): $(jq 'length' "$b_dupes")"
    echo
    echo "Artifacts:"
    find "$OUT_DIR" -maxdepth 1 -type f | sort
  } >"$summary"

  log "done. summary: $summary"
  cat "$summary"
}

main "$@"
