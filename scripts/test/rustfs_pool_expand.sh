#!/usr/bin/env bash
#
# rustfs-pool-expand.sh
# RustFS multi-pool expansion & pool decommission test script
#
# Based on the Obsidian note "RustFS pool 扩容脚本". Full workflow:
#   1. Download the RustFS package on all nodes
#   2. Install the RustFS service on all nodes (dpkg -i)
#   3. Start the first pool on node 0 and verify it
#   4. Write data (warp) and monitor storage usage up to a threshold
#   5. Expand to a second pool (node 0 + node 1), start rebalance
#   6. Wait for rebalance to finish
#   7. Expand to a third pool (all nodes), start rebalance again
#   8. Wait for rebalance to finish
#   9. Decommission pool 0 and wait for it to complete
#
# The script is driven from an admin host (e.g. a jumpbox or a GitHub
# self-hosted runner) and operates on the target nodes over SSH.
#
# Usage:
#   ./rustfs-pool-expand.sh --all                  # run all steps 1-9
#   ./rustfs-pool-expand.sh --step 5               # run a single step
#   ./rustfs-pool-expand.sh --steps 5,7,9          # run selected steps
#   ./rustfs-pool-expand.sh --all --dry-run        # preview only
#   ./rustfs-pool-expand.sh --all -y --version 1.0.0-rc.3
#
# Notes:
#   - SSH user defaults to azureuser (passwordless sudo on the nodes);
#     pass --ssh-user root if your nodes accept root login.
#   - The script talks to the RustFS admin API directly with SigV4-signed
#     requests (no rc required). jq, openssl and curl must be installed on the
#     admin host; warp is only needed for --with-warp.
#   - RUSTFS_VOLUMES must keep existing pool expressions unchanged and in
#     order when expanding.
#
set -Eeuo pipefail

# ==================== Configuration (adjust to your environment) ====================

# Target nodes; pool N is enabled by NODES[N-1] (index order matters)
if [ -n "${RUSTFS_NODES:-}" ]; then
  read -r -a NODES <<<"${RUSTFS_NODES}"
else
  NODES=(vm000 vm001 vm002)
fi

# SSH user configured on the nodes (azureuser has passwordless sudo on heal's nodes)
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

# Package: version is a GitHub release tag such as "1.0.0-rc.3".
# PACKAGE_URL is derived from RUSTFS_VERSION unless --package-url / PACKAGE_URL is given.
RUSTFS_VERSION="${RUSTFS_VERSION:-1.0.0-rc.3}"
PACKAGE_URL="${PACKAGE_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
PACKAGE_SHA256="${PACKAGE_SHA256:-}"

# Admin API endpoint and credentials (SigV4-signed requests, no rc needed).
# RUSTFS_RC_ENDPOINT is honoured as a fallback for existing setups.
API_ENDPOINT="${RUSTFS_API_ENDPOINT:-${RUSTFS_RC_ENDPOINT:-http://10.0.0.7:9000}}"
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfs@test}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs@test}"
SIGV4_REGION="us-east-1"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=60
# admin_api writes the HTTP status here so callers can read it after the
# command-substitution subshell exits ($$ is identical inside the subshell).
ADMIN_API_CODE_FILE="${TMPDIR:-/tmp}/rustfs-pool-expand-api-code.$$"

# Complete RUSTFS_VOLUMES value per stage (one space-separated expression per pool)
VOLUMES_1="http://rustfs-node1:9000/data/rustfs{1...4}/mnmd"
VOLUMES_2="http://rustfs-node1:9000/data/rustfs{1...4}/mnmd http://rustfs-node2:9000/data/rustfs{1...4}/mnmd"
VOLUMES_3="http://rustfs-node1:9000/data/rustfs{1...4}/mnmd http://rustfs-node2:9000/data/rustfs{1...4}/mnmd http://rustfs-node3:9000/data/rustfs{1...4}/mnmd"
# Topology after decommissioning pool 0 (used by --finalize-decommission)
VOLUMES_AFTER_DECOMMISSION="http://rustfs-node2:9000/data/rustfs{1...4}/mnmd http://rustfs-node3:9000/data/rustfs{1...4}/mnmd"

# RustFS service configuration (written to /etc/default/rustfs)
RUSTFS_CONFIG_FILE="/etc/default/rustfs"
RUSTFS_SERVICE="rustfs"
RUSTFS_PACKAGE_NAME="rustfs"
RUSTFS_USER="rustfs"
RUSTFS_ADDRESS=":9000"
RUSTFS_CONSOLE_ADDRESS=":9001"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"

# Data writing & monitoring (step 4)
WARP_BUCKET="test-10mb"
WARP_OBJ_SIZE="100MiB"
WARP_CONCURRENT=32
WARP_DURATION="5m"
# Warp log path; empty = auto-created unique temp file (the runner user may
# not be able to write a shared /tmp path owned by another user).
WARP_LOG_FILE="${RUSTFS_WARP_LOG_FILE:-}"
STORAGE_THRESHOLD=85            # stop writing when usage reaches N% (note suggests 80-85)
POLL_INTERVAL=30                # status polling interval (seconds)

# Timeouts (seconds)
REBALANCE_TIMEOUT=86400
DECOMMISSION_TIMEOUT=86400
SERVICE_TIMEOUT=300
DECOMMISSION_RETRIES=3          # auto clear+retry attempts after a failed decommission
DECOMMISSION_RETRY_DELAY=30     # delay between retries (seconds)
REBALANCE_START_RETRIES=6       # rebalance start retries (fleet proof may take ~10-20s after a topology change)
REBALANCE_START_RETRY_DELAY=20  # delay between rebalance start retries (seconds)

# Pool to decommission (zero-based; 0 in the note)
DECOMMISSION_POOL_ID=0

# ==================== Runtime options (set by CLI) ====================
DRY_RUN=0
ASSUME_YES=0
WITH_WARP=0
SKIP_DOWNLOAD=0
FINALIZE_DECOMMISSION=0
PREFLIGHT=0
RESET=0
LOG_FILE=""
SELECTED_STEPS=()

# ==================== Helpers ====================

log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
  # $1: prompt text; bypassed with --yes
  if [ "${ASSUME_YES}" -eq 1 ]; then
    return 0
  fi
  printf '\033[1;33m[CONFIRM]\033[0m %s (y/N) ' "$1"
  read -r answer
  case "${answer}" in
    y|Y|yes|YES) return 0 ;;
    *) die "cancelled" ;;
  esac
}

need_cmd() {
  # $1: command name; $2: description
  if [ "${DRY_RUN}" -eq 1 ]; then return 0; fi
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1 ($2); install it first"
}

# ==================== Admin API (SigV4-signed) ====================

sha256_hex() {
  # $1: ascii data
  printf '%s' "$1" | openssl dgst -sha256 -hex 2>/dev/null | awk '{print $NF}'
}

hmac_sha256_hex() {
  # $1: key in hex, $2: ascii data
  printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" -hex 2>/dev/null | awk '{print $NF}'
}

hex_of_ascii() {
  # $1: ascii string -> hex
  printf '%s' "$1" | od -An -vtx1 | tr -d ' \n'
}

# Sort a "k=v&k2=v2" query string by key (values are used verbatim, matching
# the RustFS signer). The same canonical form is used in the URL and in the
# SigV4 canonical request so the server-side verification always agrees.
canonical_query() {
  local q="$1"
  [ -z "${q}" ] && return 0
  # The trailing newline matters: `while read` drops the last item when the
  # input has no final newline (e.g. "a=1&b=2" without a trailing '&').
  printf '%s\n' "${q}" | tr '&' '\n' | while IFS= read -r pair; do
    printf '%s=%s\n' "${pair%%=*}" "${pair#*=}"
  done | sort | paste -sd '&' -
}

# Issue an admin API request. Prints the response body on stdout and writes the
# HTTP status (000 on transport failure) to ${ADMIN_API_CODE_FILE}.
admin_api() {
  # $1: method, $2: path, $3: query string
  local method="$1" path="$2" query="$3"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: admin API ${method} ${API_ENDPOINT}${path}${query:+?${query}}"
    printf '200' > "${ADMIN_API_CODE_FILE}"
    return 0
  fi

  host_port="${API_ENDPOINT#*://}"
  host_port="${host_port%%/*}"

  amz_date="$(date -u +%Y%m%dT%H%M%SZ)"
  date_stamp="${amz_date:0:8}"
  query="$(canonical_query "${query}")"

  canonical_headers="host:${host_port}
x-amz-content-sha256:UNSIGNED-PAYLOAD
x-amz-date:${amz_date}
"
  signed_headers="host;x-amz-content-sha256;x-amz-date"
  canonical_request="${method}
${path}
${query}
${canonical_headers}
${signed_headers}
UNSIGNED-PAYLOAD"

  scope="${date_stamp}/${SIGV4_REGION}/${SIGV4_SERVICE}/aws4_request"
  string_to_sign="AWS4-HMAC-SHA256
${amz_date}
${scope}
$(sha256_hex "${canonical_request}")"

  k_date="$(hmac_sha256_hex "$(hex_of_ascii "AWS4${SECRET_KEY}")" "${date_stamp}")"
  k_region="$(hmac_sha256_hex "${k_date}" "${SIGV4_REGION}")"
  k_service="$(hmac_sha256_hex "${k_region}" "${SIGV4_SERVICE}")"
  k_signing="$(hmac_sha256_hex "${k_service}" "aws4_request")"
  signature="$(hmac_sha256_hex "${k_signing}" "${string_to_sign}")"
  auth="AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/${scope}, SignedHeaders=${signed_headers}, Signature=${signature}"

  url="http://${host_port}${path}${query:+?${query}}"
  tmp="$(mktemp)"
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${tmp}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  cat "${tmp}"
  rm -f "${tmp}"
}

# Read the HTTP status written by the last admin_api call
admin_api_code() {
  cat "${ADMIN_API_CODE_FILE}"
}

# Run an admin API request and fail loudly (with the full response body) on a
# non-success status. Prints the body on stdout.
admin_api_assert() {
  # $1: method, $2: path, $3: query, $4: description
  local method="$1" path="$2" query="$3" desc="$4" body code
  body="$(admin_api "${method}" "${path}" "${query}")"
  code="$(admin_api_code)"
  if [ "${code}" != "200" ] && [ "${code}" != "201" ] && [ "${code}" != "204" ]; then
    printf '\033[1;31m[ERROR]\033[0m %s failed (HTTP %s)\n' "${desc}" "${code}" >&2
    printf '%s\n' "${body}" >&2
    die "admin API ${method} ${path} -> HTTP ${code}"
  fi
  printf '%s' "${body}"
}

# Build the GitHub release download URL from a release tag.
# "1.0.0-rc.3" -> https://github.com/rustfs/rustfs/releases/download/1.0.0-rc.3/rustfs_1.0.0.rc.3_amd64.deb
build_package_url() {
  local tag="${RUSTFS_VERSION#v}" asset
  asset="${tag//-/.}"
  printf 'https://github.com/rustfs/rustfs/releases/download/%s/rustfs_%s_%s.deb' "${tag}" "${asset}" "${ARCH}"
}

resolve_package_url() {
  if [ -n "${PACKAGE_URL}" ]; then
    printf '%s' "${PACKAGE_URL}"
  else
    build_package_url
  fi
}

# Run a remote script on a single node (script is read from stdin)
run_remote() {
  local node="$1"
  local script
  script="$(cat)"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
    printf '%s\n' "${script}" | sed 's/^/    | /'
    log "DRY-RUN: ----"
    return 0
  fi
  log "==> ${node}: executing remote script"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}"
}

# Run the same remote script on all nodes in parallel (script from stdin)
run_remote_all() {
  local script
  script="$(cat)"
  local pids=() i=0 fail=0
  for node in "${NODES[@]}"; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
      printf '%s\n' "${script}" | sed 's/^/    | /'
      log "DRY-RUN: ----"
    else
      log "==> ${node}: executing remote script"
      ( ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}" ) &
      pids[$i]=$!
      i=$((i+1))
    fi
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do
      wait "${pid}" || fail=1
    done
  fi
  [ "${fail}" -eq 0 ] || die "one or more nodes failed"
}

# Wait for the systemd service to become active on a node
wait_service_active() {
  local node="$1" waited=0
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: waiting for ${RUSTFS_SERVICE} on ${node} to become active"
    return 0
  fi
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
        "systemctl is-active --quiet ${RUSTFS_SERVICE}" 2>/dev/null; then
      log "${node}: ${RUSTFS_SERVICE} is active"
      return 0
    fi
    sleep 5
    waited=$((waited + 5))
  done
  diagnose_node_start_failure "${node}"
  die "${node}: timed out waiting for ${RUSTFS_SERVICE} (${SERVICE_TIMEOUT}s)"
}

# Known server-side issues the test can hit. Format:
#   "<error signature>|<tracking>|<hint>"
KNOWN_SERVER_ISSUES=(
  "pool activation requires a live fleet capability proof|rustfs/backlog#2031|server-side cold-start recovery is covered by this PR; if this appears, collect node journals and treat it as a regression"
)

# Print a hint when $1 matches a known server-side issue signature.
hint_server_issue() {
  local text="$1" entry sig tracking hint
  for entry in "${KNOWN_SERVER_ISSUES[@]}"; do
    sig="${entry%%|*}"
    tracking="${entry#*|}"
    hint="${tracking#*|}"
    tracking="${tracking%%|*}"
    if printf '%s' "${text}" | grep -qiF "${sig}"; then
      printf '\033[1;33m[KNOWN SERVER ISSUE]\033[0m %s (%s): %s\n' "${sig}" "${tracking}" "${hint}" >&2
      return 0
    fi
  done
  return 1
}

# Fetch the journal tail from a node whose service failed to start and
# annotate known server-side issues.
diagnose_node_start_failure() {
  local node="$1" journal
  if ! journal="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "SUDO=\"\"; [ \"\$(id -u)\" -ne 0 ] && SUDO=\"sudo -n\"; \${SUDO} journalctl -u ${RUSTFS_SERVICE} --no-pager -n 60 2>/dev/null || true")"; then
    journal="unable to collect journal (SSH command failed)"
  fi
  printf '%s\n' "--- ${node}: ${RUSTFS_SERVICE} journal (last 60 lines) ---" >&2
  printf '%s\n' "${journal}" >&2
  hint_server_issue "${journal}" || true
}

# Generate the /etc/default/rustfs content
rustfs_config_body() {
  local volumes="$1"
  cat <<EOF
RUSTFS_ACCESS_KEY=${ACCESS_KEY}
RUSTFS_SECRET_KEY=${SECRET_KEY}
RUSTFS_VOLUMES="${volumes}"
RUSTFS_ADDRESS="${RUSTFS_ADDRESS}"
RUSTFS_CONSOLE_ADDRESS="${RUSTFS_CONSOLE_ADDRESS}"
RUSTFS_CONSOLE_ENABLE=${RUSTFS_CONSOLE_ENABLE}
RUSTFS_OBS_LOGGER_LEVEL=${RUSTFS_OBS_LOGGER_LEVEL}
RUSTFS_OBS_LOG_DIRECTORY="${RUSTFS_OBS_LOG_DIRECTORY}"
EOF
}

# Expand a volume expression's {N...M} range into concrete local paths, e.g.:
#   http://rustfs-node1:9000/data/rustfs{1...4}/mnmd
#   -> /data/rustfs1/mnmd /data/rustfs2/mnmd /data/rustfs3/mnmd /data/rustfs4/mnmd
volume_dirs() {
  local volumes="$1" expr path prefix suffix i start end
  for expr in ${volumes}; do
    path="${expr#*://}"
    path="${path#*/}"
    path="/${path}"
    if [[ "${path}" =~ \{([0-9]+)\.\.\.([0-9]+)\} ]]; then
      start="${BASH_REMATCH[1]}"
      end="${BASH_REMATCH[2]}"
      prefix="${path%%\{*}"
      suffix="${path#*\}}"
      for ((i=start; i<=end; i++)); do
        printf '%s%s%s\n' "${prefix}" "${i}" "${suffix}"
      done
    else
      printf '%s\n' "${path}"
    fi
  done
}

# Ensure the volume directories exist on a node and are owned by the service user
ensure_volume_dirs() {
  local node="$1" volumes="$2"
  local -a dirs
  dirs=()
  while IFS= read -r d; do dirs+=("${d}"); done < <(volume_dirs "${volumes}" | sort -u)
  log "${node}: ensuring data dirs exist and are owned by ${RUSTFS_USER} (${dirs[*]})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    for d in "${dirs[@]}"; do
      printf '${SUDO} mkdir -p %s\n' "${d}"
      printf '${SUDO} chown -R %s:%s %s\n' "${RUSTFS_USER}" "${RUSTFS_USER}" "${d}"
    done
  } | run_remote "${node}"
}

# Write the RustFS config on a single node (with a timestamped backup first)
write_rustfs_config() {
  local node="$1" volumes="$2"
  local body
  body="$(rustfs_config_body "${volumes}")"
  log "${node}: writing config ${RUSTFS_CONFIG_FILE} (volumes=${volumes})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'if [ -f %s ]; then ${SUDO} cp -a %s %s.bak.$(date +%%Y%%m%%d%%H%%M%%S); fi\n' \
      "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}"
    printf '%s tee %s >/dev/null <<RUSTFS_EOF\n' '${SUDO}' "${RUSTFS_CONFIG_FILE}"
    printf '%s' "${body}"
    printf '\nRUSTFS_EOF\n'
    printf '${SUDO} systemctl daemon-reload\n'
  } | run_remote "${node}"
}

# Start/stop the service on one node (uses sudo automatically when not root)
service_action() {
  local action="$1" node="$2"
  log "${node}: systemctl ${action} ${RUSTFS_SERVICE}"
  if [ "${DRY_RUN}" -eq 1 ]; then return 0; fi
  if ! ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
      "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${RUSTFS_SERVICE}; else systemctl ${action} ${RUSTFS_SERVICE}; fi"; then
    if [ "${action}" = "start" ]; then
      diagnose_node_start_failure "${node}"
    fi
    die "${node}: systemctl ${action} failed"
  fi
}

service_action_all() {
  local action="$1" pids=() i=0 fail=0
  if [ "${DRY_RUN}" -eq 1 ]; then
    for node in "${NODES[@]}"; do
      service_action "${action}" "${node}"
    done
    return 0
  fi
  for node in "${NODES[@]}"; do
    service_action "${action}" "${node}" &
    pids[$i]=$!
    i=$((i+1))
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do
      wait "${pid}" || fail=1
    done
  fi
  [ "${fail}" -eq 0 ] || die "systemctl ${action} failed on one or more nodes"
}

# Start nodes in parallel. With multiple pools all nodes MUST start
# simultaneously, otherwise the first node fails with "not first disk".
start_and_wait_nodes() {
  local nodes=("$@") pids=() i=0 fail=0
  for node in "${nodes[@]}"; do
    service_action start "${node}" &
    pids[$i]=$!
    i=$((i+1))
  done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do
      wait "${pid}" || fail=1
    done
  fi
  [ "${fail}" -eq 0 ] || die "systemctl start failed on one or more nodes"
  for node in "${nodes[@]}"; do
    wait_service_active "${node}"
  done
}

# Cluster storage usage percentage from the admin API (aggregate across disks)
storage_usage_percent() {
  local body
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: GET ${API_ENDPOINT}/rustfs/admin/v3/storageinfo (storage usage)"
    printf '0\n'
    return 0
  fi
  body="$(admin_api GET /rustfs/admin/v3/storageinfo "")"
  [ "$(admin_api_code)" = "200" ] || die "storageinfo API failed (HTTP $(admin_api_code))"
  if ! printf '%s' "${body}" | jq -e '(.info.disks // .disks // []) | length' >/dev/null 2>&1; then
    printf '\033[1;31m[ERROR]\033[0m storageinfo response unexpected (expected JSON with .info.disks or .disks):\n%s\n' "${body}" >&2
    die "storageinfo response could not be parsed"
  fi
  printf '%s' "${body}" | jq -r '
    ([.info.disks // .disks // [] | .[] | select(.state != "offline" and .totalspace > 0) | .usedspace] | add // 0) as $used
    | ([.info.disks // .disks // [] | .[] | select(.state != "offline" and .totalspace > 0) | .totalspace] | add // 0) as $total
    | if $total > 0 then ($used / $total * 100) else 0 end
  '
}

# Poll storage usage until it reaches the threshold
monitor_storage() {
  local threshold="${1:-${STORAGE_THRESHOLD}}" warp_pid="${2:-}" usage
  log "monitoring storage usage until ${threshold}% (every ${POLL_INTERVAL}s)"
  while :; do
    usage="$(storage_usage_percent)"
    [ -n "${usage}" ] || usage="0"
    log "current storage usage: ${usage}%"
    awk -v u="${usage}" -v t="${threshold}" 'BEGIN{exit !(u >= t)}' && break
    if [ -n "${warp_pid}" ] && ! kill -0 "${warp_pid}" 2>/dev/null; then
      warn "warp finished at ${usage}%, below the threshold of ${threshold}%"
      warn "re-run step 4 to write more data; the data already written is enough to exercise the remaining steps"
      return 0
    fi
    sleep "${POLL_INTERVAL}"
  done
  log "storage usage reached ${usage}%, stopping writes"
}

# Verify the expected number of pools via the admin API (JSON + jq assertions)
verify_pools() {
  local expected="$1"
  local body code count nonactive
  log "verifying pools via admin API (expect at least ${expected} active): GET ${API_ENDPOINT}/rustfs/admin/v3/pools/list"
  if [ "${DRY_RUN}" -eq 1 ]; then return 0; fi
  body="$(admin_api GET /rustfs/admin/v3/pools/list "")"
  code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    printf '\033[1;31m[ERROR]\033[0m pools/list returned HTTP %s\n' "${code}" >&2
    printf '%s\n' "${body}" >&2
    die "pools/list failed"
  fi
  if ! count="$(printf '%s' "${body}" | jq -e 'length' 2>/dev/null)"; then
    printf '\033[1;31m[ERROR]\033[0m pools/list returned an unexpected (non-JSON) body:\n%s\n' "${body}" >&2
    die "pools/list response could not be parsed as JSON"
  fi
  nonactive="$(printf '%s' "${body}" | jq '[.[] | select(.status != "active")] | length')"
  if [ "${count}" -lt "${expected}" ] || [ "${nonactive}" -ne 0 ]; then
    printf '\033[1;31m[ERROR]\033[0m pool assertion failed: expected >=%s active pools, got %s (non-active: %s)\n' \
      "${expected}" "${count}" "${nonactive}" >&2
    printf '%s\n' "--- pool detail ---" >&2
    printf '%s' "${body}" | jq -r '.[] | "  pool id=\(.id) status=\(.status) decommission=\(.decommissionStatus) rebalance=\(.rebalanceStatus) used=\(.usedSize)/\(.totalSize) cmdline=\(.cmdline)"' >&2
    printf '%s\n' "--- full JSON ---" >&2
    printf '%s\n' "${body}" >&2
    die "pool verification failed"
  fi
  log "pools OK: ${count} pools, all active"
  printf '%s' "${body}" | jq -r '.[] | "  pool id=\(.id) status=\(.status) decommission=\(.decommissionStatus) rebalance=\(.rebalanceStatus)"'
}

# Print a detailed rebalance failure report (never includes credentials)
print_rebalance_failure() {
  local body="$1" reason="$2"
  printf '\033[1;31m[ERROR]\033[0m %s\n' "${reason}" >&2
  printf '%s\n' "--- rebalance summary ---" >&2
  printf '%s' "${body}" | jq -r '
    "  rebalance id=\(.id) stoppedAt=\(.stoppedAt // "null")",
    "  stopPropagation: pendingTerminalReload=\(.stopPropagation.pendingTerminalReload // false) failedPeers=[\(.stopPropagation.failedPeers // [] | join(", "))]",
    (.pools[] |
      "  pool id=\(.id) status=\(.status) stopping=\(.stopping) used=\(.used) lastError=\(.lastError // "null")"
      + (if .progress != null then
          " progress: objects=\(.progress.objects) bytes=\(.progress.bytes) remainingBuckets=\(.progress.remainingBuckets) bucket=\(.progress.bucket) object=\(.progress.object) elapsed=\(.progress.elapsed)s eta=\(.progress.eta)s"
        else " progress: null" end))' >&2
  printf '%s\n' "--- full JSON ---" >&2
  printf '%s\n' "${body}" >&2
}

# Wait for rebalance to complete (all pools must finish, not just one)
wait_rebalance() {
  local waited=0 body code active failed completed total last_error remaining
  log "waiting for rebalance to complete via admin API (timeout ${REBALANCE_TIMEOUT}s)..."
  while [ "${waited}" -lt "${REBALANCE_TIMEOUT}" ]; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: waiting for rebalance to complete"
      return 0
    fi
    body="$(admin_api GET /rustfs/admin/v3/rebalance/status "")"
    code="$(admin_api_code)"
    if [ "${code}" != "200" ]; then
      # 404 means rebalance metadata has not been persisted yet (e.g. right
      # after start); keep polling. 000 is a transport error; also retry.
      if [ "${code}" = "404" ] || [ "${code}" = "000" ]; then
        sleep "${POLL_INTERVAL}"
        waited=$((waited + POLL_INTERVAL))
        continue
      fi
      printf '\033[1;31m[ERROR]\033[0m rebalance/status returned HTTP %s\n' "${code}" >&2
      printf '%s\n' "${body}" >&2
      die "rebalance/status failed"
    fi
    failed="$(printf '%s' "${body}" | jq '[.pools[] | select(.status == "Failed" or .status == "Stopped")] | length')"
    last_error="$(printf '%s' "${body}" | jq '[.pools[] | select(.lastError != null)] | length')"
    active="$(printf '%s' "${body}" | jq '[.pools[] | select(.status == "Started")] | length')"
    completed="$(printf '%s' "${body}" | jq '[.pools[] | select(.status == "Completed")] | length')"
    total="$(printf '%s' "${body}" | jq '.pools | length')"
    remaining="$(printf '%s' "${body}" | jq '[.pools[] | select(.progress != null) | .progress.remainingBuckets] | add // 0')"
    log "rebalance: completed=${completed}/${total} active=${active} remainingBuckets=${remaining}"
    if [ "${failed}" -gt 0 ] || [ "${last_error}" -gt 0 ]; then
      print_rebalance_failure "${body}" "rebalance entered a failure state (failed=${failed}, lastError=${last_error})"
      die "rebalance failed (see detail above)"
    fi
    if [ "${active}" -eq 0 ] && [ "${completed}" -gt 0 ]; then
      log "rebalance completed: ${completed}/${total} pools completed"
      printf '%s' "${body}" | jq -r '.pools[] | "  pool id=\(.id) status=\(.status) stopping=\(.stopping) used=\(.used)"'
      return 0
    fi
    sleep "${POLL_INTERVAL}"
    waited=$((waited + POLL_INTERVAL))
  done
  body="$(admin_api GET /rustfs/admin/v3/rebalance/status "")"
  print_rebalance_failure "${body}" "timed out waiting for rebalance (${REBALANCE_TIMEOUT}s)"
  die "timed out waiting for rebalance (${REBALANCE_TIMEOUT}s)"
}

# Start rebalance via the admin API. Nightly builds gate rebalance activation
# on a live cross-pool fence fleet capability proof that is re-established
# shortly after a pool joins, so retry a few times before failing.
start_rebalance_with_retry() {
  local attempts="${REBALANCE_START_RETRIES}" delay="${REBALANCE_START_RETRY_DELAY}" attempt=1 body code id
  while :; do
    body="$(admin_api POST /rustfs/admin/v3/rebalance/start "")"
    code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      id="$(printf '%s' "${body}" | jq -r '.id // empty')"
      log "rebalance started: id=${id}"
      return 0
    fi
    warn "rebalance start attempt ${attempt}/${attempts} failed (HTTP ${code}): ${body}"
    if [ "${attempt}" -ge "${attempts}" ]; then
      hint_server_issue "${body}" || true
      die "rebalance start failed after ${attempts} attempts (see last error above)"
    fi
    attempt=$((attempt + 1))
    sleep "${delay}"
  done
}

# Print a detailed decommission failure/progress report for one pool
print_decommission_detail() {
  local body="$1" pool_id="$2" label="$3"
  printf '%s\n' "--- decommission detail (pool ${pool_id}) ${label} ---" >&2
  printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '
    .pools[] | select(.id == $id) |
    "  pool id=\(.id) status=\(.status) poolStatus=\(.poolStatus)"
    + (if .decommissionInfo != null then
        " complete=\(.decommissionInfo.complete) failed=\(.decommissionInfo.failed) canceled=\(.decommissionInfo.canceled) queued=\(.decommissionInfo.queued)",
        "  stage=\(.decommissionInfo.stage // "null") bucket=\(.decommissionInfo.bucket // "null") prefix=\(.decommissionInfo.prefix // "null") object=\(.decommissionInfo.object // "null")",
        "  waitingReason=\(.decommissionInfo.waitingReason // "null")",
        "  objectsDecommissioned=\(.decommissionInfo.objectsDecommissioned) objectsDecommissionedFailed=\(.decommissionInfo.objectsDecommissionedFailed)",
        "  bytesDecommissioned=\(.decommissionInfo.bytesDecommissioned) bytesDecommissionedFailed=\(.decommissionInfo.bytesDecommissionedFailed)",
        "  size current=\(.decommissionInfo.currentSize)/\(.decommissionInfo.totalSize)",
        "  queuedBuckets=[\(.decommissionInfo.queuedBuckets | join(", "))]",
        "  decommissionedBuckets=[\(.decommissionInfo.decommissionedBuckets | join(", "))]",
        (if (.decommissionInfo.unresolvedEntries // [] | length) > 0 then
          "  unresolvedEntries=" + ([.decommissionInfo.unresolvedEntries[] | "bucket=" + .bucket + " object=" + .object + " reason=" + .reason] | join("; "))
         else empty end)
      else "  decommissionInfo=null (no decommission state recorded)" end)' >&2
  printf '%s\n' "--- full JSON ---" >&2
  printf '%s\n' "${body}" >&2
}

# Wait for a pool decommission to complete
wait_decommission() {
  local pool_id="$1" waited=0 body code
  local complete failed canceled queued objects_failed bytes_failed remaining done
  log "waiting for pool ${pool_id} decommission to complete via admin API (timeout ${DECOMMISSION_TIMEOUT}s)..."
  while [ "${waited}" -lt "${DECOMMISSION_TIMEOUT}" ]; do
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: waiting for decommission to complete"
      return 0
    fi
    body="$(admin_api GET /rustfs/admin/v3/decommission/status "")"
    code="$(admin_api_code)"
    if [ "${code}" != "200" ]; then
      printf '\033[1;31m[ERROR]\033[0m decommission/status returned HTTP %s\n' "${code}" >&2
      printf '%s\n' "${body}" >&2
      die "decommission/status failed"
    fi
    if ! printf '%s' "${body}" | jq -e --argjson id "${pool_id}" '.pools[] | select(.id == $id)' >/dev/null 2>&1; then
      print_decommission_detail "${body}" "${pool_id}" "pool missing"
      die "pool ${pool_id} not found in decommission status"
    fi
    complete="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.complete // false')"
    failed="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.failed // false')"
    canceled="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.canceled // false')"
    queued="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.queued // false')"
    objects_failed="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.objectsDecommissionedFailed // 0')"
    bytes_failed="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | .decommissionInfo.bytesDecommissionedFailed // 0')"
    remaining="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | (.decommissionInfo.queuedBuckets // [] | length)')"
    done="$(printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | (.decommissionInfo.decommissionedBuckets // [] | length)')"
    if [ "${failed}" = "true" ] || [ "${canceled}" = "true" ] \
      || [ "${objects_failed}" -gt 0 ] || [ "${bytes_failed}" -gt 0 ]; then
      print_decommission_detail "${body}" "${pool_id}" "FAILED"
      warn "pool ${pool_id} decommission failed/canceled (objects_failed=${objects_failed}, bytes_failed=${bytes_failed}); see detail above"
      return 1
    fi
    if [ "${complete}" = "true" ]; then
      log "pool ${pool_id} decommission completed"
      printf '%s' "${body}" | jq -r --argjson id "${pool_id}" '.pools[] | select(.id == $id) | "  pool id=\(.id) status=\(.status) complete=\(.decommissionInfo.complete) objects=\(.decommissionInfo.objectsDecommissioned) bytes=\(.decommissionInfo.bytesDecommissioned) waitingReason=\(.decommissionInfo.waitingReason // "null")"'
      return 0
    fi
    log "pool ${pool_id} decommission in progress: doneBuckets=${done} queuedBuckets=${remaining} objects_failed=${objects_failed} queued=${queued}"
    sleep "${POLL_INTERVAL}"
    waited=$((waited + POLL_INTERVAL))
  done
  body="$(admin_api GET /rustfs/admin/v3/decommission/status "")"
  print_decommission_detail "${body}" "${pool_id}" "TIMED OUT"
  die "timed out waiting for pool ${pool_id} decommission (${DECOMMISSION_TIMEOUT}s)"
}

# Preflight checks before running the workflow
preflight() {
  log "preflight checks"
  need_cmd ssh "SSH client"
  need_cmd curl "HTTP client"
  need_cmd jq "JSON processor"
  need_cmd openssl "OpenSSL (SigV4 signing)"
  local url
  url="$(resolve_package_url)"
  log "package URL: ${url}"
  if [ "${DRY_RUN}" -eq 0 ]; then
    curl -fsSI --max-time 20 "${url}" >/dev/null || die "package URL not reachable: ${url}"
    for node in "${NODES[@]}"; do
      ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "hostname" >/dev/null \
        || die "cannot SSH to ${node}"
      log "${node}: SSH OK"
    done
    log "admin API connectivity: GET ${API_ENDPOINT}/rustfs/admin/v3/pools/list"
    local body code
    body="$(admin_api GET /rustfs/admin/v3/pools/list "")"
    code="$(admin_api_code)"
    if [ "${code}" != "200" ]; then
      # In the automated workflow the cluster is stopped before preflight
      # (reset), so an unreachable admin API is expected there; the test
      # starts the cluster and verifies the API in step 3. Only fail hard
      # when the service is actually running but the API is broken.
      local service_state
      service_state="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" \
        "systemctl is-active ${RUSTFS_SERVICE} 2>/dev/null || true")"
      if [ "${service_state}" = "active" ]; then
        printf '\033[1;31m[ERROR]\033[0m admin API check failed (HTTP %s) while ${RUSTFS_SERVICE} is active on %s\n' \
          "${code}" "${NODES[0]}" >&2
        printf '%s\n' "${body}" >&2
        die "cannot reach the admin API at ${API_ENDPOINT} with the configured credentials"
      fi
      printf '\033[1;33m[WARN]\033[0m admin API not reachable (HTTP %s) — ${RUSTFS_SERVICE} is not active on %s; the test will start the cluster and verify the API in step 3\n' \
        "${code}" "${NODES[0]}" >&2
      printf '%s\n' "${body}" >&2
    else
      log "admin API OK ($(printf '%s' "${body}" | jq 'length') pool(s) listed)"
    fi
    # Check that each node resolves the rustfs-node* hostnames used by the volumes
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" \
      "grep -q rustfs-node /etc/hosts" || warn "rustfs-node* hostnames not found in /etc/hosts on ${NODES[0]}"
    log "node disk space (/data):"
    for node in "${NODES[@]}"; do
      ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
        "df -h /data | tail -1 | awk '{print \"  ${node}: \" \$2 \" total, \" \$4 \" avail\"}'" || true
    done
  fi
  if [ "${WITH_WARP}" -eq 1 ]; then
    need_cmd warp "warp benchmark tool"
  fi
  log "preflight OK"
}

# Reset the test environment: purge the rustfs package (if installed) and
# recreate the data directories on all nodes. Intended for CI so every run
# starts from a clean slate. Destructive!
step0_reset() {
  log "reset: purge rustfs package and recreate data dirs on all nodes"
  confirm "This DESTROYS the RustFS install and ALL data on ${NODES[*]} (irreversible). Continue?"
  for node in "${NODES[@]}"; do
    {
      printf 'set -euo pipefail\n'
      printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
      # Stop the service if it is still running, then purge the package
      # (skipped when rustfs is not installed).
      printf '${SUDO} systemctl stop %s 2>/dev/null || true\n' "${RUSTFS_SERVICE}"
      printf 'if ${SUDO} dpkg -l %s 2>/dev/null | grep -q "^ii"; then\n' "${RUSTFS_PACKAGE_NAME}"
      printf '  ${SUDO} dpkg -P %s\n' "${RUSTFS_PACKAGE_NAME}"
      printf '  echo "purged %s"\n' "${RUSTFS_PACKAGE_NAME}"
      printf 'else\n'
      printf '  echo "%s not installed, skip purge"\n' "${RUSTFS_PACKAGE_NAME}"
      printf 'fi\n'
      # Ensure the service user exists (created by the package postinst on
      # install; a purge keeps it, but a never-installed node needs it for chown).
      printf 'id -u %s >/dev/null 2>&1 || ${SUDO} useradd -r -s /bin/false -d /opt/%s %s\n' \
        "${RUSTFS_USER}" "${RUSTFS_USER}" "${RUSTFS_USER}"
      # Recreate the volume directories with the service user as owner.
      printf 'for i in 1 2 3 4; do\n'
      printf '  ${SUDO} rm -rf /data/rustfs${i}/mnmd\n'
      printf '  ${SUDO} mkdir -p /data/rustfs${i}/mnmd\n'
      printf '  ${SUDO} chown -R %s:%s /data/rustfs${i}/mnmd\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
      printf 'done\n'
    } | run_remote "${node}"
  done
  log "reset complete"
}

# ==================== Steps ====================

step1_download() {
  log "step 1: download the package on all nodes"
  local script url
  url="$(resolve_package_url)"
  script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
if [ -f "${PACKAGES_DIR}/${PACKAGE_FILE}" ] && [ "${SKIP_DOWNLOAD}" -eq 1 ]; then
  echo "already exists: ${PACKAGES_DIR}/${PACKAGE_FILE}, skipping download"
else
  echo "downloading ${url} ..."
  curl -fSL --retry 3 -o "/tmp/${PACKAGE_FILE}" "${url}"
  \${SUDO} mkdir -p "${PACKAGES_DIR}"
  \${SUDO} install -m 0644 "/tmp/${PACKAGE_FILE}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
  \${SUDO} rm -f "/tmp/${PACKAGE_FILE}"
fi
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || { echo "checksum verification failed"; exit 1; }
fi
ls -lh "${PACKAGES_DIR}/${PACKAGE_FILE}"
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
}

step2_install() {
  log "step 2: install the RustFS service on all nodes"
  confirm "About to run dpkg -i ${PACKAGE_FILE} on all nodes. Continue?"
  local script
  script="$(cat <<'EOF'
set -euo pipefail
SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"
${SUDO} dpkg -i /home/rustfs/packages/rustfs.deb
${SUDO} systemctl daemon-reload
echo "--- installed package ---"
dpkg -l rustfs | tail -n 1
EOF
)"
  printf '%s\n' "${script}" | run_remote_all
}

step3_start_first_pool() {
  log "step 3: start the first pool on ${NODES[0]}"
  write_rustfs_config "${NODES[0]}" "${VOLUMES_1}"
  ensure_volume_dirs "${NODES[0]}" "${VOLUMES_1}"
  service_action start "${NODES[0]}"
  wait_service_active "${NODES[0]}"
  log "checking service status: systemctl status ${RUSTFS_SERVICE}"
  if [ "${DRY_RUN}" -eq 0 ]; then
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${NODES[0]}" \
      "systemctl status ${RUSTFS_SERVICE} --no-pager | head -n 15" || true
  fi
  verify_pools 1
}

step4_write_data() {
  log "step 4: write data and monitor storage usage"
  if [ "${WITH_WARP}" -eq 1 ]; then
    need_cmd warp "warp benchmark tool"
    confirm "About to run warp writes in the background (bucket=${WARP_BUCKET}, duration=${WARP_DURATION}) and monitor usage up to ${STORAGE_THRESHOLD}%. Continue?"
    if [ "${DRY_RUN}" -eq 1 ]; then
      log "DRY-RUN: warp put --host ${API_ENDPOINT#http://} --bucket ${WARP_BUCKET} ... (background)"
      log "DRY-RUN: monitoring storage usage until ${STORAGE_THRESHOLD}%"
      return 0
    fi
    local warp_log warp_pid
    warp_log="${WARP_LOG_FILE:-$(mktemp "${TMPDIR:-/tmp}/rustfs-warp.XXXXXX.log")}"
    log "starting warp writes (background), log: ${warp_log}"
    warp put --host "${API_ENDPOINT#http://}" \
      --bucket "${WARP_BUCKET}" \
      --access-key "${ACCESS_KEY}" \
      --secret-key "${SECRET_KEY}" \
      --obj.size "${WARP_OBJ_SIZE}" \
      --concurrent "${WARP_CONCURRENT}" \
      --noprefix --duration "${WARP_DURATION}" --noclear >"${warp_log}" 2>&1 &
    warp_pid=$!
    log "warp PID=${warp_pid}"
    trap 'kill "${warp_pid:-}" 2>/dev/null || true' EXIT
    monitor_storage "${STORAGE_THRESHOLD}" "${warp_pid}"
    kill "${warp_pid}" 2>/dev/null || true
    trap - EXIT
    wait "${warp_pid}" 2>/dev/null || true
    log "warp stopped"
  else
    cat <<'NOTE'
Write data manually (or re-run with --with-warp to automate):

  warp put --host "rustfs-node1:9000" --bucket "test-10mb" \
    --access-key "rustfs@test" --secret-key "rustfs@test" \
    --obj.size "100MiB" --concurrent 32 --noprefix --duration 5m --noclear

While writing, monitor storage usage:
  GET ${API_ENDPOINT}/rustfs/admin/v3/storageinfo  (SigV4-signed)
  e.g. Storage: 30.88 GiB / 1.56 TiB (1%)

Stop writing once usage reaches 80%-85%, then continue.
NOTE
    confirm "Data has been written and usage reached 80%-85%? Continue?"
  fi
}

step5_expand_pool2() {
  log "step 5: expand to the second pool (${NODES[0]} + ${NODES[1]})"
  confirm "About to stop ${NODES[0]} and update its config. Continue?"
  service_action stop "${NODES[0]}"
  write_rustfs_config "${NODES[0]}" "${VOLUMES_2}"
  write_rustfs_config "${NODES[1]}" "${VOLUMES_2}"
  ensure_volume_dirs "${NODES[0]}" "${VOLUMES_2}"
  ensure_volume_dirs "${NODES[1]}" "${VOLUMES_2}"
  start_and_wait_nodes "${NODES[0]}" "${NODES[1]}"
  verify_pools 2
}

step6_rebalance() {
  log "step 6: start data rebalance (admin API)"
  confirm "About to start rebalance (POST ${API_ENDPOINT}/rustfs/admin/v3/rebalance/start). Continue?"
  if [ "${DRY_RUN}" -eq 0 ]; then
    start_rebalance_with_retry
  fi
  wait_rebalance
}

step7_expand_pool3() {
  log "step 7: expand to the third pool (all nodes)"
  confirm "About to stop ${NODES[0]} ${NODES[1]} and update all node configs. Continue?"
  service_action stop "${NODES[0]}"
  service_action stop "${NODES[1]}"
  for node in "${NODES[@]}"; do
    write_rustfs_config "${node}" "${VOLUMES_3}"
  done
  for node in "${NODES[@]}"; do
    ensure_volume_dirs "${node}" "${VOLUMES_3}"
  done
  start_and_wait_nodes "${NODES[@]}"
  verify_pools 3
}

step8_rebalance() {
  log "step 8: start data rebalance (admin API)"
  confirm "About to start rebalance (POST ${API_ENDPOINT}/rustfs/admin/v3/rebalance/start). Continue?"
  if [ "${DRY_RUN}" -eq 0 ]; then
    start_rebalance_with_retry
  fi
  wait_rebalance
}

step9_decommission() {
  log "step 9: decommission pool ${DECOMMISSION_POOL_ID} (admin API)"
  confirm "About to decommission pool ${DECOMMISSION_POOL_ID} (up to ${DECOMMISSION_RETRIES} automatic retries). Continue?"
  local attempt=1 body
  # If the previous decommission is in a failed/cancelled state, clear its metadata first
  if [ "${DRY_RUN}" -eq 0 ]; then
    body="$(admin_api POST /rustfs/admin/v3/pools/clear "by-id=true&pool=${DECOMMISSION_POOL_ID}")"
    [ "$(admin_api_code)" = "200" ] || warn "initial decommission clear returned HTTP $(admin_api_code): ${body}"
  fi
  while :; do
    log "decommission attempt ${attempt}/${DECOMMISSION_RETRIES}"
    if [ "${DRY_RUN}" -eq 0 ]; then
      body="$(admin_api POST /rustfs/admin/v3/pools/decommission "by-id=true&pool=${DECOMMISSION_POOL_ID}")"
      if [ "$(admin_api_code)" != "200" ]; then
        printf '\033[1;31m[ERROR]\033[0m decommission start returned HTTP %s\n' "$(admin_api_code)" >&2
        printf '%s\n' "${body}" >&2
        die "decommission start failed"
      fi
      log "decommission start accepted"
    fi
    if wait_decommission "${DECOMMISSION_POOL_ID}"; then
      break
    fi
    if [ "${attempt}" -ge "${DECOMMISSION_RETRIES}" ]; then
      warn "if the source bucket has many objects and the tested version is 1.0.0-rc.3, this is the known metacache-listing decommission bug; remove the test bucket (rc rb --force rustfs/${WARP_BUCKET}) or lower --storage-threshold, then re-run step 9"
      die "pool ${DECOMMISSION_POOL_ID} still failed after ${attempt} attempts; investigate manually (POST ${API_ENDPOINT}/rustfs/admin/v3/pools/clear?by-id=true&pool=${DECOMMISSION_POOL_ID} to reset)"
    fi
    warn "attempt ${attempt} failed; clearing metadata and retrying in ${DECOMMISSION_RETRY_DELAY}s"
    if [ "${DRY_RUN}" -eq 0 ]; then
      body="$(admin_api POST /rustfs/admin/v3/pools/clear "by-id=true&pool=${DECOMMISSION_POOL_ID}")"
      [ "$(admin_api_code)" = "200" ] || warn "decommission clear failed (HTTP $(admin_api_code)): ${body}; retrying anyway"
    fi
    attempt=$((attempt + 1))
    sleep "${DECOMMISSION_RETRY_DELAY}"
  done
  if [ "${FINALIZE_DECOMMISSION}" -eq 1 ]; then
    log "decommission done; removing pool ${DECOMMISSION_POOL_ID} from all node configs and restarting"
    confirm "About to update all node configs (remove the decommissioned pool) and restart services. Continue?"
    for node in "${NODES[@]}"; do
      write_rustfs_config "${node}" "${VOLUMES_AFTER_DECOMMISSION}"
    done
    service_action_all restart
    for node in "${NODES[@]}"; do
      wait_service_active "${node}"
    done
    verify_pools 2
  fi
}

# ==================== CLI parsing ====================

usage() {
  cat <<'USAGE'
Usage: rustfs-pool-expand.sh [options]

Options:
  --all                      Run the full workflow (steps 1-9)
  --step N                   Run a single step (repeatable)
  --steps 1,3,5-7            Run steps in order
  --with-warp                Automate step 4 (warp writes + usage monitoring)
  --finalize-decommission    After decommission, remove the pool from the topology and restart
  --skip-download            Skip download when the package already exists
  --version VER              RustFS release tag to test, e.g. 1.0.0-rc.3 (default from config)
  --package-url URL          Full package download URL (overrides --version)
  --sha256 HEX               Verify the downloaded package checksum
  --preflight                Run preflight checks before the selected steps
  --reset                    Reset all nodes (stop services, wipe data dirs + config). Destructive!
  --ssh-user USER            SSH user for the nodes (default azureuser)
  --ssh-port PORT            SSH port for the nodes (default 22)
  --endpoint URL             Cluster admin API endpoint, e.g. http://10.0.0.7:9000 (default from config)
  --rc-endpoint URL          Deprecated alias for --endpoint
  --storage-threshold N      Stop writing when usage reaches N% (default 85)
  --warp-duration DUR        warp write duration, e.g. 5m, 20s (default 5m)
  --rebalance-timeout N      Rebalance wait timeout in seconds (default 86400)
  --decommission-timeout N   Decommission wait timeout in seconds (default 86400)
  --service-timeout N        Service start wait timeout in seconds (default 300)
  --poll-interval N          Status polling interval in seconds (default 30)
  --log-file FILE            Append all output to FILE
  --dry-run                  Preview commands without executing them
  -y, --yes                  Skip all confirmation prompts
  -h, --help                 Show this help

Examples:
  ./rustfs-pool-expand.sh --all
  ./rustfs-pool-expand.sh --all --with-warp --yes --version 1.0.0-rc.3
  ./rustfs-pool-expand.sh --step 5
  ./rustfs-pool-expand.sh --all --dry-run
USAGE
}

expand_steps() {
  # Expand a "1,3,5-7" step spec into SELECTED_STEPS
  local spec="$1" part start end i
  IFS=',' read -ra parts <<<"${spec}"
  for part in "${parts[@]}"; do
    if [[ "${part}" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      start="${BASH_REMATCH[1]}"; end="${BASH_REMATCH[2]}"
      for ((i=start; i<=end; i++)); do SELECTED_STEPS+=("${i}"); done
    elif [[ "${part}" =~ ^[0-9]+$ ]]; then
      SELECTED_STEPS+=("${part}")
    else
      die "cannot parse step spec: ${part}"
    fi
  done
}

run_steps() {
  local step
  for step in "${SELECTED_STEPS[@]}"; do
    case "${step}" in
      1) step1_download ;;
      2) step2_install ;;
      3) step3_start_first_pool ;;
      4) step4_write_data ;;
      5) step5_expand_pool2 ;;
      6) step6_rebalance ;;
      7) step7_expand_pool3 ;;
      8) step8_rebalance ;;
      9) step9_decommission ;;
      *) die "unknown step: ${step}" ;;
    esac
    log "step ${step} completed"
  done
}

main() {
  [ "$#" -eq 0 ] && { usage; exit 0; }
  local opt all=0
  while [ "$#" -gt 0 ]; do
    opt="$1"; shift
    case "${opt}" in
      --all) all=1 ;;
      --step) SELECTED_STEPS+=("$1"); shift ;;
      --steps) expand_steps "$1"; shift ;;
      --with-warp) WITH_WARP=1 ;;
      --finalize-decommission) FINALIZE_DECOMMISSION=1 ;;
      --skip-download) SKIP_DOWNLOAD=1 ;;
      --version) RUSTFS_VERSION="$1"; shift ;;
      --package-url) PACKAGE_URL="$1"; shift ;;
      --sha256) PACKAGE_SHA256="$1"; shift ;;
      --preflight) PREFLIGHT=1 ;;
      --reset) RESET=1 ;;
      --ssh-user) SSH_USER="$1"; shift ;;
      --ssh-port) SSH_PORT="$1"; shift ;;
      --endpoint) API_ENDPOINT="$1"; shift ;;
      --rc-endpoint) API_ENDPOINT="$1"; shift ;;
      --storage-threshold) STORAGE_THRESHOLD="$1"; shift ;;
      --warp-duration) WARP_DURATION="$1"; shift ;;
      --rebalance-timeout) REBALANCE_TIMEOUT="$1"; shift ;;
      --decommission-timeout) DECOMMISSION_TIMEOUT="$1"; shift ;;
      --service-timeout) SERVICE_TIMEOUT="$1"; shift ;;
      --poll-interval) POLL_INTERVAL="$1"; shift ;;
      --log-file) LOG_FILE="$1"; shift ;;
      --dry-run) DRY_RUN=1 ;;
      -y|--yes) ASSUME_YES=1 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown option: ${opt} (see --help)" ;;
    esac
  done
  trap 'rm -f "${ADMIN_API_CODE_FILE}"' EXIT
  if [ -n "${LOG_FILE}" ]; then
    mkdir -p "$(dirname "${LOG_FILE}")"
    if ! touch "${LOG_FILE}" 2>/dev/null; then
      # A fixed /tmp path may be owned by another user (e.g. a previous root
      # run); fall back to a unique, always-writable temp file.
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-pool-test.XXXXXX.log")"
      warn "log file not writable; using ${LOG_FILE}"
    fi
    exec > >(tee -a "${LOG_FILE}") 2>&1
  fi
  if [ "${RESET}" -eq 1 ]; then
    step0_reset
    log "reset finished"
    exit 0
  fi
  if [ "${all}" -eq 1 ]; then
    SELECTED_STEPS=(1 2 3 4 5 6 7 8 9)
  fi
  PACKAGE_URL="$(resolve_package_url)"
  if [ "${PREFLIGHT}" -eq 1 ]; then
    preflight
    if [ "${#SELECTED_STEPS[@]}" -eq 0 ]; then
      log "preflight only (no steps selected); done"
      exit 0
    fi
  fi
  [ "${#SELECTED_STEPS[@]}" -gt 0 ] || die "no steps selected (--all / --step / --steps)"
  if [ "${DRY_RUN}" -eq 0 ]; then
    log "nodes: ${NODES[*]}  ssh user: ${SSH_USER}  version: ${RUSTFS_VERSION}"
    log "package: ${PACKAGE_URL}"
  else
    warn "DRY-RUN mode: only printing the commands that would run"
  fi
  run_steps
  log "all done"
}

# Allow sourcing the file for unit tests without running main.
if [ "${RUSTFS_POOL_SCRIPT_SOURCE_ONLY:-0}" != "1" ]; then
  main "$@"
fi
