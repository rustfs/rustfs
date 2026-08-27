#!/usr/bin/env bash
#
# rustfs-heal-test.sh
# RustFS node-outage heal test script
#
# Based on the Obsidian note "RustFS Heal 测试步骤". Full workflow:
#   1. Download the RustFS package on all nodes
#   2. Install RustFS on all nodes (dpkg -i), write the 3x4 single-pool
#      config, start all nodes in parallel and verify the topology via the
#      admin API
#   3. Write data (warp); when the surviving nodes reach STOP_NODE_AT_GB
#      stop vm002 (simulated node outage), keep writing until the surviving
#      nodes reach WARP_STOP_AT_GB
#   4. Restart vm002 (the node that was offline while data was written)
#   5. Start cluster heal (POST /rustfs/admin/v3/heal/ {"recursive":true})
#   6. Monitor the heal task (POST /rustfs/admin/v3/heal/?clientToken=...)
#      until the server verdict is a terminal success (finished, 0 failed),
#      then verify the data is readable back from the cluster
#   7. Result analysis: heal stats, per-node disk usage, success verdict
#
# The script is driven from an admin host (e.g. a jumpbox or a GitHub
# self-hosted runner) and operates on the target nodes over SSH.
#
# Usage:
#   ./rustfs-heal-test.sh --all                    # run all steps 1-7
#   ./rustfs-heal-test.sh --step 5                 # run a single step
#   ./rustfs-heal-test.sh --steps 3,4,5,6,7        # run selected steps
#   ./rustfs-heal-test.sh --all --dry-run          # preview only
#   ./rustfs-heal-test.sh --all -y --package-url <nightly deb URL>
#
# Notes:
#   - SSH user defaults to azureuser (passwordless sudo on the nodes);
#     pass --ssh-user root if your nodes accept root login.
#   - The script talks to the RustFS admin API directly with SigV4-signed
#     requests (no rc required). jq, openssl and curl must be installed on the
#     admin host; warp is needed for the data-write step.
#   - Credentials default to rustfs@test / rustfs@test (the config written on
#     the nodes); override via RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY.
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
ADMIN_API_CODE_FILE="${TMPDIR:-/tmp}/rustfs-heal-test-api-code.$$"

# 3x4 topology (3 nodes x 4 disks each), same expression on every node:
# http://rustfs-node{1...3}:9000/data/rustfs{1...4}/mnmd
VOLUMES="http://rustfs-node{1...3}:9000/data/rustfs{1...4}/mnmd"

# Node that is stopped during the write phase (index into NODES; 2 = vm002)
OUTAGE_NODE_INDEX="${RUSTFS_OUTAGE_NODE_INDEX:-2}"

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

# Data writing & monitoring (step 3)
WARP_BUCKET="test-10mb"
WARP_OBJ_SIZE="100MiB"
WARP_CONCURRENT=32
# Warp log path; empty = auto-created unique temp file (the runner user may
# not be able to write a shared /tmp path owned by another user).
WARP_LOG_FILE="${RUSTFS_WARP_LOG_FILE:-}"
# Disk-usage thresholds (per surviving node, GiB, via df -B1G | grep /data/rustfs)
STOP_NODE_AT_GB="${RUSTFS_STOP_NODE_AT_GB:-15}"   # stop the outage node when surviving nodes reach this
WARP_STOP_AT_GB="${RUSTFS_WARP_STOP_AT_GB:-40}"   # stop warp when surviving nodes reach this
POLL_INTERVAL=15                # status polling interval (seconds)

# Timeouts (seconds)
SERVICE_TIMEOUT=300
WARP_TIMEOUT=3600               # max time waiting for the write phase thresholds
HEAL_TIMEOUT=86400              # max time waiting for heal to complete
HEAL_START_RETRIES=6            # heal start retries (fleet capability proof timing)
HEAL_START_RETRY_DELAY=20       # delay between heal start retries (seconds)
# Per-task heal timeout on the server (default is 5 minutes, far too short
# for healing tens of GiB); written into /etc/default/rustfs.
HEAL_TASK_TIMEOUT_SECS="${RUSTFS_HEAL_TASK_TIMEOUT_SECS:-21600}"
# Disable the background scanner so the explicit heal is the only repair
# mechanism (otherwise automatic repairs can mask the outage effect).
HEAL_AUTO_HEAL_ENABLE="${RUSTFS_HEAL_AUTO_HEAL_ENABLE:-false}"

# ==================== Runtime options (set by CLI) ====================
DRY_RUN=0
ASSUME_YES=0
SKIP_DOWNLOAD=0
PREFLIGHT=0
RESET=0
LOG_FILE=""
SELECTED_STEPS=()
HEAL_CLIENT_TOKEN=""

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
  # $1: method, $2: path, $3: query string, $4: optional JSON body,
  # $5: "discard" to skip body capture (only the status code matters)
  local method="$1" path="$2" query="$3" body="${4:-}" discard="${5:-}"
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
  local curl_body=()
  if [ -n "${body}" ]; then
    curl_body=(-d "${body}" -H "Content-Type: application/json")
  fi
  local out_file="${tmp}"
  [ "${discard}" = "discard" ] && out_file="/dev/null"
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${out_file}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_body[@]}" -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  if [ "${discard}" != "discard" ]; then
    cat "${tmp}"
  fi
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
  "pool activation requires a live fleet capability proof|rustfs/backlog#2031|multi-pool cold start with rebalance metadata fails on nightly builds; server fix pending, no script workaround"
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
  journal="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "SUDO=\"\"; [ \"\$(id -u)\" -ne 0 ] && SUDO=\"sudo -n\"; \${SUDO} journalctl -u ${RUSTFS_SERVICE} --no-pager -n 60 2>/dev/null || true")"
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
RUSTFS_HEAL_TASK_TIMEOUT_SECS=${HEAL_TASK_TIMEOUT_SECS}
RUSTFS_HEAL_AUTO_HEAL_ENABLE=${HEAL_AUTO_HEAL_ENABLE}
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

# Used disk space on a node across all /data/rustfs* mounts (GiB, via df -B1G)
node_used_gb() {
  local node="$1" total=0 gb
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: df -B1G | grep /data/rustfs on ${node}"
    printf '0\n'
    return 0
  fi
  while IFS= read -r gb; do
    [ -n "${gb}" ] && total=$((total + gb))
  done < <(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "df -B1G | awk '/\\/data\\/rustfs/ {gsub(/[^0-9]/,\"\",\$3); print \$3+0}'" 2>/dev/null)
  printf '%s' "${total}"
}

# Read a heal-progress counter. The heal API serializes progress in camelCase
# (objectsScanned / objectsHealed / objectsFailed / progressPercentage); keep a
# snake_case fallback for older builds. Prints "null" when the field is absent.
heal_progress_field() {
  local body="$1" camel="$2" snake="$3"
  printf '%s' "${body}" | jq -r --arg c "${camel}" --arg s "${snake}" '
    (.progress // null) as $p
    | if $p == null then "null" else (($p[$c] // $p[$s] // null) | if . == null then "null" else tostring end) end'
}

# Sample data verification after heal: list the test bucket and GET a sample of
# objects. Every GET must return 200 — this is the end-to-end proof that the
# cluster can still reconstruct the data after repair.
verify_data_readable() {
  local list body code keys key count checked ok
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: S3 read-back verification of ${WARP_BUCKET}"
    return 0
  fi
  list="$(admin_api GET "/${WARP_BUCKET}" "list-type=2&max-keys=1000")"
  code="$(admin_api_code)"
  if [ "${code}" != "200" ]; then
    printf '\033[1;31m[ERROR]\033[0m bucket list failed (HTTP %s): %s\n' "${code}" "${list}" >&2
    return 1
  fi
  # sed -n '1,20p' reads the whole stream (unlike head, which closes the pipe
  # early and SIGPIPEs grep/sed under pipefail).
  keys="$(printf '%s' "${list}" | grep -oE '<Key>[^<]+</Key>' | sed 's#</\?Key>##g' | sed -n '1,20p')"
  count="$(printf '%s\n' "${keys}" | sed '/^$/d' | wc -l | tr -d ' ')"
  log "data verification: ${count} object(s) sampled from the bucket; reading each (status-code check)..."
  checked=0; ok=0
  while IFS= read -r key; do
    [ -z "${key}" ] && continue
    admin_api GET "/${WARP_BUCKET}/${key}" "" "" discard
    code="$(admin_api_code)"
    checked=$((checked + 1))
    if [ "${code}" = "200" ]; then
      ok=$((ok + 1))
    else
      printf '\033[1;31m[ERROR]\033[0m GET %s failed (HTTP %s)\n' "${key}" "${code}" >&2
    fi
  done <<<"${keys}"
  log "data verification: ${ok}/${checked} objects read successfully"
  [ "${checked}" -gt 0 ] && [ "${ok}" -eq "${checked}" ]
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
  need_cmd warp "warp benchmark tool"
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

step2_install_and_start() {
  log "step 2: install rustfs, write the 3x4 config, start all nodes simultaneously"
  confirm "About to dpkg -i and start rustfs on all nodes. Continue?"
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
  for node in "${NODES[@]}"; do
    write_rustfs_config "${node}" "${VOLUMES}"
  done
  for node in "${NODES[@]}"; do
    ensure_volume_dirs "${node}" "${VOLUMES}"
  done
  start_and_wait_nodes "${NODES[@]}"
  verify_pools 1
}

step3_write_data_with_node_outage() {
  log "step 3: write data with warp; stop ${NODES[${OUTAGE_NODE_INDEX}]} at ${STOP_NODE_AT_GB}GB, stop warp at ${WARP_STOP_AT_GB}GB"
  need_cmd warp "warp benchmark tool"
  confirm "About to run warp writes (bucket=${WARP_BUCKET}) and stop ${NODES[${OUTAGE_NODE_INDEX}]} mid-write. Continue?"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: warp put + monitor node disk usage"
    return 0
  fi

  local warp_log warp_pid outage_node="${NODES[${OUTAGE_NODE_INDEX}]}"
  local survived_a survived_b used_a used_b used_c waited=0 node_stopped=0 target_reached=0

  warp_log="${WARP_LOG_FILE:-$(mktemp "${TMPDIR:-/tmp}/rustfs-warp.XXXXXX.log")}"
  log "starting warp writes (background), log: ${warp_log}"
  warp put --host "${API_ENDPOINT#http://}" \
    --bucket "${WARP_BUCKET}" \
    --access-key "${ACCESS_KEY}" \
    --secret-key "${SECRET_KEY}" \
    --obj.size "${WARP_OBJ_SIZE}" \
    --concurrent "${WARP_CONCURRENT}" \
    --noprefix --noclear >"${warp_log}" 2>&1 &
  warp_pid=$!
  log "warp PID=${warp_pid}"
  trap 'kill "${warp_pid:-}" 2>/dev/null || true' EXIT

  # Surviving nodes: everything except the outage node
  survived_a="${NODES[0]}"
  survived_b="${NODES[1]}"
  if [ "${OUTAGE_NODE_INDEX}" = "0" ]; then
    survived_a="${NODES[1]}"
    survived_b="${NODES[2]}"
  elif [ "${OUTAGE_NODE_INDEX}" = "1" ]; then
    survived_b="${NODES[2]}"
  fi
  log "surviving nodes: ${survived_a} ${survived_b}; outage node: ${outage_node}"

  log "monitoring disk usage every ${POLL_INTERVAL}s (stop node @ ${STOP_NODE_AT_GB}GB, stop warp @ ${WARP_STOP_AT_GB}GB)"
  while [ "${waited}" -lt "${WARP_TIMEOUT}" ]; do
    used_a="$(node_used_gb "${survived_a}")"
    used_b="$(node_used_gb "${survived_b}")"
    used_c="$(node_used_gb "${outage_node}")"
    log "used: ${survived_a}=${used_a}GB ${survived_b}=${used_b}GB ${outage_node}=${used_c}GB (node_stopped=${node_stopped})"

    if [ "${node_stopped}" -eq 0 ] \
      && [ "${used_a}" -ge "${STOP_NODE_AT_GB}" ] && [ "${used_b}" -ge "${STOP_NODE_AT_GB}" ]; then
      log "surviving nodes reached ${STOP_NODE_AT_GB}GB; stopping ${outage_node}"
      service_action stop "${outage_node}"
      node_stopped=1
      # Fail closed: the outage node must actually be down before continuing.
      if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${outage_node}" \
          "systemctl is-active ${RUSTFS_SERVICE} 2>/dev/null" | grep -q active; then
        die "${outage_node} is still active after systemctl stop; cannot continue the outage scenario"
      fi
      log "${outage_node} service is down"
    fi

    if [ "${used_a}" -ge "${WARP_STOP_AT_GB}" ] && [ "${used_b}" -ge "${WARP_STOP_AT_GB}" ]; then
      log "surviving nodes reached ${WARP_STOP_AT_GB}GB; stopping warp"
      target_reached=1
      break
    fi

    if ! kill -0 "${warp_pid}" 2>/dev/null; then
      die "warp exited early at ${used_a}/${used_b}GB before reaching ${WARP_STOP_AT_GB}GB (log: ${warp_log})"
      break
    fi

    sleep "${POLL_INTERVAL}"
    waited=$((waited + POLL_INTERVAL))
  done
  kill "${warp_pid}" 2>/dev/null || true
  trap - EXIT
  wait "${warp_pid}" 2>/dev/null || true
  log "warp stopped (log: ${warp_log})"

  if [ "${node_stopped}" -eq 0 ]; then
    die "never reached ${STOP_NODE_AT_GB}GB on both surviving nodes within ${WARP_TIMEOUT}s; the outage node was NOT stopped — test invalid"
  fi
  if [ "${target_reached}" -eq 0 ]; then
    die "warp did not reach ${WARP_STOP_AT_GB}GB on the surviving nodes within ${WARP_TIMEOUT}s — test invalid"
  fi
  used_a="$(node_used_gb "${survived_a}")"
  used_b="$(node_used_gb "${survived_b}")"
  log "final used before heal: ${survived_a}=${used_a}GB ${survived_b}=${used_b}GB ${outage_node}=$(node_used_gb "${outage_node}")GB"
}

step4_restart_node() {
  local node="${NODES[${OUTAGE_NODE_INDEX}]}"
  log "step 4: restart ${node} after the outage"
  confirm "About to start rustfs on ${node}. Continue?"
  if [ "${DRY_RUN}" -eq 1 ]; then
    service_action start "${node}"
    log "DRY-RUN: waiting for ${node} to become active"
    return 0
  fi
  service_action start "${node}"
  wait_service_active "${node}"
  # Bounded readiness: wait for the admin API to report an active pool.
  local attempts=0
  while ! verify_pools 1 >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 12 ]; then
      verify_pools 1   # final call fails loudly
    fi
    log "cluster not ready yet (attempt ${attempts}/12); waiting 5s"
    sleep 5
  done
  log "${node} is back; cluster reports an active pool"
}

step5_start_heal() {
  log "step 5: start cluster heal (POST /rustfs/admin/v3/heal/ {\"recursive\":true,...})"
  confirm "About to start heal on the cluster. Continue?"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: POST /rustfs/admin/v3/heal/ {\"recursive\":true,...}"
    return 0
  fi
  local attempts="${HEAL_START_RETRIES}" delay="${HEAL_START_RETRY_DELAY}" attempt=1 body code
  while :; do
    body="$(admin_api POST /rustfs/admin/v3/heal/ "" \
      '{"recursive":true,"dryRun":false,"remove":false,"recreate":false,"scanMode":"normal","updateParity":false,"nolock":false}')"
    code="$(admin_api_code)"
    if [ "${code}" = "200" ]; then
      if [ -n "${body}" ]; then
        HEAL_CLIENT_TOKEN="$(printf '%s' "${body}" | jq -r '.clientToken // empty')"
        log "heal started: clientToken=${HEAL_CLIENT_TOKEN}"
      else
        log "heal start accepted (empty response)"
      fi
      return 0
    fi
    if [ "${code}" = "400" ] || [ "${code}" = "403" ]; then
      printf '\033[1;31m[ERROR]\033[0m heal start rejected (HTTP %s): %s\n' "${code}" "${body}" >&2
      die "heal start rejected (HTTP ${code}); fix the request, not the retry"
    fi
    warn "heal start attempt ${attempt}/${attempts} failed (HTTP ${code}): ${body}"
    hint_server_issue "${body}" || true
    if [ "${attempt}" -ge "${attempts}" ]; then
      die "heal start failed after ${attempts} attempts (see last error above)"
    fi
    attempt=$((attempt + 1))
    sleep "${delay}"
  done
}

step6_monitor_heal() {
  log "step 6: monitor heal task until the server verdict is done"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: waiting for heal to complete"
    return 0
  fi
  if [ -z "${HEAL_CLIENT_TOKEN}" ]; then
    die "no heal client token (run step 5 first, or pass --heal-token)"
  fi
  local waited=0 body code summary failed healed scanned pct prog_present
  local failed_n healed_n scanned_n pct_n vm002_used warned501=0 pre_outage_used
  pre_outage_used="$(node_used_gb "${NODES[${OUTAGE_NODE_INDEX}]}")"
  log "outage node usage before heal: ${pre_outage_used}GB"
  while [ "${waited}" -lt "${HEAL_TIMEOUT}" ]; do
    body="$(admin_api POST /rustfs/admin/v3/heal/ "clientToken=${HEAL_CLIENT_TOKEN}" "")"
    code="$(admin_api_code)"
    if [ "${code}" != "200" ]; then
      if [ "${code}" = "501" ] && [ "${warned501}" -eq 0 ]; then
        # background task status should work; keep polling if the cluster's
        # background-heal aggregator is unavailable (501 on some topologies).
        warn "heal task status returned HTTP 501 (${body}); retrying"
        warned501=1
      fi
      sleep "${POLL_INTERVAL}"
      waited=$((waited + POLL_INTERVAL))
      continue
    fi
    summary="$(printf '%s' "${body}" | jq -r '.summary // "running"')"
    prog_present="$(printf '%s' "${body}" | jq -r '.progress != null')"
    failed="$(heal_progress_field "${body}" objectsFailed objects_failed)"
    healed="$(heal_progress_field "${body}" objectsHealed objects_healed)"
    scanned="$(heal_progress_field "${body}" objectsScanned objects_scanned)"
    pct="$(heal_progress_field "${body}" progressPercentage progress_percentage)"
    failed_n="${failed}"; [ "${failed_n}" = "null" ] && failed_n=0
    healed_n="${healed}"; [ "${healed_n}" = "null" ] && healed_n=0
    scanned_n="${scanned}"; [ "${scanned_n}" = "null" ] && scanned_n=0
    pct_n="${pct}"; [ "${pct_n}" = "null" ] && pct_n=0
    vm002_used="$(node_used_gb "${NODES[${OUTAGE_NODE_INDEX}]}")"
    log "heal: summary=${summary} scanned=${scanned_n} healed=${healed_n} failed=${failed_n} pct=${pct_n} ${NODES[${OUTAGE_NODE_INDEX}]}_used=${vm002_used}GB"

    if [ "${prog_present}" = "false" ] && [ "${summary}" = "running" ]; then
      warn "heal progress is null while the task is running (server-side reporting gap; see rustfs/backlog#2035)"
    fi

    if [ "${failed_n}" -gt 0 ] || printf '%s' "${summary}" | grep -qiE 'fail|error|stopped'; then
      printf '\033[1;31m[ERROR]\033[0m heal reported failed objects (%s)\n' "${failed_n}" >&2
      printf '%s\n' "--- full heal status JSON ---" >&2
      printf '%s\n' "${body}" >&2
      die "heal failed (summary=${summary}, objects_failed=${failed_n})"
    fi

    # Success only for a real terminal summary; "running"/"notFound"/"" mean
    # the task is still going (or lives on another node) — keep polling.
    if printf '%s' "${summary}" | grep -qiE '^(finished|completed|success|done)$'; then
      log "heal done: summary=${summary} failed=0 (server verdict)"
      if [ "${vm002_used}" -le "${pre_outage_used}" ]; then
        warn "heal finished but ${NODES[${OUTAGE_NODE_INDEX}]} usage did not grow (${pre_outage_used}GB -> ${vm002_used}GB); the repair may not have landed on its disks"
      fi
      final_status_file="$(mktemp "${TMPDIR:-/tmp}/rustfs-heal-final-status.XXXXXX.json" 2>/dev/null \
        || printf '%s' "${TMPDIR:-/tmp}/rustfs-heal-final-status.$$.json")"
      printf '%s\n' "${body}" > "${final_status_file}" 2>/dev/null \
        && log "final heal status saved: ${final_status_file}" \
        || warn "could not save final heal status to ${final_status_file}"
      return 0
    fi

    sleep "${POLL_INTERVAL}"
    waited=$((waited + POLL_INTERVAL))
  done
  printf '\033[1;31m[ERROR]\033[0m timed out waiting for heal (${HEAL_TIMEOUT}s); final status:\n' >&2
  printf '%s\n' "${body}" >&2
  die "timed out waiting for heal (${HEAL_TIMEOUT}s)"
}

step7_analyze_results() {
  log "step 7: result analysis"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: analyzing heal results"
    return 0
  fi
  if [ -z "${HEAL_CLIENT_TOKEN}" ]; then
    die "no heal client token (run step 5 first, or pass --heal-token)"
  fi
  local body summary failed healed scanned pct prog_present
  local failed_n healed_n scanned_n pct_n i used
  body="$(admin_api POST /rustfs/admin/v3/heal/ "clientToken=${HEAL_CLIENT_TOKEN}" "")"
  if [ "$(admin_api_code)" = "200" ]; then
    summary="$(printf '%s' "${body}" | jq -r '.summary // "unknown"')"
    prog_present="$(printf '%s' "${body}" | jq -r '.progress != null')"
    failed="$(heal_progress_field "${body}" objectsFailed objects_failed)"
    healed="$(heal_progress_field "${body}" objectsHealed objects_healed)"
    scanned="$(heal_progress_field "${body}" objectsScanned objects_scanned)"
    pct="$(heal_progress_field "${body}" progressPercentage progress_percentage)"
    failed_n="${failed}"; [ "${failed_n}" = "null" ] && failed_n=0
    healed_n="${healed}"; [ "${healed_n}" = "null" ] && healed_n=0
    scanned_n="${scanned}"; [ "${scanned_n}" = "null" ] && scanned_n=0
    pct_n="${pct}"; [ "${pct_n}" = "null" ] && pct_n=0
  fi
  printf '%s\n' "--- heal status ---"
  printf '  summary=%s scanned=%s healed=%s failed=%s progress=%s%%\n' \
    "${summary}" "${scanned_n}" "${healed_n}" "${failed_n}" "${pct_n}"
  printf '%s\n' "--- per-node disk usage (GiB) ---"
  for i in "${!NODES[@]}"; do
    used="$(node_used_gb "${NODES[$i]}")"
    printf '  %s: %sGB\n' "${NODES[$i]}" "${used}"
  done

  local outage_used
  outage_used="$(node_used_gb "${NODES[${OUTAGE_NODE_INDEX}]}")"

  if ! verify_data_readable; then
    die "heal test FAILED: data read-back verification failed (see errors above)"
  fi

  if printf '%s' "${summary}" | grep -qiE '^(finished|completed|success|done)$' \
    && [ "${failed_n}" -eq 0 ]; then
    log "heal test PASSED: cluster heal complete, 0 failed, data read-back OK, ${NODES[${OUTAGE_NODE_INDEX}]}_used=${outage_used}GB"
    return 0
  fi
  die "heal test FAILED: summary=${summary} failed=${failed_n} ${NODES[${OUTAGE_NODE_INDEX}]}_used=${outage_used}GB"
}
# ==================== CLI parsing ====================

usage() {
  cat <<'USAGE'
Usage: rustfs-heal-test.sh [options]

Options:
  --all                      Run the full workflow (steps 1-7)
  --step N                   Run a single step (repeatable)
  --steps 1,3,5-7            Run steps in order
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
  --stop-node-gb N           Stop the outage node when surviving nodes reach N GiB (default 15)
  --warp-stop-gb N           Stop warp when surviving nodes reach N GiB (default 40)
  --heal-token TOKEN         clientToken of a heal started earlier (for steps 6/7 reruns)
  --warp-timeout N           Write phase timeout in seconds (default 3600)
  --heal-timeout N           Heal wait timeout in seconds (default 86400)
  --service-timeout N        Service start wait timeout in seconds (default 300)
  --poll-interval N          Status polling interval in seconds (default 15)
  --log-file FILE            Append all output to FILE
  --dry-run                  Preview commands without executing them
  -y, --yes                  Skip all confirmation prompts
  -h, --help                 Show this help

Examples:
  ./rustfs-heal-test.sh --all
  ./rustfs-heal-test.sh --all -y --package-url https://dl.rustfs.com/artifacts/rustfs/packages/nightly/rustfs-nightly-latest.deb
  ./rustfs-heal-test.sh --steps 5,6,7
  ./rustfs-heal-test.sh --all --dry-run
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
      2) step2_install_and_start ;;
      3) step3_write_data_with_node_outage ;;
      4) step4_restart_node ;;
      5) step5_start_heal ;;
      6) step6_monitor_heal ;;
      7) step7_analyze_results ;;
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
      --stop-node-gb) STOP_NODE_AT_GB="$1"; shift ;;
      --warp-stop-gb) WARP_STOP_AT_GB="$1"; shift ;;
      --heal-token) HEAL_CLIENT_TOKEN="$1"; shift ;;
      --warp-timeout) WARP_TIMEOUT="$1"; shift ;;
      --heal-timeout) HEAL_TIMEOUT="$1"; shift ;;
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
      LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/rustfs-heal-test.XXXXXX.log")"
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
    SELECTED_STEPS=(1 2 3 4 5 6 7)
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
if [ "${RUSTFS_HEAL_SCRIPT_SOURCE_ONLY:-0}" != "1" ]; then
  main "$@"
fi
