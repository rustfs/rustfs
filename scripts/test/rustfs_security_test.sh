#!/usr/bin/env bash
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# rustfs-security-test.sh
# RustFS unified IAM / STS / OIDC security test suite (backlog #2024).
#
# Integrates the backlog security case matrix (IAM-101..114, SA-101..104,
# STS-101..105, OIDC-101..108) with the request flows proven by
# admin_iam_crud_test / sts_query_compat_test / oidc_keycloak_live.sh.
#
# Every case runs in three topologies, matching the other functional suites:
#   1. single-single  (vm000, /data/rustfs1/mnmd)
#   2. single-multi   (vm000, /data/rustfs{1...4}/mnmd)
#   3. multi-multi    (vm000+vm001+vm002, 3x4)
#
# Per topology the flow is identical to the S3/KMS/Tier suites: clean the
# shared VMs, install the package, start the cluster, run the cases, tear
# the environment down, and aggregate one report for the dashboard.
#
# Usage:
#   ./rustfs-security-test.sh --all-topologies -y --package-url <deb url>
#   ./rustfs-security-test.sh --topology single-single -y --version 1.0.0-rc.4-preview.1
#   ./rustfs-security-test.sh --all-topologies -y --oidc-live --package-url <deb url>
#
# Environment:
#   RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY   admin credentials
#   RUSTFS_NODES / RUSTFS_SSH_USER          shared test VMs (default vm000..002 / azureuser)
#   RUSTFS_API_ENDPOINT                     override the topology endpoint
#   REPORT_FILE                             markdown report path (default /tmp/rustfs-security-report.md)
set -Eeuo pipefail

TEST_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-security.XXXXXX")"
KEEP_TMP=0
cleanup_tmp() {
  if [ "${KEEP_TMP}" -eq 1 ]; then
    echo "security suite logs retained in ${TEST_TMP}" >&2
  else
    rm -rf "${TEST_TMP}"
  fi
}
trap cleanup_tmp EXIT
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ==================== Configuration ====================
read -r -a NODES <<< "${RUSTFS_NODES:-vm000 vm001 vm002}"
SSH_USER="${RUSTFS_SSH_USER:-azureuser}"
SSH_PORT="${RUSTFS_SSH_PORT:-22}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -p "${SSH_PORT}")

RUSTFS_VERSION="${RUSTFS_VERSION:-1.0.0-rc.4-preview.1}"
PACKAGE_URL="${PACKAGE_URL:-}"
ARCH="${RUSTFS_ARCH:-amd64}"
PACKAGES_DIR="/home/rustfs/packages"
PACKAGE_FILE="rustfs.deb"
PACKAGE_SHA256="${PACKAGE_SHA256:-}"

ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
REGION="us-east-1"

RUSTFS_CONFIG_FILE="/etc/default/rustfs"
RUSTFS_SERVICE="rustfs"
RUSTFS_PACKAGE_NAME="rustfs"
RUSTFS_USER="rustfs"
RUSTFS_ADDRESS=":9000"
RUSTFS_CONSOLE_ADDRESS=":9001"
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_OBS_LOGGER_LEVEL=error
RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/"

TOPO_SINGLE_SINGLE="single-single"
TOPO_SINGLE_MULTI="single-multi"
TOPO_MULTI_MULTI="multi-multi"

# Endpoints used by the S3 clients for each topology (same as the S3 suite).
ENDPOINT_SINGLE_SINGLE="http://rustfs-node1:9000"
ENDPOINT_SINGLE_MULTI="http://rustfs-node1:9000"
ENDPOINT_MULTI_MULTI="http://127.0.0.1:9000"

# Volumes per topology (same as the S3 suite).
VOLUMES_SINGLE_SINGLE="/data/rustfs1/mnmd"
VOLUMES_SINGLE_MULTI="/data/rustfs{1...4}/mnmd"
VOLUMES_MULTI_MULTI="http://rustfs-node{1...3}:9000/data/rustfs{1...4}/mnmd"

SERVICE_TIMEOUT=300

API_ENDPOINT="${ENDPOINT_SINGLE_SINGLE}"
SIGV4_REGION="${REGION}"
SIGV4_SERVICE="s3"
API_REQUEST_TIMEOUT=120
ADMIN_API_CODE_FILE="${TEST_TMP}/api-code"

REPORT_FILE="${REPORT_FILE:-/tmp/rustfs-security-report.md}"
REPORT_SECTIONS_FILE="${TEST_TMP}/report-sections.md"
RUSTFS_VERSION_INFO=""

# ==================== Runtime options ====================
DRY_RUN=0
ASSUME_YES=0
KEEP_ENV=0
OIDC_LIVE=0
OIDC_RUN=0
SELECTED_TOPOLOGIES=()
ENDPOINT=""
VOLUMES=""

# ==================== Helpers ====================

log()  { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

confirm() {
  [ "${ASSUME_YES}" -eq 1 ] && return 0
  printf '\033[1;33m[CONFIRM]\033[0m %s (y/N) ' "$1"
  read -r answer
  case "${answer}" in y|Y|yes|YES) return 0 ;; *) die "cancelled" ;; esac
}

need_cmd() {
  [ "${DRY_RUN}" -eq 1 ] && return 0
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1 ($2); install it first"
}

build_package_url() {
  local tag="${RUSTFS_VERSION#v}" asset
  asset="${tag//-/.}"
  printf 'https://github.com/rustfs/rustfs/releases/download/%s/rustfs_%s_%s.deb' "${tag}" "${asset}" "${ARCH}"
}

resolve_package_url() {
  if [ -n "${PACKAGE_URL}" ]; then printf '%s' "${PACKAGE_URL}";
  else build_package_url; fi
}

run_remote() {
  local node="$1" script
  script="$(cat)"
  if [ "${DRY_RUN}" -eq 1 ]; then
    log "DRY-RUN: ssh ${SSH_USER}@${node} <<'REMOTE'"
    printf '%s\n' "${script}" | sed 's/^/    | /'
    return 0
  fi
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'bash -s' <<<"${script}"
}

service_action() {
  local action="$1" node="$2"
  log "${node}: systemctl ${action} ${RUSTFS_SERVICE}"
  [ "${DRY_RUN}" -eq 1 ] && return 0
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" \
    "if [ \"\$(id -u)\" -ne 0 ]; then sudo -n systemctl ${action} ${RUSTFS_SERVICE}; else systemctl ${action} ${RUSTFS_SERVICE}; fi" \
    || die "${node}: systemctl ${action} failed"
}

wait_service_active() {
  local node="$1" waited=0
  [ "${DRY_RUN}" -eq 1 ] && return 0
  while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
    if ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" "systemctl is-active --quiet ${RUSTFS_SERVICE}" 2>/dev/null; then
      log "${node}: ${RUSTFS_SERVICE} is active"; return 0
    fi
    sleep 5; waited=$((waited+5))
  done
  die "${node}: timed out waiting for ${RUSTFS_SERVICE} (${SERVICE_TIMEOUT}s)"
}

start_and_wait_nodes() {
  local nodes=("$@") pids=() i=0 fail=0
  for node in "${nodes[@]}"; do service_action start "${node}" & pids[$i]=$!; i=$((i+1)); done
  if [ "${#pids[@]}" -gt 0 ]; then
    for pid in "${pids[@]}"; do wait "${pid}" || fail=1; done
  fi
  [ "${fail}" -eq 0 ] || die "systemctl start failed on one or more nodes"
  for node in "${nodes[@]}"; do wait_service_active "${node}"; done
}

cleanup_node() {
  local node="$1"
  log "cleanup: purge ${RUSTFS_PACKAGE_NAME} and remove data dirs on ${node}"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf '${SUDO} systemctl stop %s 2>/dev/null || true\n' "${RUSTFS_SERVICE}"
    printf 'if ${SUDO} dpkg -l %s 2>/dev/null | grep -q "^ii"; then ${SUDO} dpkg -P %s; fi\n' "${RUSTFS_PACKAGE_NAME}" "${RUSTFS_PACKAGE_NAME}"
    printf 'for i in 1 2 3 4; do ${SUDO} rm -rf /data/rustfs${i}/mnmd; done\n'
    printf '${SUDO} rm -rf /var/log/rustfs /var/lib/rustfs/kms\n'
  } | run_remote "${node}"
}

volume_dirs() {
  local volumes="$1" expr path prefix suffix i start end
  for expr in ${volumes}; do
    path="${expr#*://}"; path="${path#*/}"; path="/${path}"
    if [[ "${path}" =~ \{([0-9]+)\.\.\.([0-9]+)\} ]]; then
      start="${BASH_REMATCH[1]}"; end="${BASH_REMATCH[2]}"
      prefix="${path%%\{*}"; suffix="${path#*\}}"
      for ((i=start; i<=end; i++)); do printf '%s%s%s\n' "${prefix}" "${i}" "${suffix}"; done
    else
      printf '%s\n' "${path}"
    fi
  done
}

ensure_volume_dirs() {
  local node="$1" volumes="$2" dirs=() d
  while IFS= read -r d; do dirs+=("${d}"); done < <(volume_dirs "${volumes}" | sort -u)
  log "${node}: ensuring data dirs (${dirs[*]})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'id -u %s >/dev/null 2>&1 || ${SUDO} useradd -r -s /bin/false -d /opt/%s %s\n' "${RUSTFS_USER}" "${RUSTFS_USER}" "${RUSTFS_USER}"
    for d in "${dirs[@]}"; do
      printf '${SUDO} mkdir -p %s\n' "${d}"
      printf '${SUDO} chown -R %s:%s %s\n' "${RUSTFS_USER}" "${RUSTFS_USER}" "${d}"
    done
    printf '${SUDO} mkdir -p /var/lib/rustfs/kms\n'
    printf '${SUDO} chown -R %s:%s /var/lib/rustfs/kms\n' "${RUSTFS_USER}" "${RUSTFS_USER}"
  } | run_remote "${node}"
}

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

write_rustfs_config() {
  local node="$1" volumes="$2" body
  body="$(rustfs_config_body "${volumes}")"
  log "${node}: writing config ${RUSTFS_CONFIG_FILE} (volumes=${volumes})"
  {
    printf 'set -euo pipefail\n'
    printf 'SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo -n"\n'
    printf 'if [ -f %s ]; then ${SUDO} cp -a %s %s.bak.$(date +%%Y%%m%%d%%H%%M%%S); fi\n' "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}" "${RUSTFS_CONFIG_FILE}"
    printf '%s tee %s >/dev/null <<RUSTFS_EOF\n' '${SUDO}' "${RUSTFS_CONFIG_FILE}"
    printf '%s' "${body}"
    printf '\nRUSTFS_EOF\n'
    printf '${SUDO} systemctl daemon-reload\n'
  } | run_remote "${node}"
}

resolve_topology() {
  case "$1" in
    "${TOPO_SINGLE_SINGLE}") ENDPOINT="${ENDPOINT_SINGLE_SINGLE}"; VOLUMES="${VOLUMES_SINGLE_SINGLE}"; return 0 ;;
    "${TOPO_SINGLE_MULTI}")  ENDPOINT="${ENDPOINT_SINGLE_MULTI}";  VOLUMES="${VOLUMES_SINGLE_MULTI}";  return 0 ;;
    "${TOPO_MULTI_MULTI}")   ENDPOINT="${ENDPOINT_MULTI_MULTI}";   VOLUMES="${VOLUMES_MULTI_MULTI}";   return 0 ;;
    *) die "unknown topology: $1 (single-single|single-multi|multi-multi)" ;;
  esac
}

topology_nodes() {
  case "$1" in
    "${TOPO_SINGLE_SINGLE}"|"${TOPO_SINGLE_MULTI}") printf '%s' "${NODES[0]}" ;;
    *) printf '%s' "${NODES[*]}" ;;
  esac
}

env_setup() {
  local topo="$1" url script
  resolve_topology "${topo}"
  if [ -n "${RUSTFS_API_ENDPOINT:-}" ]; then
    ENDPOINT="${RUSTFS_API_ENDPOINT}"
  fi
  url="$(resolve_package_url)"
  read -r -a nodes <<<"$(topology_nodes "${topo}")"
  log "environment setup: topology=${topo} nodes=${nodes[*]} endpoint=${ENDPOINT}"
  API_ENDPOINT="${ENDPOINT}"

  # Clean slate on all three nodes (even if only some are used by this topology).
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done

  # Download + install on the nodes used by this topology.
  for node in "${nodes[@]}"; do
    script="$(cat <<EOF
set -euo pipefail
SUDO=""; [ "\$(id -u)" -ne 0 ] && SUDO="sudo -n"
\${SUDO} mkdir -p "${PACKAGES_DIR}"
PKG_TMP="\$(mktemp /tmp/rustfs-security-pkg.XXXXXX.deb)"
curl -fSL --retry 3 -o "\${PKG_TMP}" "${url}"
\${SUDO} install -m 0644 "\${PKG_TMP}" "${PACKAGES_DIR}/${PACKAGE_FILE}"
rm -f "\${PKG_TMP}"
if [ -n "${PACKAGE_SHA256}" ]; then
  echo "${PACKAGE_SHA256}  ${PACKAGES_DIR}/${PACKAGE_FILE}" | sha256sum -c - || exit 1
fi
\${SUDO} dpkg -i "${PACKAGES_DIR}/${PACKAGE_FILE}"
\${SUDO} systemctl daemon-reload
EOF
)"
    printf '%s\n' "${script}" | run_remote "${node}"
  done

  for node in "${nodes[@]}"; do
    write_rustfs_config "${node}" "${VOLUMES}"
    ensure_volume_dirs "${node}" "${VOLUMES}"
  done

  start_and_wait_nodes "${nodes[@]}"

  if [ "${DRY_RUN}" -eq 0 ]; then
    local waited=0
    while [ "${waited}" -lt "${SERVICE_TIMEOUT}" ]; do
      if curl -fsS --max-time 5 "${ENDPOINT}/minio/health/live" >/dev/null 2>&1; then
        log "endpoint ready: ${ENDPOINT}"; break
      fi
      sleep 5; waited=$((waited+5))
    done
    [ "${waited}" -lt "${SERVICE_TIMEOUT}" ] || die "endpoint ${ENDPOINT} not ready"
    export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
    export AWS_DEFAULT_REGION="${REGION}"
    export AWS_EC2_METADATA_DISABLED=true
  fi
}

env_teardown() {
  local topo="$1"
  log "environment teardown: ${topo}"
  for node in "${NODES[@]}"; do cleanup_node "${node}"; done
}

# ==================== Admin API (SigV4, optional JSON body) ====================

sha256_hex() { printf '%s' "$1" | openssl dgst -sha256 -hex 2>/dev/null | awk '{print $NF}'; }
hmac_sha256_hex() { printf '%s' "$2" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$1" -hex 2>/dev/null | awk '{print $NF}'; }
hex_of_ascii() { printf '%s' "$1" | od -An -vtx1 | tr -d ' \n'; }

canonical_query() {
  local q="$1"
  [ -z "${q}" ] && return 0
  printf '%s\n' "${q}" | tr '&' '\n' | while IFS= read -r pair; do
    printf '%s=%s\n' "${pair%%=*}" "${pair#*=}"
  done | sort | paste -sd '&' -
}

admin_api() {
  local method="$1" path="$2" query="${3:-}" body="${4:-}"
  local amz_date date_stamp host_port
  local canonical_headers signed_headers canonical_request string_to_sign
  local scope k_date k_region k_service k_signing signature auth
  local url tmp code

  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '200' > "${ADMIN_API_CODE_FILE}"
    return 0
  fi

  local payload_hash="UNSIGNED-PAYLOAD"
  if [ -n "${body}" ]; then
    payload_hash="$(sha256_hex "${body}")"
  fi

  host_port="${API_ENDPOINT#*://}"
  host_port="${host_port%%/*}"
  amz_date="$(date -u +%Y%m%dT%H%M%SZ)"
  date_stamp="${amz_date:0:8}"
  query="$(canonical_query "${query}")"

  canonical_headers="host:${host_port}
x-amz-content-sha256:${payload_hash}
x-amz-date:${amz_date}
"
  signed_headers="host;x-amz-content-sha256;x-amz-date"
  canonical_request="${method}
${path}
${query}
${canonical_headers}
${signed_headers}
${payload_hash}"

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
  local curl_args=()
  [ -n "${body}" ] && curl_args+=(--data-binary "${body}")
  code="$(curl -sS --max-time "${API_REQUEST_TIMEOUT}" -o "${tmp}" -w '%{http_code}' \
    -H "Host: ${host_port}" \
    -H "x-amz-content-sha256: ${payload_hash}" \
    -H "x-amz-date: ${amz_date}" \
    -H "Authorization: ${auth}" \
    "${curl_args[@]}" \
    -X "${method}" "${url}")" || code="000"
  printf '%s' "${code}" > "${ADMIN_API_CODE_FILE}"
  cat "${tmp}"
  rm -f "${tmp}"
}

admin_api_code() { cat "${ADMIN_API_CODE_FILE}"; }

assert_admin_ok() {
  local expected="$1" context="$2"
  local code
  code="$(admin_api_code)"
  [ "${code}" = "${expected}" ] || {
    echo "admin ${context}: expected HTTP ${expected}, got ${code}" >&2
    return 1
  }
}

# ==================== AWS / S3 helpers ====================

aws_run() {
  AWS_OUT="$(aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

aws_run_as() {
  # usage: aws_run_as AK SK [TOKEN] -- args...
  local ak="$1" sk="$2" token=""
  shift 2
  if [ "${1:-}" != "--" ]; then token="$1"; shift; fi
  [ "${1:-}" = "--" ] && shift
  local env_args=(AWS_ACCESS_KEY_ID="${ak}" AWS_SECRET_ACCESS_KEY="${sk}" AWS_EC2_METADATA_DISABLED=true AWS_DEFAULT_REGION="${REGION}")
  if [ -n "${token}" ]; then env_args+=(AWS_SESSION_TOKEN="${token}"); fi
  AWS_OUT="$(env "${env_args[@]}" aws --endpoint-url "${ENDPOINT}" --region "${REGION}" "$@" 2>&1)"; AWS_CODE=$?
}

assert_aws_ok() {
  [ "${AWS_CODE}" -eq 0 ] || { echo "aws failed (exit ${AWS_CODE}): $*"; echo "${AWS_OUT}"; return 1; }
}

assert_aws_fails() {
  [ "${AWS_CODE}" -ne 0 ] || { echo "aws unexpectedly succeeded: $*"; echo "${AWS_OUT}"; return 1; }
}

new_bucket() {
  local b="$1"
  aws_run s3api create-bucket --bucket "${b}"
  assert_aws_ok || return 1
}

rm_bucket() {
  local b="$1"
  aws_run s3api delete-bucket --bucket "${b}" 2>/dev/null || true
}

# ==================== Test harness ====================

declare -a FAILED_TESTS=()
declare -a CASE_RESULTS=()
PASS=0
FAIL=0
SKIP=0

run_test() {
  local id="$1" name="$2" fn="$3"
  local out="${TEST_TMP}/${id}.out"
  if [ "${DRY_RUN}" -eq 1 ]; then
    printf '  [DRY-RUN] %s %s\n' "${id}" "${name}"
    CASE_RESULTS+=("${id}|${name}|PASS")
    return 0
  fi
  printf '\n--- %s %s ---\n' "${id}" "${name}"
  if "${fn}" > "${out}" 2>&1; then
    PASS=$((PASS+1)); printf '\033[1;32m[PASS]\033[0m %s %s\n' "${id}" "${name}"
    CASE_RESULTS+=("${id}|${name}|PASS")
  else
    FAIL=$((FAIL+1)); FAILED_TESTS+=("${id} ${name}")
    CASE_RESULTS+=("${id}|${name}|FAIL")
    printf '\033[1;31m[FAIL]\033[0m %s %s\n' "${id}" "${name}"
    sed 's/^/      | /' "${out}" | tail -50
  fi
}

skip_test() {
  local id="$1" name="$2"
  SKIP=$((SKIP+1))
  CASE_RESULTS+=("${id}|${name}|SKIP")
  printf '\033[1;33m[SKIP]\033[0m %s %s\n' "${id}" "${name}"
}

# ==================== IAM: users / groups / policies ====================

admin_user_add() { admin_api PUT "/rustfs/admin/v3/add-user?accessKey=${1}" "" "{\"secretKey\":\"${2}\",\"status\":\"enabled\"}"; }
admin_user_remove() { admin_api DELETE "/rustfs/admin/v3/remove-user?accessKey=${1}"; }
admin_policy_add() { admin_api PUT "/rustfs/admin/v3/add-canned-policy?name=${1}" "" "${2}"; }
admin_policy_attach() { admin_api POST "/rustfs/admin/v3/idp/builtin/policy/attach" "" "{\"policies\":[\"${2}\"],\"user\":\"${1}\"}"; }
admin_policy_detach() { admin_api POST "/rustfs/admin/v3/idp/builtin/policy/detach" "" "{\"policies\":[\"${2}\"],\"user\":\"${1}\"}"; }

case_iam_101_user_crud() {
  local user="sec101u" secret="sec101secret" out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_api GET /rustfs/admin/v3/list-users)"; assert_admin_ok 200 "list-users"
  printf '%s' "${out}" | grep -q "${user}" || { echo "list-users missing ${user}"; return 1; }
  out="$(admin_api GET "/rustfs/admin/v3/user-info?accessKey=${user}")"; assert_admin_ok 200 "user-info"
  printf '%s' "${out}" | grep -q "${user}" || { echo "user-info missing ${user}"; return 1; }
  printf '%s' "${out}" | grep -q "${secret}" && { echo "user-info leaked secret"; return 1; }
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_ok "user credentials can list buckets"
  out="$(admin_user_remove "${user}")"; assert_admin_ok 200 "remove-user"
  out="$(admin_api GET "/rustfs/admin/v3/user-info?accessKey=${user}")"
  [ "$(admin_api_code)" != "200" ] || { echo "user-info succeeded after delete"; return 1; }
}

case_iam_102_user_disable() {
  local user="sec102u" secret="sec102secret" out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_ok "enabled user works"
  out="$(admin_api PUT "/rustfs/admin/v3/set-user-status?accessKey=${user}&status=disabled")"; assert_admin_ok 200 "disable"
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_fails "disabled user denied"
  out="$(admin_api PUT "/rustfs/admin/v3/set-user-status?accessKey=${user}&status=enabled")"; assert_admin_ok 200 "enable"
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_ok "re-enabled user works"
  admin_user_remove "${user}" >/dev/null
}

case_iam_103_delete_invalidates() {
  local user="sec103u" secret="sec103secret" out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_ok "user works before delete"
  out="$(admin_user_remove "${user}")"; assert_admin_ok 200 "remove-user"
  aws_run_as "${user}" "${secret}" -- s3 ls; assert_aws_fails "deleted user denied"
}

case_iam_105_readonly_boundary() {
  local user="sec105u" secret="sec105secret" out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_policy_attach "${user}" readonly)"; assert_admin_ok 200 "attach readonly"
  new_bucket sec105-bucket || return 1
  aws_run_as "${user}" "${secret}" -- s3 ls s3://sec105-bucket; assert_aws_ok "readonly can list bucket"
  aws_run_as "${user}" "${secret}" -- s3api put-object --bucket sec105-bucket --key hello.txt --body /etc/hostname
  assert_aws_fails "readonly cannot write"
  rm_bucket sec105-bucket
  admin_user_remove "${user}" >/dev/null
}

case_iam_106_custom_policy_crud() {
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::sec106-bucket/*"]}]}'
  local out
  out="$(admin_policy_add sec106p "${policy}")"; assert_admin_ok 200 "add-canned-policy"
  out="$(admin_api GET /rustfs/admin/v3/list-canned-policies)"; assert_admin_ok 200 "list-canned-policies"
  printf '%s' "${out}" | grep -q sec106p || { echo "list missing policy"; return 1; }
  out="$(admin_api GET "/rustfs/admin/v3/info-canned-policy?name=sec106p")"; assert_admin_ok 200 "info-canned-policy"
  printf '%s' "${out}" | grep -q "s3:GetObject" || { echo "policy body did not round-trip"; return 1; }
  out="$(admin_api DELETE "/rustfs/admin/v3/remove-canned-policy?name=sec106p")"; assert_admin_ok 200 "remove-canned-policy"
  out="$(admin_api GET /rustfs/admin/v3/list-canned-policies)"; assert_admin_ok 200 "list after remove"
  printf '%s' "${out}" | grep -q sec106p && { echo "policy still listed"; return 1; }
}

case_iam_107_attach_detach() {
  local user="sec107u" secret="sec107secret"
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec107-bucket"]}]}'
  local out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_policy_add sec107p "${policy}")"; assert_admin_ok 200 "add policy"
  new_bucket sec107-bucket || return 1
  out="$(admin_policy_attach "${user}" sec107p)"; assert_admin_ok 200 "attach"
  aws_run_as "${user}" "${secret}" -- s3 ls s3://sec107-bucket; assert_aws_ok "policy grants list"
  out="$(admin_policy_detach "${user}" sec107p)"; assert_admin_ok 200 "detach"
  aws_run_as "${user}" "${secret}" -- s3 ls s3://sec107-bucket; assert_aws_fails "detached user denied"
  out="$(admin_policy_detach "${user}" sec107p)"; assert_admin_ok 200 "detach idempotent"
  rm_bucket sec107-bucket
  admin_user_remove "${user}" >/dev/null
}

case_iam_109_deny_precedence() {
  local user="sec109u" secret="sec109secret"
  local allow='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec109-bucket"]}]}'
  local deny='{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec109-bucket"]}]}'
  local out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_policy_add sec109-allow "${allow}")"; assert_admin_ok 200 "add allow"
  out="$(admin_policy_add sec109-deny "${deny}")"; assert_admin_ok 200 "add deny"
  new_bucket sec109-bucket || return 1
  out="$(admin_api POST /rustfs/admin/v3/idp/builtin/policy/attach "" "{\"policies\":[\"sec109-allow\",\"sec109-deny\"],\"user\":\"${user}\"}")"
  assert_admin_ok 200 "attach allow+deny"
  aws_run_as "${user}" "${secret}" -- s3 ls s3://sec109-bucket; assert_aws_fails "deny overrides allow"
  rm_bucket sec109-bucket
  admin_user_remove "${user}" >/dev/null
}

case_iam_113_access_key_list_hides_secret() {
  local user="sec113u" secret="sec113secret-xyz" out
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_api GET "/rustfs/admin/v3/list-access-keys-bulk?all=true")"; assert_admin_ok 200 "list-access-keys-bulk"
  printf '%s' "${out}" | grep -q '"secretKey"' && { echo "bulk listing exposed secretKey"; return 1; }
  printf '%s' "${out}" | grep -q "${secret}" && { echo "bulk listing exposed secret"; return 1; }
  admin_user_remove "${user}" >/dev/null
}

# ==================== Service accounts ====================

create_service_account() {
  # usage: create_service_account TARGET [POLICY_JSON]; prints AK<TAB>SK
  local target="$1" policy="${2:-}"
  local body="{\"targetUser\":\"${target}\"}"
  [ -n "${policy}" ] && body="{\"targetUser\":\"${target}\",\"policy\":${policy}}"
  local out
  out="$(admin_api PUT /rustfs/admin/v3/add-service-accounts "" "${body}")"
  assert_admin_ok 200 "add-service-accounts"
  printf '%s' "${out}" | jq -r '.credentials.accessKey + "\t" + .credentials.secretKey'
}

case_sa_101_create_and_use() {
  local sa
  sa="$(create_service_account "${ACCESS_KEY}")"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3 ls; assert_aws_ok "service account can list buckets"
  admin_api DELETE "/rustfs/admin/v3/delete-service-account?accessKey=${sa_ak}" >/dev/null
}

case_sa_103_parent_boundary() {
  local user="sec103p" secret="sec103secret" out
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::sec103p-bucket/*"]}]}'
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_policy_attach "${user}" readonly)"; assert_admin_ok 200 "attach readonly"
  new_bucket sec103p-bucket || return 1
  local sa
  sa="$(create_service_account "${user}" "${policy}")"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3api put-object --bucket sec103p-bucket --key hello.txt --body /etc/hostname
  assert_aws_fails "service account cannot exceed readonly parent"
  admin_api DELETE "/rustfs/admin/v3/delete-service-account?accessKey=${sa_ak}" >/dev/null
  rm_bucket sec103p-bucket
  admin_user_remove "${user}" >/dev/null
}

case_sa_104_delete_invalidates() {
  local sa
  sa="$(create_service_account "${ACCESS_KEY}")"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3 ls; assert_aws_ok "service account works"
  local out
  out="$(admin_api DELETE "/rustfs/admin/v3/delete-service-account?accessKey=${sa_ak}")"; assert_admin_ok 200 "delete-service-account"
  aws_run_as "${sa_ak}" "${sa_sk}" -- s3 ls; assert_aws_fails "deleted service account denied"
}

# ==================== STS ====================

assume_role() {
  # usage: assume_role [AK] [SK] [DURATION]; prints AWSAK<TAB>AWSK<TAB>TOKEN<TAB>EXPIRATION
  local ak="${1:-${ACCESS_KEY}}" sk="${2:-${SECRET_KEY}}" duration="${3:-900}"
  aws_run_as "${ak}" "${sk}" -- sts assume-role \
    --role-arn arn:aws:iam::123456789012:role/test \
    --role-session-name secsts \
    --duration-seconds "${duration}"
  [ "${AWS_CODE}" -eq 0 ] || return 1
  printf '%s' "${AWS_OUT}" | jq -r '.Credentials.AccessKeyId + "\t" + .Credentials.SecretAccessKey + "\t" + .Credentials.SessionToken + "\t" + .Credentials.Expiration'
}

case_sts_101_assume_role() {
  local creds
  creds="$(assume_role)" || { echo "AssumeRole failed"; return 1; }
  local sts_ak sts_sk sts_token sts_exp
  IFS=$'\t' read -r sts_ak sts_sk sts_token sts_exp <<<"${creds}"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_token}" -- s3 ls; assert_aws_ok "STS credentials can list buckets"
}

case_sts_102_duration_clamp() {
  local creds
  creds="$(assume_role "${ACCESS_KEY}" "${SECRET_KEY}" 604800)" || { echo "AssumeRole with 7d failed"; return 1; }
  local sts_ak sts_sk sts_token sts_exp
  IFS=$'\t' read -r sts_ak sts_sk sts_token sts_exp <<<"${creds}"
  local expiry_epoch now_epoch max_epoch
  expiry_epoch="$(date -u -d "${sts_exp}" +%s)"
  now_epoch="$(date -u +%s)"
  max_epoch=$(( now_epoch + 12 * 3600 + 120 ))
  [ "${expiry_epoch}" -le "${max_epoch}" ] || {
    echo "expected DurationSeconds clamped to 12h, got expiry ${sts_exp}" >&2
    return 1
  }
}

case_sts_104_temp_boundary() {
  local user="sec104u" secret="sec104secret" out
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sts:AssumeRole"],"Resource":["arn:aws:s3:::*"]},{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec104-a-bucket"]}]}'
  out="$(admin_user_add "${user}" "${secret}")"; assert_admin_ok 200 "add-user"
  out="$(admin_policy_add sec104p "${policy}")"; assert_admin_ok 200 "add policy"
  out="$(admin_policy_attach "${user}" sec104p)"; assert_admin_ok 200 "attach"
  new_bucket sec104-a-bucket || return 1
  new_bucket sec104-b-bucket || return 1
  local creds
  creds="$(assume_role "${user}" "${secret}")" || { echo "user AssumeRole failed"; return 1; }
  local sts_ak sts_sk sts_token sts_exp
  IFS=$'\t' read -r sts_ak sts_sk sts_token sts_exp <<<"${creds}"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_token}" -- s3 ls s3://sec104-a-bucket; assert_aws_ok "STS creds can list allowed bucket"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_token}" -- s3api put-object --bucket sec104-b-bucket --key hello.txt --body /etc/hostname
  assert_aws_fails "STS creds cannot exceed identity scope"
  rm_bucket sec104-a-bucket
  rm_bucket sec104-b-bucket
  admin_user_remove "${user}" >/dev/null
}

case_sts_105_revoke() {
  local creds
  creds="$(assume_role)" || { echo "AssumeRole failed"; return 1; }
  local sts_ak sts_sk sts_token sts_exp out
  IFS=$'\t' read -r sts_ak sts_sk sts_token sts_exp <<<"${creds}"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_token}" -- s3 ls; assert_aws_ok "STS creds work before revoke"
  out="$(admin_api POST "/rustfs/admin/v3/revoke-tokens/builtin?user=${ACCESS_KEY}&fullRevoke=true")"; assert_admin_ok 200 "revoke-tokens"
  aws_run_as "${sts_ak}" "${sts_sk}" "${sts_token}" -- s3 ls; assert_aws_fails "STS creds invalid after revoke"
}

# ==================== OIDC / SSO ====================

case_oidc_102_validate_rejects_bad_config() {
  local body='{"provider_id":"bad","enabled":true,"display_name":"bad","config_url":"http://127.0.0.1:1/nope","client_id":"nope","scopes":["openid"]}'
  local out
  out="$(admin_api POST /rustfs/admin/v3/oidc/validate "" "${body}")"
  [ "$(admin_api_code)" != "200" ] || { echo "validate accepted unreachable config_url"; return 1; }
}

case_oidc_108_reject_garbage_jwt() {
  local expired_jwt
  expired_jwt="$(python3 - <<'PY'
import base64
import json
import time

def b64(data: dict) -> str:
    raw = json.dumps(data, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

header = b64({"alg": "none", "typ": "JWT"})
claims = b64({"iss": "https://idp.invalid", "aud": "rustfs", "exp": int(time.time()) - 3600})
print(f"{header}.{claims}.")
PY
)"
  aws_run sts assume-role-with-web-identity \
    --role-arn arn:aws:iam::123456789012:role/test \
    --role-session-name sec108 \
    --duration-seconds 900 \
    --web-identity-token "${expired_jwt}"
  assert_aws_fails "expired/garbage web identity token"
  printf '%s' "${AWS_OUT}" | grep -qiE 'AccessDenied' || { echo "expected AccessDenied, got: ${AWS_OUT}"; return 1; }
}

case_oidc_103_keycloak_live() {
  local script="${ROOT_DIR}/scripts/test/oidc_keycloak_live.sh"
  [ -f "${script}" ] || { echo "oidc_keycloak_live.sh missing" >&2; return 1; }
  local binary
  binary="$(command -v rustfs || true)"
  [ -n "${binary}" ] || { echo "rustfs binary not found on admin host" >&2; return 1; }
  bash "${script}" "${binary}"
}

# ==================== Report ====================

detect_rustfs_version() {
  local node="${NODES[0]}" out
  [ "${DRY_RUN}" -eq 1 ] && { printf 'N/A'; return 0; }
  out="$(ssh "${SSH_OPTS[@]}" "${SSH_USER}@${node}" 'rustfs --version' 2>/dev/null | tr -d '\r' | head -n 1 || true)"
  printf '%s' "${out:-N/A}"
}

write_report() {
  local package_source="${PACKAGE_URL:-$(resolve_package_url)}"
  local outcome
  outcome="success"
  [ "${FAIL}" -eq 0 ] || outcome="failure"
  {
    echo "# RustFS security test report"
    echo ""
    echo "- Run: ${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-rustfs/rustfs}/actions/runs/${GITHUB_RUN_ID:-local}"
    echo "- Trigger: ${GITHUB_EVENT_NAME:-local}"
    echo "- Package: ${package_source}"
    echo "- RustFS Version: ${RUSTFS_VERSION_INFO:-N/A}"
    echo "- Test Step Outcome: ${outcome}"
    echo "- Result: ${PASS} passed / ${FAIL} failed / ${SKIP} skipped"
    echo ""
  } > "${REPORT_FILE}"

  cat "${REPORT_SECTIONS_FILE}" >> "${REPORT_FILE}"

  if [ "${#FAILED_TESTS[@]}" -gt 0 ]; then
    echo "## Failure details" >> "${REPORT_FILE}"
    echo "" >> "${REPORT_FILE}"
    for failure in "${FAILED_TESTS[@]}"; do
      id="${failure%% *}"
      {
        echo "### ${failure}"
        echo ""
        echo '```text'
        tail -n 40 "${TEST_TMP}/${id}.out" 2>/dev/null || true
        echo '```'
      } >> "${REPORT_FILE}"
    done
  else
    echo "All security cases passed." >> "${REPORT_FILE}"
  fi

  cat "${REPORT_FILE}" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
}

record_topology_results() {
  local topo="$1" id desc result
  {
    echo "## Topology: ${topo}"
    echo ""
    echo "| ID | Case | Result |"
    echo "|---|---|---|"
  } >> "${REPORT_SECTIONS_FILE}"
  for entry in "${CASE_RESULTS[@]}"; do
    IFS='|' read -r id desc result <<<"${entry}"
    echo "| ${id} | ${desc} | ${result} |" >> "${REPORT_SECTIONS_FILE}"
  done
  echo "" >> "${REPORT_SECTIONS_FILE}"
}

# ==================== Runner ====================

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --all-topologies) SELECTED_TOPOLOGIES=("${TOPO_SINGLE_SINGLE}" "${TOPO_SINGLE_MULTI}" "${TOPO_MULTI_MULTI}") ;;
      --topology) [ "$#" -ge 2 ] || die "--topology requires a value"; SELECTED_TOPOLOGIES+=("$2"); shift ;;
      --package-url) [ "$#" -ge 2 ] || die "--package-url requires a value"; PACKAGE_URL="$2"; shift ;;
      --version) [ "$#" -ge 2 ] || die "--version requires a value"; RUSTFS_VERSION="$2"; shift ;;
      --oidc-live) OIDC_LIVE=1 ;;
      --keep-env) KEEP_ENV=1 ;;
      -y) ASSUME_YES=1 ;;
      --dry-run) DRY_RUN=1 ;;
      *) die "unknown option: $1" ;;
    esac
    shift
  done
  [ "${#SELECTED_TOPOLOGIES[@]}" -gt 0 ] || die "select a topology (--topology <name>) or --all-topologies"
}

run_security_cases() {
  run_test IAM-101 "user CRUD lifecycle" case_iam_101_user_crud
  run_test IAM-102 "enable/disable user revokes access" case_iam_102_user_disable
  run_test IAM-103 "user deletion invalidates credentials" case_iam_103_delete_invalidates
  run_test IAM-105 "builtin readonly policy boundary" case_iam_105_readonly_boundary
  run_test IAM-106 "custom policy CRUD" case_iam_106_custom_policy_crud
  run_test IAM-107 "policy attach/detach (idempotent)" case_iam_107_attach_detach
  run_test IAM-109 "deny precedence over allow" case_iam_109_deny_precedence
  run_test IAM-113 "access-key bulk listing hides secrets" case_iam_113_access_key_list_hides_secret
  run_test SA-101 "service account create and use" case_sa_101_create_and_use
  run_test SA-103 "service account cannot exceed parent" case_sa_103_parent_boundary
  run_test SA-104 "service account deletion invalidates" case_sa_104_delete_invalidates
  run_test STS-101 "AssumeRole returns usable credentials" case_sts_101_assume_role
  run_test STS-102 "AssumeRole duration clamped to 12h" case_sts_102_duration_clamp
  run_test STS-104 "temporary credentials respect identity scope" case_sts_104_temp_boundary
  run_test STS-105 "revoke-tokens invalidates temporary credentials" case_sts_105_revoke
  run_test OIDC-102 "OIDC validate rejects bad provider config" case_oidc_102_validate_rejects_bad_config
  run_test OIDC-108 "STS rejects expired/garbage web identity token" case_oidc_108_reject_garbage_jwt
  if [ "${OIDC_LIVE}" -eq 1 ] && [ "${OIDC_RUN}" -eq 0 ]; then
    OIDC_RUN=1
    if command -v docker >/dev/null 2>&1 && command -v rustfs >/dev/null 2>&1; then
      run_test OIDC-103 "live Keycloak SSO discovery/JWT/STS" case_oidc_103_keycloak_live
    else
      skip_test OIDC-103 "live Keycloak SSO discovery/JWT/STS (docker/rustfs required)"
    fi
  else
    skip_test OIDC-103 "live Keycloak SSO discovery/JWT/STS (run once per suite)"
  fi
}

main() {
  parse_args "$@"
  need_cmd ssh "openssh client"
  need_cmd curl "curl"
  need_cmd openssl "openssl"
  need_cmd aws "aws cli"
  need_cmd jq "jq"
  need_cmd python3 "python3"

  confirm "Run security suite on topologies: ${SELECTED_TOPOLOGIES[*]}?"

  for topo in "${SELECTED_TOPOLOGIES[@]}"; do
    log "===== topology ${topo} ====="
    env_setup "${topo}"
    if [ -z "${RUSTFS_VERSION_INFO}" ]; then
      RUSTFS_VERSION_INFO="$(detect_rustfs_version)"
    fi
    CASE_RESULTS=()
    run_security_cases
    record_topology_results "${topo}"
    if [ "${KEEP_ENV}" -eq 0 ]; then
      env_teardown "${topo}"
    fi
  done

  write_report

  if [ "${FAIL}" -gt 0 ]; then
    KEEP_TMP=1
    echo "security suite: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped" >&2
    return 1
  fi
  echo "security suite: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped"
}

main "$@"
