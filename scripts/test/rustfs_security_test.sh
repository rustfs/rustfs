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
# STS-101..105, OIDC-101..108) with request flows already proven by
# admin_iam_crud_test, sts_query_compat_test and oidc_keycloak_live.sh.
#
# v1 implements the core IAM / service-account / STS / OIDC-negative cases
# against a local single-node RustFS instance started from the given binary.
# OIDC-103 (live Keycloak SSO) is delegated to oidc_keycloak_live.sh when
# RUSTFS_SECURITY_OIDC_LIVE=1 and docker is available.
#
# Usage:
#   rustfs_security_test.sh [RUSTFS_BINARY]
#
# Environment:
#   RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY   admin credentials (default rustfsadmin)
#   RUSTFS_SECURITY_OIDC_LIVE               "1" runs the Keycloak live gate
#   REPORT_FILE                             markdown report path (default /tmp/rustfs-security-report.md)
#   RUSTFS_SECURITY_TMP                     working directory (default mktemp -d)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_BINARY="${1:-${ROOT_DIR}/target/debug/rustfs}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
OIDC_LIVE="${RUSTFS_SECURITY_OIDC_LIVE:-0}"
REPORT_FILE="${REPORT_FILE:-/tmp/rustfs-security-report.md}"
WORK_DIR="${RUSTFS_SECURITY_TMP:-$(mktemp -d /tmp/rustfs-security-XXXXXX)}"
LOG_FILE="${WORK_DIR}/security.log"
SERVER_LOG="${WORK_DIR}/server.log"
REQ_BODY="${WORK_DIR}/req-body.out"
VOL_DIR="${WORK_DIR}/data"
mkdir -p "${VOL_DIR}"

RUSTFS_PID=""
RUSTFS_PORT=""
PASS=()
FAIL=()
SKIP=()
RESULTS=()

cleanup() {
  local status=$?
  if [[ -n "${RUSTFS_PID}" ]] && kill -0 "${RUSTFS_PID}" 2>/dev/null; then
    kill "${RUSTFS_PID}" 2>/dev/null || true
    wait "${RUSTFS_PID}" 2>/dev/null || true
  fi
  if [[ "${status}" -eq 0 ]]; then
    rm -rf "${WORK_DIR}"
  else
    echo "security suite logs retained in ${WORK_DIR}" >&2
  fi
}
trap cleanup EXIT INT TERM

for command in curl python3 awscurl; do
  command -v "${command}" >/dev/null || {
    echo "missing required command: ${command}" >&2
    exit 1
  }
done
[[ -x "${RUSTFS_BINARY}" ]] || {
  echo "RustFS binary is not executable: ${RUSTFS_BINARY}" >&2
  exit 1
}

free_port() {
  python3 - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

start_rustfs() {
  RUSTFS_PORT="$(free_port)"
  RUSTFS_ADDRESS="127.0.0.1:${RUSTFS_PORT}" \
  RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY}" \
  RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY}" \
  RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_OBS_LOG_DIRECTORY="${WORK_DIR}/logs" \
    "${RUSTFS_BINARY}" server "${VOL_DIR}" >"${SERVER_LOG}" 2>&1 &
  RUSTFS_PID=$!

  for _ in $(seq 1 120); do
    if curl --noproxy '*' -fsS "http://127.0.0.1:${RUSTFS_PORT}/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${RUSTFS_PID}" 2>/dev/null; then
      echo "RustFS exited before becoming ready; server log tail:" >&2
      tail -n 50 "${SERVER_LOG}" >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "RustFS did not become ready in time; server log tail:" >&2
  tail -n 50 "${SERVER_LOG}" >&2 || true
  return 1
}

BASE="http://127.0.0.1:${RUSTFS_PORT}"
ADMIN="${BASE}/rustfs/admin/v3"

admin_req() {
  # usage: admin_req METHOD URL [JSON_BODY] [AK] [SK] [TOKEN]
  # echoes HTTP status; response body lands in $REQ_BODY
  local method="$1" url="$2" body="${3:-}" ak="${4:-${RUSTFS_ACCESS_KEY}}" sk="${5:-${RUSTFS_SECRET_KEY}}" token="${6:-}"
  local awscurl_args=(--service s3 --region us-east-1 --access_key "${ak}" --secret_key "${sk}")
  if [[ -n "${token}" ]]; then
    awscurl_args+=(--security_token "${token}")
  fi
  local curl_args=(-sS --noproxy '*' -X "${method}" -o "${REQ_BODY}" -w '%{http_code}')
  if [[ -n "${body}" ]]; then
    curl_args+=(-d "${body}" -H 'Content-Type: application/json')
  fi
  awscurl "${awscurl_args[@]}" "${curl_args[@]}" "${url}" 2>>"${LOG_FILE}"
}

s3_req() {
  # usage: s3_req METHOD URL [AK] [SK] [TOKEN]
  local method="$1" url="$2" ak="${3:-${RUSTFS_ACCESS_KEY}}" sk="${4:-${RUSTFS_SECRET_KEY}}" token="${5:-}"
  local awscurl_args=(--service s3 --region us-east-1 --access_key "${ak}" --secret_key "${sk}")
  if [[ -n "${token}" ]]; then
    awscurl_args+=(--security_token "${token}")
  fi
  awscurl "${awscurl_args[@]}" -sS --noproxy '*' -X "${method}" -o "${REQ_BODY}" -w '%{http_code}' "${url}" 2>>"${LOG_FILE}"
}

sts_signed_req() {
  # usage: sts_signed_req FORM [AK] [SK] [TOKEN] ; echoes HTTP status, body in $REQ_BODY
  local form="$1" ak="${2:-${RUSTFS_ACCESS_KEY}}" sk="${3:-${RUSTFS_SECRET_KEY}}" token="${4:-}"
  local awscurl_args=(--service sts --region us-east-1 --access_key "${ak}" --secret_key "${sk}")
  if [[ -n "${token}" ]]; then
    awscurl_args+=(--security_token "${token}")
  fi
  awscurl "${awscurl_args[@]}" -sS --noproxy '*' -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d "${form}" -o "${REQ_BODY}" -w '%{http_code}' "${BASE}/" 2>>"${LOG_FILE}"
}

sts_unsigned_req() {
  # usage: sts_unsigned_req FORM ; echoes HTTP status, body in $REQ_BODY
  local form="$1"
  curl --noproxy '*' -sS -X POST \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d "${form}" -o "${REQ_BODY}" -w '%{http_code}' "${BASE}/" 2>>"${LOG_FILE}"
}

assert_status() {
  local expected="$1" actual="$2" context="$3"
  [[ "${actual}" == "${expected}" ]] || {
    echo "expected HTTP ${expected}, got ${actual} (${context}); body:" >&2
    head -c 800 "${REQ_BODY}" >&2 || true
    return 1
  }
}

assert_contains() {
  local needle="$1" file="$2" context="$3"
  grep -q -- "${needle}" "${file}" || {
    echo "expected '${needle}' in ${file} (${context})" >&2
    return 1
  }
}

assert_not_contains() {
  local needle="$1" file="$2" context="$3"
  if grep -q -- "${needle}" "${file}"; then
    echo "unexpected '${needle}' in ${file} (${context})" >&2
    return 1
  fi
}

make_user() {
  # usage: make_user USER SECRET ; creates an enabled user
  local user="$1" secret="$2"
  admin_req PUT "${ADMIN}/v3/add-user?accessKey=${user}" "{\"secretKey\":\"${secret}\",\"status\":\"enabled\"}"
}

make_policy() {
  # usage: make_policy NAME POLICY_JSON
  local name="$1" policy="$2"
  admin_req PUT "${ADMIN}/v3/add-canned-policy?name=${name}" "${policy}"
}

attach_policy() {
  # usage: attach_policy USER POLICY...
  local user="$1"; shift
  local policies="["
  local first=1
  for p in "$@"; do
    if [[ "${first}" -eq 1 ]]; then first=0; else policies+=","; fi
    policies+="\"${p}\""
  done
  policies+="]"
  admin_req POST "${ADMIN}/v3/idp/builtin/policy/attach" "{\"policies\":${policies},\"user\":\"${user}\"}"
}

detach_policy() {
  local user="$1"; shift
  local policies="["
  local first=1
  for p in "$@"; do
    if [[ "${first}" -eq 1 ]]; then first=0; else policies+=","; fi
    policies+="\"${p}\""
  done
  policies+="]"
  admin_req POST "${ADMIN}/v3/idp/builtin/policy/detach" "{\"policies\":${policies},\"user\":\"${user}\"}"
}

create_service_account() {
  # usage: create_service_account TARGET_USER [POLICY_JSON] ; prints AK<TAB>SK
  local target="$1" policy="${2:-}"
  local body="{\"targetUser\":\"${target}\"}"
  if [[ -n "${policy}" ]]; then
    body="{\"targetUser\":\"${target}\",\"policy\":${policy}}"
  fi
  local status
  status="$(admin_req PUT "${ADMIN}/v3/add-service-accounts" "${body}")"
  assert_status 200 "${status}" "add-service-accounts"
  python3 - "${REQ_BODY}" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1]))
creds = data["credentials"]
print(f"{creds['accessKey']}\t{creds['secretKey']}")
PY
}

parse_sts_credentials() {
  python3 - "${REQ_BODY}" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
values = {}
for element in root.iter():
    values[element.tag.rsplit("}", 1)[-1]] = element.text or ""
for field in ("AccessKeyId", "SecretAccessKey", "SessionToken"):
    assert values.get(field), f"missing {field} in STS response"
print("\t".join(values[field] for field in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
PY
}

# ---------------------------------------------------------------------------
# IAM: users / groups / policies
# ---------------------------------------------------------------------------

case_iam_101_user_crud() {
  local user="sec101u" secret="sec101secret"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(admin_req GET "${ADMIN}/v3/list-users")"
  assert_status 200 "${status}" "list-users"
  assert_contains "${user}" "${REQ_BODY}" "list-users contains user"
  status="$(admin_req GET "${ADMIN}/v3/user-info?accessKey=${user}")"
  assert_status 200 "${status}" "user-info"
  assert_contains "${user}" "${REQ_BODY}" "user-info contains user"
  assert_not_contains "${secret}" "${REQ_BODY}" "user-info must not leak secret"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 200 "${status}" "user credentials can list buckets"
  status="$(admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}")"
  assert_status 200 "${status}" "remove-user"
  status="$(admin_req GET "${ADMIN}/v3/user-info?accessKey=${user}")"
  [[ "${status}" != 200 ]] || {
    echo "expected user-info to fail after delete, got HTTP 200" >&2
    return 1
  }
}

case_iam_102_user_disable() {
  local user="sec102u" secret="sec102secret"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 200 "${status}" "enabled user works"
  status="$(admin_req PUT "${ADMIN}/v3/set-user-status?accessKey=${user}&status=disabled")"
  assert_status 200 "${status}" "set-user-status disabled"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 403 "${status}" "disabled user denied"
  status="$(admin_req PUT "${ADMIN}/v3/set-user-status?accessKey=${user}&status=enabled")"
  assert_status 200 "${status}" "set-user-status enabled"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 200 "${status}" "re-enabled user works"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_iam_103_delete_invalidates() {
  local user="sec103u" secret="sec103secret"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 200 "${status}" "user works before delete"
  status="$(admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}")"
  assert_status 200 "${status}" "remove-user"
  status="$(s3_req GET "${BASE}/" "${user}" "${secret}")"
  assert_status 403 "${status}" "deleted user denied"
}

case_iam_105_readonly_boundary() {
  local user="sec105u" secret="sec105secret"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(attach_policy "${user}" readonly)"
  assert_status 200 "${status}" "attach readonly"
  status="$(s3_req PUT "${BASE}/sec105-bucket")"
  assert_status 200 "${status}" "root creates bucket"
  status="$(s3_req GET "${BASE}/sec105-bucket" "${user}" "${secret}")"
  assert_status 200 "${status}" "readonly can list bucket"
  status="$(s3_req PUT "${BASE}/sec105-bucket/hello.txt" "${user}" "${secret}")"
  assert_status 403 "${status}" "readonly cannot write"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_iam_106_custom_policy_crud() {
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::sec106-bucket/*"]}]}'
  local status
  status="$(make_policy sec106p "${policy}")"
  assert_status 200 "${status}" "add-canned-policy"
  status="$(admin_req GET "${ADMIN}/v3/list-canned-policies")"
  assert_status 200 "${status}" "list-canned-policies"
  assert_contains sec106p "${REQ_BODY}" "list contains policy"
  status="$(admin_req GET "${ADMIN}/v3/info-canned-policy?name=sec106p")"
  assert_status 200 "${status}" "info-canned-policy"
  assert_contains "s3:GetObject" "${REQ_BODY}" "policy body round-trips"
  status="$(admin_req DELETE "${ADMIN}/v3/remove-canned-policy?name=sec106p")"
  assert_status 200 "${status}" "remove-canned-policy"
  status="$(admin_req GET "${ADMIN}/v3/list-canned-policies")"
  assert_status 200 "${status}" "list-canned-policies after remove"
  assert_not_contains sec106p "${REQ_BODY}" "policy removed from list"
}

case_iam_107_attach_detach() {
  local user="sec107u" secret="sec107secret"
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec107-bucket"]}]}'
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(make_policy sec107p "${policy}")"
  assert_status 200 "${status}" "add policy"
  status="$(s3_req PUT "${BASE}/sec107-bucket")"
  assert_status 200 "${status}" "root creates bucket"
  status="$(attach_policy "${user}" sec107p)"
  assert_status 200 "${status}" "attach"
  status="$(s3_req GET "${BASE}/sec107-bucket" "${user}" "${secret}")"
  assert_status 200 "${status}" "policy grants list"
  status="$(detach_policy "${user}" sec107p)"
  assert_status 200 "${status}" "detach"
  status="$(s3_req GET "${BASE}/sec107-bucket" "${user}" "${secret}")"
  assert_status 403 "${status}" "detached user denied"
  status="$(detach_policy "${user}" sec107p)"
  assert_status 200 "${status}" "detach is idempotent"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_iam_109_deny_precedence() {
  local user="sec109u" secret="sec109secret"
  local allow='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec109-bucket"]}]}'
  local deny='{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec109-bucket"]}]}'
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(make_policy sec109-allow "${allow}")"
  assert_status 200 "${status}" "add allow policy"
  status="$(make_policy sec109-deny "${deny}")"
  assert_status 200 "${status}" "add deny policy"
  status="$(s3_req PUT "${BASE}/sec109-bucket")"
  assert_status 200 "${status}" "root creates bucket"
  status="$(attach_policy "${user}" sec109-allow sec109-deny)"
  assert_status 200 "${status}" "attach allow+deny"
  status="$(s3_req GET "${BASE}/sec109-bucket" "${user}" "${secret}")"
  assert_status 403 "${status}" "deny overrides allow"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_iam_113_access_key_list_hides_secret() {
  local user="sec113u" secret="sec113secret-xyz"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(admin_req GET "${ADMIN}/v3/list-access-keys-bulk?all=true")"
  assert_status 200 "${status}" "list-access-keys-bulk"
  assert_not_contains '"secretKey"' "${REQ_BODY}" "bulk listing must not expose secretKey"
  assert_not_contains "${secret}" "${REQ_BODY}" "bulk listing must not expose secrets"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

# ---------------------------------------------------------------------------
# Service accounts
# ---------------------------------------------------------------------------

case_sa_101_create_and_use() {
  local sa
  local status
  status="$(admin_req PUT "${ADMIN}/v3/add-service-accounts" "{\"targetUser\":\"${RUSTFS_ACCESS_KEY}\"}")"
  assert_status 200 "${status}" "add-service-accounts"
  sa="$(python3 - "${REQ_BODY}" <<'PY'
import json, sys
creds = json.load(open(sys.argv[1]))["credentials"]
print(f"{creds['accessKey']}\t{creds['secretKey']}")
PY
)"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  status="$(s3_req GET "${BASE}/" "${sa_ak}" "${sa_sk}")"
  assert_status 200 "${status}" "service account can list buckets"
  admin_req DELETE "${ADMIN}/v3/delete-service-account?accessKey=${sa_ak}" >/dev/null
}

case_sa_103_parent_boundary() {
  local user="sec103p" secret="sec103secret"
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(attach_policy "${user}" readonly)"
  assert_status 200 "${status}" "attach readonly to parent"
  status="$(s3_req PUT "${BASE}/sec103p-bucket")"
  assert_status 200 "${status}" "root creates bucket"
  local sa
  sa="$(create_service_account "${user}" '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::sec103p-bucket/*"]}]}')"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  status="$(s3_req PUT "${BASE}/sec103p-bucket/hello.txt" "${sa_ak}" "${sa_sk}")"
  assert_status 403 "${status}" "service account cannot exceed readonly parent"
  admin_req DELETE "${ADMIN}/v3/delete-service-account?accessKey=${sa_ak}" >/dev/null
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_sa_104_delete_invalidates() {
  local sa status
  sa="$(create_service_account "${RUSTFS_ACCESS_KEY}")"
  local sa_ak="${sa%%$'\t'*}" sa_sk="${sa#*$'\t'}"
  status="$(s3_req GET "${BASE}/" "${sa_ak}" "${sa_sk}")"
  assert_status 200 "${status}" "service account works"
  status="$(admin_req DELETE "${ADMIN}/v3/delete-service-account?accessKey=${sa_ak}")"
  assert_status 200 "${status}" "delete-service-account"
  status="$(s3_req GET "${BASE}/" "${sa_ak}" "${sa_sk}")"
  assert_status 403 "${status}" "deleted service account denied"
}

# ---------------------------------------------------------------------------
# STS
# ---------------------------------------------------------------------------

case_sts_101_assume_role() {
  local status
  status="$(sts_signed_req "Action=AssumeRole&Version=2011-06-15&DurationSeconds=900&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=sec101")"
  assert_status 200 "${status}" "AssumeRole"
  local creds sts_ak sts_sk sts_token
  creds="$(parse_sts_credentials)"
  sts_ak="${creds%%$'\t'*}"; creds="${creds#*$'\t'}"
  sts_sk="${creds%%$'\t'*}"; sts_token="${creds#*$'\t'}"
  status="$(s3_req GET "${BASE}/" "${sts_ak}" "${sts_sk}" "${sts_token}")"
  assert_status 200 "${status}" "STS credentials can list buckets"
}

case_sts_102_duration_clamp() {
  local status
  status="$(sts_signed_req "Action=AssumeRole&Version=2011-06-15&DurationSeconds=604800&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=sec102")"
  assert_status 200 "${status}" "AssumeRole with 7d duration"
  local expiration
  expiration="$(python3 - "${REQ_BODY}" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
for element in root.iter():
    if element.tag.rsplit("}", 1)[-1] == "Expiration":
        print(element.text or "")
        break
PY
)"
  [[ -n "${expiration}" ]] || { echo "missing Expiration in STS response" >&2; return 1; }
  local expiry_epoch
  expiry_epoch="$(date -u -d "${expiration}" +%s)"
  local now_epoch max_epoch
  now_epoch="$(date -u +%s)"
  max_epoch=$(( now_epoch + 12 * 3600 + 120 ))
  [[ "${expiry_epoch}" -le "${max_epoch}" ]] || {
    echo "expected DurationSeconds clamped to 12h, got expiry ${expiration}" >&2
    return 1
  }
}

case_sts_104_temp_boundary() {
  local user="sec104u" secret="sec104secret"
  local policy='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sts:AssumeRole"],"Resource":["arn:aws:s3:::*"]},{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::sec104-a-bucket"]}]}'
  local status
  status="$(make_user "${user}" "${secret}")"
  assert_status 200 "${status}" "add-user"
  status="$(make_policy sec104p "${policy}")"
  assert_status 200 "${status}" "add policy"
  status="$(attach_policy "${user}" sec104p)"
  assert_status 200 "${status}" "attach"
  status="$(s3_req PUT "${BASE}/sec104-a-bucket")"
  assert_status 200 "${status}" "root creates a-bucket"
  status="$(s3_req PUT "${BASE}/sec104-b-bucket")"
  assert_status 200 "${status}" "root creates b-bucket"
  status="$(sts_signed_req "Action=AssumeRole&Version=2011-06-15&DurationSeconds=900&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=sec104" "${user}" "${secret}")"
  assert_status 200 "${status}" "user AssumeRole"
  local creds sts_ak sts_sk sts_token
  creds="$(parse_sts_credentials)"
  sts_ak="${creds%%$'\t'*}"; creds="${creds#*$'\t'}"
  sts_sk="${creds%%$'\t'*}"; sts_token="${creds#*$'\t'}"
  status="$(s3_req GET "${BASE}/sec104-a-bucket" "${sts_ak}" "${sts_sk}" "${sts_token}")"
  assert_status 200 "${status}" "STS creds can list allowed bucket"
  status="$(s3_req PUT "${BASE}/sec104-b-bucket/hello.txt" "${sts_ak}" "${sts_sk}" "${sts_token}")"
  assert_status 403 "${status}" "STS creds cannot exceed identity scope"
  admin_req DELETE "${ADMIN}/v3/remove-user?accessKey=${user}" >/dev/null
}

case_sts_105_revoke() {
  local status
  status="$(sts_signed_req "Action=AssumeRole&Version=2011-06-15&DurationSeconds=900&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=sec105")"
  assert_status 200 "${status}" "AssumeRole"
  local creds sts_ak sts_sk sts_token
  creds="$(parse_sts_credentials)"
  sts_ak="${creds%%$'\t'*}"; creds="${creds#*$'\t'}"
  sts_sk="${creds%%$'\t'*}"; sts_token="${creds#*$'\t'}"
  status="$(s3_req GET "${BASE}/" "${sts_ak}" "${sts_sk}" "${sts_token}")"
  assert_status 200 "${status}" "STS creds work before revoke"
  status="$(admin_req POST "${ADMIN}/v3/revoke-tokens/builtin?user=${RUSTFS_ACCESS_KEY}&fullRevoke=true")"
  assert_status 200 "${status}" "revoke-tokens"
  status="$(s3_req GET "${BASE}/" "${sts_ak}" "${sts_sk}" "${sts_token}")"
  assert_status 403 "${status}" "STS creds invalid after revoke"
}

# ---------------------------------------------------------------------------
# OIDC / SSO (non-live checks; live Keycloak gate is OIDC-103)
# ---------------------------------------------------------------------------

case_oidc_102_validate_rejects_bad_config() {
  local body='{"provider_id":"bad","enabled":true,"display_name":"bad","config_url":"http://127.0.0.1:1/nope","client_id":"nope","scopes":["openid"]}'
  local status
  status="$(admin_req POST "${ADMIN}/v3/oidc/validate" "${body}")"
  [[ "${status}" != 200 ]] || {
    echo "expected validate to reject unreachable config_url, got HTTP 200" >&2
    return 1
  }
}

case_oidc_108_reject_garbage_jwt() {
  # Expired, unsigned token: server must refuse it at the STS web-identity gate.
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
  local status
  status="$(sts_unsigned_req "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&DurationSeconds=900&WebIdentityToken=${expired_jwt}")"
  assert_status 403 "${status}" "expired/garbage web identity token"
  assert_contains "<Code>AccessDenied</Code>" "${REQ_BODY}" "STS error is AccessDenied"
}

case_oidc_103_keycloak_live() {
  local live_script="${ROOT_DIR}/scripts/test/oidc_keycloak_live.sh"
  [[ -f "${live_script}" ]] || { echo "oidc_keycloak_live.sh missing" >&2; return 1; }
  bash "${live_script}" "${RUSTFS_BINARY}"
}

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

sec_run() {
  local id="$1" desc="$2"
  shift 2
  local out="${WORK_DIR}/${id}.log"
  if "$@" >"${out}" 2>&1; then
    PASS+=("${id}")
    RESULTS+=("${id}|${desc}|PASS")
    echo "PASS ${id} ${desc}"
  else
    FAIL+=("${id}")
    RESULTS+=("${id}|${desc}|FAIL")
    echo "FAIL ${id} ${desc} (see ${out})"
  fi
}

main() {
  start_rustfs

  sec_run IAM-101 "user CRUD lifecycle" case_iam_101_user_crud
  sec_run IAM-102 "enable/disable user revokes access" case_iam_102_user_disable
  sec_run IAM-103 "user deletion invalidates credentials" case_iam_103_delete_invalidates
  sec_run IAM-105 "builtin readonly policy boundary" case_iam_105_readonly_boundary
  sec_run IAM-106 "custom policy CRUD" case_iam_106_custom_policy_crud
  sec_run IAM-107 "policy attach/detach (idempotent)" case_iam_107_attach_detach
  sec_run IAM-109 "deny precedence over allow" case_iam_109_deny_precedence
  sec_run IAM-113 "access-key bulk listing hides secrets" case_iam_113_access_key_list_hides_secret
  sec_run SA-101 "service account create and use" case_sa_101_create_and_use
  sec_run SA-103 "service account cannot exceed parent" case_sa_103_parent_boundary
  sec_run SA-104 "service account deletion invalidates" case_sa_104_delete_invalidates
  sec_run STS-101 "AssumeRole returns usable credentials" case_sts_101_assume_role
  sec_run STS-102 "AssumeRole duration clamped to 12h" case_sts_102_duration_clamp
  sec_run STS-104 "temporary credentials respect identity scope" case_sts_104_temp_boundary
  sec_run STS-105 "revoke-tokens invalidates temporary credentials" case_sts_105_revoke
  sec_run OIDC-102 "OIDC validate rejects bad provider config" case_oidc_102_validate_rejects_bad_config
  sec_run OIDC-108 "STS rejects expired/garbage web identity token" case_oidc_108_reject_garbage_jwt
  if [[ "${OIDC_LIVE}" == "1" || "${OIDC_LIVE}" == "true" ]] && command -v docker >/dev/null 2>&1; then
    sec_run OIDC-103 "live Keycloak SSO discovery/JWT/STS" case_oidc_103_keycloak_live
  else
    SKIP+=("OIDC-103")
    RESULTS+=("OIDC-103|live Keycloak SSO discovery/JWT/STS|SKIP")
    echo "SKIP OIDC-103 live Keycloak SSO (RUSTFS_SECURITY_OIDC_LIVE=1 and docker required)"
  fi

  write_report

  if [[ "${#FAIL[@]}" -gt 0 ]]; then
    echo "security suite: ${#PASS[@]} passed, ${#FAIL[@]} failed, ${#SKIP[@]} skipped" >&2
    return 1
  fi
  echo "security suite: ${#PASS[@]} passed, ${#FAIL[@]} failed, ${#SKIP[@]} skipped"
}

write_report() {
  local package_source="${PACKAGE_SOURCE:-local build}"
  local version_info
  version_info="$("${RUSTFS_BINARY}" --version 2>/dev/null | tr -d '\r' | head -n 1 || true)"
  version_info="${version_info:-N/A}"
  local outcome="success"
  [[ "${#FAIL[@]}" -eq 0 ]] || outcome="failure"
  {
    echo "# RustFS security test report"
    echo ""
    echo "- Run: ${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-rustfs/rustfs}/actions/runs/${GITHUB_RUN_ID:-local}"
    echo "- Trigger: ${GITHUB_EVENT_NAME:-local}"
    echo "- Package: ${package_source}"
    echo "- RustFS Version: ${version_info}"
    echo "- Test Step Outcome: ${outcome}"
    echo "- Result: ${#PASS[@]} passed / ${#FAIL[@]} failed / ${#SKIP[@]} skipped"
    echo ""
    echo "| ID | Case | Result |"
    echo "|---|---|---|"
  } >"${REPORT_FILE}"

  local entry id desc result
  for entry in "${RESULTS[@]}"; do
    IFS='|' read -r id desc result <<<"${entry}"
    echo "| ${id} | ${desc} | ${result} |" >>"${REPORT_FILE}"
  done

  {
    echo ""
    if [[ "${#FAIL[@]}" -gt 0 ]]; then
      echo "## Failure details"
      echo ""
      for id in "${FAIL[@]}"; do
        echo "### ${id}"
        echo ""
        echo '```text'
        tail -n 40 "${WORK_DIR}/${id}.log" 2>/dev/null || true
        echo '```'
      done
    else
      echo "All security cases passed."
    fi
  } >>"${REPORT_FILE}"

  cat "${REPORT_FILE}" >>"${GITHUB_STEP_SUMMARY:-/dev/null}"
}

main "$@"
