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

# Live gate for `claim_userinfo` and claim value mapping against a real Keycloak that
# behaves like an identity proxy: the `eduperson_entitlement` claim is released
# only through the UserInfo endpoint and its values are URNs with `=` and `#`, which are
# not valid RustFS policy names.
#
# Checks:
#   1. AssumeRoleWithWebIdentity with WebIdentityToken + WebIdentityAccessToken succeeds and
#      the session is bound to the policy named by the mapped value
#      (demo-project:member): listing and reading the pilot bucket works,
#      writing to it is denied.
#   2. AssumeRoleWithWebIdentity without WebIdentityAccessToken is rejected with 400 while
#      claim_userinfo is on.
#
# Usage: scripts/test/oidc_keycloak_userinfo_live.sh [path/to/rustfs]
# Requires: curl, python3, aws (AWS CLI v2) and either docker (Keycloak runs from the pinned
# image) or KEYCLOAK_DIST=<path/to/keycloak-26.x.tar.gz> plus a Java 21 runtime on JAVA_HOME
# or PATH (Keycloak runs from a fresh extraction of that archive; no container needed).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_BINARY="${1:-${ROOT_DIR}/target/debug/rustfs}"
REALM_FIXTURE="${ROOT_DIR}/scripts/test/fixtures/keycloak-rustfs-userinfo-realm.json"
KEYCLOAK_IMAGE="${KEYCLOAK_IMAGE:-quay.io/keycloak/keycloak@sha256:6a7217a100bd3e5de4063a27a538ef999a3c5a88c4b4ec0ffc0a642aee7b2597}"
KEYCLOAK_DIST="${KEYCLOAK_DIST:-}"
WORK_DIR="${RUNNER_TEMP:-/tmp}/rustfs-keycloak-userinfo-live-${$}"
KEYCLOAK_CONTAINER="rustfs-keycloak-userinfo-live-${$}"
KEYCLOAK_PID=""
RUSTFS_PID=""
DOCKER=(docker)
ADMIN_ACCESS_KEY=rustfsadmin
ADMIN_SECRET_KEY=rustfsadmin
PILOT_BUCKET=egi-pilot
MAPPED_POLICY='demo-project:member'

mkdir -p "${WORK_DIR}"
# stop_background PID: end a process this script started in the background; when it leads
# its own process group (Keycloak started through setsid) the whole group goes with it.
stop_background() {
  local pid="$1"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  kill -- "-${pid}" 2>/dev/null || kill "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}
cleanup() {
  local status=$?
  stop_background "${RUSTFS_PID}"
  stop_background "${KEYCLOAK_PID}"
  if [[ -z "${KEYCLOAK_DIST}" ]]; then
    if [[ "${status}" -ne 0 ]]; then
      "${DOCKER[@]}" logs "${KEYCLOAK_CONTAINER}" >"${WORK_DIR}/keycloak.log" 2>&1 || true
    fi
    "${DOCKER[@]}" rm -f "${KEYCLOAK_CONTAINER}" >/dev/null 2>&1 || true
  fi
  if [[ "${status}" -eq 0 ]]; then
    rm -rf "${WORK_DIR}"
  else
    echo "live-gate logs retained in ${WORK_DIR}" >&2
  fi
}
trap cleanup EXIT INT TERM

for command in curl python3 aws; do
  command -v "${command}" >/dev/null || {
    echo "missing required command: ${command}" >&2
    exit 1
  }
done
if [[ -z "${KEYCLOAK_DIST}" ]]; then
  command -v docker >/dev/null || {
    echo "missing required command: docker (or set KEYCLOAK_DIST to a Keycloak archive)" >&2
    exit 1
  }
  if ! docker info >/dev/null 2>&1; then
    DOCKER=(sudo -n docker)
    "${DOCKER[@]}" info >/dev/null
  fi
elif [[ ! -f "${KEYCLOAK_DIST}" ]]; then
  echo "KEYCLOAK_DIST is not a file: ${KEYCLOAK_DIST}" >&2
  exit 1
fi
[[ -x "${RUSTFS_BINARY}" ]] || {
  echo "RustFS binary is not executable: ${RUSTFS_BINARY}" >&2
  exit 1
}
[[ -f "${REALM_FIXTURE}" ]] || {
  echo "Keycloak realm fixture is missing: ${REALM_FIXTURE}" >&2
  exit 1
}

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

KEYCLOAK_PORT="$(free_port)"
RUSTFS_PORT="$(free_port)"
KEYCLOAK_ORIGIN="http://127.0.0.1:${KEYCLOAK_PORT}"
ISSUER="${KEYCLOAK_ORIGIN}/realms/rustfs-ci"
DISCOVERY_URL="${ISSUER}/.well-known/openid-configuration"
RUSTFS_ORIGIN="http://127.0.0.1:${RUSTFS_PORT}"

start_keycloak() {
  if [[ -n "${KEYCLOAK_DIST}" ]]; then
    local java="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
    command -v "${java}" >/dev/null || {
      echo "Java runtime not found at '${java}' (set JAVA_HOME or put java on PATH)" >&2
      exit 1
    }
    local home="${WORK_DIR}/keycloak"
    mkdir -p "${home}"
    tar -xzf "${KEYCLOAK_DIST}" -C "${home}" --strip-components=1
    mkdir -p "${home}/data/import"
    cp "${REALM_FIXTURE}" "${home}/data/import/rustfs-ci-realm.json"
    env \
      KC_BOOTSTRAP_ADMIN_USERNAME=admin \
      KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
      KC_HEALTH_ENABLED=true \
      "KC_HOSTNAME=${KEYCLOAK_ORIGIN}" \
      setsid "${home}/bin/kc.sh" start-dev --import-realm \
      --http-host 127.0.0.1 --http-port "${KEYCLOAK_PORT}" \
      >"${WORK_DIR}/keycloak.log" 2>&1 &
    KEYCLOAK_PID=$!
    return 0
  fi
  "${DOCKER[@]}" run --detach --rm \
    --name "${KEYCLOAK_CONTAINER}" \
    --memory 1g \
    --publish "127.0.0.1:${KEYCLOAK_PORT}:8080" \
    --env KC_BOOTSTRAP_ADMIN_USERNAME=admin \
    --env KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
    --env KC_HEALTH_ENABLED=true \
    --env "KC_HOSTNAME=${KEYCLOAK_ORIGIN}" \
    --volume "${REALM_FIXTURE}:/opt/keycloak/data/import/rustfs-ci-realm.json:ro" \
    "${KEYCLOAK_IMAGE}" start-dev --import-realm >/dev/null
}
keycloak_alive() {
  if [[ -n "${KEYCLOAK_DIST}" ]]; then
    kill -0 "${KEYCLOAK_PID}" 2>/dev/null
  else
    [[ "$("${DOCKER[@]}" inspect -f '{{.State.Running}}' "${KEYCLOAK_CONTAINER}" 2>/dev/null || true)" == "true" ]]
  fi
}
keycloak_log() {
  if [[ -n "${KEYCLOAK_DIST}" ]]; then
    cat "${WORK_DIR}/keycloak.log"
  else
    "${DOCKER[@]}" logs "${KEYCLOAK_CONTAINER}"
  fi
}

start_keycloak
for _ in $(seq 1 180); do
  if curl --noproxy '*' -fsS "${DISCOVERY_URL}" >"${WORK_DIR}/discovery.json" 2>/dev/null; then
    break
  fi
  if ! keycloak_alive; then
    keycloak_log >&2 || true
    echo "Keycloak exited before discovery became ready" >&2
    exit 1
  fi
  sleep 1
done
curl --noproxy '*' -fsS "${DISCOVERY_URL}" >"${WORK_DIR}/discovery.json"
python3 - "${WORK_DIR}/discovery.json" "${ISSUER}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    discovery = json.load(source)
assert discovery.get("issuer") == sys.argv[2], discovery
for field in ("token_endpoint", "jwks_uri", "userinfo_endpoint"):
    assert discovery.get(field), discovery
PY

mkdir -p "${WORK_DIR}/rustfs-data"
env \
  NO_PROXY=127.0.0.1,localhost \
  RUSTFS_ACCESS_KEY="${ADMIN_ACCESS_KEY}" \
  RUSTFS_SECRET_KEY="${ADMIN_SECRET_KEY}" \
  RUSTFS_OBS_LOGGER_LEVEL=debug \
  RUSTFS_OUTBOUND_ALLOW_ORIGINS="${KEYCLOAK_ORIGIN}" \
  RUSTFS_IDENTITY_OPENID_ENABLE=on \
  RUSTFS_IDENTITY_OPENID_CONFIG_URL="${DISCOVERY_URL}" \
  RUSTFS_IDENTITY_OPENID_ISSUER="${ISSUER}" \
  RUSTFS_IDENTITY_OPENID_CLIENT_ID=rustfs-ci \
  RUSTFS_IDENTITY_OPENID_CLIENT_SECRET=rustfs-ci-secret \
  RUSTFS_IDENTITY_OPENID_SCOPES=openid,profile,email \
  RUSTFS_IDENTITY_OPENID_CLAIM_USERINFO=on \
  RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM=eduperson_entitlement \
  RUSTFS_IDENTITY_OPENID_CLAIM_VALUE_PATTERN='^urn:mace:example\.org:group:([^:#]+):role=([^#]+)#aai\.example\.org$' \
  RUSTFS_IDENTITY_OPENID_CLAIM_VALUE_REPLACEMENT='$1:$2' \
  "${RUSTFS_BINARY}" --address "127.0.0.1:${RUSTFS_PORT}" "${WORK_DIR}/rustfs-data" \
  >"${WORK_DIR}/rustfs.log" 2>&1 &
RUSTFS_PID=$!

for _ in $(seq 1 90); do
  if curl --noproxy '*' -fsS "${RUSTFS_ORIGIN}/health/ready" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${RUSTFS_PID}" 2>/dev/null; then
    cat "${WORK_DIR}/rustfs.log" >&2
    echo "RustFS exited before becoming ready" >&2
    exit 1
  fi
  sleep 1
done
curl --noproxy '*' -fsS "${RUSTFS_ORIGIN}/health/ready" >/dev/null

# Signed admin API request (SigV4, service s3, region us-east-1) with the Python standard
# library only: the live gates must not depend on pip-installed tools.
# usage: admin_request METHOD PATH QUERY BODY_FILE  -> prints the HTTP status, body in ${WORK_DIR}/admin-response
admin_request() {
  python3 - "$1" "${RUSTFS_ORIGIN}" "$2" "$3" "$4" "${ADMIN_ACCESS_KEY}" "${ADMIN_SECRET_KEY}" "${WORK_DIR}/admin-response" <<'PY'
import datetime
import hashlib
import hmac
import sys
import urllib.error
import urllib.parse
import urllib.request

method, origin, path, query, body_file, access_key, secret_key, out = sys.argv[1:9]
body = open(body_file, "rb").read() if body_file else b""
host = urllib.parse.urlparse(origin).netloc
now = datetime.datetime.now(datetime.timezone.utc)
amz_date = now.strftime("%Y%m%dT%H%M%SZ")
date_stamp = now.strftime("%Y%m%d")
payload_hash = hashlib.sha256(body).hexdigest()
canonical_query = "&".join(
    f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
    for k, v in sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
)
headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
signed_headers = ";".join(sorted(headers))
canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
canonical_request = "\n".join([method, path, canonical_query, canonical_headers, signed_headers, payload_hash])
scope = f"{date_stamp}/us-east-1/s3/aws4_request"
string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()])

def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

signing_key = sign(sign(sign(sign(("AWS4" + secret_key).encode(), date_stamp), "us-east-1"), "s3"), "aws4_request")
signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
authorization = f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
url = f"{origin}{path}" + (f"?{canonical_query}" if canonical_query else "")
request = urllib.request.Request(url, data=body if body else None, method=method)
for name, value in headers.items():
    if name != "host":
        request.add_header(name, value)
request.add_header("Authorization", authorization)
request.add_header("Content-Type", "application/json")
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(request, timeout=30) as response:
        status = response.status
        data = response.read()
except urllib.error.HTTPError as error:
    status = error.code
    data = error.read()
open(out, "wb").write(data)
print(status)
PY
}

cat >"${WORK_DIR}/pilot-policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": ["arn:aws:s3:::${PILOT_BUCKET}", "arn:aws:s3:::${PILOT_BUCKET}/*"]
    }
  ]
}
JSON
POLICY_STATUS="$(admin_request PUT /rustfs/admin/v3/add-canned-policy "name=${MAPPED_POLICY}" "${WORK_DIR}/pilot-policy.json")"
[[ "${POLICY_STATUS}" == 200 || "${POLICY_STATUS}" == 204 ]] || {
  cat "${WORK_DIR}/admin-response" >&2
  echo "expected add-canned-policy to succeed, got HTTP ${POLICY_STATUS}" >&2
  exit 1
}
INFO_STATUS="$(admin_request GET /rustfs/admin/v3/info-canned-policy "name=${MAPPED_POLICY}" "")"
[[ "${INFO_STATUS}" == 200 ]] || {
  cat "${WORK_DIR}/admin-response" >&2
  echo "expected info-canned-policy to find ${MAPPED_POLICY}, got HTTP ${INFO_STATUS}" >&2
  exit 1
}

admin_aws() {
  env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
    AWS_ACCESS_KEY_ID="${ADMIN_ACCESS_KEY}" \
    AWS_SECRET_ACCESS_KEY="${ADMIN_SECRET_KEY}" \
    AWS_EC2_METADATA_DISABLED=true \
    aws --endpoint-url "${RUSTFS_ORIGIN}" --region us-east-1 "$@"
}
admin_aws s3api create-bucket --bucket "${PILOT_BUCKET}" >/dev/null
printf 'hello from the pilot bucket\n' >"${WORK_DIR}/hello.txt"
admin_aws s3api put-object --bucket "${PILOT_BUCKET}" --key hello.txt --body "${WORK_DIR}/hello.txt" >/dev/null

# Password grant for the test user: returns "<id_token>\t<access_token>".
tokens_for_user() {
  local response_file="${WORK_DIR}/token.json"
  if ! curl --noproxy '*' -sS --fail-with-body "${ISSUER}/protocol/openid-connect/token" \
    --data-urlencode grant_type=password \
    --data-urlencode client_id=rustfs-ci \
    --data-urlencode client_secret=rustfs-ci-secret \
    --data-urlencode username=alice \
    --data-urlencode password=alice-password \
    --data-urlencode scope=openid \
    >"${response_file}"; then
    cat "${response_file}" >&2
    return 1
  fi
  python3 - "${response_file}" "${ISSUER}" <<'PY'
import base64
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    tokens = json.load(source)
id_token = tokens["id_token"]
payload = id_token.split(".")[1]
payload += "=" * (-len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
assert claims["iss"] == sys.argv[2], claims
# The realm releases the entitlements through UserInfo only: the ID token must not carry them.
assert "eduperson_entitlement" not in claims, claims
print(id_token + "\t" + tokens["access_token"])
PY
}

IFS=$'\t' read -r ID_TOKEN ACCESS_TOKEN < <(tokens_for_user)
[[ -n "${ID_TOKEN}" && -n "${ACCESS_TOKEN}" ]] || {
  echo "password grant did not return an ID token and an access token" >&2
  exit 1
}

# UserInfo really is the only place the claim lives (sanity check of the fixture).
curl --noproxy '*' -fsS -H "Authorization: Bearer ${ACCESS_TOKEN}" "${ISSUER}/protocol/openid-connect/userinfo" >"${WORK_DIR}/userinfo.json"
python3 - "${WORK_DIR}/userinfo.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    userinfo = json.load(source)
values = userinfo["eduperson_entitlement"]
assert "urn:mace:example.org:group:demo-project:role=member#aai.example.org" in values, userinfo
PY

sts_request() {
  # sts_request OUT_PREFIX [extra --data-urlencode args...]
  local out_prefix="$1"
  shift
  curl --noproxy '*' -sS \
    -D "${WORK_DIR}/${out_prefix}.headers" \
    -o "${WORK_DIR}/${out_prefix}.xml" \
    -w '%{http_code}' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -X POST "${RUSTFS_ORIGIN}/" \
    --data-urlencode Action=AssumeRoleWithWebIdentity \
    --data-urlencode Version=2011-06-15 \
    --data-urlencode DurationSeconds=900 \
    --data-urlencode "WebIdentityToken=${ID_TOKEN}" \
    "$@"
}

GOOD_STATUS="$(sts_request sts-good --data-urlencode "WebIdentityAccessToken=${ACCESS_TOKEN}")"
[[ "${GOOD_STATUS}" == 200 ]] || {
  cat "${WORK_DIR}/sts-good.xml" >&2
  cat "${WORK_DIR}/rustfs.log" >&2
  echo "expected AssumeRoleWithWebIdentity with WebIdentityAccessToken to return HTTP 200, got ${GOOD_STATUS}" >&2
  exit 1
}
grep -Eiq '^content-type: application/xml' "${WORK_DIR}/sts-good.headers"

IFS=$'\t' read -r STS_ACCESS_KEY STS_SECRET_KEY STS_SESSION_TOKEN < <(
  python3 - "${WORK_DIR}/sts-good.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
values = {}
for element in root.iter():
    values[element.tag.rsplit("}", 1)[-1]] = element.text or ""
for field in ("AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration", "SubjectFromWebIdentityToken"):
    assert values.get(field), f"missing required STS field: {field}"
print("\t".join(values[field] for field in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
PY
)
[[ -n "${STS_ACCESS_KEY}" && -n "${STS_SECRET_KEY}" && -n "${STS_SESSION_TOKEN}" ]] || {
  cat "${WORK_DIR}/sts-good.xml" >&2
  echo "AssumeRoleWithWebIdentity response did not carry a complete credential set" >&2
  exit 1
}

sts_aws() {
  env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
    AWS_ACCESS_KEY_ID="${STS_ACCESS_KEY}" \
    AWS_SECRET_ACCESS_KEY="${STS_SECRET_KEY}" \
    AWS_SESSION_TOKEN="${STS_SESSION_TOKEN}" \
    AWS_EC2_METADATA_DISABLED=true \
    aws --endpoint-url "${RUSTFS_ORIGIN}" --region us-east-1 "$@"
}

# Allowed by demo-project:member: list and read the pilot bucket.
sts_aws s3api list-objects-v2 --bucket "${PILOT_BUCKET}" --output json >"${WORK_DIR}/list-objects.json"
grep -q '"Key": "hello.txt"' "${WORK_DIR}/list-objects.json"
sts_aws s3api get-object --bucket "${PILOT_BUCKET}" --key hello.txt "${WORK_DIR}/hello-downloaded.txt" >/dev/null
cmp -s "${WORK_DIR}/hello.txt" "${WORK_DIR}/hello-downloaded.txt"

# Denied: the mapped policy has no s3:PutObject.
if sts_aws s3api put-object --bucket "${PILOT_BUCKET}" --key denied.txt --body "${WORK_DIR}/hello.txt" >"${WORK_DIR}/put-denied.out" 2>&1; then
  cat "${WORK_DIR}/put-denied.out" >&2
  echo "expected PutObject with the mapped read-only policy to be denied" >&2
  exit 1
fi
grep -q 'AccessDenied' "${WORK_DIR}/put-denied.out"

# Without WebIdentityAccessToken the provider cannot read UserInfo: a 400 naming the missing
# parameter (nothing was verified against the caller), never a half-authorized session.
MISSING_STATUS="$(sts_request sts-missing)"
[[ "${MISSING_STATUS}" == 400 ]] || {
  cat "${WORK_DIR}/sts-missing.xml" >&2
  echo "expected AssumeRoleWithWebIdentity without WebIdentityAccessToken to return HTTP 400, got ${MISSING_STATUS}" >&2
  exit 1
}
grep -q 'WebIdentityAccessToken is required' "${WORK_DIR}/sts-missing.xml"

# The debug diagnostics name the policies bound to the session.
MAPPED_LINE="$(grep -F 'AssumeRoleWithWebIdentity mapped OIDC policies and groups' "${WORK_DIR}/rustfs.log" | grep -F "${MAPPED_POLICY}" | head -1 || true)"
[[ -n "${MAPPED_LINE}" ]] || {
  echo "rustfs.log has no 'AssumeRoleWithWebIdentity mapped OIDC policies and groups' line naming ${MAPPED_POLICY}" >&2
  exit 1
}
echo "${MAPPED_LINE}"
echo "OIDC Keycloak UserInfo live gate passed"
