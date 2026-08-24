#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUSTFS_BINARY="${1:-${ROOT_DIR}/target/debug/rustfs}"
REALM_FIXTURE="${ROOT_DIR}/scripts/test/fixtures/keycloak-rustfs-ci-realm.json"
KEYCLOAK_IMAGE="${KEYCLOAK_IMAGE:-quay.io/keycloak/keycloak@sha256:6a7217a100bd3e5de4063a27a538ef999a3c5a88c4b4ec0ffc0a642aee7b2597}"
WORK_DIR="${RUNNER_TEMP:-/tmp}/rustfs-keycloak-live-${$}"
KEYCLOAK_CONTAINER="rustfs-keycloak-live-${$}"
RUSTFS_PID=""
mkdir -p "${WORK_DIR}"

cleanup() {
  local status=$?
  if [[ -n "${RUSTFS_PID}" ]] && kill -0 "${RUSTFS_PID}" 2>/dev/null; then
    kill "${RUSTFS_PID}" 2>/dev/null || true
    wait "${RUSTFS_PID}" 2>/dev/null || true
  fi
  if [[ "${status}" -ne 0 ]]; then
    docker logs "${KEYCLOAK_CONTAINER}" >"${WORK_DIR}/keycloak.log" 2>&1 || true
    echo "live-gate logs retained in ${WORK_DIR}" >&2
  fi
  docker rm -f "${KEYCLOAK_CONTAINER}" >/dev/null 2>&1 || true
  if [[ "${status}" -eq 0 ]]; then
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT INT TERM

for command in curl docker python3 awscurl; do
  command -v "${command}" >/dev/null || {
    echo "missing required command: ${command}" >&2
    exit 1
  }
done
[[ -x "${RUSTFS_BINARY}" ]] || {
  echo "RustFS binary is not executable: ${RUSTFS_BINARY}" >&2
  exit 1
}
[[ -f "${REALM_FIXTURE}" ]] || {
  echo "Keycloak realm fixture is missing: ${REALM_FIXTURE}" >&2
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

KEYCLOAK_PORT="$(free_port)"
RUSTFS_PORT="$(free_port)"
KEYCLOAK_ORIGIN="http://127.0.0.1:${KEYCLOAK_PORT}"
ISSUER="${KEYCLOAK_ORIGIN}/realms/rustfs-ci"
DISCOVERY_URL="${ISSUER}/.well-known/openid-configuration"
RUSTFS_ORIGIN="http://127.0.0.1:${RUSTFS_PORT}"

docker run --detach --rm \
  --name "${KEYCLOAK_CONTAINER}" \
  --memory 1g \
  --publish "127.0.0.1:${KEYCLOAK_PORT}:8080" \
  --env KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  --env KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  --env KC_HEALTH_ENABLED=true \
  --env "KC_HOSTNAME=${KEYCLOAK_ORIGIN}" \
  --volume "${REALM_FIXTURE}:/opt/keycloak/data/import/rustfs-ci-realm.json:ro" \
  "${KEYCLOAK_IMAGE}" start-dev --import-realm >/dev/null

for _ in $(seq 1 120); do
  if curl --noproxy '*' -fsS "${DISCOVERY_URL}" >"${WORK_DIR}/discovery.json" 2>/dev/null; then
    break
  fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "${KEYCLOAK_CONTAINER}" 2>/dev/null || true)" != "true" ]]; then
    docker logs "${KEYCLOAK_CONTAINER}" >&2 || true
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
expected_issuer = sys.argv[2]
assert discovery.get("issuer") == expected_issuer, discovery
for field in ("token_endpoint", "jwks_uri"):
    assert discovery.get(field), discovery
PY

mkdir -p "${WORK_DIR}/rustfs-data"
env \
  NO_PROXY=127.0.0.1,localhost \
  RUSTFS_ACCESS_KEY=rustfsadmin \
  RUSTFS_SECRET_KEY=rustfsadmin \
  RUSTFS_OUTBOUND_ALLOW_ORIGINS="${KEYCLOAK_ORIGIN}" \
  RUSTFS_IDENTITY_OPENID_ENABLE=on \
  RUSTFS_IDENTITY_OPENID_CONFIG_URL="${DISCOVERY_URL}" \
  RUSTFS_IDENTITY_OPENID_ISSUER="${ISSUER}" \
  RUSTFS_IDENTITY_OPENID_CLIENT_ID=rustfs-ci \
  RUSTFS_IDENTITY_OPENID_CLIENT_SECRET=rustfs-ci-secret \
  RUSTFS_IDENTITY_OPENID_SCOPES=openid,profile,email \
  RUSTFS_IDENTITY_OPENID_ROLE_POLICY=consoleAdmin \
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

token_for_client() {
  local client_id="$1"
  local client_secret="$2"
  local response_file="${WORK_DIR}/token-${client_id}.json"
  if ! curl --noproxy '*' -sS --fail-with-body "${ISSUER}/protocol/openid-connect/token" \
    --data-urlencode grant_type=password \
    --data-urlencode "client_id=${client_id}" \
    --data-urlencode "client_secret=${client_secret}" \
    --data-urlencode username=alice \
    --data-urlencode password=alice-password \
    --data-urlencode scope=openid \
    >"${response_file}"; then
    cat "${response_file}" >&2
    return 1
  fi
  python3 - "${response_file}" "${ISSUER}" "${client_id}" <<'PY'
import base64
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    token = json.load(source)["id_token"]
payload = token.split(".")[1]
payload += "=" * (-len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
assert claims["iss"] == sys.argv[2], claims
audience = claims["aud"]
if isinstance(audience, str):
    audience = [audience]
assert sys.argv[3] in audience, claims
print(token)
PY
}

GOOD_TOKEN="$(token_for_client rustfs-ci rustfs-ci-secret)"
GOOD_STATUS="$(curl --noproxy '*' -sS \
  -D "${WORK_DIR}/sts-good.headers" \
  -o "${WORK_DIR}/sts-good.xml" \
  -w '%{http_code}' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -X POST "${RUSTFS_ORIGIN}/" \
  --data-urlencode Action=AssumeRoleWithWebIdentity \
  --data-urlencode Version=2011-06-15 \
  --data-urlencode DurationSeconds=900 \
  --data-urlencode "WebIdentityToken=${GOOD_TOKEN}")"
[[ "${GOOD_STATUS}" == 200 ]] || {
  cat "${WORK_DIR}/sts-good.xml" >&2
  cat "${WORK_DIR}/rustfs.log" >&2
  echo "expected valid Keycloak token to return HTTP 200, got ${GOOD_STATUS}" >&2
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
    assert values.get(field), values
print("\t".join(values[field] for field in ("AccessKeyId", "SecretAccessKey", "SessionToken")))
PY
)

awscurl --fail-with-body --service s3 --region us-east-1 \
  --access_key "${STS_ACCESS_KEY}" \
  --secret_key "${STS_SECRET_KEY}" \
  --security_token "${STS_SESSION_TOKEN}" \
  "${RUSTFS_ORIGIN}/" >"${WORK_DIR}/list-buckets.xml"
grep -q '<ListAllMyBucketsResult' "${WORK_DIR}/list-buckets.xml"

TAMPERED_TOKEN="$(python3 - "${GOOD_TOKEN}" <<'PY'
import sys

parts = sys.argv[1].split(".")
assert len(parts) == 3 and parts[2]
parts[2] = ("A" if parts[2][0] != "A" else "B") + parts[2][1:]
print(".".join(parts))
PY
)"
TAMPERED_STATUS="$(curl --noproxy '*' -sS \
  -o "${WORK_DIR}/sts-tampered.xml" \
  -w '%{http_code}' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -X POST "${RUSTFS_ORIGIN}/" \
  --data-urlencode Action=AssumeRoleWithWebIdentity \
  --data-urlencode Version=2011-06-15 \
  --data-urlencode DurationSeconds=900 \
  --data-urlencode "WebIdentityToken=${TAMPERED_TOKEN}")"
[[ "${TAMPERED_STATUS}" == 403 ]] || {
  cat "${WORK_DIR}/sts-tampered.xml" >&2
  echo "expected tampered token to return HTTP 403, got ${TAMPERED_STATUS}" >&2
  exit 1
}
grep -q '<Code>AccessDenied</Code>' "${WORK_DIR}/sts-tampered.xml"

BAD_TOKEN="$(token_for_client wrong-audience wrong-audience-secret)"
BAD_STATUS="$(curl --noproxy '*' -sS \
  -o "${WORK_DIR}/sts-bad.xml" \
  -w '%{http_code}' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -X POST "${RUSTFS_ORIGIN}/" \
  --data-urlencode Action=AssumeRoleWithWebIdentity \
  --data-urlencode Version=2011-06-15 \
  --data-urlencode DurationSeconds=900 \
  --data-urlencode "WebIdentityToken=${BAD_TOKEN}")"
[[ "${BAD_STATUS}" == 403 ]] || {
  cat "${WORK_DIR}/sts-bad.xml" >&2
  echo "expected wrong-audience token to return HTTP 403, got ${BAD_STATUS}" >&2
  exit 1
}
grep -q '<Code>AccessDenied</Code>' "${WORK_DIR}/sts-bad.xml"

echo "OIDC Keycloak live gate passed"
