#!/usr/bin/env bash
set -euo pipefail

# Start an ephemeral Vault, issue a least-privilege AppRole, and run the two
# ignored RustFS KMS checks without ever exposing the generated credentials.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VAULT_BIN="${RUSTFS_TEST_VAULT_BIN:-vault}"

if ! command -v "$VAULT_BIN" >/dev/null 2>&1; then
  echo "Vault binary not found; set RUSTFS_TEST_VAULT_BIN or install vault" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to allocate an ephemeral loopback port" >&2
  exit 1
fi
TMP_DIR="$(mktemp -d -t rustfs-vault-approle-live.XXXXXX)"
VAULT_PID=""

cleanup() {
  if [[ -n "$VAULT_PID" ]]; then
    kill "$VAULT_PID" 2>/dev/null || true
    wait "$VAULT_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

VAULT_PORT="$(python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"
VAULT_ADDR="http://127.0.0.1:${VAULT_PORT}"
ROOT_TOKEN="rustfs-approle-live-root-$$-${RANDOM}"
INHERITED_NO_PROXY="${NO_PROXY:-}"
INHERITED_NO_PROXY_LOWER="${no_proxy:-}"
if [[ -n "$INHERITED_NO_PROXY" ]]; then
  LOCAL_NO_PROXY="${INHERITED_NO_PROXY},127.0.0.1,localhost"
else
  LOCAL_NO_PROXY="127.0.0.1,localhost"
fi
if [[ -n "$INHERITED_NO_PROXY_LOWER" ]]; then
  LOCAL_NO_PROXY_LOWER="${INHERITED_NO_PROXY_LOWER},127.0.0.1,localhost"
else
  LOCAL_NO_PROXY_LOWER="$LOCAL_NO_PROXY"
fi

"$VAULT_BIN" server \
  -dev \
  -dev-root-token-id "$ROOT_TOKEN" \
  -dev-listen-address "127.0.0.1:${VAULT_PORT}" \
  >"${TMP_DIR}/vault.log" 2>&1 &
VAULT_PID=$!

vault_cli() {
  NO_PROXY="$LOCAL_NO_PROXY" no_proxy="$LOCAL_NO_PROXY_LOWER" \
    VAULT_ADDR="$VAULT_ADDR" VAULT_TOKEN="$ROOT_TOKEN" "$VAULT_BIN" "$@"
}

ready=0
for _ in $(seq 1 120); do
  if vault_cli status >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.25
done
if [[ "$ready" != 1 ]]; then
  echo "Vault did not become ready" >&2
  exit 1
fi

vault_cli secrets enable -path=transit transit >/dev/null
vault_cli auth enable approle >/dev/null

POLICY_NAME="rustfs-kms-live-$$"
ROLE_NAME="rustfs-kms-live-$$"
POLICY_FILE="${TMP_DIR}/policy.hcl"
cat >"$POLICY_FILE" <<'EOF'
# KV2 master-key records.
path "secret/data/rustfs/kms/keys/*" {
  capabilities = ["create", "read", "update"]
}
path "secret/metadata/rustfs/kms/keys/*" {
  capabilities = ["list", "read", "delete"]
}
path "secret/metadata/rustfs/kms/keys" {
  capabilities = ["list"]
}

# Transit metadata uses the default RustFS KV2 path.
path "secret/data/rustfs/kms/transit-metadata/*" {
  capabilities = ["create", "read", "update"]
}
path "secret/metadata/rustfs/kms/transit-metadata/*" {
  capabilities = ["list", "read", "delete"]
}
path "secret/metadata/rustfs/kms/transit-metadata" {
  capabilities = ["list"]
}

# Transit key lifecycle and data-path operations.
path "transit/keys" {
  capabilities = ["list"]
}
path "transit/keys/*" {
  capabilities = ["create", "read", "update"]
}
path "transit/encrypt/*" {
  capabilities = ["update"]
}
path "transit/decrypt/*" {
  capabilities = ["update"]
}
EOF

vault_cli policy write "$POLICY_NAME" "$POLICY_FILE" >/dev/null
vault_cli write "auth/approle/role/${ROLE_NAME}" \
  token_policies="$POLICY_NAME" \
  token_ttl=10m \
  token_max_ttl=20m \
  secret_id_ttl=30m \
  secret_id_num_uses=0 \
  >/dev/null

ROLE_ID="$(vault_cli read -field=role_id "auth/approle/role/${ROLE_NAME}/role-id")"
SECRET_ID="$(vault_cli write -f -field=secret_id "auth/approle/role/${ROLE_NAME}/secret-id")"

run_live_test() {
  local backend="$1"
  local test_name="$2"

  local -a backend_env=(
    -u RUSTFS_KMS_BACKEND
    -u RUSTFS_KMS_VAULT_ADDRESS
    -u RUSTFS_KMS_VAULT_NAMESPACE
    -u RUSTFS_KMS_VAULT_SKIP_TLS_VERIFY
    -u RUSTFS_KMS_VAULT_MOUNT_PATH
    -u RUSTFS_KMS_VAULT_KV_MOUNT
    -u RUSTFS_KMS_VAULT_KEY_PREFIX
    -u RUSTFS_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT
    -u RUSTFS_KMS_VAULT_TRANSIT_METADATA_PREFIX
    -u RUSTFS_KMS_VAULT_TOKEN
    -u RUSTFS_KMS_VAULT_TOKEN_FILE
    -u RUSTFS_KMS_VAULT_APPROLE_ROLE_ID
    -u RUSTFS_KMS_VAULT_APPROLE_SECRET_ID
    -u RUSTFS_KMS_VAULT_APPROLE_SECRET_ID_FILE
    -u RUSTFS_KMS_VAULT_APPROLE_MOUNT
    -u RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS
    -u RUSTFS_KMS_TIMEOUT_SECS
    -u RUSTFS_KMS_RETRY_ATTEMPTS
    -u RUSTFS_KMS_ENABLE_CACHE
    -u NO_PROXY
    -u no_proxy
    RUSTFS_KMS_BACKEND="$backend"
    RUSTFS_KMS_VAULT_ADDRESS="$VAULT_ADDR"
    RUSTFS_KMS_VAULT_APPROLE_ROLE_ID="$ROLE_ID"
    RUSTFS_KMS_VAULT_APPROLE_SECRET_ID="$SECRET_ID"
    RUSTFS_KMS_VAULT_APPROLE_MOUNT=approle
    RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true
    RUSTFS_KMS_TIMEOUT_SECS=10
    RUSTFS_KMS_RETRY_ATTEMPTS=1
    RUSTFS_KMS_ENABLE_CACHE=false
    NO_PROXY="$LOCAL_NO_PROXY"
    no_proxy="$LOCAL_NO_PROXY_LOWER"
  )

  if [[ "$backend" == vault ]]; then
    backend_env+=(
      RUSTFS_KMS_VAULT_KV_MOUNT=secret
      RUSTFS_KMS_VAULT_KEY_PREFIX=rustfs/kms/keys
    )
  else
    backend_env+=(
      RUSTFS_KMS_VAULT_MOUNT_PATH=transit
      RUSTFS_KMS_VAULT_TRANSIT_METADATA_KV_MOUNT=secret
      RUSTFS_KMS_VAULT_TRANSIT_METADATA_PREFIX=rustfs/kms/transit-metadata
    )
  fi

  env \
    "${backend_env[@]}" \
    cargo test -p rustfs-kms --test vault_approle_live "$test_name" -- \
      --ignored --nocapture --test-threads=1
}

cd "$PROJECT_ROOT"
run_live_test vault vault_kv2_approle_auth_live
run_live_test vault-transit vault_transit_approle_auth_live
echo "Vault AppRole KV2 and Transit live checks passed"
