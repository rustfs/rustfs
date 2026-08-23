#!/usr/bin/env bash
set -euo pipefail

# Run the ignored RustFS KMS failover test against an ephemeral, official
# three-node Vault cluster using integrated Raft storage. The active container
# is killed only after KV2 and Transit decrypt loops report ready.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VAULT_IMAGE="${RUSTFS_TEST_VAULT_IMAGE:-hashicorp/vault:1.17.6}"

for command in docker jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the live Vault HA test" >&2
    exit 1
  fi
done
if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available" >&2
  exit 1
fi
if ! docker image inspect "$VAULT_IMAGE" >/dev/null 2>&1; then
  docker pull "$VAULT_IMAGE" >/dev/null
fi

TMP_DIR="$(mktemp -d -t rustfs-vault-ha-live.XXXXXX)"
RUN_ID="rustfs-kms-ha-$$-${RANDOM}"
NETWORK="${RUN_ID}"
MARKER="${TMP_DIR}/failover-ready"
ROOT_TOKEN=""
UNSEAL_KEY=""
TEST_PID=""
declare -a NODES=("${RUN_ID}-1" "${RUN_ID}-2" "${RUN_ID}-3")

cleanup() {
  if [[ -n "$TEST_PID" ]]; then
    kill "$TEST_PID" 2>/dev/null || true
    wait "$TEST_PID" 2>/dev/null || true
  fi
  for node in "${NODES[@]}"; do
    docker rm -f "$node" >/dev/null 2>&1 || true
  done
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

docker network create "$NETWORK" >/dev/null

start_node() {
  local index="$1"
  local node="${NODES[$((index - 1))]}"
  local config
  config="$(jq -cn \
    --arg api "http://${node}:8200" \
    --arg cluster "http://${node}:8201" \
    --arg node_id "node${index}" \
    '{
      ui: false,
      disable_mlock: true,
      api_addr: $api,
      cluster_addr: $cluster,
      listener: [{tcp: {
        address: "0.0.0.0:8200",
        cluster_address: "0.0.0.0:8201",
        tls_disable: true
      }}],
      storage: {raft: {path: "/vault/file", node_id: $node_id, performance_multiplier: 1}}
    }')"
  docker run -d \
    --name "$node" \
    --network "$NETWORK" \
    --cap-add IPC_LOCK \
    -p 127.0.0.1::8200 \
    -e VAULT_LOCAL_CONFIG="$config" \
    "$VAULT_IMAGE" server >/dev/null
}

vault_exec() {
  local node="$1"
  shift
  docker exec \
    -e VAULT_ADDR=http://127.0.0.1:8200 \
    -e VAULT_TOKEN="$ROOT_TOKEN" \
    "$node" vault "$@"
}

wait_started() {
  local node="$1"
  local status
  for _ in $(seq 1 120); do
    status="$(docker exec -e VAULT_ADDR=http://127.0.0.1:8200 "$node" vault status -format=json 2>/dev/null || true)"
    if jq -e '.initialized == false or .sealed == true or .sealed == false' >/dev/null 2>&1 <<<"$status"; then
      return 0
    fi
    sleep 0.25
  done
  echo "$node did not start" >&2
  docker logs "$node" >&2 || true
  return 1
}

active_node() {
  local node status address
  for node in "${NODES[@]}"; do
    if ! docker inspect "$node" >/dev/null 2>&1; then
      continue
    fi
    status="$(docker exec -e VAULT_ADDR=http://127.0.0.1:8200 "$node" vault status -format=json 2>/dev/null || true)"
    address="http://${node}:8200"
    if jq -e --arg address "$address" \
      '.ha_enabled == true and .sealed == false and (.is_self == true or (.leader_address == $address and .active_time != "0001-01-01T00:00:00Z"))' \
      >/dev/null 2>&1 <<<"$status"; then
      printf '%s\n' "$node"
      return 0
    fi
  done
  return 1
}

host_address() {
  local node="$1"
  local port
  port="$(docker port "$node" 8200/tcp | awk -F: 'NR == 1 {print $NF}')"
  if [[ ! "$port" =~ ^[0-9]+$ ]]; then
    echo "failed to resolve host port for $node" >&2
    return 1
  fi
  printf 'http://127.0.0.1:%s\n' "$port"
}

for index in 1 2 3; do
  start_node "$index"
  wait_started "${NODES[$((index - 1))]}"
done

INIT_JSON="$(docker exec -e VAULT_ADDR=http://127.0.0.1:8200 "${NODES[0]}" vault operator init -format=json -key-shares=1 -key-threshold=1)"
ROOT_TOKEN="$(jq -r '.root_token' <<<"$INIT_JSON")"
UNSEAL_KEY="$(jq -r '.unseal_keys_b64[0]' <<<"$INIT_JSON")"
if [[ -z "$ROOT_TOKEN" || -z "$UNSEAL_KEY" || "$ROOT_TOKEN" == null || "$UNSEAL_KEY" == null ]]; then
  echo "Vault initialization did not return the expected credentials" >&2
  exit 1
fi

vault_exec "${NODES[0]}" operator unseal "$UNSEAL_KEY" >/dev/null
for node in "${NODES[@]:1}"; do
  vault_exec "$node" operator raft join "http://${NODES[0]}:8200" >/dev/null
  vault_exec "$node" operator unseal "$UNSEAL_KEY" >/dev/null
done

for _ in $(seq 1 240); do
  if vault_exec "${NODES[0]}" operator raft list-peers -format=json 2>/dev/null \
    | jq -e '.data.config.servers | length == 3 and all(.[]; .voter == true)' >/dev/null; then
    break
  fi
  sleep 0.25
done
if ! vault_exec "${NODES[0]}" operator raft list-peers -format=json \
  | jq -e '.data.config.servers | length == 3 and all(.[]; .voter == true)' >/dev/null; then
  echo "Vault Raft cluster did not stabilize with three voters" >&2
  vault_exec "${NODES[0]}" operator raft list-peers -format=json >&2 || true
  exit 1
fi

OLD_LEADER="$(active_node)"
if [[ -z "$OLD_LEADER" ]]; then
  echo "Vault did not elect an active node" >&2
  exit 1
fi
STANDBY=""
for node in "${NODES[@]}"; do
  if [[ "$node" != "$OLD_LEADER" ]]; then
    STANDBY="$node"
    break
  fi
done
VAULT_ADDR="$(host_address "$STANDBY")"

vault_exec "$OLD_LEADER" audit enable file file_path=/tmp/vault-audit.log >/dev/null
vault_exec "$OLD_LEADER" secrets enable -path=secret kv-v2 >/dev/null
vault_exec "$OLD_LEADER" secrets enable -path=transit transit >/dev/null
vault_exec "$OLD_LEADER" auth enable approle >/dev/null

POLICY_NAME="rustfs-kms-ha"
ROLE_NAME="rustfs-kms-ha"
POLICY_FILE="${TMP_DIR}/kms-policy.hcl"
cat >"$POLICY_FILE" <<'EOF'
path "secret/data/rustfs/kms/ha-kv2/*" {
  capabilities = ["create", "read", "update"]
}
path "secret/metadata/rustfs/kms/ha-kv2/*" {
  capabilities = ["list", "read", "delete"]
}
path "secret/metadata/rustfs/kms/ha-kv2" {
  capabilities = ["list"]
}
path "secret/data/rustfs/kms/ha-transit-metadata/*" {
  capabilities = ["create", "read", "update"]
}
path "secret/metadata/rustfs/kms/ha-transit-metadata/*" {
  capabilities = ["list", "read", "delete"]
}
path "secret/metadata/rustfs/kms/ha-transit-metadata" {
  capabilities = ["list"]
}
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
docker cp "$POLICY_FILE" "${OLD_LEADER}:/tmp/kms-policy.hcl"
vault_exec "$OLD_LEADER" policy write "$POLICY_NAME" /tmp/kms-policy.hcl >/dev/null
vault_exec "$OLD_LEADER" write "auth/approle/role/${ROLE_NAME}" \
  token_policies="$POLICY_NAME" \
  token_ttl=10m \
  token_max_ttl=20m \
  secret_id_ttl=30m \
  secret_id_num_uses=0 \
  >/dev/null
ROLE_ID="$(vault_exec "$OLD_LEADER" read -field=role_id "auth/approle/role/${ROLE_NAME}/role-id")"
SECRET_ID="$(vault_exec "$OLD_LEADER" write -f -field=secret_id "auth/approle/role/${ROLE_NAME}/secret-id")"

cd "$PROJECT_ROOT"
cargo test -p rustfs-kms --test vault_ha_failover_live --no-run
env \
  -u RUSTFS_KMS_VAULT_TOKEN \
  -u RUSTFS_KMS_VAULT_TOKEN_FILE \
  HTTP_PROXY= HTTPS_PROXY= http_proxy= https_proxy= \
  NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
  RUSTFS_TEST_VAULT_ADDRESS="$VAULT_ADDR" \
  RUSTFS_TEST_VAULT_ROLE_ID="$ROLE_ID" \
  RUSTFS_TEST_VAULT_SECRET_ID="$SECRET_ID" \
  RUSTFS_TEST_VAULT_FAILOVER_MARKER="$MARKER" \
  RUSTFS_TEST_VAULT_OLD_LEADER="$OLD_LEADER" \
  cargo test -p rustfs-kms --test vault_ha_failover_live \
    vault_raft_leader_failure_recovers_kv2_and_transit_decrypts -- \
    --ignored --nocapture --test-threads=1 &
TEST_PID=$!

for _ in $(seq 1 240); do
  if [[ -f "$MARKER" ]]; then
    break
  fi
  if ! kill -0 "$TEST_PID" 2>/dev/null; then
    wait "$TEST_PID"
  fi
  sleep 0.25
done
if [[ ! -f "$MARKER" ]]; then
  echo "KMS live test did not reach failover readiness" >&2
  exit 1
fi

docker kill "$OLD_LEADER" >/dev/null
NEW_LEADER=""
for _ in $(seq 1 240); do
  NEW_LEADER="$(active_node || true)"
  if [[ -n "$NEW_LEADER" && "$NEW_LEADER" != "$OLD_LEADER" ]]; then
    break
  fi
  sleep 0.25
done
if [[ -z "$NEW_LEADER" || "$NEW_LEADER" == "$OLD_LEADER" ]]; then
  echo "Vault did not elect a replacement leader after killing $OLD_LEADER" >&2
  exit 1
fi
printf '%s' "$NEW_LEADER" >"${MARKER%/*}/failover-ready.elected"

wait "$TEST_PID"
TEST_PID=""

for index in 1 2 3; do
  docker cp "${NODES[$((index - 1))]}:/tmp/vault-audit.log" "${TMP_DIR}/vault-audit-${index}.log" >/dev/null 2>&1 || true
done
APPROLE_LOGINS="$(jq -s '[.[] | select(.type == "request" and .request.path == "auth/approle/login" and .request.operation == "update")] | length' \
  "${TMP_DIR}"/vault-audit-*.log 2>/dev/null || true)"
if [[ "$APPROLE_LOGINS" != 2 ]]; then
  echo "expected exactly two AppRole logins (one per backend), got ${APPROLE_LOGINS:-unavailable}" >&2
  exit 1
fi

echo "Vault HA failover passed: 3 Raft voters, ${OLD_LEADER} killed, ${NEW_LEADER} elected, 2 AppRole logins"
