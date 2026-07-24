#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_internode_grpc_ab_bench.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"$RUNNER" --stage p2 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null

BEFORE_ENV="${TMP_DIR}/p2-before/server-env.sh"
AFTER_ENV="${TMP_DIR}/p2-after/server-env.sh"

rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$AFTER_ENV"

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$BEFORE_ENV"; then
  echo "p2 before must not request msgpack-only" >&2
  exit 1
fi

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$BEFORE_ENV"; then
  echo "p2 before must not confirm fleet msgpack-only" >&2
  exit 1
fi

echo "internode grpc A/B bench env tests passed"
