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
"$RUNNER" --stage p2 --phase request-only --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase canary --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p2 --phase rollback --out-root "$TMP_DIR" --dry-run >/dev/null

BEFORE_ENV="${TMP_DIR}/p2-before/server-env.sh"
REQUEST_ONLY_ENV="${TMP_DIR}/p2-request-only/server-env.sh"
CANARY_ENV="${TMP_DIR}/p2-canary/server-env.sh"
AFTER_ENV="${TMP_DIR}/p2-after/server-env.sh"
ROLLBACK_ENV="${TMP_DIR}/p2-rollback/server-env.sh"
P0_BEFORE_ENV="${TMP_DIR}/p0-before/server-env.sh"
P0_AFTER_ENV="${TMP_DIR}/p0-after/server-env.sh"
P1_BEFORE_ENV="${TMP_DIR}/p1-before/server-env.sh"
P1_AFTER_ENV="${TMP_DIR}/p1-after/server-env.sh"
P3_BEFORE_ENV="${TMP_DIR}/p3-before/server-env.sh"
P3_AFTER_ENV="${TMP_DIR}/p3-after/server-env.sh"

rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$REQUEST_ONLY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$REQUEST_ONLY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$CANARY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$CANARY_ENV"
rg -q 'selected canary RustFS node' "$CANARY_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' "$ROLLBACK_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false' "$ROLLBACK_ENV"
"$RUNNER" --stage p0 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p0 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p1 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p1 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p3 --phase before --out-root "$TMP_DIR" --dry-run >/dev/null
"$RUNNER" --stage p3 --phase after --out-root "$TMP_DIR" --dry-run >/dev/null

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' "$BEFORE_ENV"; then
  echo "p2 before must not request msgpack-only" >&2
  exit 1
fi

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$BEFORE_ENV"; then
  echo "p2 before must not confirm fleet msgpack-only" >&2
  exit 1
fi

if grep -q 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true' "$REQUEST_ONLY_ENV"; then
  echo "p2 request-only must not confirm fleet msgpack-only" >&2
  exit 1
fi

if "$RUNNER" --stage p1 --phase canary --out-root "$TMP_DIR" --dry-run >/dev/null 2>&1; then
  echo "expected canary phase to be p2-only" >&2
  exit 1
fi

rg -qx 'export RUSTFS_INTERNODE_RPC_TCP_NODELAY=false' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE=0' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE=0' "$P0_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE=4194304' "$P0_BEFORE_ENV"
rg -qx '# \(cluster defaults — no overrides needed for this stage/phase\)' "$P0_AFTER_ENV"

rg -qx 'export RUSTFS_INTERNODE_CHANNEL_ISOLATION=false' "$P1_BEFORE_ENV"
rg -qx 'export RUSTFS_INTERNODE_CHANNEL_ISOLATION=true' "$P1_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_BULK_CHANNELS=2' "$P1_AFTER_ENV"
if rg -q 'export RUSTFS_INTERNODE_BULK_CHANNELS=' "$P1_BEFORE_ENV"; then
  echo "p1 before should not pin an explicit bulk channel count" >&2
  exit 1
fi

rg -qx 'export RUSTFS_INTERNODE_PREWARM=true' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_BYPASS=true' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS=5' "$P3_AFTER_ENV"
rg -qx 'export RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD=3' "$P3_AFTER_ENV"

echo "internode grpc A/B bench env tests passed"
