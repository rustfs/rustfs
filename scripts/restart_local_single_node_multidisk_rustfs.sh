#!/usr/bin/env bash
set -euo pipefail

PIDFILE=/private/tmp/issue708-single-node-multidisk/rustfs.pid
LOGFILE=/private/tmp/issue708-single-node-multidisk/rustfs.log

mkdir -p /private/tmp/issue708-single-node-multidisk/{d1,d2,d3,d4}

if [[ -f "$PIDFILE" ]]; then
  old_pid="$(cat "$PIDFILE")"
  if kill -0 "$old_pid" >/dev/null 2>&1; then
    kill "$old_pid" >/dev/null 2>&1 || true
    sleep 2
  fi
fi

if [[ -n "${RUSTFS_TUNING_ENV_FILE:-}" && -f "${RUSTFS_TUNING_ENV_FILE:-}" ]]; then
  set -a
  source "$RUSTFS_TUNING_ENV_FILE"
  set +a
fi

nohup env \
  RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
  RUSTFS_ADDRESS=127.0.0.1:9000 \
  RUSTFS_ACCESS_KEY=rustfsadmin \
  RUSTFS_SECRET_KEY=rustfsadmin \
  RUSTFS_RPC_SECRET=rustfs-rpc-secret \
  RUSTFS_REGION=us-east-1 \
  RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318 \
  target/debug/rustfs server \
  /private/tmp/issue708-single-node-multidisk/d1 \
  /private/tmp/issue708-single-node-multidisk/d2 \
  /private/tmp/issue708-single-node-multidisk/d3 \
  /private/tmp/issue708-single-node-multidisk/d4 \
  >"$LOGFILE" 2>&1 &

new_pid=$!
echo "$new_pid" > "$PIDFILE"
sleep 5
curl -fsS http://127.0.0.1:9000/health >/dev/null
