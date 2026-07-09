#!/usr/bin/env bash
set -euo pipefail

PIDFILE=/private/tmp/issue708-single-node-multidisk/rustfs.pid
LOGFILE=/private/tmp/issue708-single-node-multidisk/rustfs.log
RUSTFS_CMD_PATTERN='target/debug/rustfs server /private/tmp/issue708-single-node-multidisk/d1 /private/tmp/issue708-single-node-multidisk/d2 /private/tmp/issue708-single-node-multidisk/d3 /private/tmp/issue708-single-node-multidisk/d4'

mkdir -p /private/tmp/issue708-single-node-multidisk/{d1,d2,d3,d4}

if [[ -f "$PIDFILE" ]]; then
  old_pid="$(cat "$PIDFILE")"
  if kill -0 "$old_pid" >/dev/null 2>&1; then
    kill "$old_pid" >/dev/null 2>&1 || true
    sleep 2
  fi
fi

lingering_pids="$(pgrep -f "$RUSTFS_CMD_PATTERN" || true)"
if [[ -n "$lingering_pids" ]]; then
  while IFS= read -r lingering_pid; do
    [[ -z "$lingering_pid" ]] && continue
    kill "$lingering_pid" >/dev/null 2>&1 || true
  done <<< "$lingering_pids"
  sleep 2
fi

if [[ -n "${RUSTFS_TUNING_ENV_FILE:-}" && -f "${RUSTFS_TUNING_ENV_FILE:-}" ]]; then
  set -a
  source "$RUSTFS_TUNING_ENV_FILE"
  set +a
fi

launch_rustfs() {
  if command -v setsid >/dev/null 2>&1; then
    setsid env \
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
      </dev/null >"$LOGFILE" 2>&1 &
  else
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
      </dev/null >"$LOGFILE" 2>&1 &
  fi
}

launch_rustfs

new_pid=$!
sleep 5

actual_pid="$(pgrep -f "$RUSTFS_CMD_PATTERN" | head -n 1 || true)"
if [[ -z "$actual_pid" ]]; then
  echo "failed to locate restarted rustfs process; launcher pid was $new_pid" >&2
  exit 1
fi

echo "$actual_pid" > "$PIDFILE"
curl -fsS http://127.0.0.1:9000/health >/dev/null
