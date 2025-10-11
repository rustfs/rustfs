#!/usr/bin/env sh
set -e

# Ensure local data directories exist
for d in /data/rustfs1 /data/rustfs2 /data/rustfs3 /data/rustfs4; do
  if [ ! -d "$d" ]; then
    echo "[wait] missing data dir: $d"
    exit 1
  fi
done

# Optional: wait for peers to accept connections (best-effort, 120s max per peer)
PEERS="rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4"
for h in $PEERS; do
  i=0
  echo "[wait] waiting for $h:9000 ..."
  until nc -z "$h" 9000 2>/dev/null; do
    i=$((i+1))
    if [ $i -ge 120 ]; then
      echo "[wait] $h:9000 not ready after 120s, continuing..."
      break
    fi
    sleep 1
  done
done

echo "[wait] starting app..."
exec "$@"
