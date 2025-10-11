#!/usr/bin/env sh
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

set -e

# current hostname
SELF="$(hostname -s 2>/dev/null || hostname)"

# Confirm that the local data directory exists
for d in /data/rustfs1 /data/rustfs2 /data/rustfs3 /data/rustfs4; do
  if [ ! -d "$d" ]; then
    echo "[wait] missing data dir: $d"
    exit 1
  fi
done

# Optional: Wait for the peer port to be connectable (up to 120 seconds per peer, continue after timeout to avoid cluster waiting)
PEERS="rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4"
for h in $PEERS; do
  # skip waiting for itself
  if [ "$h" = "$SELF" ]; then
    continue
  fi
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

# Execute the incoming CMD first (the default CMD of the image will be passed in as a parameter)
if [ "$#" -gt 0 ]; then
  exec "$@"
fi

# If no CMD is passed in, fallback to the environment variable RUSTFS_CMD
if [ -n "$RUSTFS_CMD" ]; then
  # Use shell to parse string commands, allowing parameters to be included
  exec /bin/sh -lc "$RUSTFS_CMD"
fi

echo "[wait] no CMD provided and RUSTFS_CMD is empty; nothing to run"
exit 127
