#!/bin/sh
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

# wait-and-start.sh
# Ensures /data/rustfs{1..4} exist, waits up to 120s for peers (best-effort),
# then executes CMD or falls back to RUSTFS_CMD.
# Avoids circular wait by continuing after timeout.

set -e

echo "========================================="
echo "RustFS MNMD Startup Coordination"
echo "========================================="

# 1. Ensure data directories exist
echo "Step 1: Creating data directories..."
for i in 1 2 3 4; do
  DIR="/data/rustfs${i}"
  if [ ! -d "$DIR" ]; then
    echo "  Creating $DIR"
    mkdir -p "$DIR"
  else
    echo "  $DIR already exists"
  fi
done
echo "Data directories ready."
echo ""

# 2. Wait for peer nodes (best-effort, skip self)
echo "Step 2: Waiting for peer nodes (max 120s)..."
HOSTNAME=$(hostname)
TIMEOUT=120
ELAPSED=0
WAIT_INTERVAL=2

# Extract node names from RUSTFS_VOLUMES if available
# Expected format: http://rustfs-node{1...4}:9000/data/rustfs{1...4}
PEERS=""
if [ -n "$RUSTFS_VOLUMES" ]; then
  # Simple extraction - look for rustfs-node followed by a number
  for i in 1 2 3 4; do
    NODE="rustfs-node${i}"
    if [ "$NODE" != "$HOSTNAME" ]; then
      PEERS="$PEERS $NODE"
    fi
  done
fi

# If we couldn't extract peers, use default list
if [ -z "$PEERS" ]; then
  PEERS="rustfs-node1 rustfs-node2 rustfs-node3 rustfs-node4"
  # Remove self from the list
  PEERS=$(echo "$PEERS" | sed "s/$HOSTNAME//g")
fi

echo "  Current node: $HOSTNAME"
echo "  Peers to check: $PEERS"

# Check each peer
for PEER in $PEERS; do
  echo "  Checking $PEER..."
  PEER_ELAPSED=0
  
  while [ $PEER_ELAPSED -lt $TIMEOUT ]; do
    # Try to resolve the hostname first
    if nslookup "$PEER" >/dev/null 2>&1 || getent hosts "$PEER" >/dev/null 2>&1; then
      # Check if port 9000 is accessible
      if nc -z "$PEER" 9000 2>/dev/null; then
        echo "    ✓ $PEER is ready"
        break
      fi
    fi
    
    PEER_ELAPSED=$((PEER_ELAPSED + WAIT_INTERVAL))
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
    
    if [ $PEER_ELAPSED -ge $TIMEOUT ]; then
      echo "    ⚠ $PEER timeout after ${TIMEOUT}s (continuing anyway)"
      break
    fi
    
    if [ $ELAPSED -ge $TIMEOUT ]; then
      echo "    ⚠ Overall timeout reached after ${TIMEOUT}s (continuing anyway)"
      break 2
    fi
    
    sleep $WAIT_INTERVAL
  done
done

echo "Peer check completed after ${ELAPSED}s."
echo ""

# 3. Execute the command
echo "Step 3: Starting RustFS..."

# Determine what command to run
if [ $# -gt 0 ]; then
  # Use provided arguments
  CMD="$@"
  echo "  Using provided command: $CMD"
else
  # Fall back to RUSTFS_CMD environment variable or default
  CMD="${RUSTFS_CMD:-rustfs}"
  echo "  Using fallback command: $CMD"
fi

echo ""
echo "========================================="
echo "Executing: $CMD"
echo "========================================="
echo ""

# Execute the command
exec $CMD
