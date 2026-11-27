#!/bin/bash
# Run the four workloads one after another against an already started topology.
# Usage: jepsen/scripts/run-topology.sh 4x4
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
topology=${1:?usage: run-topology.sh 4x4|3x2}
case "$topology" in
  4x4) concurrency=8 ;;
  3x2) concurrency=6 ;;
  *) echo "topology must be 4x4 or 3x2" >&2; exit 2 ;;
esac

key=${JEPSEN_SSH_KEY:-$HOME/.ssh/id_ed25519}
time_limit=${JEPSEN_TIME_LIMIT:-90}
faults=${JEPSEN_FAULTS:-partition,kill,pause,packet}

cd "$root"
for workload in elle register versioned list; do
  echo "=== ${topology} ${workload} ==="
  lein run test \
    --topology "$topology" \
    --workload "$workload" \
    --time-limit "$time_limit" \
    --concurrency "$concurrency" \
    --username "${JEPSEN_SSH_USER:-jepsen}" \
    --ssh-private-key "$key" \
    --faults "$faults" \
    --rate 10
done
