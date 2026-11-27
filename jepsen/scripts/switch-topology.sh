#!/bin/bash
# Bring up one topology and make sure the other is gone.
# Usage, from anywhere: jepsen/scripts/switch-topology.sh 4x4
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
requested=${1:?usage: switch-topology.sh 4x4|3x2}
case "$requested" in
  4x4)
    this="$root/docker/compose-4x4.yaml"
    other="$root/docker/compose-3x2.yaml"
    ;;
  3x2)
    this="$root/docker/compose-3x2.yaml"
    other="$root/docker/compose-4x4.yaml"
    ;;
  *)
    echo "topology must be 4x4 or 3x2" >&2
    exit 2
    ;;
esac

if [ -z "${JEPSEN_SSH_PUBKEY:-}" ]; then
  echo "export JEPSEN_SSH_PUBKEY to one public-key line before starting nodes." >&2
  exit 2
fi

echo "Stopping the other Jepsen topology and both smoke stacks."
docker compose -f "$other" down -v --remove-orphans
docker compose -f "$root/docker/smoke-4x4.yaml" down -v --remove-orphans
docker compose -f "$root/docker/smoke-3x2.yaml" down -v --remove-orphans

echo "Starting ${requested}. Memory cap is 2 GiB per container. Do not start the other topology until this one is down."
docker compose -f "$this" up -d --build
docker compose -f "$this" ps
