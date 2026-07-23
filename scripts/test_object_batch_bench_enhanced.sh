#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_object_batch_bench_enhanced.sh"
TMP_DIR="$(mktemp -d)"
OUT_DIR="${TMP_DIR}/run"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"$RUNNER" \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key test-access \
  --secret-key test-secret \
  --sizes 1MiB \
  --rounds 1 \
  --retry-per-round 1 \
  --cooldown-secs 0 \
  --duration 1s \
  --out-dir "$OUT_DIR" \
  --warp-bin true \
  --dry-run \
  --service-metrics-dir "${OUT_DIR}/metrics" \
  --server-image-ref rustfs/rustfs:bench \
  --server-image-digest sha256:0123456789abcdef \
  --server-revision 9f61bad94 \
  --require-server-provenance \
  --label topology=4x2 \
  --label workload=get \
  --node-metrics-url node1=http://127.0.0.1:9001/metrics \
  --node-docker-container node1=rustfs-bench-1 >/dev/null

rg -qx 'server_image_ref=rustfs/rustfs:bench' "${OUT_DIR}/run_manifest.env"
rg -qx 'server_image_digest=sha256:0123456789abcdef' "${OUT_DIR}/run_manifest.env"
rg -qx 'server_revision=9f61bad94' "${OUT_DIR}/run_manifest.env"
rg -qx 'run_label_topology=4x2' "${OUT_DIR}/run_manifest.env"
rg -qx 'run_label_workload=get' "${OUT_DIR}/run_manifest.env"
rg -q '^node1,rustfs-bench-1,not_run_dry_run,N/A,N/A,N/A,N/A$' "${OUT_DIR}/node_inventory.csv"
rg -q '^1MiB,warp,1,1,before,node1,not_run_dry_run,' "${OUT_DIR}/node_metrics_captures.csv"
rg -q '^1MiB,warp,1,1,after,node1,rustfs-bench-1,not_run_dry_run,' "${OUT_DIR}/node_resource_captures.csv"

if "$RUNNER" \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key test-access \
  --secret-key test-secret \
  --warp-bin true \
  --dry-run \
  --require-server-provenance >/dev/null 2>&1; then
  echo "expected --require-server-provenance to reject missing identity" >&2
  exit 1
fi

"$RUNNER" \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key test-access \
  --secret-key test-secret \
  --sizes 1MiB \
  --rounds 1 \
  --retry-per-round 1 \
  --cooldown-secs 0 \
  --out-dir "${TMP_DIR}/default" \
  --warp-bin true \
  --dry-run >/dev/null

rg -qx 'run_label_count=0' "${TMP_DIR}/default/run_manifest.env"
