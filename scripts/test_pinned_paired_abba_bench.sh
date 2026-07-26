#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_pinned_paired_abba_bench.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if "$RUNNER" --dry-run >/dev/null 2>&1; then
  echo "expected missing pinned identities to fail" >&2
  exit 1
fi

if "$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --rustfs-endpoint http://127.0.0.1:9000 \
  --rustfs-image-ref rustfs/rustfs:bench \
  --rustfs-image-digest latest \
  --rustfs-revision 42fc84063 \
  --rustfs-ack-contract strict \
  --minio-endpoint http://127.0.0.1:9100 \
  --minio-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z \
  --minio-image-digest sha256:1111222233334444 \
  --minio-release RELEASE.2025-07-18T21-56-31Z \
  --minio-ack-contract strict \
  --out-dir "$TMP_DIR/bad-digest" \
  --dry-run >/dev/null 2>&1; then
  echo "expected mutable rustfs digest to fail" >&2
  exit 1
fi

if "$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --rustfs-endpoint http://127.0.0.1:9000 \
  --rustfs-image-ref rustfs/rustfs:bench \
  --rustfs-image-digest sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --rustfs-revision 42fc84063 \
  --rustfs-ack-contract relaxed \
  --minio-endpoint http://127.0.0.1:9100 \
  --minio-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z \
  --minio-image-digest sha256:1111222233334444111122223333444411112222333344441111222233334444 \
  --minio-release RELEASE.2025-07-18T21-56-31Z \
  --minio-ack-contract strict \
  --sizes 0B \
  --out-dir "$TMP_DIR/bad-zero-size" \
  --dry-run >"$TMP_DIR/bad-zero-size.log" 2>&1; then
  echo "expected warp zero-byte benchmark size to fail" >&2
  exit 1
fi
rg -q "warp requires object size > 0" "$TMP_DIR/bad-zero-size.log"

"$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --rustfs-endpoint http://127.0.0.1:9000 \
  --rustfs-image-ref rustfs/rustfs:bench \
  --rustfs-image-digest sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --rustfs-revision 42fc84063 \
  --rustfs-ack-contract relaxed \
  --minio-endpoint http://127.0.0.1:9100 \
  --minio-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z \
  --minio-image-digest sha256:1111222233334444111122223333444411112222333344441111222233334444 \
  --minio-release RELEASE.2025-07-18T21-56-31Z \
  --minio-ack-contract strict \
  --concurrencies 1 \
  --sizes 1048576B \
  --rounds-per-leg 1 \
  --cooldown-secs 0 \
  --out-dir "$TMP_DIR/good-no-optional-arrays" \
  --dry-run >"$TMP_DIR/dry-run-no-optional-arrays.log"

"$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --rustfs-endpoint http://127.0.0.1:9000 \
  --rustfs-image-ref rustfs/rustfs:bench \
  --rustfs-image-digest sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --rustfs-revision 42fc84063 \
  --rustfs-ack-contract relaxed \
  --minio-endpoint http://127.0.0.1:9100 \
  --minio-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z \
  --minio-image-digest sha256:1111222233334444111122223333444411112222333344441111222233334444 \
  --minio-release RELEASE.2025-07-18T21-56-31Z \
  --minio-ack-contract strict \
  --concurrencies 1 \
  --sizes 1048576B \
  --rounds-per-leg 1 \
  --cooldown-secs 0 \
  --out-dir "$TMP_DIR/good-no-optional-arrays" \
  --dry-run >"$TMP_DIR/dry-run-no-optional-arrays.log"

"$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --rustfs-endpoint http://127.0.0.1:9000 \
  --rustfs-image-ref rustfs/rustfs:bench \
  --rustfs-image-digest sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --rustfs-revision 42fc84063 \
  --rustfs-ack-contract relaxed \
  --minio-endpoint http://127.0.0.1:9100 \
  --minio-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z \
  --minio-image-digest sha256:1111222233334444111122223333444411112222333344441111222233334444 \
  --minio-release RELEASE.2025-07-18T21-56-31Z \
  --minio-ack-contract strict \
  --concurrencies 1,64 \
  --sizes 1048575B,1048576B,1048577B \
  --rounds-per-leg 5 \
  --cooldown-secs 0 \
  --label topology=four_node_16_disk \
  --rustfs-node-metrics-url node1=http://127.0.0.1:9001/metrics \
  --minio-node-metrics-url node1=http://127.0.0.1:9101/minio/v2/metrics/cluster \
  --out-dir "$TMP_DIR/good" \
  --dry-run >"$TMP_DIR/dry-run.log"

MANIFEST="$TMP_DIR/good/paired_manifest.env"
SCHEDULE="$TMP_DIR/good/abba_schedule.csv"

rg -qx "schedule=ABBA" "$MANIFEST"
rg -qx "rustfs_ack_contract=relaxed" "$MANIFEST"
rg -qx "minio_ack_contract=strict" "$MANIFEST"
rg -qx "access_key=REDACTED" "$MANIFEST"
rg -qx "secret_key=REDACTED" "$MANIFEST"
rg -qx "leg,product,endpoint,server_image_ref,server_image_digest,server_revision_or_release,ack_contract,leg_out_dir" "$SCHEDULE"
rg -qx "A1,minio,http://127.0.0.1:9100,quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z,sha256:1111222233334444111122223333444411112222333344441111222233334444,RELEASE.2025-07-18T21-56-31Z,strict,$TMP_DIR/good/A1-minio" "$SCHEDULE"
rg -qx "B1,rustfs,http://127.0.0.1:9000,rustfs/rustfs:bench,sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,42fc84063,relaxed,$TMP_DIR/good/B1-rustfs" "$SCHEDULE"
rg -qx "B2,rustfs,http://127.0.0.1:9000,rustfs/rustfs:bench,sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,42fc84063,relaxed,$TMP_DIR/good/B2-rustfs" "$SCHEDULE"
rg -qx "A2,minio,http://127.0.0.1:9100,quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z,sha256:1111222233334444111122223333444411112222333344441111222233334444,RELEASE.2025-07-18T21-56-31Z,strict,$TMP_DIR/good/A2-minio" "$SCHEDULE"

rg -q -- "--server-image-ref rustfs/rustfs:bench" "$TMP_DIR/dry-run.log"
rg -q -- "--server-image-ref quay.io/minio/minio:RELEASE.2025-07-18T21-56-31Z" "$TMP_DIR/dry-run.log"
rg -q -- "--require-server-provenance" "$TMP_DIR/dry-run.log"
rg -q -- "--label bench_issue=1432" "$TMP_DIR/dry-run.log"
rg -q -- "--label topology=four_node_16_disk" "$TMP_DIR/dry-run.log"
rg -q -- "--node-metrics-url node1=http://127.0.0.1:9001/metrics" "$TMP_DIR/dry-run.log"
rg -q -- "--node-metrics-url node1=http://127.0.0.1:9101/minio/v2/metrics/cluster" "$TMP_DIR/dry-run.log"

echo "pinned paired ABBA bench tests passed"
