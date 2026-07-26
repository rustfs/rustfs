#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_exact_1mib_handoff_abba.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if "$RUNNER" --dry-run >/dev/null 2>&1; then
  echo "expected missing arm specifications to fail" >&2
  exit 1
fi

if "$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --arm-a "A|http://127.0.0.1:9000|rustfs/rustfs:current|latest|42fc84063|65536|auto|selected" \
  --arm-b "B|http://127.0.0.1:9001|rustfs/rustfs:candidate|sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|42fc84063-inner|1048576|auto|selected" \
  --arm-c "C|http://127.0.0.1:9002|rustfs/rustfs:current-outer|sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc|42fc84063|65536|1048576|env_override" \
  --arm-d "D|http://127.0.0.1:9003|rustfs/rustfs:candidate-outer|sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd|42fc84063-inner|1048576|1048576|env_override" \
  --out-dir "$TMP_DIR/bad-digest" \
  --dry-run >/dev/null 2>&1; then
  echo "expected mutable arm digest to fail" >&2
  exit 1
fi

if "$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --arm-a "A|http://127.0.0.1:9000|rustfs/rustfs:current|sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|42fc84063|65536|auto|manual" \
  --arm-b "B|http://127.0.0.1:9001|rustfs/rustfs:candidate|sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|42fc84063-inner|1048576|auto|selected" \
  --arm-c "C|http://127.0.0.1:9002|rustfs/rustfs:current-outer|sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc|42fc84063|65536|1048576|env_override" \
  --arm-d "D|http://127.0.0.1:9003|rustfs/rustfs:candidate-outer|sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd|42fc84063-inner|1048576|1048576|env_override" \
  --out-dir "$TMP_DIR/bad-source" \
  --dry-run >/dev/null 2>&1; then
  echo "expected unsupported outer source to fail" >&2
  exit 1
fi

"$RUNNER" \
  --access-key minioadmin \
  --secret-key minioadmin \
  --arm-a "A|http://127.0.0.1:9000|rustfs/rustfs:current|sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|42fc84063|65536|auto|selected" \
  --arm-b "B|http://127.0.0.1:9001|rustfs/rustfs:candidate|sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|42fc84063-inner|1048576|auto|selected" \
  --arm-c "C|http://127.0.0.1:9002|rustfs/rustfs:current-outer|sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc|42fc84063|65536|1048576|env_override" \
  --arm-d "D|http://127.0.0.1:9003|rustfs/rustfs:candidate-outer|sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd|42fc84063-inner|1048576|1048576|env_override" \
  --concurrencies 64 \
  --sizes 1048575B,1048576B,1048577B \
  --rounds-per-leg 5 \
  --cooldown-secs 0 \
  --out-dir "$TMP_DIR/good" \
  --dry-run >"$TMP_DIR/dry-run.log"

MANIFEST="$TMP_DIR/good/handoff_manifest.env"
SCHEDULE="$TMP_DIR/good/handoff_abba_schedule.csv"

rg -qx "benchmark_issue=rustfs/backlog#1434" "$MANIFEST"
rg -qx "schedule=ABBA_then_CDDC" "$MANIFEST"
rg -F -q -x 'sizes=1048575B\,1048576B\,1048577B' "$MANIFEST"
rg -qx "concurrencies=64" "$MANIFEST"
rg -qx "access_key=REDACTED" "$MANIFEST"
rg -qx "secret_key=REDACTED" "$MANIFEST"

rg -qx "leg,arm,endpoint,server_image_ref,server_image_digest,server_revision,inner_capacity,outer_capacity,outer_source,leg_out_dir" "$SCHEDULE"
rg -qx "A1,A,http://127.0.0.1:9000,rustfs/rustfs:current,sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,42fc84063,65536,auto,selected,$TMP_DIR/good/A1-arm-A" "$SCHEDULE"
rg -qx "B1,B,http://127.0.0.1:9001,rustfs/rustfs:candidate,sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,42fc84063-inner,1048576,auto,selected,$TMP_DIR/good/B1-arm-B" "$SCHEDULE"
rg -qx "B2,B,http://127.0.0.1:9001,rustfs/rustfs:candidate,sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb,42fc84063-inner,1048576,auto,selected,$TMP_DIR/good/B2-arm-B" "$SCHEDULE"
rg -qx "A2,A,http://127.0.0.1:9000,rustfs/rustfs:current,sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,42fc84063,65536,auto,selected,$TMP_DIR/good/A2-arm-A" "$SCHEDULE"
rg -qx "C1,C,http://127.0.0.1:9002,rustfs/rustfs:current-outer,sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc,42fc84063,65536,1048576,env_override,$TMP_DIR/good/C1-arm-C" "$SCHEDULE"
rg -qx "D1,D,http://127.0.0.1:9003,rustfs/rustfs:candidate-outer,sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd,42fc84063-inner,1048576,1048576,env_override,$TMP_DIR/good/D1-arm-D" "$SCHEDULE"
rg -qx "D2,D,http://127.0.0.1:9003,rustfs/rustfs:candidate-outer,sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd,42fc84063-inner,1048576,1048576,env_override,$TMP_DIR/good/D2-arm-D" "$SCHEDULE"
rg -qx "C2,C,http://127.0.0.1:9002,rustfs/rustfs:current-outer,sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc,42fc84063,65536,1048576,env_override,$TMP_DIR/good/C2-arm-C" "$SCHEDULE"

rg -q -- "--service-metrics-dir" "$TMP_DIR/dry-run.log"
rg -F -q -- "--service-metrics-filter-regex rustfs_io_get_object_\\|rustfs_s3_get_object_" "$TMP_DIR/dry-run.log"
rg -q -- "--require-server-provenance" "$TMP_DIR/dry-run.log"
rg -q -- "--label bench_issue=1434" "$TMP_DIR/dry-run.log"
rg -q -- "--label handoff_arm=D" "$TMP_DIR/dry-run.log"
rg -q -- "--label expected_reader_path=legacy_duplex" "$TMP_DIR/dry-run.log"
rg -q -- "--label expected_inner_capacity=1048576" "$TMP_DIR/dry-run.log"
rg -q -- "--label expected_outer_capacity=1048576" "$TMP_DIR/dry-run.log"
rg -q -- "--label expected_outer_source=env_override" "$TMP_DIR/dry-run.log"

echo "exact 1MiB handoff ABBA tests passed"
