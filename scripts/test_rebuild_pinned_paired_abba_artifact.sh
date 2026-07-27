#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/rebuild_pinned_paired_abba_artifact.sh"
TMP_DIR="$(mktemp -d)"
SOURCE_DIR="${TMP_DIR}/source"
OUT_DIR="${TMP_DIR}/corrected"
ARCHIVE_PATH="${TMP_DIR}/corrected.tar.gz"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$SOURCE_DIR/A1-minio/c64/logs" "$SOURCE_DIR/B1-rustfs/c64/logs"

cat > "$SOURCE_DIR/paired_manifest.env" <<'MANIFEST'
benchmark_issue=rustfs/backlog#1432
rustfs_ack_contract=relaxed
minio_ack_contract=strict
MANIFEST

cat > "$SOURCE_DIR/abba_schedule.csv" <<SCHEDULE
leg,product,endpoint,server_image_ref,server_image_digest,server_revision_or_release,ack_contract,leg_out_dir
A1,minio,http://127.0.0.1:9000,quay.io/minio/minio:RELEASE,sha256:1111111111111111111111111111111111111111111111111111111111111111,RELEASE.2025-07-18T21-56-31Z,strict,$SOURCE_DIR/A1-minio
B1,rustfs,http://127.0.0.1:9001,rustfs/rustfs:test,sha256:2222222222222222222222222222222222222222222222222222222222222222,02ad75e55236,relaxed,$SOURCE_DIR/B1-rustfs
SCHEDULE

cat > "$SOURCE_DIR/A1-minio/c64/logs/1MiB-r1-a1.log" <<'LOG'
 -       PUT Average: 1 Obj/s, 1.0 MiB/s
Report: GET. Concurrency: 64. Ran: 7s
 * Average: 200.00 MiB/s, 200.00 obj/s
 * Reqs: Avg: 5.0ms, 50%: 4.0ms, 90%: 8.0ms, 99%: 12.0ms
LOG

cat > "$SOURCE_DIR/B1-rustfs/c64/logs/1MiB-r1-a1.log" <<'LOG'
 -       PUT Average: 1 Obj/s, 1.0 MiB/s
Report: GET. Concurrency: 64. Ran: 7s
 * Average: 150.00 MiB/s, 150.00 obj/s
 * Reqs: Avg: 6.0ms, 50%: 5.0ms, 90%: 9.0ms, 99%: 13.0ms
LOG

cat > "$SOURCE_DIR/A1-minio/c64/round_results.csv" <<CSV
size,tool,round,attempt,concurrency,status,exit_code,started_at_utc,finished_at_utc,throughput_human,throughput_bps,reqps,latency_human,latency_ms,log_file,req_p90_human,req_p90_ms,req_p99_human,req_p99_ms
1MiB,warp,1,1,64,ok,0,now,now,1.0 MiB/s,1048576.000000,1.00,1.0 ms,1.000000,$SOURCE_DIR/A1-minio/c64/logs/1MiB-r1-a1.log,1.0 ms,1.000000,1.0 ms,1.000000
CSV

cat > "$SOURCE_DIR/B1-rustfs/c64/round_results.csv" <<CSV
size,tool,round,attempt,concurrency,status,exit_code,started_at_utc,finished_at_utc,throughput_human,throughput_bps,reqps,latency_human,latency_ms,log_file,req_p90_human,req_p90_ms,req_p99_human,req_p99_ms
1MiB,warp,1,1,64,ok,0,now,now,1.0 MiB/s,1048576.000000,1.00,1.0 ms,1.000000,$SOURCE_DIR/B1-rustfs/c64/logs/1MiB-r1-a1.log,1.0 ms,1.000000,1.0 ms,1.000000
CSV

"$RUNNER" --source-dir "$SOURCE_DIR" --out-dir "$OUT_DIR" --archive "$ARCHIVE_PATH" >/dev/null

rg -q '^A1,minio,strict,64,1MiB,warp,1,1,ok,0,200.00 MiB/s,209715200.000000,200.00,5.0 ms,5.000000,8.0 ms,8.000000,12.0 ms,12.000000,' "$OUT_DIR/corrected_round_results.csv"
rg -q '^B1,rustfs,relaxed,64,1MiB,warp,1,1,ok,0,150.00 MiB/s,157286400.000000,150.00,6.0 ms,6.000000,9.0 ms,9.000000,13.0 ms,13.000000,' "$OUT_DIR/corrected_round_results.csv"
rg -q '^minio,strict,64,1MiB,1,0,209715200.000000,200.000000,5.000000,8.000000,12.000000$' "$OUT_DIR/corrected_ack_contract_summary.csv"
rg -q '^rustfs,relaxed,64,1MiB,1,0,157286400.000000,150.000000,6.000000,9.000000,13.000000$' "$OUT_DIR/corrected_ack_contract_summary.csv"
rg -q '^false,relaxed,strict,64,1MiB,157286400.000000,209715200.000000,-25.00,150.000000,200.000000,-25.00$' "$OUT_DIR/corrected_product_comparison.csv"
rg -q '^ack_comparability_rule=' "$OUT_DIR/corrected_artifact_manifest.env"
test -s "$OUT_DIR/source_evidence/A1-minio/c64/logs/1MiB-r1-a1.log"
test -s "$OUT_DIR/source_evidence/A1-minio/c64/round_results.csv"
rg -q 'source_evidence/A1-minio/c64/logs/1MiB-r1-a1.log' "$OUT_DIR/sha256sums.txt"
test -s "$OUT_DIR/sha256sums.txt"
test -s "$ARCHIVE_PATH"
test -s "$ARCHIVE_PATH.sha256"
