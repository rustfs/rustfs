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
rg -qx 'warp_report_operation=PUT' "${OUT_DIR}/run_manifest.env"
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

FAKE_WARP="${TMP_DIR}/fake-warp"
cat >"$FAKE_WARP" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ " $* " == *" --analyze.v "* ]]
[[ " $* " == *" --no-color "* ]]
if [[ "${FAKE_WARP_ZERO_LATENCY:-0}" == "1" ]]; then
  cat <<'LOG'
Operation: PUT. Concurrency: 8. Ran: 7s
Requests considered: 1000:
 * Average: 160.00 MiB/s, 40960.00 obj/s
 * Avg: 0s, 50%: 0s, 90%: 0s, 99%: 0s, Fastest: 0s, Slowest: 1ms, StdDev: 0s
LOG
  exit 0
fi
cat <<'LOG'
 -       PUT Average: 161 Obj/s, 5.0MiB/s; Current 161 Obj/s, 5.0MiB/s.
Operation: DELETE - total: 100, 10.0%, Concurrency: 8, Ran 7s
 * Throughput: 11.00 obj/s
Requests considered: 100:
 * Avg: 100ms, 50%: 50ms, 90%: 150ms, 99%: 200ms, Fastest: 1ms, Slowest: 300ms, StdDev: 20ms
Operation: GET. Concurrency: 64. Ran: 7s
Requests considered: 1000:
 * Average: 653.90 MiB/s, 20925.58 obj/s
 * Avg: 3.5ms, 50%: 2.0ms, 90%: 3.6ms, 99%: 24.1ms, Fastest: 0.2ms, Slowest: 607.7ms, StdDev: 20.6ms
Operation: PUT - total: 300, 15.0%, Size: 32767 bytes. Concurrency: 8, Ran 7s
 * Throughput: 63.53 MiB/s, 254.13 obj/s
Requests considered: 300:
 * Avg: 18ms, 50%: 3ms, 90%: 58ms, 99%: 216ms, Fastest: 2ms, Slowest: 608ms, StdDev: 45ms
Operation: STAT - total: 600, 30.0%, Concurrency: 8, Ran 7s
 * Throughput: 508.42 obj/s
Requests considered: 600:
 * Avg: 0s, 50%: 0s, 90%: 1ms, 99%: 1ms, Fastest: 0s, Slowest: 150ms, StdDev: 3ms
LOG
case "${FAKE_WARP_ERRORS:-}" in
  spaced) echo 'Total Errors: 1.' ;;
  compact) echo 'Total Errors:1.' ;;
esac
EOF
chmod +x "$FAKE_WARP"

"$RUNNER" \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key test-access \
  --secret-key test-secret \
  --sizes 32767B \
  --rounds 1 \
  --retry-per-round 1 \
  --cooldown-secs 0 \
  --duration 1s \
  --out-dir "${TMP_DIR}/fake-warp-run" \
  --warp-bin "$FAKE_WARP" \
  --server-image-ref rustfs/rustfs:bench \
  --server-image-digest sha256:0123456789abcdef \
  --server-revision 9f61bad94 \
  --require-server-provenance >/dev/null 2>&1

rg -q '^32767B,warp,1,1,128,ok,0,[^,]+,[^,]+,63.53 MiB/s,66616033.280000,254.13,18 ms,18.000000,[^,]+,58 ms,58.000000,216 ms,216.000000$' "${TMP_DIR}/fake-warp-run/round_results.csv"

cat >"${TMP_DIR}/warp-no-details.log" <<'EOF'
warp: Starting benchmark in 3s...
Operation: PUT. Concurrency: 8
 * Average: 2.76 MiB/s, 707.03 obj/s
EOF
"$RUNNER" --extract-metrics-from-log "${TMP_DIR}/warp-no-details.log" >"${TMP_DIR}/warp-no-details.csv"
rg -qx '2.76 MiB/s,2894069.760000,707.03,N/A,N/A,N/A,N/A,N/A,N/A' "${TMP_DIR}/warp-no-details.csv"

cat >"${TMP_DIR}/warp-legacy-report.log" <<'EOF'
Report:
 * Average: 10.00 MiB/s, 40.00 obj/s
 * Reqs: Avg: 2ms, 50%: 1ms, 90%: 3ms, 99%: 4ms, Fastest: 1ms, Slowest: 5ms, StdDev: 1ms
EOF
"$RUNNER" --warp-mode put --extract-metrics-from-log "${TMP_DIR}/warp-legacy-report.log" >"${TMP_DIR}/warp-legacy-report.csv"
rg -qx '10.00 MiB/s,10485760.000000,40.00,2 ms,2.000000,3 ms,3.000000,4 ms,4.000000' "${TMP_DIR}/warp-legacy-report.csv"

for error_format in spaced compact; do
  if FAKE_WARP_ERRORS="$error_format" "$RUNNER" \
    --tool warp \
    --endpoint http://127.0.0.1:9000 \
    --access-key test-access \
    --secret-key test-secret \
    --sizes 32767B \
    --rounds 1 \
    --retry-per-round 1 \
    --retry-sleep-secs 1 \
    --cooldown-secs 0 \
    --duration 1s \
    --out-dir "${TMP_DIR}/fake-warp-errors-${error_format}" \
    --warp-bin "$FAKE_WARP" >/dev/null 2>&1; then
    echo "expected Warp ${error_format} request errors to fail the benchmark" >&2
    exit 1
  fi
  rg -q ',failed,1,' "${TMP_DIR}/fake-warp-errors-${error_format}/round_results.csv"
done

"$RUNNER" \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key test-access \
  --secret-key test-secret \
  --sizes 32767B \
  --rounds 1 \
  --retry-per-round 1 \
  --cooldown-secs 0 \
  --duration 1s \
  --out-dir "${TMP_DIR}/fake-warp-candidate" \
  --warp-bin "$FAKE_WARP" \
  --baseline-csv "${TMP_DIR}/fake-warp-run/median_summary.csv" \
  --server-image-ref rustfs/rustfs:bench \
  --server-image-digest sha256:0123456789abcdef \
  --server-revision 9f61bad94 \
  --require-server-provenance >/dev/null 2>&1

awk -F',' '
  NR == 1 {
    if (NF != 25 || $15 != "delta_p90_latency_pct" || $18 != "delta_p99_latency_pct" || $25 != "delta_error_rate_pct") exit 1
  }
  NR == 2 {
    if ($15 != "0.00" || $18 != "0.00" || $23 != "0.00" || $25 != "0.00") exit 1
    found = 1
  }
  END { exit found ? 0 : 1 }
' "${TMP_DIR}/fake-warp-candidate/baseline_compare.csv"

for leg in baseline candidate; do
  zero_args=(
    --tool warp
    --endpoint http://127.0.0.1:9000
    --access-key test-access
    --secret-key test-secret
    --sizes 4KiB
    --rounds 1
    --retry-per-round 1
    --cooldown-secs 0
    --duration 1s
    --out-dir "${TMP_DIR}/fake-warp-zero-${leg}"
    --warp-bin "$FAKE_WARP"
  )
  if [[ "$leg" == "candidate" ]]; then
    zero_args+=(--baseline-csv "${TMP_DIR}/fake-warp-zero-baseline/median_summary.csv")
  fi
  FAKE_WARP_ZERO_LATENCY=1 "$RUNNER" "${zero_args[@]}" >/dev/null 2>&1
done

"${SCRIPT_DIR}/hotpath_warp_ab_gate.sh" \
  --compare-csv "${TMP_DIR}/fake-warp-zero-candidate/baseline_compare.csv" \
  --require-tail-error >/dev/null

echo "object batch benchmark enhanced tests passed"
