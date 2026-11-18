#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FAKE_WARP="${TMP_DIR}/warp"
cat >"${FAKE_WARP}" <<'WARP'
#!/usr/bin/env bash
cat <<'REPORT'
Report: DELETE. Concurrency: 16. Ran: 12s
 * Average: 34.41 obj/s
 * Reqs: Avg: 338.1ms, 99%: 873.4ms

Report: GET. Concurrency: 16. Ran: 12s
 * Average: 15.02 MiB/s, 153.76 obj/s
 * Reqs: Avg: 2.8ms, 99%: 7.7ms

Report: PUT. Concurrency: 16. Ran: 12s
 * Average: 5.08 MiB/s, 52.00 obj/s
 * Reqs: Avg: 93.9ms, 99%: 492.7ms

Report: STAT. Concurrency: 16. Ran: 12s
 * Average: 102.88 obj/s
 * Reqs: Avg: 2.6ms, 99%: 8.4ms
REPORT
WARP
chmod +x "${FAKE_WARP}"

"${SCRIPT_DIR}/run_object_batch_bench.sh" \
  --tool warp \
  --warp-bin "${FAKE_WARP}" \
  --endpoint http://127.0.0.1:9000 \
  --access-key test \
  --secret-key test \
  --sizes 100KiB \
  --concurrency 16 \
  --duration 1s \
  --out-dir "${TMP_DIR}/out" >/dev/null

expected="100KiB,warp,16,ok,5.08 MiB/s,52.00 obj/s,93.9ms,0,${TMP_DIR}/out/warp_100KiB.log"
actual="$(tail -n1 "${TMP_DIR}/out/summary.csv")"
if [[ "${actual}" != "${expected}" ]]; then
  echo "unexpected warp PUT summary" >&2
  echo "expected: ${expected}" >&2
  echo "actual:   ${actual}" >&2
  exit 1
fi

cat >"${FAKE_WARP}" <<'WARP'
#!/usr/bin/env bash
cat <<'REPORT'
Report: GET. Concurrency: 16. Ran: 12s
 * Average: 15.02 MiB/s, 153.76 obj/s
 * Reqs: Avg: 2.8ms, 99%: 7.7ms
REPORT
WARP
chmod +x "${FAKE_WARP}"

"${SCRIPT_DIR}/run_object_batch_bench.sh" \
  --tool warp \
  --warp-bin "${FAKE_WARP}" \
  --endpoint http://127.0.0.1:9000 \
  --access-key test \
  --secret-key test \
  --sizes 100KiB \
  --concurrency 16 \
  --duration 1s \
  --out-dir "${TMP_DIR}/missing-put" >/dev/null

expected="100KiB,warp,16,ok,N/A,N/A,N/A,0,${TMP_DIR}/missing-put/warp_100KiB.log"
actual="$(tail -n1 "${TMP_DIR}/missing-put/summary.csv")"
if [[ "${actual}" != "${expected}" ]]; then
  echo "missing PUT report must not reuse GET metrics" >&2
  echo "expected: ${expected}" >&2
  echo "actual:   ${actual}" >&2
  exit 1
fi

if "${SCRIPT_DIR}/run_object_batch_bench.sh" \
  --tool warp \
  --warp-bin "${FAKE_WARP}" \
  --warp-mode invalid \
  --endpoint http://127.0.0.1:9000 \
  --access-key test \
  --secret-key test \
  --sizes 100KiB \
  --out-dir "${TMP_DIR}/invalid-mode" >/dev/null 2>&1; then
  echo "invalid warp mode must be rejected" >&2
  exit 1
fi

echo "object batch benchmark tests passed"
