#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE="${SCRIPT_DIR}/hotpath_warp_ab_gate.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

header='size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct,new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct,new_median_p90_latency_ms,baseline_median_p90_latency_ms,delta_p90_latency_pct,new_median_p99_latency_ms,baseline_median_p99_latency_ms,delta_p99_latency_pct,new_ok_rounds,baseline_ok_rounds,new_failed_rounds,baseline_failed_rounds,new_error_rate_pct,baseline_error_rate_pct,delta_error_rate_pct'

printf '%s\n' "$header" >"$TMP_DIR/equal-budget.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,9,10,1,0,10.00,0.00,10.00' >>"$TMP_DIR/equal-budget.csv"
"$GATE" --require-tail-error --fail-pct 10 --warn-pct 5 --compare-csv "$TMP_DIR/equal-budget.csv" >/dev/null

printf '%s\n' "$header" >"$TMP_DIR/rounded-error.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,2,5,1,1,33.33,16.67,16.67' >>"$TMP_DIR/rounded-error.csv"
if "$GATE" --require-tail-error --fail-pct 20 --warn-pct 10 --compare-csv "$TMP_DIR/rounded-error.csv" >/dev/null; then
  :
else
  echo "expected strict gate to accept error-rate deltas rounded from raw fractions" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/p99-regression.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,4,3,33.33,3,3,0,0,0.00,0.00,0.00' >>"$TMP_DIR/p99-regression.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/p99-regression.csv" >/dev/null 2>&1; then
  echo "expected p99-only regression to fail the strict gate" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/missing-evidence.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,N/A,N/A,N/A,N/A,N/A,N/A,0,3,3,0,N/A,0.00,N/A' >>"$TMP_DIR/missing-evidence.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/missing-evidence.csv" >/dev/null 2>&1; then
  echo "expected missing tail/error evidence or zero successes to fail the strict gate" >&2
  exit 1
fi

printf '%s\n' 'size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct' >"$TMP_DIR/old-schema.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00' >>"$TMP_DIR/old-schema.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/old-schema.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject the old comparison schema" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/header-only.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/header-only.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject a comparison without evidence rows" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/malformed-number.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,3,3,0,0,0x0,0.00,0.00' >>"$TMP_DIR/malformed-number.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/malformed-number.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject malformed numeric evidence" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/out-of-range-error.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,3,3,0,0,101.00,0.00,101.00' >>"$TMP_DIR/out-of-range-error.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/out-of-range-error.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject out-of-range error evidence" >&2
  exit 1
fi

printf '%s\n' "$header" >"$TMP_DIR/inconsistent-error.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,3,3,0,0,100.00,0.00,0.00' >>"$TMP_DIR/inconsistent-error.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/inconsistent-error.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject error rates inconsistent with round counts" >&2
  exit 1
fi

printf '%s\n' "$header,unexpected" >"$TMP_DIR/extra-column.csv"
printf '%s\n' '1MiB,warp,1,100,100,0.00,1,1,0.00,100,100,0.00,2,2,0.00,3,3,0.00,3,3,0,0,0.00,0.00,0.00,unexpected' >>"$TMP_DIR/extra-column.csv"
if "$GATE" --require-tail-error --compare-csv "$TMP_DIR/extra-column.csv" >/dev/null 2>&1; then
  echo "expected strict gate to reject extra columns" >&2
  exit 1
fi

echo "hotpath warp A/B gate tests passed"
