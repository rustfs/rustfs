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

"$GATE" --require-tail-error \
  --labeled-compare-csv sync-on/put-4kib/B1-vs-A1 "$TMP_DIR/equal-budget.csv" \
  --labeled-compare-csv sync-off/put-4kib/B1-vs-A1 "$TMP_DIR/equal-budget.csv" \
  --markdown "$TMP_DIR/labeled.md" >/dev/null
rg -qF '| Configuration | Workload | Metric |' "$TMP_DIR/labeled.md"
rg -qF '| sync-on/put-4kib/B1-vs-A1 | 1MiB/warp@1 |' "$TMP_DIR/labeled.md"
rg -qF '| sync-off/put-4kib/B1-vs-A1 | 1MiB/warp@1 |' "$TMP_DIR/labeled.md"

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

printf '%s\n' "$header" >"$TMP_DIR/stable.csv"
printf '%s\n' '100KiB,warp,128,100,100,0,1,1,0,100,100,0,2,2,0,3,3,0,3,3,0,0,0,0,0' >>"$TMP_DIR/stable.csv"
"$GATE" --baseline-drift --require-tail-error --compare-csv "$TMP_DIR/stable.csv" >"$TMP_DIR/stable.md"
rg -qF 'baseline drift gate — ✅ PASS' "$TMP_DIR/stable.md"

for metric_spec in 'reqps:4:6:higher' 'latency:7:9:lower' 'throughput:10:12:higher' 'p90-latency:13:15:lower' 'p99-latency:16:18:lower'; do
  IFS=: read -r metric value_column delta_column direction <<<"$metric_spec"
  for delta in -20 20; do
    csv="$TMP_DIR/drift-${metric}-${delta}.csv"
    awk -F, -v OFS=, -v value_column="$value_column" -v delta_column="$delta_column" -v delta="$delta" '
      NR > 1 { $value_column = $(value_column + 1) * (1 + delta / 100); $delta_column = delta }
      { print }
    ' "$TMP_DIR/stable.csv" >"$csv"
    if "$GATE" --baseline-drift --require-tail-error --compare-csv "$csv" >"$TMP_DIR/drift.md"; then
      echo "expected ${metric} delta ${delta}% to fail baseline stability" >&2
      exit 1
    fi
    rg -qF "| $metric | ${delta}% | ❌ FAIL |" "$TMP_DIR/drift.md"
    if [[ "$direction" == higher && "$delta" -gt 0 ]] || [[ "$direction" == lower && "$delta" -lt 0 ]]; then
      "$GATE" --require-tail-error --compare-csv "$csv" >"$TMP_DIR/candidate.md"
      rg -qF "| $metric | ${delta}% | ✅ PASS |" "$TMP_DIR/candidate.md"
    elif "$GATE" --require-tail-error --compare-csv "$csv" >/dev/null; then
      echo "expected candidate ${metric} regression ${delta}% to retain its failure" >&2
      exit 1
    fi
  done
done

# Error-rate deltas use percentage points and retain their round-count contract.
printf '%s\n' "$header" >"$TMP_DIR/error-improvement.csv"
printf '%s\n' '100KiB,warp,128,100,100,0,1,1,0,100,100,0,2,2,0,3,3,0,10,8,0,2,0,20,-20' >>"$TMP_DIR/error-improvement.csv"
"$GATE" --require-tail-error --compare-csv "$TMP_DIR/error-improvement.csv" >/dev/null
if "$GATE" --baseline-drift --require-tail-error --compare-csv "$TMP_DIR/error-improvement.csv" >"$TMP_DIR/error-drift.md"; then
  echo "expected falling baseline error rate beyond budget to fail stability" >&2
  exit 1
fi
rg -qF '| error-rate | -20% | ❌ FAIL |' "$TMP_DIR/error-drift.md"

for delta in -10 -5 5 10; do
  awk -F, -v OFS=, -v delta="$delta" 'NR > 1 { $4 = $5 * (1 + delta / 100); $6 = delta } { print }' \
    "$TMP_DIR/stable.csv" >"$TMP_DIR/boundary.csv"
  "$GATE" --baseline-drift --require-tail-error --compare-csv "$TMP_DIR/boundary.csv" >"$TMP_DIR/boundary.md"
  verdict='✅ PASS'
  [[ "$delta" == -10 || "$delta" == 10 ]] && verdict='⚠️ WARN'
  rg -qF "| reqps | ${delta}% | $verdict |" "$TMP_DIR/boundary.md"
done

for invalid_csv in missing-evidence old-schema header-only malformed-number out-of-range-error inconsistent-error extra-column; do
  if "$GATE" --baseline-drift --require-tail-error --compare-csv "$TMP_DIR/${invalid_csv}.csv" >/dev/null 2>&1; then
    echo "expected baseline drift mode to reject invalid ${invalid_csv} evidence" >&2
    exit 1
  fi
done

if "$GATE" --baseline-drift --allow-regression --compare-csv "$TMP_DIR/stable.csv" >"$TMP_DIR/drift-exemption.log" 2>&1; then
  echo "expected baseline drift mode to reject a regression exemption" >&2
  exit 1
fi
rg -qF -- '--baseline-drift cannot be combined with --allow-regression' "$TMP_DIR/drift-exemption.log"
"$GATE" --require-tail-error --allow-regression --compare-csv "$TMP_DIR/p99-regression.csv" >"$TMP_DIR/candidate-exemption.md"
rg -qF 'Gate FAIL exempted via' "$TMP_DIR/candidate-exemption.md"

echo "hotpath warp A/B gate tests passed"
