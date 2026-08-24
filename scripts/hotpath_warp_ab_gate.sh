#!/usr/bin/env bash
# hotpath_warp_ab_gate.sh — relative-budget gate for the hotpath warp A/B rig
# (rustfs/backlog#935 HP-14, item 4).
#
# The load driver scripts/run_object_batch_bench_enhanced.sh already emits a
# baseline_compare.csv with delta_{reqps,latency,throughput}_pct columns but
# does not itself decide pass/fail. This script applies the relative budget:
# a metric that regresses past --fail-pct fails the gate, past --warn-pct
# warns, otherwise passes. Because #4221 shows a deliberate 26x cost is
# sometimes the correct durability fix, --allow-regression downgrades every
# FAIL to an exempted WARN and records the reason instead of blocking.
#
# Metric direction:
#   reqps, throughput  -> higher is better; regression is a negative delta.
#   latency (incl p99) -> lower is better;  regression is a positive delta.
#
# Input CSV header (from run_object_batch_bench_enhanced.sh compare_baseline):
#   size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct,
#   new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,
#   new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct
#
# Exit code: 0 when no unexempted FAIL (PASS or WARN), 1 otherwise.

set -euo pipefail

FAIL_PCT=10
WARN_PCT=5
ALLOW_REGRESSION="false"
REQUIRE_TAIL_ERROR="false"
MARKDOWN_OUT=""
EXEMPTION_REASON="deliberate correctness tradeoff"
declare -a COMPARE_CSVS=()
declare -a COMPARE_LABELS=()

usage() {
  cat <<'USAGE'
Usage: hotpath_warp_ab_gate.sh --compare-csv <file> [--compare-csv <file> ...] [options]

  --compare-csv <file>   baseline_compare.csv to evaluate (repeatable).
  --labeled-compare-csv <label> <file>
                         Evaluate a CSV and identify its configuration in the
                         result table (repeatable).
  --fail-pct <n>         Regression budget that fails the gate (default 10).
  --warn-pct <n>         Regression budget that warns (default 5).
  --allow-regression     Downgrade every FAIL to an exempted WARN (deliberate
                         correctness tradeoff); the gate then always exits 0.
  --require-tail-error   Require the extended p90/p99/error-rate schema and
                         reject missing tail/error or zero-success evidence.
  --exemption-reason <s> Reason recorded when --allow-regression is set.
  --markdown <file>      Also write the result table as Markdown to <file>.
  -h, --help             Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compare-csv) COMPARE_CSVS+=("$2"); COMPARE_LABELS+=(""); shift 2 ;;
    --labeled-compare-csv)
      COMPARE_LABELS+=("$2")
      COMPARE_CSVS+=("$3")
      shift 3
      ;;
    --fail-pct) FAIL_PCT="$2"; shift 2 ;;
    --warn-pct) WARN_PCT="$2"; shift 2 ;;
    --allow-regression) ALLOW_REGRESSION="true"; shift ;;
    --require-tail-error) REQUIRE_TAIL_ERROR="true"; shift ;;
    --exemption-reason) EXEMPTION_REASON="$2"; shift 2 ;;
    --markdown) MARKDOWN_OUT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ${#COMPARE_CSVS[@]} -eq 0 ]]; then
  echo "error: at least one --compare-csv is required" >&2
  usage >&2
  exit 2
fi
for csv in "${COMPARE_CSVS[@]}"; do
  [[ -f "$csv" ]] || { echo "error: compare CSV not found: $csv" >&2; exit 2; }
done

if [[ "$REQUIRE_TAIL_ERROR" == "true" ]]; then
  strict_header='size,tool,concurrency,new_median_reqps,baseline_median_reqps,delta_reqps_pct,new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct,new_median_p90_latency_ms,baseline_median_p90_latency_ms,delta_p90_latency_pct,new_median_p99_latency_ms,baseline_median_p99_latency_ms,delta_p99_latency_pct,new_ok_rounds,baseline_ok_rounds,new_failed_rounds,baseline_failed_rounds,new_error_rate_pct,baseline_error_rate_pct,delta_error_rate_pct'
  for csv in "${COMPARE_CSVS[@]}"; do
    [[ "$(head -n1 "$csv")" == "$strict_header" ]] || {
      echo "error: strict mode requires the current tail/error compare schema: $csv" >&2
      exit 2
    }
    awk 'NR == 2 { found = 1 } END { exit found ? 0 : 1 }' "$csv" || {
      echo "error: strict mode requires at least one comparison row: $csv" >&2
      exit 2
    }
  done
fi

has_labels="false"
for label in "${COMPARE_LABELS[@]}"; do
  [[ -n "$label" ]] && has_labels="true"
done
label_separator=$'\034'
compare_labels="$(IFS="$label_separator"; echo "${COMPARE_LABELS[*]}")"

# One awk pass over all CSVs. Emits TSV rows
# "verdict\tconfiguration\tworkload\tmetric\tdelta"
# on stdout and a final "OVERALL\t<verdict>" line. Verdict is PASS/WARN/FAIL.
gate_output="$(
  awk -v fail_pct="$FAIL_PCT" -v warn_pct="$WARN_PCT" -v strict="$REQUIRE_TAIL_ERROR" \
      -v label_blob="$compare_labels" -v label_separator="$label_separator" '
    function classify(delta, higher_is_better,   regress) {
      if (delta == "" || delta == "N/A") return "SKIP"
      # Signed regression magnitude: positive means "worse than baseline".
      regress = higher_is_better ? -delta : delta
      if (regress > fail_pct) return "FAIL"
      if (regress > warn_pct) return "WARN"
      return "PASS"
    }
    function emit(v, configuration, workload, metric, delta,   rank) {
      if (v == "SKIP") return
      print v "\t" configuration "\t" workload "\t" metric "\t" delta "%"
      rank = (v == "FAIL") ? 3 : (v == "WARN") ? 2 : 1
      if (rank > worst) worst = rank
    }
    function contract_failure(configuration, workload, metric) {
      print "FAIL\t" configuration "\t" workload "\t" metric "\tinvalid evidence"
      if (3 > worst) worst = 3
    }
    function decimal(value) {
      return value ~ /^-?[0-9]+([.][0-9]+)?$/
    }
    function count(value) {
      return value ~ /^[0-9]+$/
    }
    function error_rate(value) {
      return decimal(value) && value + 0 >= 0 && value + 0 <= 100
    }
    function error_delta(value) {
      return decimal(value) && value + 0 >= -100 && value + 0 <= 100
    }
    function abs(value) {
      return value < 0 ? -value : value
    }
    BEGIN {
      worst = 1
      split(label_blob, file_labels, label_separator)
    }
    FNR == 1 {
      file_index++
      configuration = file_labels[file_index]
      if (configuration == "") configuration = "-"
      next
    }
    {
      n = split($0, f, ",")
      if (n < 12) {
        if (strict == "true") contract_failure(configuration, FILENAME, "compare-schema")
        next
      }
      workload = f[1] "/" f[2] "@" f[3]    # size/tool@concurrency
      emit(classify(f[6],  1), configuration, workload, "reqps",      f[6])
      emit(classify(f[9],  0), configuration, workload, "latency",    f[9])
      emit(classify(f[12], 1), configuration, workload, "throughput", f[12])
      if (strict == "true") {
        valid = n == 25
        for (i = 4; i <= 18; i++) {
          if (!decimal(f[i])) valid = 0
        }
        for (i = 19; i <= 22; i++) {
          if (!count(f[i])) valid = 0
        }
        if (f[19] + 0 == 0 || f[20] + 0 == 0 || !error_rate(f[23]) || !error_rate(f[24]) || !error_delta(f[25])) valid = 0
        new_error_rate = f[21] / (f[19] + f[21]) * 100
        baseline_error_rate = f[22] / (f[20] + f[22]) * 100
        if (abs(f[23] - new_error_rate) > 0.005 || abs(f[24] - baseline_error_rate) > 0.005 || abs(f[25] - (new_error_rate - baseline_error_rate)) > 0.005) valid = 0
        if (!valid) {
          contract_failure(configuration, workload, "tail-error-evidence")
          next
        }
        emit(classify(f[15], 0), configuration, workload, "p90-latency", f[15])
        emit(classify(f[18], 0), configuration, workload, "p99-latency", f[18])
        emit(classify(f[25], 0), configuration, workload, "error-rate", f[25])
      }
    }
    END {
      print "OVERALL\t" (worst == 3 ? "FAIL" : worst == 2 ? "WARN" : "PASS")
    }
  ' "${COMPARE_CSVS[@]}"
)"

overall="$(printf '%s\n' "$gate_output" | awk -F'\t' '$1=="OVERALL"{print $2}')"
rows="$(printf '%s\n' "$gate_output" | awk -F'\t' '$1!="OVERALL"')"

exempted="false"
effective="$overall"
if [[ "$overall" == "FAIL" && "$ALLOW_REGRESSION" == "true" ]]; then
  exempted="true"
  effective="WARN"
fi

emoji() { case "$1" in FAIL) echo '❌';; WARN) echo '⚠️';; PASS) echo '✅';; *) echo '';; esac; }

render() {
  echo "### Hotpath warp A/B gate — $(emoji "$effective") $effective"
  echo
  echo "Budget: regression > ${FAIL_PCT}% fails, > ${WARN_PCT}% warns (relative to baseline)."
  if [[ "$exempted" == "true" ]]; then
    echo
    echo "> Gate FAIL exempted via \`--allow-regression\`: ${EXEMPTION_REASON}."
  fi
  echo
  if [[ "$has_labels" == "true" ]]; then
    echo "| Configuration | Workload | Metric | Δ vs baseline | Verdict |"
    echo "| --- | --- | --- | --- | --- |"
  else
    echo "| Workload | Metric | Δ vs baseline | Verdict |"
    echo "| --- | --- | --- | --- |"
  fi
  if [[ -z "$rows" ]]; then
    if [[ "$has_labels" == "true" ]]; then
      echo "| _(no rows)_ | | | | |"
    else
      echo "| _(no rows)_ | | | |"
    fi
  else
    printf '%s\n' "$rows" | while IFS=$'\t' read -r v configuration workload metric delta; do
      [[ -z "$v" ]] && continue
      if [[ "$has_labels" == "true" ]]; then
        echo "| $configuration | $workload | $metric | $delta | $(emoji "$v") $v |"
      else
        echo "| $workload | $metric | $delta | $(emoji "$v") $v |"
      fi
    done
  fi
}

table="$(render)"
printf '%s\n' "$table"
if [[ -n "$MARKDOWN_OUT" ]]; then
  printf '%s\n' "$table" >"$MARKDOWN_OUT"
fi

if [[ "$overall" == "FAIL" && "$exempted" != "true" ]]; then
  exit 1
fi
exit 0
