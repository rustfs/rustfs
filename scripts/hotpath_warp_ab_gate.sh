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
MARKDOWN_OUT=""
EXEMPTION_REASON="deliberate correctness tradeoff"
declare -a COMPARE_CSVS=()

usage() {
  cat <<'USAGE'
Usage: hotpath_warp_ab_gate.sh --compare-csv <file> [--compare-csv <file> ...] [options]

  --compare-csv <file>   baseline_compare.csv to evaluate (repeatable).
  --fail-pct <n>         Regression budget that fails the gate (default 10).
  --warn-pct <n>         Regression budget that warns (default 5).
  --allow-regression     Downgrade every FAIL to an exempted WARN (deliberate
                         correctness tradeoff); the gate then always exits 0.
  --exemption-reason <s> Reason recorded when --allow-regression is set.
  --markdown <file>      Also write the result table as Markdown to <file>.
  -h, --help             Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compare-csv) COMPARE_CSVS+=("$2"); shift 2 ;;
    --fail-pct) FAIL_PCT="$2"; shift 2 ;;
    --warn-pct) WARN_PCT="$2"; shift 2 ;;
    --allow-regression) ALLOW_REGRESSION="true"; shift ;;
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

# One awk pass over all CSVs. Emits TSV rows "verdict\tworkload\tmetric\tdelta"
# on stdout and a final "OVERALL\t<verdict>" line. Verdict is PASS/WARN/FAIL.
gate_output="$(
  awk -v fail_pct="$FAIL_PCT" -v warn_pct="$WARN_PCT" '
    function classify(delta, higher_is_better,   regress) {
      if (delta == "" || delta == "N/A") return "SKIP"
      # Signed regression magnitude: positive means "worse than baseline".
      regress = higher_is_better ? -delta : delta
      if (regress > fail_pct) return "FAIL"
      if (regress > warn_pct) return "WARN"
      return "PASS"
    }
    function emit(v, workload, metric, delta,   rank) {
      if (v == "SKIP") return
      print v "\t" workload "\t" metric "\t" delta "%"
      rank = (v == "FAIL") ? 3 : (v == "WARN") ? 2 : 1
      if (rank > worst) worst = rank
    }
    BEGIN { worst = 1 }
    FNR == 1 { next }                      # skip each file header
    {
      n = split($0, f, ",")
      if (n < 12) next
      workload = f[1] "/" f[2] "@" f[3]    # size/tool@concurrency
      emit(classify(f[6],  1), workload, "reqps",      f[6])
      emit(classify(f[9],  0), workload, "latency",    f[9])
      emit(classify(f[12], 1), workload, "throughput", f[12])
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
  echo "| Workload | Metric | Δ vs baseline | Verdict |"
  echo "| --- | --- | --- | --- |"
  if [[ -z "$rows" ]]; then
    echo "| _(no rows)_ | | | |"
  else
    printf '%s\n' "$rows" | while IFS=$'\t' read -r v workload metric delta; do
      [[ -z "$v" ]] && continue
      echo "| $workload | $metric | $delta | $(emoji "$v") $v |"
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
