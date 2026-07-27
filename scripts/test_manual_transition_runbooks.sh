#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MIXED_MATRIX="${PROJECT_ROOT}/scripts/manual_transition_mixed_rollout_matrix.sh"
SOAK_MATRIX="${PROJECT_ROOT}/scripts/manual_transition_soak_matrix.sh"
MIXED_RUNBOOK="${PROJECT_ROOT}/scripts/manual_transition_mixed_rollout_runbook.sh"
STRESS_RUNBOOK="${PROJECT_ROOT}/scripts/manual_transition_nightly_stress_runbook.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$MIXED_MATRIX" "$SOAK_MATRIX" "$MIXED_RUNBOOK" "$STRESS_RUNBOOK"

if "$MIXED_MATRIX" \
  --endpoint http://127.0.0.1:9000 \
  --phase-matrix bad:60:60:1 \
  --out-dir "$TMP_DIR/bad-mixed" \
  --dry-run >"$TMP_DIR/bad-mixed.out" 2>"$TMP_DIR/bad-mixed.err"; then
  echo "expected mixed rollout ratios that do not sum to 100 to fail" >&2
  exit 1
fi
rg -q "phase ratios must sum to 100" "$TMP_DIR/bad-mixed.err"

if "$SOAK_MATRIX" \
  --endpoint http://127.0.0.1:9000 \
  --window-spec quick:1:1:1:bad \
  --soak-ratios bad:90:90 \
  --out-dir "$TMP_DIR/bad-soak" \
  --dry-run >"$TMP_DIR/bad-soak.out" 2>"$TMP_DIR/bad-soak.err"; then
  echo "expected soak read/write percentages that do not sum to 100 to fail" >&2
  exit 1
fi
rg -q "read/write percentages must sum to 100" "$TMP_DIR/bad-soak.err"

"$MIXED_MATRIX" \
  --endpoint http://127.0.0.1:9000 \
  --phase-matrix request-only:100:0:1,canary:90:10:1 \
  --concurrencies 2 \
  --object-counts 3k \
  --out-dir "$TMP_DIR/mixed" \
  --dry-run >"$TMP_DIR/mixed.out"

rg -qx "phase,old_pct,new_pct,duration_min,concurrency,object_count,read_ratio,gate,admin_check" "$TMP_DIR/mixed/mixed_rollout_matrix.csv"
rg -q "# check-request-only" "$TMP_DIR/mixed/run_phase_commands.sh"
rg -q "/rustfs/admin/v3/ilm/transition/jobs/<JOB_ID_request-only_c2_q3k>" "$TMP_DIR/mixed/run_phase_commands.sh"
rg -q "/rustfs/admin/v3/metrics\\?types=1&n=1" "$TMP_DIR/mixed/run_phase_commands.sh"
rg -q "manual_transition_journal_audit.sh --endpoint http://127.0.0.1:9000 --job-id \"<JOB_ID_request-only_c2_q3k>\" --sys-bucket .rustfs.sys" "$TMP_DIR/mixed/run_phase_commands.sh"

"$SOAK_MATRIX" \
  --endpoint http://127.0.0.1:9000 \
  --window-spec quick:1:1:1:balanced \
  --soak-ratios balanced:70:30 \
  --workload-sizes 4KiB \
  --out-dir "$TMP_DIR/soak" \
  --dry-run >"$TMP_DIR/soak.out"

rg -qx "window,window_duration_min,concurrency,ops_per_min,mix_name,read_pct,write_pct,size,expected_ops,expected_run_id,run_check" "$TMP_DIR/soak/nightly_soak_matrix.csv"
rg -qx "quick,1,1,1,balanced,70,30,4KiB,1,1,within-budget" "$TMP_DIR/soak/nightly_soak_matrix.csv"

"$MIXED_RUNBOOK" \
  --endpoint http://127.0.0.1:9000 \
  --phase-matrix request-only:100:0:1 \
  --concurrencies 2 \
  --object-counts 3k \
  --out-dir "$TMP_DIR/mixed-runbook" \
  --dry-run >"$TMP_DIR/mixed-runbook.out"

test -x "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "Manual transition mixed-version rollout runbook" "$TMP_DIR/mixed-runbook/manual_transition_mixed_rollout_runbook.md"
rg -q "MIXED_MATRIX_CSV='$TMP_DIR/mixed-runbook/mixed_rollout_matrix.csv'" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"

"$STRESS_RUNBOOK" \
  --endpoint http://127.0.0.1:9000 \
  --window-spec quick:1:1:1:balanced \
  --soak-ratios balanced:70:30 \
  --workload-sizes 4KiB \
  --out-dir "$TMP_DIR/stress-runbook" \
  --dry-run >"$TMP_DIR/stress-runbook.out"

test -x "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "failure-snapshots" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "UNKNOWN_FAILURE_RATIO_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "QUEUE_MISMATCH_RATIO_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "UNKNOWN_FAILURE_COUNT_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "snapshot_failure" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "Manual transition nightly/stress stress-runbook" "$TMP_DIR/stress-runbook/manual_transition_nightly_stress_runbook.md"
rg -q "SOAK_MATRIX_CSV='$TMP_DIR/stress-runbook/nightly_soak_matrix.csv'" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
