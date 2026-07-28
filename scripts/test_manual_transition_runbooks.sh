#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MIXED_MATRIX="${PROJECT_ROOT}/scripts/manual_transition_mixed_rollout_matrix.sh"
SOAK_MATRIX="${PROJECT_ROOT}/scripts/manual_transition_soak_matrix.sh"
MIXED_RUNBOOK="${PROJECT_ROOT}/scripts/manual_transition_mixed_rollout_runbook.sh"
STRESS_RUNBOOK="${PROJECT_ROOT}/scripts/manual_transition_nightly_stress_runbook.sh"
FAILURE_SAMPLES="${PROJECT_ROOT}/scripts/manual_transition_failure_samples.sh"
MIXED_DOCKER_HARNESS="${PROJECT_ROOT}/scripts/manual_transition_mixed_version_docker_harness.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$MIXED_MATRIX" "$SOAK_MATRIX" "$MIXED_RUNBOOK" "$STRESS_RUNBOOK" "$FAILURE_SAMPLES" "$MIXED_DOCKER_HARNESS"

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
  --access-key hotadmin \
  --secret-key hotsecret \
  --phase-matrix request-only:100:0:1 \
  --concurrencies 2 \
  --object-counts 3k \
  --out-dir "$TMP_DIR/mixed-runbook" \
  --dry-run >"$TMP_DIR/mixed-runbook.out"

test -x "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "Manual transition mixed-version rollout runbook" "$TMP_DIR/mixed-runbook/manual_transition_mixed_rollout_runbook.md"
rg -q "manual_transition_failure_samples.sh" "$TMP_DIR/mixed-runbook/manual_transition_mixed_rollout_runbook.md"
rg -q "non-empty lifecycle-matching objects" "$TMP_DIR/mixed-runbook/manual_transition_mixed_rollout_runbook.md"
rg -q "in-flight rollback validation" "$TMP_DIR/mixed-runbook/manual_transition_mixed_rollout_runbook.md"
rg -q "MIXED_MATRIX_CSV='$TMP_DIR/mixed-runbook/mixed_rollout_matrix.csv'" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "ACCESS_KEY='hotadmin'" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "SECRET_KEY='hotsecret'" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "curl_admin" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "POLL_SECONDS" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "S3_ENDPOINT=" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "IN_FLIGHT_ROLLBACK_HOOK" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "seed_phase_workload" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "head_phase_probe" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "put-lifecycle" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "put-object" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"
rg -q "head-object" "$TMP_DIR/mixed-runbook/run_mixed_rollout_plan.sh"

if bash "$MIXED_RUNBOOK" --endpoint http://127.0.0.1:9000 --access-key hotadmin --dry-run >/tmp/manual_transition_mixed_runbook.err 2>&1; then
  echo "mixed rollout runbook should fail when SigV4 credentials are incomplete" >&2
  exit 1
fi
if ! rg -q "ERROR: --access-key and --secret-key must be provided together" /tmp/manual_transition_mixed_runbook.err; then
  echo "mixed rollout runbook missing incomplete SigV4 credential guard output" >&2
  exit 1
fi

if bash "$MIXED_RUNBOOK" --endpoint http://127.0.0.1:9000 --admin-token token --access-key hotadmin --secret-key hotsecret --dry-run >/tmp/manual_transition_mixed_runbook.err 2>&1; then
  echo "mixed rollout runbook should reject mixed bearer and SigV4 credentials" >&2
  exit 1
fi
if ! rg -q "ERROR: --admin-token cannot be combined with --access-key/--secret-key" /tmp/manual_transition_mixed_runbook.err; then
  echo "mixed rollout runbook missing mixed credential guard output" >&2
  exit 1
fi

"$STRESS_RUNBOOK" \
  --endpoint http://127.0.0.1:9000 \
  --access-key hotadmin \
  --secret-key hotsecret \
  --window-spec quick:1:1:1:balanced \
  --soak-ratios balanced:70:30 \
  --workload-sizes 4KiB \
  --out-dir "$TMP_DIR/stress-runbook" \
  --dry-run >"$TMP_DIR/stress-runbook.out"

test -x "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "failure-snapshots" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "run-results" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "UNKNOWN_FAILURE_RATIO_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "QUEUE_MISMATCH_RATIO_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "UNKNOWN_FAILURE_COUNT_THRESHOLD" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "ACCESS_KEY='hotadmin'" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "SECRET_KEY='hotsecret'" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "AWS_SIGV4_SCOPE='aws:amz:us-east-1:s3'" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "curl_admin" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "failures=\\$\\(\\(failures \\+ 1\\)\\)" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
if rg -q "headers\\[@\\]" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"; then
  echo "nightly stress runner must not expand an unset headers array" >&2
  exit 1
fi
if rg -q "run_entry .*\\|\\| true" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"; then
  echo "nightly stress runner must not hide failed rows" >&2
  exit 1
fi
rg -q "snapshot_failure" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "manual_transition_failure_samples.sh" "$TMP_DIR/stress-runbook/manual_transition_nightly_stress_runbook.md"
rg -q "already-in-flight transitions" "$TMP_DIR/stress-runbook/manual_transition_nightly_stress_runbook.md"
rg -q "is_terminal_status" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "status_json" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "@tsv" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "report_already_in_flight" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "already_in_flight=" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q 'failure_reason // "__none__"' "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q 'failure_reason" != "__none__"' "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
if rg -F -q 'failure_reason // ""' "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"; then
  echo "nightly stress runner must not emit an empty TSV failure_reason field" >&2
  exit 1
fi
rg -q "job_id, status, bucket, prefix, tier" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
if rg -F -q 'join(\"' "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"; then
  echo "nightly stress runner must not emit escaped jq quotes" >&2
  exit 1
fi
rg -q "unknown_failure_ratio" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "queue_mismatch_ratio" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "report_unknown_failure" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"
rg -q "Manual transition nightly/stress stress-runbook" "$TMP_DIR/stress-runbook/manual_transition_nightly_stress_runbook.md"
rg -q "SOAK_MATRIX_CSV='$TMP_DIR/stress-runbook/nightly_soak_matrix.csv'" "$TMP_DIR/stress-runbook/run_nightly_stress_plan.sh"

if bash "$STRESS_RUNBOOK" --endpoint http://127.0.0.1:9000 --access-key hotadmin --dry-run >/tmp/manual_transition_stress_runbook.err 2>&1; then
  echo "nightly stress runbook should fail when SigV4 credentials are incomplete" >&2
  exit 1
fi
if ! rg -q "ERROR: --access-key and --secret-key must be provided together" /tmp/manual_transition_stress_runbook.err; then
  echo "nightly stress runbook missing incomplete SigV4 credential guard output" >&2
  exit 1
fi

if bash "$STRESS_RUNBOOK" --endpoint http://127.0.0.1:9000 --admin-token token --access-key hotadmin --secret-key hotsecret --dry-run >/tmp/manual_transition_stress_runbook.err 2>&1; then
  echo "nightly stress runbook should reject mixed bearer and SigV4 credentials" >&2
  exit 1
fi
if ! rg -q "ERROR: --admin-token cannot be combined with --access-key/--secret-key" /tmp/manual_transition_stress_runbook.err; then
  echo "nightly stress runbook missing mixed credential guard output" >&2
  exit 1
fi

bash scripts/monitor_manual_transition_ci.sh --help >/tmp/monitor_manual_transition_ci.help
rg -q "Usage:" /tmp/monitor_manual_transition_ci.help
if bash scripts/monitor_manual_transition_ci.sh --issues >/tmp/monitor_manual_transition_ci.err 2>&1; then
  echo "monitor script should fail when --issues has no value" >&2
  exit 1
fi
if ! rg -q "ERROR: missing value for --issues" /tmp/monitor_manual_transition_ci.err; then
  echo "monitor script missing missing-value guard for --issues" >&2
  exit 1
fi

if bash scripts/monitor_manual_transition_ci.sh --runs 0 >/tmp/monitor_manual_transition_ci.err 2>&1; then
  echo "monitor script should fail on invalid --runs" >&2
  exit 1
fi
if ! rg -q "ERROR: --runs must be a positive integer" /tmp/monitor_manual_transition_ci.err; then
  echo "monitor script missing invalid --runs guard output" >&2
  exit 1
fi

bash "$FAILURE_SAMPLES" --help >/tmp/manual_transition_failure_samples.help
rg -q "Usage:" /tmp/manual_transition_failure_samples.help
bash "$MIXED_DOCKER_HARNESS" --help >/tmp/manual_transition_mixed_version_docker_harness.help
rg -q "mixed_version_docker_harness" /tmp/manual_transition_mixed_version_docker_harness.help
rg -q -- "--old-image" /tmp/manual_transition_mixed_version_docker_harness.help
rg -q -- "--new-image" /tmp/manual_transition_mixed_version_docker_harness.help
rg -q -- "--no-rollback" /tmp/manual_transition_mixed_version_docker_harness.help
if bash "$FAILURE_SAMPLES" --endpoint http://127.0.0.1:9000 --sample >/tmp/manual_transition_failure_samples.err 2>&1; then
  echo "failure samples script should fail when --sample has no value" >&2
  exit 1
fi
if ! rg -q "ERROR: missing value for --sample" /tmp/manual_transition_failure_samples.err; then
  echo "failure samples script missing missing-value guard for --sample" >&2
  exit 1
fi

if bash "$FAILURE_SAMPLES" --endpoint http://127.0.0.1:9000 --min-distinct-reasons 0 --dry-run >/tmp/manual_transition_failure_samples.err 2>&1; then
  echo "failure samples script should fail on invalid --min-distinct-reasons" >&2
  exit 1
fi
if ! rg -q "ERROR: --min-distinct-reasons must be a positive integer" /tmp/manual_transition_failure_samples.err; then
  echo "failure samples script missing invalid --min-distinct-reasons guard output" >&2
  exit 1
fi

if bash "$FAILURE_SAMPLES" --endpoint http://127.0.0.1:9000 --access-key hotadmin --dry-run >/tmp/manual_transition_failure_samples.err 2>&1; then
  echo "failure samples script should fail when SigV4 credentials are incomplete" >&2
  exit 1
fi
if ! rg -q "ERROR: --access-key and --secret-key must be provided together" /tmp/manual_transition_failure_samples.err; then
  echo "failure samples script missing incomplete SigV4 credential guard output" >&2
  exit 1
fi

if bash "$FAILURE_SAMPLES" --endpoint http://127.0.0.1:9000 --admin-token token --access-key hotadmin --secret-key hotsecret --dry-run >/tmp/manual_transition_failure_samples.err 2>&1; then
  echo "failure samples script should reject mixed bearer and SigV4 credentials" >&2
  exit 1
fi
if ! rg -q "ERROR: --admin-token cannot be combined with --access-key/--secret-key" /tmp/manual_transition_failure_samples.err; then
  echo "failure samples script missing mixed credential guard output" >&2
  exit 1
fi

"$FAILURE_SAMPLES" \
  --endpoint http://127.0.0.1:9000 \
  --access-key hotadmin \
  --secret-key hotsecret \
  --sample auth:11111111-1111-4111-8111-111111111111:RemoteAuth \
  --sample network:22222222-2222-4222-8222-222222222222:RemoteNetwork \
  --out-dir "$TMP_DIR/failure-samples" \
  --dry-run >"$TMP_DIR/failure-samples.out"

rg -q "Manual transition failure attribution sample plan" "$TMP_DIR/failure-samples/sample_plan.md"
rg -q "auth:<JOB_ID_AUTH>:<expected_reason_key>" "$TMP_DIR/failure-samples/sample_plan.md"
rg -q -- "--access-key <ACCESS_KEY> --secret-key <SECRET_KEY>" "$TMP_DIR/failure-samples/commands.txt"
rg -q -- "--min-distinct-reasons 2" "$TMP_DIR/failure-samples/commands.txt"
