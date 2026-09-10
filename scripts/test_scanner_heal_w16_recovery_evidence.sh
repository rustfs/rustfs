#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${PROJECT_ROOT}/scripts/run_scanner_heal_w16_recovery_evidence.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$RUNNER"

bash "$RUNNER" --help >"$TMP_DIR/help.out"
rg -q "RUSTFS_SCANNER_HEAL_W16_OUTPUT_ROOT" "$TMP_DIR/help.out"
rg -q "G04-root_floor_intent_crash_evidence.json" "$TMP_DIR/help.out"
rg -q "G12-settlement_quota_path_evidence.json" "$TMP_DIR/help.out"

env -u CARGO_TARGET_DIR bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence" >"$TMP_DIR/dry-run.out"

rg -q "tests=g04 g12" "$TMP_DIR/dry-run.out"
rg -q "rustfs-scanner scanner_recovery_intent" "$TMP_DIR/dry-run.out"
rg -q "e2e_test distributed::replication_quota_test::four_node_four_drive_hard_quota_rejects_over_limit_put" "$TMP_DIR/dry-run.out"
rg -q "target_dir=$PROJECT_ROOT/target" "$TMP_DIR/dry-run.out"

RUSTFS_SCANNER_HEAL_W16_OUTPUT_ROOT="$TMP_DIR/root-out" \
  bash "$RUNNER" --dry-run >"$TMP_DIR/dry-run-output-root.out"
rg -q "run_dir=$TMP_DIR/root-out/" "$TMP_DIR/dry-run-output-root.out"

CARGO_TARGET_DIR="$TMP_DIR/shared-target" bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence-with-target" >"$TMP_DIR/dry-run-target.out"
rg -q "target_dir=$TMP_DIR/shared-target" "$TMP_DIR/dry-run-target.out"
rg -q "current_binary=$TMP_DIR/shared-target/debug/rustfs" "$TMP_DIR/dry-run-target.out"

mkdir -p "$TMP_DIR/nonempty/g04-crash-boundaries"
touch "$TMP_DIR/nonempty/g04-crash-boundaries/existing.json"
if bash "$RUNNER" --dry-run --out-dir "$TMP_DIR/nonempty" >"$TMP_DIR/nonempty.out" 2>"$TMP_DIR/nonempty.err"; then
  echo "W16 runner should reject non-empty evidence case directories" >&2
  exit 1
fi
rg -q "evidence case directory is not empty" "$TMP_DIR/nonempty.err"

bash "$RUNNER" --self-test
