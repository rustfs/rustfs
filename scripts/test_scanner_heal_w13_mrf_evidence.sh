#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${PROJECT_ROOT}/scripts/run_scanner_heal_w13_mrf_evidence.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$RUNNER"

bash "$RUNNER" --help >"$TMP_DIR/help.out"
rg -q "RUSTFS_SCANNER_HEAL_W13_OUTPUT_ROOT" "$TMP_DIR/help.out"
rg -q "RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT" "$TMP_DIR/help.out"
rg -q "G07-mrf_responsibility_oracle.json" "$TMP_DIR/help.out"
rg -q "G08-disk_full_matrix.json" "$TMP_DIR/help.out"
rg -q "P4-mrf_cleanup_gc_soak_evidence.json" "$TMP_DIR/help.out"

env -u CARGO_TARGET_DIR bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence" >"$TMP_DIR/dry-run.out"

rg -q "tests=g07 g08 p4" "$TMP_DIR/dry-run.out"
rg -q "test_filter=rustfs-heal heal::mrf_queue::tests::w13_mrf_release_evidence_outputs_bundle_artifacts" "$TMP_DIR/dry-run.out"
rg -q "target_dir=$PROJECT_ROOT/target" "$TMP_DIR/dry-run.out"

RUSTFS_SCANNER_HEAL_W13_OUTPUT_ROOT="$TMP_DIR/root-out" \
  bash "$RUNNER" --dry-run --test g07 >"$TMP_DIR/dry-run-output-root.out"
rg -q "run_dir=$TMP_DIR/root-out/" "$TMP_DIR/dry-run-output-root.out"

CARGO_TARGET_DIR="$TMP_DIR/shared-target" bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence-with-target" \
  --test g08 \
  --enospc-root "$TMP_DIR/enospc" >"$TMP_DIR/dry-run-target.out"
rg -q "target_dir=$TMP_DIR/shared-target" "$TMP_DIR/dry-run-target.out"
rg -q "current_binary=$TMP_DIR/shared-target/debug/rustfs" "$TMP_DIR/dry-run-target.out"
rg -q "enospc_root=$TMP_DIR/enospc" "$TMP_DIR/dry-run-target.out"

mkdir -p "$TMP_DIR/nonempty/g07-mrf-responsibility"
touch "$TMP_DIR/nonempty/g07-mrf-responsibility/existing.json"
if bash "$RUNNER" --dry-run --out-dir "$TMP_DIR/nonempty" >"$TMP_DIR/nonempty.out" 2>"$TMP_DIR/nonempty.err"; then
  echo "W13 runner should reject non-empty evidence case directories" >&2
  exit 1
fi
rg -q "evidence case directory is not empty" "$TMP_DIR/nonempty.err"

if bash "$RUNNER" --plan-only --test p4 --soak-seconds 10 >/dev/null 2>&1; then
  echo "W13 runner should reject short P4 release soak without --allow-short-soak" >&2
  exit 1
fi

bash "$RUNNER" --self-test
"$PROJECT_ROOT/scripts/python_bin.sh" "$PROJECT_ROOT/scripts/run_scanner_heal_mrf_evidence.py" --self-test
