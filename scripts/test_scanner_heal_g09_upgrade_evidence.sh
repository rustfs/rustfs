#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${PROJECT_ROOT}/scripts/run_scanner_heal_g09_upgrade_evidence.sh"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$RUNNER"

bash "$RUNNER" --help >"$TMP_DIR/help.out"
rg -q "RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR" "$TMP_DIR/help.out"
rg -q "mixed-version-upgrade/G09-mixed_version_reader_evidence.json" "$TMP_DIR/help.out"
rg -q "bucket-config-rollback/G09-rollback_payload_evidence.json" "$TMP_DIR/help.out"

if bash "$RUNNER" --dry-run --sha256 bad >"$TMP_DIR/bad-sha.out" 2>"$TMP_DIR/bad-sha.err"; then
  echo "G09 runner should reject invalid SHA-256 input" >&2
  exit 1
fi
rg -q -- "--sha256 must be a 64-character lowercase hex digest" "$TMP_DIR/bad-sha.err"

VALID_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence" \
  --source-dir "$TMP_DIR/source" \
  --version 1.2.3 \
  --asset rustfs-linux-x86_64-gnu-v1.2.3.zip \
  --sha256 "$VALID_SHA" \
  --repository rustfs/rustfs >"$TMP_DIR/dry-run.out"

rg -q "upgrade_compatibility_test::rolling_upgrade_from_rc2_preserves_mixed_version_contracts" "$TMP_DIR/dry-run.out"
rg -q "upgrade_compatibility_test::rollback_to_previous_release_reads_current_bucket_metadata" "$TMP_DIR/dry-run.out"
rg -q "$TMP_DIR/evidence/mixed-version-upgrade/G09-mixed_version_writer_evidence.json" "$TMP_DIR/dry-run.out"
rg -q "target_dir=$PROJECT_ROOT/target" "$TMP_DIR/dry-run.out"
rg -q "https://github.com/rustfs/rustfs/releases/download/1.2.3/rustfs-linux-x86_64-gnu-v1.2.3.zip" "$TMP_DIR/dry-run.out"

CARGO_TARGET_DIR="$TMP_DIR/shared-target" bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence-with-target" \
  --source-dir "$TMP_DIR/source" \
  --sha256 "$VALID_SHA" >"$TMP_DIR/dry-run-target.out"
rg -q "target_dir=$TMP_DIR/shared-target" "$TMP_DIR/dry-run-target.out"
rg -q "current_binary=$TMP_DIR/shared-target/debug/rustfs" "$TMP_DIR/dry-run-target.out"

mkdir -p "$TMP_DIR/nonempty/mixed-version-upgrade"
touch "$TMP_DIR/nonempty/mixed-version-upgrade/existing.json"
if bash "$RUNNER" --dry-run --out-dir "$TMP_DIR/nonempty" --sha256 "$VALID_SHA" >"$TMP_DIR/nonempty.out" 2>"$TMP_DIR/nonempty.err"; then
  echo "G09 runner should reject non-empty evidence case directories" >&2
  exit 1
fi
rg -q "evidence case directory is not empty" "$TMP_DIR/nonempty.err"
