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
env -u CARGO_TARGET_DIR bash "$RUNNER" \
  --dry-run \
  --out-dir "$TMP_DIR/evidence" \
  --source-dir "$TMP_DIR/source" \
  --version 1.2.3 \
  --asset rustfs-linux-x86_64-gnu-v1.2.3.zip \
  --sha256 "$VALID_SHA" \
  --repository rustfs/rustfs >"$TMP_DIR/dry-run.out"

python3 - "$PROJECT_ROOT/crates/e2e_test/src/upgrade_compatibility_test.rs" "$TMP_DIR/dry-run.out" <<'PY'
from pathlib import Path
import re
import sys

source = Path(sys.argv[1])
declarations = re.findall(
    r"(?m)^#\[tokio::test\]\n((?:#\[[^\n]*\]\n)*)async fn (\w+)\(\)",
    source.read_text(),
)
plan = Path(sys.argv[2]).read_text()
filter_section = plan.split("test_filters:\n", 1)[1].split("required_artifacts:\n", 1)[0]
filters = re.findall(r"(?m)^  [^:\n]+: (\S+)$", filter_section)
if len(filters) != 2:
    raise SystemExit("G09 all-case plan must select two tests")
for selector in filters:
    matches = [attributes for attributes, name in declarations if selector == f"{source.stem}::{name}"]
    if len(matches) != 1 or not re.search(r"(?m)^#\[ignore(?:\s*=|\])", matches[0]):
        raise SystemExit(f"G09 filter does not select exactly one ignored test: {selector}")
    print(f"PASS: G09 filter selects an ignored test: {selector}")
PY
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
