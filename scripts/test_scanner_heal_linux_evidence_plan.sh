#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="$SCRIPT_DIR/run_scanner_heal_linux_evidence_plan.py"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --self-test

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --phase preflight >"$TMP_DIR/preflight.out"
rg -q "stage=preflight" "$TMP_DIR/preflight.out"
rg -q "scripts/check_test_wiring.py --self-test" "$TMP_DIR/preflight.out"

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" \
  --phase functional \
  --source-revision aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --format json >"$TMP_DIR/functional.json"

"${RUSTFS_PYTHON_BIN:-python3}" - "$TMP_DIR/functional.json" <<'PY'
import json
import pathlib
import sys

plan = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert plan["schema"] == 1
assert plan["source_revision"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
assert [stage["id"] for stage in plan["stages"]] == ["functional"]
commands = [
    " ".join(entry["command"])
    for stage in plan["stages"]
    for step in stage["steps"]
    for entry in step["commands"]
]
assert any("background-target-crash-ec8-4-multi-pool" in command for command in commands)
assert any("run_scanner_heal_status_outcome_probe.py" in command for command in commands)
assert all("--case release" not in command for command in commands)
PY

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --write-plan --out-dir "$TMP_DIR/plan" >"$TMP_DIR/path.out"
PLAN_PATH="$(tr -d '\n' <"$TMP_DIR/path.out")"
test -s "$PLAN_PATH"
rg -q '"evidence_type": "plan_only"' "$PLAN_PATH"
rg -q '"stop_on_product_failure": true' "$PLAN_PATH"

if "${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --source-revision bad >/dev/null 2>"$TMP_DIR/bad.err"; then
  echo "invalid source revision should fail" >&2
  exit 1
fi
rg -q "source-revision" "$TMP_DIR/bad.err"
