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

if "${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" \
  --phase functional \
  --source-revision aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --status-root "$TMP_DIR/missing-run" >"$TMP_DIR/status-missing.json"; then
  echo "missing evidence status should fail closed" >&2
  exit 1
fi
rg -q '"decision": "blocked"' "$TMP_DIR/status-missing.json"
rg -q '"release_approved": false' "$TMP_DIR/status-missing.json"
rg -q '"next_step"' "$TMP_DIR/status-missing.json"

if "${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" \
  --phase functional \
  --status-root "$TMP_DIR/missing-run" \
  --run-preflight >/dev/null 2>"$TMP_DIR/status-mode.err"; then
  echo "status mode should reject preflight execution" >&2
  exit 1
fi
rg -q "status-root cannot be combined" "$TMP_DIR/status-mode.err"

"${RUSTFS_PYTHON_BIN:-python3}" - "$RUNNER" "$TMP_DIR/complete-run" <<'PY'
import json
import pathlib
import subprocess
import sys

runner = pathlib.Path(sys.argv[1])
run_root = pathlib.Path(sys.argv[2])
plan = json.loads(subprocess.check_output([
    sys.executable,
    str(runner),
    "--phase",
    "functional",
    "--source-revision",
    "a" * 40,
    "--format",
    "json",
], text=True))
for stage in plan["stages"]:
    for step in stage["steps"]:
        for output in step.get("expected_outputs", []):
            path = pathlib.Path(output.replace("$RUN_ROOT", str(run_root)))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n")
status = json.loads(subprocess.check_output([
    sys.executable,
    str(runner),
    "--phase",
    "functional",
    "--source-revision",
    "a" * 40,
    "--status-root",
    str(run_root),
], text=True))
assert status["decision"] == "complete"
assert status["release_approved"] is False
assert status["artifact_totals"]["missing"] == 0
PY

"${RUSTFS_PYTHON_BIN:-python3}" - "$TMP_DIR" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
same_head = root / "same-head.json"
old_head = root / "old-head.json"
case_level = root / "case-level.json"
same_head.write_text(json.dumps({
    "schema": 1,
    "evidence": "measured",
    "source_revision": "a" * 40,
    "gates": {"G01": {}, "G02": {}},
}) + "\n")
old_head.write_text(json.dumps({
    "schema": 1,
    "evidence": "measured",
    "source_revision": "b" * 40,
    "gates": {"G03": {}, "G09": {}},
}) + "\n")
case_level.write_text(json.dumps({
    "schema": 1,
    "evidence": "case",
    "source_revision": "a" * 40,
    "gates": {"G14": {}},
}) + "\n")
PY

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" \
  --source-revision aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --descriptor-ledger "$TMP_DIR/same-head.json" "$TMP_DIR/old-head.json" "$TMP_DIR/case-level.json" \
  "$TMP_DIR/missing.json" >"$TMP_DIR/ledger.json"

"${RUSTFS_PYTHON_BIN:-python3}" - "$TMP_DIR/ledger.json" <<'PY'
import json
import pathlib
import sys

ledger = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert ledger["kind"] == "scanner-heal-descriptor-ledger"
assert ledger["release_approved"] is False
assert ledger["same_head_verified_gates"] == ["G01", "G02"]
assert ledger["old_head_measured_gates"] == ["G03", "G09"]
assert "G14" in ledger["missing_measured_gates"]
assert "G03" in ledger["missing_current_head_gates"]
assert ledger["totals"]["invalid_descriptors"] == 1
classifications = {entry["file_name"]: entry["classification"] for entry in ledger["entries"]}
assert classifications["same-head.json"] == "same-head verified"
assert classifications["old-head.json"] == "old-head measured, drift-readable"
assert classifications["case-level.json"] == "case-level only"
assert classifications["missing.json"] == "invalid"
PY

if "${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" \
  --phase performance \
  --run-preflight >/dev/null 2>"$TMP_DIR/no-preflight.err"; then
  echo "preflight execution without the preflight stage should fail" >&2
  exit 1
fi
rg -q "requires the preflight stage" "$TMP_DIR/no-preflight.err"

if "${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --source-revision bad >/dev/null 2>"$TMP_DIR/bad.err"; then
  echo "invalid source revision should fail" >&2
  exit 1
fi
rg -q "source-revision" "$TMP_DIR/bad.err"
