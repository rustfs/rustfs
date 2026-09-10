#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/run_scanner_heal_status_outcome_evidence.py"
PROBE="$SCRIPT_DIR/run_scanner_heal_status_outcome_probe.py"

"${RUSTFS_PYTHON_BIN:-python3}" "$PROBE" --self-test
"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --self-test
