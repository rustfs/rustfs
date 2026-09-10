#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$SCRIPT_DIR/run_scanner_heal_legacy_rollback_evidence.py"

"${RUSTFS_PYTHON_BIN:-python3}" "$RUNNER" --self-test
