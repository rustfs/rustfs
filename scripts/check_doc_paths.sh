#!/usr/bin/env bash
# Check repository paths and local guidance links without fetching external URLs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/check_doc_paths.py" "$@"
