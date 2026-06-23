#!/bin/sh

# Run a single fuzz target (no build phase).
# Intended for CI matrix jobs that share a pre-built fuzz harness.
#
# Required env:
#   FUZZ_TARGET     — name of the fuzz target to run
#
# Optional env:
#   MAX_TOTAL_TIME  — seconds to fuzz (default: 60)
#   ARTIFACT_ROOT   — artifact output directory (default: artifacts)

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
FUZZ_DIR="$REPO_ROOT/fuzz"
MAX_TOTAL_TIME=${MAX_TOTAL_TIME:-60}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-artifacts}

if [ -z "${FUZZ_TARGET:-}" ]; then
    echo "ERROR: FUZZ_TARGET is required" >&2
    exit 1
fi

cd "$FUZZ_DIR"
mkdir -p "$ARTIFACT_ROOT"

artifact_dir="$ARTIFACT_ROOT/$FUZZ_TARGET"
mkdir -p "$artifact_dir"
echo "==> cargo +nightly fuzz run $FUZZ_TARGET (-max_total_time=$MAX_TOTAL_TIME, -artifact_prefix=$artifact_dir/)"
cargo +nightly fuzz run "$FUZZ_TARGET" -- -max_total_time="$MAX_TOTAL_TIME" -artifact_prefix="$artifact_dir/"
