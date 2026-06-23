#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
FUZZ_DIR="$REPO_ROOT/fuzz"
MAX_TOTAL_TIME=${MAX_TOTAL_TIME:-60}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-artifacts}
FUZZ_TARGET=${FUZZ_TARGET:-}
BUILD_ONLY=${BUILD_ONLY:-0}

cd "$FUZZ_DIR"
mkdir -p "$ARTIFACT_ROOT"

targets="path_containment bucket_validation local_metadata"
if [ -n "$FUZZ_TARGET" ]; then
    targets="$FUZZ_TARGET"
fi

# Phase 1: build all targets once (avoids repeated compilation)
for target in $targets; do
    echo "==> cargo +nightly fuzz build $target"
    cargo +nightly fuzz build "$target"
done

if [ "$BUILD_ONLY" = "1" ]; then
    echo "==> Build-only mode; skipping fuzz runs."
    exit 0
fi

# Phase 2: run each target (incremental — no recompilation)
for target in $targets; do
    artifact_dir="$ARTIFACT_ROOT/$target"
    mkdir -p "$artifact_dir"
    echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME, -artifact_prefix=$artifact_dir/)"
    cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME" -artifact_prefix="$artifact_dir/"
done
