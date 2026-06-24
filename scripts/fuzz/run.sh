#!/bin/sh

# Unified fuzz runner script.
#
# Modes:
#   ./scripts/fuzz/run.sh                        # build + run all smoke targets (60s each)
#   BUILD_ONLY=1 ./scripts/fuzz/run.sh           # build only, no fuzz run
#   FUZZ_TARGET=path_containment ./scripts/fuzz/run.sh   # build + run single target
#   MAX_TOTAL_TIME=300 ./scripts/fuzz/run.sh     # nightly-style 300s per target
#
# Environment variables:
#   FUZZ_TARGET     — run only this target (default: all smoke targets)
#   MAX_TOTAL_TIME  — seconds to fuzz per target (default: 60)
#   ARTIFACT_ROOT   — artifact output directory (default: artifacts)
#   BUILD_ONLY      — set to 1 to skip fuzz runs (default: 0)
#   SKIP_BUILD      — set to 1 to skip build phase (default: 0)
#   USE_PREBUILT_BINARY — set to 1 to run a prebuilt fuzz binary directly
#   PREBUILT_BINARY_DIR — directory containing prebuilt fuzz binaries

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
FUZZ_DIR="$REPO_ROOT/fuzz"
MAX_TOTAL_TIME=${MAX_TOTAL_TIME:-60}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-artifacts}
FUZZ_TARGET=${FUZZ_TARGET:-}
BUILD_ONLY=${BUILD_ONLY:-0}
SKIP_BUILD=${SKIP_BUILD:-0}
USE_PREBUILT_BINARY=${USE_PREBUILT_BINARY:-0}
PREBUILT_BINARY_DIR=${PREBUILT_BINARY_DIR:-}

cd "$FUZZ_DIR"
mkdir -p "$ARTIFACT_ROOT"

targets="path_containment bucket_validation local_metadata"
if [ -n "$FUZZ_TARGET" ]; then
    targets="$FUZZ_TARGET"
fi

# Phase 1: build (unless skipped)
if [ "$SKIP_BUILD" != "1" ]; then
    for target in $targets; do
        echo "==> cargo +nightly fuzz build $target"
        cargo +nightly fuzz build "$target"
    done
fi

if [ "$BUILD_ONLY" = "1" ]; then
    echo "==> Build-only mode; skipping fuzz runs."
    exit 0
fi

# Phase 2: run each target (incremental — no recompilation if already built)
for target in $targets; do
    artifact_dir="$ARTIFACT_ROOT/$target"
    corpus_dir="$FUZZ_DIR/corpus/$target"
    mkdir -p "$artifact_dir"
    mkdir -p "$corpus_dir"

    if [ "$USE_PREBUILT_BINARY" = "1" ]; then
        binary_dir="$PREBUILT_BINARY_DIR"
        if [ -z "$binary_dir" ]; then
            if [ -n "${CARGO_BUILD_TARGET:-}" ]; then
                binary_dir="$FUZZ_DIR/target/${CARGO_BUILD_TARGET}/release"
            else
                binary_dir="$FUZZ_DIR/target/release"
            fi
        fi
        binary_path="$binary_dir/$target"
        if [ ! -x "$binary_path" ]; then
            echo "Missing executable prebuilt fuzz binary: $binary_path" >&2
            exit 1
        fi
        echo "==> $binary_path (-max_total_time=$MAX_TOTAL_TIME, -artifact_prefix=$artifact_dir/, corpus=$corpus_dir)"
        "$binary_path" -max_total_time="$MAX_TOTAL_TIME" -artifact_prefix="$artifact_dir/" "$corpus_dir"
        continue
    fi

    echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME, -artifact_prefix=$artifact_dir/)"
    cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME" -artifact_prefix="$artifact_dir/"
done
