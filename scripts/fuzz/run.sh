#!/bin/sh

# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
#   FUZZ_SEED       — replay one libFuzzer seed (default: generate and record one per target)
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
FUZZ_SEED=${FUZZ_SEED:-}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-artifacts}
FUZZ_TARGET=${FUZZ_TARGET:-}
BUILD_ONLY=${BUILD_ONLY:-0}
SKIP_BUILD=${SKIP_BUILD:-0}
USE_PREBUILT_BINARY=${USE_PREBUILT_BINARY:-0}
PREBUILT_BINARY_DIR=${PREBUILT_BINARY_DIR:-}

if [ -n "$FUZZ_SEED" ]; then
    case "$FUZZ_SEED" in
        0*|*[!0-9]*)
            echo "FUZZ_SEED must be a positive decimal integer without leading zeroes: $FUZZ_SEED" >&2
            exit 1
            ;;
    esac
fi

cd "$FUZZ_DIR"
mkdir -p "$ARTIFACT_ROOT"

# All buildable fuzz bins registered in fuzz/Cargo.toml. Keep in sync with the
# smoke/nightly matrices in .github/workflows/fuzz.yml. The *_storage_api.rs
# files are `mod` submodules of their parent targets, not standalone bins.
targets="archive_extract bucket_validation local_metadata path_containment policy_ingress"
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

    seed="$FUZZ_SEED"
    if [ -z "$seed" ]; then
        seed=$(printf '%s\n' "${GITHUB_RUN_ID:-local}:${GITHUB_RUN_ATTEMPT:-0}:$target:$(date +%s):$$" | cksum | awk '{print $1}')
        if [ "$seed" = "0" ]; then
            seed=1
        fi
    fi
    revision=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf 'unknown')
    if [ "$revision" = "unknown" ]; then
        git_dirty="unknown"
    elif [ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal 2>/dev/null)" ]; then
        git_dirty="true"
    else
        git_dirty="false"
    fi
    if [ "$USE_PREBUILT_BINARY" = "1" ]; then
        runner_mode="prebuilt"
    else
        runner_mode="cargo-fuzz"
    fi
    {
        printf 'target=%s\n' "$target"
        printf 'seed=%s\n' "$seed"
        printf 'max_total_time=%s\n' "$MAX_TOTAL_TIME"
        printf 'git_revision=%s\n' "$revision"
        printf 'git_dirty=%s\n' "$git_dirty"
        printf 'runner_mode=%s\n' "$runner_mode"
    } > "$artifact_dir/run-manifest.txt"

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
        echo "==> $binary_path (-max_total_time=$MAX_TOTAL_TIME, -seed=$seed, -artifact_prefix=$artifact_dir/, corpus=$corpus_dir)"
        "$binary_path" -max_total_time="$MAX_TOTAL_TIME" -seed="$seed" -artifact_prefix="$artifact_dir/" "$corpus_dir"
        continue
    fi

    echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME, -seed=$seed, -artifact_prefix=$artifact_dir/)"
    cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME" -seed="$seed" -artifact_prefix="$artifact_dir/"
done
