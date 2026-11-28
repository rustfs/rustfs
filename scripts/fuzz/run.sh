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
#   FUZZ_TARGET     — run only this target (default: all smoke targets; comma- or space-separated list allowed)
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

case "$MAX_TOTAL_TIME" in
    ''|0*|*[!0-9]*)
        echo "MAX_TOTAL_TIME must be a positive decimal integer without leading zeroes: $MAX_TOTAL_TIME" >&2
        exit 1
        ;;
esac

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
targets="archive_extract bucket_validation local_metadata notify_xml path_containment policy_ingress sigv4_request"
registered_targets="$targets"
if [ -n "$FUZZ_TARGET" ]; then
    requested=$(printf '%s' "$FUZZ_TARGET" | tr ',' ' ')
    validated=""
    for candidate in $requested; do
        matched=0
        for known in $registered_targets; do
            if [ "$candidate" = "$known" ]; then
                matched=1
                break
            fi
        done
        if [ "$matched" != "1" ]; then
            echo "Unregistered FUZZ_TARGET: $candidate" >&2
            exit 1
        fi
        validated="$validated $candidate"
    done
    targets=$(printf '%s' "$validated" | sed 's/^ //')
    if [ -z "$targets" ]; then
        echo "FUZZ_TARGET resolved to an empty target list" >&2
        exit 1
    fi
fi

sha256_of_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$1"
    fi
}

corpus_digest_info() {
    python3 - "$1" <<'PY'
import hashlib
import pathlib
import sys

corpus = pathlib.Path(sys.argv[1])
files = sorted(p for p in corpus.rglob("*") if p.is_file())
h = hashlib.sha256()
for path in files:
    rel = path.relative_to(corpus).as_posix().encode("utf-8")
    data = path.read_bytes()
    h.update(rel + b"\0" + str(len(data)).encode("ascii") + b"\0")
    h.update(hashlib.sha256(data).digest())
print(f"{len(files)} {h.hexdigest()}")
PY
}

# Phase 1: build (unless skipped)
if [ "$SKIP_BUILD" != "1" ]; then
    for target in $targets; do
        echo "==> cargo +nightly fuzz build $target"
        if [ -n "${CARGO_TARGET_DIR:-}" ]; then
            cargo +nightly fuzz build --target-dir "$CARGO_TARGET_DIR" "$target"
        else
            cargo +nightly fuzz build "$target"
        fi
    done
fi

if [ "$BUILD_ONLY" = "1" ]; then
    echo "==> Build-only mode; skipping fuzz runs."
    exit 0
fi

toolchain=$(rustc +nightly --version 2>/dev/null || rustc --version 2>/dev/null || printf 'unknown')

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

    corpus_info=$(corpus_digest_info "$corpus_dir")
    corpus_files=$(printf '%s\n' "$corpus_info" | awk '{print $1}')
    corpus_sha256=$(printf '%s\n' "$corpus_info" | awk '{print $2}')

    started_epoch=$(date +%s)
    exit_code=0

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
        set +e
        "$binary_path" -max_total_time="$MAX_TOTAL_TIME" -seed="$seed" -artifact_prefix="$artifact_dir/" "$corpus_dir"
        exit_code=$?
        set -e
    else
        echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME, -seed=$seed, -artifact_prefix=$artifact_dir/)"
        set +e
        if [ -n "${CARGO_TARGET_DIR:-}" ]; then
            cargo +nightly fuzz run --target-dir "$CARGO_TARGET_DIR" "$target" -- -max_total_time="$MAX_TOTAL_TIME" -seed="$seed" -artifact_prefix="$artifact_dir/"
        else
            cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME" -seed="$seed" -artifact_prefix="$artifact_dir/"
        fi
        exit_code=$?
        set -e
    fi

    finished_epoch=$(date +%s)
    elapsed_seconds=$((finished_epoch - started_epoch))

    reproducer_file=""
    reproducer_sha256=""
    for candidate in "$artifact_dir"/crash-* "$artifact_dir"/timeout-* "$artifact_dir"/oom-*; do
        if [ -f "$candidate" ]; then
            reproducer_file=$(basename "$candidate")
            reproducer_sha256=$(sha256_of_file "$candidate")
            break
        fi
    done

    {
        printf 'target=%s\n' "$target"
        printf 'toolchain=%s\n' "$toolchain"
        printf 'seed=%s\n' "$seed"
        printf 'max_total_time=%s\n' "$MAX_TOTAL_TIME"
        printf 'elapsed_seconds=%s\n' "$elapsed_seconds"
        printf 'exit_code=%s\n' "$exit_code"
        printf 'corpus_files=%s\n' "$corpus_files"
        printf 'corpus_sha256=%s\n' "$corpus_sha256"
        printf 'reproducer_file=%s\n' "$reproducer_file"
        printf 'reproducer_sha256=%s\n' "$reproducer_sha256"
        printf 'git_revision=%s\n' "$revision"
        printf 'git_dirty=%s\n' "$git_dirty"
        printf 'runner_mode=%s\n' "$runner_mode"
    } > "$artifact_dir/run-manifest.txt"

    python3 - "$artifact_dir/run-summary.json" \
        "$target" "$toolchain" "$seed" "$MAX_TOTAL_TIME" "$elapsed_seconds" "$exit_code" \
        "$corpus_files" "$corpus_sha256" "$reproducer_file" "$reproducer_sha256" \
        "$revision" "$git_dirty" "$runner_mode" <<'PY'
import json
import pathlib
import sys

(
    out_path,
    target,
    toolchain,
    seed,
    max_total_time,
    elapsed_seconds,
    exit_code,
    corpus_files,
    corpus_sha256,
    reproducer_file,
    reproducer_sha256,
    git_revision,
    git_dirty,
    runner_mode,
) = sys.argv[1:]

payload = {
    "schema_version": 1,
    "target": target,
    "toolchain": toolchain,
    "seed": int(seed),
    "max_total_time": int(max_total_time),
    "elapsed_seconds": int(elapsed_seconds),
    "exit_code": int(exit_code),
    "corpus_files": int(corpus_files),
    "corpus_sha256": corpus_sha256,
    "reproducer_file": reproducer_file or None,
    "reproducer_sha256": reproducer_sha256 or None,
    "git_revision": git_revision,
    "git_dirty": git_dirty,
    "runner_mode": runner_mode,
}
pathlib.Path(out_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

    if [ "$exit_code" -ne 0 ]; then
        exit "$exit_code"
    fi
done
