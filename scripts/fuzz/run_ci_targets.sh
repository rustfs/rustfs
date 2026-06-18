#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
FUZZ_DIR="$REPO_ROOT/fuzz"
MAX_TOTAL_TIME=${MAX_TOTAL_TIME:-60}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-artifacts}
FUZZ_TARGET=${FUZZ_TARGET:-}

cd "$FUZZ_DIR"
mkdir -p "$ARTIFACT_ROOT"

targets="path_containment bucket_validation local_metadata"
if [ -n "$FUZZ_TARGET" ]; then
    targets="$FUZZ_TARGET"
fi

for target in $targets; do
    artifact_dir="$ARTIFACT_ROOT/$target"
    mkdir -p "$artifact_dir"
    echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME, -artifact_prefix=$artifact_dir/)"
    cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME" -artifact_prefix="$artifact_dir/"
done
