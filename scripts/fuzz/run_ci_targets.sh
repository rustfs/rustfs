#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
FUZZ_DIR="$REPO_ROOT/fuzz"
MAX_TOTAL_TIME=${MAX_TOTAL_TIME:-60}

cd "$FUZZ_DIR"

for target in path_containment bucket_validation local_metadata; do
    echo "==> cargo +nightly fuzz run $target (-max_total_time=$MAX_TOTAL_TIME)"
    cargo +nightly fuzz run "$target" -- -max_total_time="$MAX_TOTAL_TIME"
done
