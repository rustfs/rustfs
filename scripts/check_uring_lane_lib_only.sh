#!/usr/bin/env bash
# The io_uring CI lane runs `cargo test -p rustfs-ecstore --lib uring_`.
#
# `--lib` is there to avoid compiling and linking the 7 integration binaries
# under crates/ecstore/tests/, which selected zero tests for this filter. That is
# only safe while it stays true: add a matching test under tests/ and `--lib`
# would skip it silently, with CI staying green — the failure mode is invisible,
# so it is asserted here instead.
#
# The pattern must use the same `uring_` substring semantics as libtest, which
# also matches names containing `during_`. Narrowing it to `io_uring` would make
# this guard disagree with the filter it is protecting.
#
# Usage: scripts/check_uring_lane_lib_only.sh
set -euo pipefail

cd "$(dirname "$0")/.."

TESTS_DIR="crates/ecstore/tests"

if [ ! -d "$TESTS_DIR" ]; then
    echo "OK: $TESTS_DIR does not exist; nothing for --lib to miss"
    exit 0
fi

hits="$(grep -rEn '^[[:space:]]*(pub[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+[A-Za-z0-9_]*uring_' \
        "$TESTS_DIR" || true)"

if [ -n "$hits" ]; then
    echo "ERROR: test functions matching the 'uring_' substring found under $TESTS_DIR:" >&2
    printf '%s\n' "$hits" | sed 's/^/  /' >&2
    echo "" >&2
    echo "The io_uring lane in .github/workflows/ci.yml uses --lib, so these would" >&2
    echo "never run and CI would stay green. Pick one:" >&2
    echo "  - move them into a #[cfg(test)] mod under crates/ecstore/src, or" >&2
    echo "  - drop --lib from that step (and pay the link cost for all 7 binaries), or" >&2
    echo "  - add an explicit --test <name> for the binary and update this script." >&2
    exit 1
fi

echo "OK: no 'uring_'-matching test functions under $TESTS_DIR; --lib is safe"
