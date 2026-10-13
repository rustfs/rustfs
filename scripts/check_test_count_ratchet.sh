#!/usr/bin/env bash
# Per-crate test-count ratchet for the gateway migration (rustfs/backlog#2734
# section 1 rule 3; task T0.3 in rustfs/backlog#2738).
#
# No PR may lower the number of tests in any workspace crate, and no PR may
# add #[ignore]: "delete the test to get green" is rejected in CI. The counter
# is deliberately per crate, never a workspace total — a drop in one crate
# must not hide behind an addition in another.
#
# Workspace members come from `cargo metadata --no-deps` (the same source that
# `Cargo.toml` [workspace].members feeds), so a renamed or added crate is seen
# without a hand-kept list. For each member two counters are taken over every
# .rs file under its package directory (src/, tests/, benches/, examples/):
#
#   tests    lines matching TEST_PATTERN: #[test], #[tokio::test],
#            #[tokio::test(flavor = ...)], and each #[test_case(...)] or
#            #[test_case::test_case(...)] case. The spec's literal
#            `#\[(tokio::)?test\]` misses the tokio attributes that carry
#            arguments and every test_case case (792 and 477 lines when this
#            ratchet landed), so the pattern is widened here on purpose.
#   ignored  lines matching IGNORE_PATTERN: #[ignore] and #[ignore = "..."].
#
# Both patterns are anchored at line start. rustfmt (enforced by Quick Checks)
# keeps attributes on their own line, so the anchor costs nothing and keeps
# doc comments that mention `#[test]` out of the count.
#
# The counts are compared row by row with .config/test-count-baseline.txt
# (`<package>\t<tests>\t<ignored>`, one row per member). The baseline is an
# EXACT value, not a floor, in both columns:
#
#   - tests below the row fails: `crate <name>: <actual> < baseline <n>`.
#     Restore the tests. If the removal is intentional, lower the row in the
#     same PR and justify it in the PR description, so the reduction is visible
#     in the diff instead of disappearing into a green check.
#   - tests above the row fails as well, telling the author to raise the row in
#     the same PR. An exact baseline cannot drift upward silently, so the next
#     deletion is always measured against the true count.
#   - ignored above the row fails (the line contains `ignored`); adding
#     #[ignore] is not accepted on gateway/integration. Ignored below the row
#     fails too, asking for the row to be lowered so that ratchet stays tight.
#   - a row whose crate is no longer a member fails (deleting a crate deletes
#     its tests), and a member without a row fails.
#
# `--update` rewrites the baseline from the current tree. Review the diff it
# produces before committing it; it is the only sanctioned way to change the
# file other than editing the single row a PR touched.
#
# Usage:
#   scripts/check_test_count_ratchet.sh            # compare against the baseline
#   scripts/check_test_count_ratchet.sh --update   # regenerate the baseline

set -euo pipefail

cd "$(dirname "$0")/.."

BASELINE_FILE=".config/test-count-baseline.txt"
TEST_PATTERN='^[[:space:]]*#\[((tokio::)?test([]]|\()|(test_case::)?test_case\()'
IGNORE_PATTERN='^[[:space:]]*#\[ignore([]]|[[:space:]]*=)'

mode="check"
case "${1:-}" in
    "") ;;
    --update) mode="update" ;;
    *)
        echo "usage: $0 [--update]" >&2
        exit 2
        ;;
esac

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# One `<package>\t<dir>` line per workspace member, sorted. python3 is already
# a Quick Checks dependency (check_test_wiring.py); jq is not.
cargo metadata --no-deps --format-version 1 --offline |
    python3 -c '
import json, os, sys
meta = json.load(sys.stdin)
members = set(meta["workspace_members"])
root = meta["workspace_root"]
for pkg in meta["packages"]:
    if pkg["id"] in members:
        pkg_dir = os.path.relpath(os.path.dirname(pkg["manifest_path"]), root)
        print(pkg["name"] + "\t" + pkg_dir)
' | LC_ALL=C sort >"$TMP_DIR/packages"

if ! grep -q . "$TMP_DIR/packages"; then
    echo "error: cargo metadata reported no workspace members" >&2
    exit 1
fi

# grep exits 1 on zero matches (a legitimate count of 0 for a crate without
# tests) and >1 on real errors; only the latter may abort the check.
count_matching_lines() {
    local pattern="$1" dir="$2" status=0
    grep -rE --include='*.rs' -- "$pattern" "$dir" >"$TMP_DIR/matches" || status=$?
    if ((status > 1)); then
        echo "error: 'grep -rE $pattern $dir' failed with status $status" >&2
        exit 1
    fi
    grep -c . "$TMP_DIR/matches" || true
}

while IFS=$'\t' read -r name dir; do
    if [[ ! -d "$dir" ]]; then
        echo "error: package $name has no directory at $dir" >&2
        exit 1
    fi
    tests="$(count_matching_lines "$TEST_PATTERN" "$dir")"
    ignored="$(count_matching_lines "$IGNORE_PATTERN" "$dir")"
    printf '%s\t%s\t%s\n' "$name" "$tests" "$ignored"
done <"$TMP_DIR/packages" >"$TMP_DIR/actual"

crate_count="$(grep -c . "$TMP_DIR/actual")"
total_tests="$(awk -F'\t' '{sum += $2} END {print sum + 0}' "$TMP_DIR/actual")"
total_ignored="$(awk -F'\t' '{sum += $3} END {print sum + 0}' "$TMP_DIR/actual")"

# Sanity assertion: every crate counting 0 means the counter broke (a grep
# that searched the wrong tree or a pattern no rustfmt-formatted attribute
# matches), not that the workspace has no tests. Fail hard rather than
# writing or comparing against an all-zero table.
if ((total_tests == 0)); then
    echo "error: counted 0 tests across $crate_count crates; the counter itself is broken" >&2
    exit 1
fi

if [[ "$mode" == "update" ]]; then
    {
        echo "# Per-crate test-count baseline consumed by scripts/check_test_count_ratchet.sh"
        echo "# (rustfs/backlog#2734 section 1 rule 3; T0.3 in rustfs/backlog#2738)."
        echo "#"
        echo "# One row per workspace member: <package>, tests, ignored, tab-separated."
        echo "# tests counts #[test], #[tokio::test], #[tokio::test(...)] and #[test_case(...)]"
        echo "# attribute lines under the package directory; ignored counts #[ignore] and"
        echo "# #[ignore = \"...\"]. Both columns are exact: CI fails when a crate's tests fall"
        echo "# below or rise above its row, or when its ignored count changes. Raise a row in"
        echo "# the PR that adds tests; lowering one is a reviewed statement that tests were"
        echo "# intentionally removed. Regenerate with:"
        echo "#   scripts/check_test_count_ratchet.sh --update"
        cat "$TMP_DIR/actual"
    } >"$BASELINE_FILE"
    echo "wrote $BASELINE_FILE: $crate_count crates, $total_tests tests, $total_ignored ignored"
    exit 0
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "error: $BASELINE_FILE is missing; create it with '$0 --update' and commit it" >&2
    exit 1
fi

grep -Ev '^[[:space:]]*(#|$)' "$BASELINE_FILE" >"$TMP_DIR/baseline" || true

if ! grep -q . "$TMP_DIR/baseline"; then
    echo "error: $BASELINE_FILE has no baseline rows" >&2
    exit 1
fi
if grep -nEv $'^[^\t]+\t[0-9]+\t[0-9]+$' "$TMP_DIR/baseline" >"$TMP_DIR/malformed"; then
    printf 'error: malformed rows in %s (expected <package>\\t<tests>\\t<ignored>):\n' "$BASELINE_FILE" >&2
    sed 's/^/  line /' "$TMP_DIR/malformed" >&2
    exit 1
fi
if cut -f1 "$TMP_DIR/baseline" | LC_ALL=C sort | uniq -d | grep . >"$TMP_DIR/duplicates"; then
    echo "error: duplicate rows in $BASELINE_FILE for:" >&2
    sed 's/^/  /' "$TMP_DIR/duplicates" >&2
    exit 1
fi

# Every violation is one line. The `<actual> < baseline <n>` and `ignored`
# shapes are the acceptance contract of rustfs/backlog#2738; keep them.
awk -F'\t' '
NR == FNR {
    baseline_tests[$1] = $2
    baseline_ignored[$1] = $3
    next
}
{
    seen[$1] = 1
    if (!($1 in baseline_tests)) {
        printf "crate %s: no baseline row (%d tests, %d ignored); add it in this PR\n", $1, $2, $3
        next
    }
    if ($2 + 0 < baseline_tests[$1] + 0) {
        printf "crate %s: %d < baseline %d\n", $1, $2, baseline_tests[$1]
    } else if ($2 + 0 > baseline_tests[$1] + 0) {
        printf "crate %s: %d > baseline %d; raise the baseline row to %d in this PR\n", $1, $2, baseline_tests[$1], $2
    }
    if ($3 + 0 > baseline_ignored[$1] + 0) {
        printf "crate %s: %d ignored > baseline %d; adding #[ignore] is not accepted\n", $1, $3, baseline_ignored[$1]
    } else if ($3 + 0 < baseline_ignored[$1] + 0) {
        printf "crate %s: %d ignored < baseline %d; lower the baseline row to %d in this PR\n", $1, $3, baseline_ignored[$1], $3
    }
}
END {
    for (name in baseline_tests) {
        if (!(name in seen)) {
            printf "crate %s: not a workspace member (baseline %d tests); remove the row in this PR if the crate was intentionally deleted\n", name, baseline_tests[name]
        }
    }
}
' "$TMP_DIR/baseline" "$TMP_DIR/actual" | LC_ALL=C sort >"$TMP_DIR/violations"

if grep -q . "$TMP_DIR/violations"; then
    cat "$TMP_DIR/violations" >&2
    echo "error: test-count ratchet violated (rustfs/backlog#2734 section 1 rule 3)." >&2
    echo "  A crate below its baseline lost tests: restore them. If the removal is" >&2
    echo "  intentional, lower that row in $BASELINE_FILE in the same PR and" >&2
    echo "  justify it in the PR description. A crate above its baseline raises its" >&2
    echo "  row in the same PR ('$0 --update' rewrites the file; review the diff)." >&2
    exit 1
fi

echo "test-count ratchet OK: $crate_count crates, $total_tests tests, $total_ignored ignored match $BASELINE_FILE"
