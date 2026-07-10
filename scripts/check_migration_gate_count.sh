#!/usr/bin/env bash
# Count-floor guard for the migration-critical test gate (backlog#1153 infra-12).
#
# ci.yml's migration gate selects tests BY NAME SUBSTRING. A rename that drops
# a test out of the filter silently thins the gate — potentially to zero —
# without any CI signal (this is how the layered gate died when #878 closed).
# This script owns the filter expression so the count check and the test run
# cannot drift, and fails fast when the number of selected tests drops below
# the committed floor in .config/migration-gate-floor.txt.
#
# NAMING CONVENTION DEPENDENCY: migration-gate tests are matched by these
# reserved name substrings:
#
#   data_movement, rebalance, decommission, source_cleanup, delete_marker
#
# Renaming a test away from all of these substrings removes it from the gate.
# The floor equals the exact selected-test count at the time it was last
# committed, so ANY reduction fails CI. To shrink the gate intentionally
# (test removed or consciously renamed), update the floor file in the same PR
# so the reduction is visible in the diff. Adding tests never requires a floor
# bump (the floor is a lower bound), but bumping it keeps the guard tight.
#
# Per-pattern floors are deliberately omitted: with an exact-count total floor,
# a single test dropping out of any pattern already fails the check, so
# per-pattern bookkeeping would add churn without detection power.
#
# Usage:
#   scripts/check_migration_gate_count.sh          # count check + run the gate
#   scripts/check_migration_gate_count.sh check    # count check only
#   scripts/check_migration_gate_count.sh run      # run the gate only

set -euo pipefail

cd "$(dirname "$0")/.."

# Single source of truth for the migration-gate filter. ci.yml must invoke
# this script instead of inlining the expression.
MIGRATION_GATE_FILTER='test(data_movement) or test(rebalance) or test(decommission) or test(source_cleanup) or test(delete_marker)'
FLOOR_FILE=".config/migration-gate-floor.txt"

mode="${1:-all}"
case "$mode" in
    all | check | run) ;;
    *)
        echo "usage: $0 [all|check|run]" >&2
        exit 2
        ;;
esac

if [[ "$mode" == "all" || "$mode" == "check" ]]; then
    floor="$(grep -Ev '^[[:space:]]*(#|$)' "$FLOOR_FILE" | head -n1 | tr -d '[:space:]')"
    if ! [[ "$floor" =~ ^[0-9]+$ ]]; then
        echo "error: $FLOOR_FILE does not contain a numeric floor (got: '$floor')" >&2
        exit 1
    fi

    # Count via the structured JSON listing, not the human format: the
    # human format's indentation varies across nextest versions (CI's newer
    # nextest emitted a layout where no line matched '^    ', collapsing the
    # count to 0 and failing every PR). A nextest failure aborts via set -e
    # with its stderr visible; a JSON schema change makes jq fail loudly
    # rather than silently passing.
    count="$(cargo nextest list -p rustfs-ecstore --lib -E "$MIGRATION_GATE_FILTER" --message-format json \
        | jq '[."rust-suites"[].testcases[] | select(."filter-match".status == "matches")] | length')"
    if ! [[ "$count" =~ ^[0-9]+$ ]]; then
        echo "error: could not parse nextest JSON listing (got count: '$count')" >&2
        exit 1
    fi

    if ((count < floor)); then
        echo "error: migration gate selects $count tests, below the committed floor of $floor." >&2
        echo "" >&2
        echo "The gate matches tests by name substring (data_movement / rebalance /" >&2
        echo "decommission / source_cleanup / delete_marker). A rename or removal has" >&2
        echo "thinned the selection. Either restore the test names, or — if the" >&2
        echo "reduction is intentional — update $FLOOR_FILE in this PR so the" >&2
        echo "gate change is explicit and reviewable." >&2
        exit 1
    fi
    echo "migration gate count OK: $count selected tests (floor: $floor)"
fi

if [[ "$mode" == "all" || "$mode" == "run" ]]; then
    cargo nextest run -p rustfs-ecstore --lib -E "$MIGRATION_GATE_FILTER"
fi
