#!/usr/bin/env bash
# Count-floor guard for the security negative-auth smoke subset (backlog#1151
# sec-5), built on the infra-12 mechanism (scripts/check_migration_gate_count.sh,
# backlog#1153).
#
# The e2e-smoke profile in .config/nextest.toml selects tests BY MODULE NAME.
# The three attacker-facing S3 auth-rejection suites — negative_sigv4_test
# (header SigV4, sec-1), presigned_negative_test (presigned URL, sec-2), and
# admin_auth_test (admin gate + root-credential lifecycle, sec-4) — are wired
# into that filter so they run on every PR. A rename that drops a suite out of
# the filter (or a deletion) silently thins the security gate to nothing with no
# CI signal, exactly the failure mode the migration-gate guard was built for.
#
# This script lists what the e2e-smoke profile ACTUALLY selects and counts the
# security suites among them, then fails when that number drops below the
# committed floor in .config/security-smoke-floor.txt. Because it lists via the
# profile (not a standalone name filter), it verifies real smoke MEMBERSHIP: a
# suite renamed without updating the nextest.toml filter stops being selected,
# its tests vanish from the listing, and the count falls below the floor. To
# shrink the subset intentionally, update the floor file in the same PR so the
# reduction is explicit and reviewable (adding tests never requires a bump —
# the floor is a lower bound — but bumping keeps the guard tight).
#
# NAMING CONVENTION DEPENDENCY: the security suites are matched by these
# reserved module-name prefixes:
#
#   negative_sigv4_test, presigned_negative_test, admin_auth_test
#
# Renaming a suite away from all of these prefixes removes it from the count.
# Keep this regex in sync with the module names in the e2e-smoke filter.
#
# Usage:
#   scripts/check_security_smoke_count.sh          # count check (alias of check)
#   scripts/check_security_smoke_count.sh check    # count check only

set -euo pipefail

cd "$(dirname "$0")/.."

# Anchored module-name prefixes of the security negative-auth smoke subset.
# Kept deliberately in lockstep with the e2e-smoke filter in .config/nextest.toml.
SECURITY_SMOKE_REGEX='^(negative_sigv4|presigned_negative|admin_auth)_test::'
FLOOR_FILE=".config/security-smoke-floor.txt"

mode="${1:-check}"
case "$mode" in
    all | check) ;;
    *)
        echo "usage: $0 [check]" >&2
        exit 2
        ;;
esac

floor="$(grep -Ev '^[[:space:]]*(#|$)' "$FLOOR_FILE" | head -n1 | tr -d '[:space:]')"
if ! [[ "$floor" =~ ^[0-9]+$ ]]; then
    echo "error: $FLOOR_FILE does not contain a numeric floor (got: '$floor')" >&2
    exit 1
fi

# List via the e2e-smoke PROFILE so the default-filter is applied: only the
# tests the PR smoke suite would actually run appear in the output. Count the
# ones whose test name starts with a security module prefix. The structured JSON
# listing is used (not the human format) for the same reason infra-12 does: the
# human format's indentation varies across nextest versions; a JSON schema change
# makes jq fail loudly rather than silently collapsing the count to zero.
count="$(cargo nextest list --profile e2e-smoke -p e2e_test --message-format json \
    | jq --arg re "$SECURITY_SMOKE_REGEX" \
        '[."rust-suites"[].testcases | to_entries[] | select(.key | test($re))] | length')"
if ! [[ "$count" =~ ^[0-9]+$ ]]; then
    echo "error: could not parse nextest JSON listing (got count: '$count')" >&2
    exit 1
fi

if ((count < floor)); then
    echo "error: e2e-smoke selects $count security auth-rejection tests, below the committed floor of $floor." >&2
    echo "" >&2
    echo "The security smoke subset is matched by module name (negative_sigv4_test /" >&2
    echo "presigned_negative_test / admin_auth_test) in the e2e-smoke default-filter" >&2
    echo "(.config/nextest.toml). A rename or removal has dropped one out of the smoke" >&2
    echo "suite. Either restore the module names (and this script's regex), or — if the" >&2
    echo "reduction is intentional — update $FLOOR_FILE in this PR so the" >&2
    echo "security-gate change is explicit and reviewable." >&2
    exit 1
fi
echo "security smoke count OK: $count selected auth-rejection tests (floor: $floor)"
