#!/usr/bin/env bash
# ci.yml's pull_request paths-ignore and ci-docs-only.yml's paths must be equal.
#
# ci-docs-only.yml exists to report the required checks for pull requests that
# ci.yml skips. The two lists are the complement of each other, so any drift
# breaks one of two ways, both silent:
#
#   - an entry only in ci.yml's paths-ignore: a PR touching only those files
#     triggers neither workflow, nobody reports "Test and Lint" or "Quick
#     Checks", and the PR waits on a required check forever;
#   - an entry only in ci-docs-only.yml's paths: both workflows run, which is
#     merely wasteful — but it also means the lists no longer describe the same
#     intent, and the next edit is made against a wrong assumption.
#
# The push paths-ignore in ci.yml is deliberately NOT compared: no required
# check is reported for push events, so it does not have to pair with anything.
#
# Also asserts ci-docs-only.yml still declares both companion job names, since a
# rename there produces exactly the permanent-pending failure above.
#
# Usage: scripts/check_ci_paths_sync.sh
set -euo pipefail

cd "$(dirname "$0")/.."

CI=".github/workflows/ci.yml"
DOCS=".github/workflows/ci-docs-only.yml"

# Print the quoted list items that follow $2 within the block introduced by $1.
# Both files keep these as a flat list of quoted scalars, so no YAML parser is
# needed and the script stays dependency-free like its check_* siblings.
extract() {
    local file="$1" event="$2" key="$3"
    awk -v event="$event" -v key="$key" '
        $0 ~ "^  " event ":[[:space:]]*$" { in_event = 1; next }
        in_event && /^  [a-z_]+:[[:space:]]*$/ { in_event = 0 }
        in_event && $0 ~ "^    " key ":[[:space:]]*$" { in_list = 1; next }
        in_list {
            if ($0 ~ /^      - /) {
                item = $0
                sub(/^      - /, "", item)
                gsub(/^"|"$/, "", item)
                print item
                next
            }
            if ($0 !~ /^[[:space:]]*#/ && $0 !~ /^[[:space:]]*$/) in_list = 0
        }
    ' "$file" | sort
}

ci_list="$(extract "$CI" "pull_request" "paths-ignore")"
docs_list="$(extract "$DOCS" "pull_request" "paths")"

if [ -z "$ci_list" ] || [ -z "$docs_list" ]; then
    echo "ERROR: could not read one of the path lists — did the file structure change?" >&2
    echo "  $CI pull_request.paths-ignore: $(printf '%s' "$ci_list" | grep -c . || true) entries" >&2
    echo "  $DOCS pull_request.paths: $(printf '%s' "$docs_list" | grep -c . || true) entries" >&2
    exit 1
fi

status=0

if ! diff_out="$(diff <(printf '%s\n' "$ci_list") <(printf '%s\n' "$docs_list"))"; then
    echo "ERROR: $CI pull_request paths-ignore and $DOCS paths have drifted." >&2
    echo "  '<' is only in $CI, '>' is only in $DOCS:" >&2
    printf '%s\n' "$diff_out" | sed 's/^/    /' >&2
    status=1
fi

for job_name in "Test and Lint" "Quick Checks"; do
    if ! grep -q "name: ${job_name}\$" "$DOCS"; then
        echo "ERROR: $DOCS no longer declares a job named '${job_name}'." >&2
        echo "  It is a required status check; without a companion job here, a" >&2
        echo "  docs-only PR waits on it forever." >&2
        status=1
    fi
done

if [ "$status" -ne 0 ]; then
    exit 1
fi

echo "OK: ci.yml and ci-docs-only.yml path lists agree ($(printf '%s\n' "$ci_list" | wc -l | tr -d ' ') entries)"
