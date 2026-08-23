#!/usr/bin/env bash
# Every `uses: ./.github/actions/setup` must state `cache-save-if` explicitly.
#
# The composite action's default is "false" (fail-safe: a forgotten input costs
# a cold cache, not a slice of the repository-wide 10GB Actions cache quota).
# But a silent default in either direction is how audit.yml ended up saving a
# ~843MB PR-scoped entry on every dependency change, evicting the main-scoped
# lanes that the expensive jobs restore from. Requiring the input to be spelled
# out keeps the decision visible in review.
#
# Usage: scripts/security/check_cache_save_if.sh
set -euo pipefail

cd "$(dirname "$0")/../.."

status=0

for file in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -e "$file" ] || continue

    # Walk the file, and for every `uses: ./.github/actions/setup` scan the rest
    # of that step for cache-save-if. `uses:` and `with:` are siblings at the
    # same indentation, so the step ends only at a line indented *less* than the
    # `uses:` line (the next `- name:` item, or a dedent out of the steps list).
    awk -v file="$file" '
        function flush() {
            if (pending && !found) {
                printf "%s:%d: `uses: ./.github/actions/setup` without an explicit `cache-save-if:`\n", file, uses_line > "/dev/stderr"
                bad++
            }
            pending = 0; found = 0
        }
        {
            line = $0
            sub(/[[:space:]]+$/, "", line)
            if (line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next

            match(line, /^[[:space:]]*/)
            indent = RLENGTH

            if (pending && indent < uses_indent) flush()

            if (line ~ /uses:[[:space:]]*\.\/\.github\/actions\/setup[[:space:]]*$/) {
                flush()
                pending = 1; found = 0
                uses_indent = indent
                uses_line = NR
                next
            }

            if (pending && line ~ /^[[:space:]]*cache-save-if:/) found = 1
        }
        END { flush(); exit (bad > 0) }
    ' "$file" || status=1
done

if [ "$status" -ne 0 ]; then
    echo "" >&2
    echo "Add an explicit cache-save-if to each call above, e.g." >&2
    echo "    cache-save-if: \${{ github.ref == 'refs/heads/main' }}   # this lane owns the key" >&2
    echo "    cache-save-if: 'false'                                   # this lane only reads it" >&2
    echo "Exactly one lane per shared-key may save; see rustfs/backlog#1600." >&2
    exit 1
fi

echo "OK: every ./.github/actions/setup call states cache-save-if explicitly"

# rust-cache hashes every CARGO*, CC*, CFLAGS*, CXX*, CMAKE*, and RUST*
# variable that is present when the setup action runs. The dedicated writer
# and the CI readers therefore need identical workflow-level compiler env.
compiler_env() {
    awk '
        /^env:[[:space:]]*$/ { in_env = 1; next }
        in_env && /^[^[:space:]]/ { exit }
        in_env && /^  (CARGO|CC|CFLAGS|CXX|CMAKE|RUST)[A-Z0-9_]*:/ { print }
    ' "$1" | sort
}

ci_env="$(compiler_env .github/workflows/ci.yml)"
warm_env="$(compiler_env .github/workflows/cache-warm.yml)"

if [ "$ci_env" != "$warm_env" ]; then
    echo "CI and cache-warm compiler environments differ; rust-cache keys will not match:" >&2
    diff -u <(printf '%s\n' "$ci_env") <(printf '%s\n' "$warm_env") >&2 || true
    exit 1
fi

echo "OK: cache-warm and CI compiler environments match"
