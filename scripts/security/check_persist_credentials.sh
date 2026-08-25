#!/usr/bin/env bash
# Every actions/checkout must set persist-credentials: false, or carry an
# explicit exemption comment.
#
# By default checkout writes its token into .git/config as an http extraheader,
# where it stays for the rest of the job. On this repository that matters more
# than usual: pull_request jobs run on self-hosted runners and execute the PR's
# own build.rs, proc-macros and tests, any of which can read that file. The
# post-failure cancellation job holds actions: write but never checks out
# repository code, so untrusted build scripts cannot read that token from Git.
#
# A checkout is exempt only when the token IS the credential the job needs —
# helm-package pushes to rustfs/helm with it. Mark those with a comment
# containing `persist-credentials-exempt` inside the step, so the decision is
# visible in review instead of being an omission.
#
# Usage: scripts/security/check_persist_credentials.sh
set -euo pipefail

cd "$(dirname "$0")/../.."

status=0

for file in .github/workflows/*.yml .github/workflows/*.yaml .github/actions/*/action.yml; do
    [ -e "$file" ] || continue

    awk -v file="$file" '
        function flush() {
            if (pending && !found_pc && !found_exempt) {
                printf "%s:%d: actions/checkout without `persist-credentials: false`\n", file, checkout_line > "/dev/stderr"
                bad++
            }
            pending = 0; found_pc = 0; found_exempt = 0
        }
        {
            line = $0
            sub(/[[:space:]]+$/, "", line)
            if (line ~ /^[[:space:]]*$/) next

            match(line, /^[[:space:]]*/)
            indent = RLENGTH

            if (line ~ /uses:[[:space:]]*actions\/checkout@/) {
                flush()
                pending = 1
                # `- uses:` puts the step keys two columns right of the dash.
                checkout_indent = (line ~ /^[[:space:]]*- /) ? indent + 2 : indent
                checkout_line = NR
                next
            }

            if (!pending) next
            if (indent < checkout_indent) { flush(); next }
            if (line ~ /persist-credentials:/) found_pc = 1
            if (line ~ /persist-credentials-exempt/) found_exempt = 1
        }
        END { flush(); exit (bad > 0) }
    ' "$file" || status=1
done

if [ "$status" -ne 0 ]; then
    echo "" >&2
    echo "Add to each checkout above:" >&2
    echo "        with:" >&2
    echo "          persist-credentials: false" >&2
    echo "" >&2
    echo "If the job genuinely needs the token to push, keep it and say why in a" >&2
    echo "comment containing 'persist-credentials-exempt'. See rustfs/backlog#1602." >&2
    exit 1
fi

echo "OK: every actions/checkout clears its credentials or is explicitly exempt"
