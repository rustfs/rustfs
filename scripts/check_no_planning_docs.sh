#!/usr/bin/env bash
set -euo pipefail

# Guard: planning-type documents must not be committed.
#
# One-shot implementation/optimization plans, migration-progress ledgers,
# design specs, and agent-generated working notes (e.g. anything a
# `superpowers`/scratch workflow produces) belong in the issue tracker or a
# local worktree, never in the repository — see AGENTS.md "Sources of Truth".
#
# .gitignore already keeps everything under docs/ out of the tree except the
# whitelisted durable subtrees (architecture/, operations/, testing/), but
# `git add -f` bypasses .gitignore. This guard closes that hole: any tracked
# file under docs/superpowers/ fails the check regardless of how it got added.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_NO_PLANNING_DOCS_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

cd "$ROOT_DIR"

status=0

while IFS= read -r file; do
    [[ -z "$file" ]] && continue
    printf '%s: planning-type document must not be committed (keep it in the issue tracker or a local worktree)\n' \
        "$file" >&2
    status=1
done < <(git ls-files 'docs/superpowers/*')

if [[ "$status" -ne 0 ]]; then
    printf '\nRemove the file(s) above with `git rm`. See AGENTS.md "Sources of Truth".\n' >&2
fi

exit "$status"
