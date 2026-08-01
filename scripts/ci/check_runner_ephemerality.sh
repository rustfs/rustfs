#!/usr/bin/env bash
# Assert that self-hosted runners are ephemeral: one job per runner, ever.
#
# WHY THIS IS CHECKED CONTINUOUSLY RATHER THAN ONCE
#
# rustfs/rustfs is public, and pull_request jobs run on self-hosted runners —
# they execute the PR's own build.rs, proc-macros and tests, which is arbitrary
# code. What keeps that from reaching the release build is that each runner is a
# fresh ARC pod that handles exactly one job and is destroyed. Nothing in this
# repository enforces it: it is a property of the ARC scale-set configuration,
# which lives outside the repo and can be changed without any PR. So it is
# asserted from the outside, on real data.
#
# A repeated runner_name means a runner served two jobs, i.e. state survived
# between them, i.e. a poisoned PR job could leave something behind for whatever
# runs next — including, while the release build shares the sm-standard-2 pool,
# a release build.
#
# Usage: scripts/ci/check_runner_ephemerality.sh [run-count]
#   run-count defaults to 40. MIN_SAMPLE (default 10) is the number of observed
#   assignments below which the window is reported INCONCLUSIVE rather than OK.
#
# Exit codes: 0 ephemeral, 1 a runner served two jobs, 2 inconclusive or broken.
set -euo pipefail

REPO="${GITHUB_REPOSITORY:-rustfs/rustfs}"
LIMIT="${1:-40}"
# Below this many observed assignments the window says nothing useful: with the
# pool saturated, most sm-* jobs sit queued with runner_name still null, and a
# handful of samples would pass by coincidence rather than by evidence.
MIN_SAMPLE="${MIN_SAMPLE:-10}"

command -v gh >/dev/null || { echo "ERROR: gh CLI not found" >&2; exit 2; }

runs="$(gh run list --repo "$REPO" --limit "$LIMIT" --json databaseId --jq '.[].databaseId')"
[ -n "$runs" ] || { echo "ERROR: no runs returned for $REPO" >&2; exit 2; }

names="$(
    for run in $runs; do
        gh api "repos/${REPO}/actions/runs/${run}/jobs" --paginate \
            --jq '.jobs[] | select(.runner_name != null) | select(.runner_name | startswith("sm-")) | .runner_name' 2>/dev/null || true
    done
)"

total="$(printf '%s\n' "$names" | grep -c . || true)"
if [ "${total:-0}" -eq 0 ]; then
    echo "ERROR: no self-hosted (sm-*) runner names found across $LIMIT runs." >&2
    echo "  Either the label scheme changed or the API shape did — this check" >&2
    echo "  must not silently pass by finding nothing." >&2
    exit 2
fi

if [ "$total" -lt "$MIN_SAMPLE" ]; then
    echo "INCONCLUSIVE: only ${total} self-hosted job assignments across ${LIMIT} runs" >&2
    echo "  (need ${MIN_SAMPLE}). Most sm-* jobs are probably still queued, so" >&2
    echo "  runner_name is null. Re-run with a larger window when the pool drains." >&2
    exit 2
fi

dupes="$(printf '%s\n' "$names" | sort | uniq -d)"

if [ -n "$dupes" ]; then
    echo "ERROR: self-hosted runners served more than one job — not ephemeral:" >&2
    printf '%s\n' "$dupes" | while read -r name; do
        count="$(printf '%s\n' "$names" | grep -c "^${name}$")"
        echo "  ${name}: ${count} jobs" >&2
    done
    echo "" >&2
    echo "State can survive between jobs on those runners. Because this is a" >&2
    echo "public repository whose pull_request jobs run PR-authored code, and" >&2
    echo "because build.yml's release legs share the sm-standard-2 pool, a" >&2
    echo "poisoned PR job could reach a release build. Check the ARC scale-set" >&2
    echo "configuration (rustfs/backlog#1602)." >&2
    exit 1
fi

echo "OK: ${total} self-hosted job assignments across ${LIMIT} runs, all on distinct runners"
