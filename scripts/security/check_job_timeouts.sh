#!/usr/bin/env bash
# Every job that occupies a runner must declare timeout-minutes.
#
# GitHub's default is 360 minutes. This repository has a history of runners
# stalling intermittently (#5394) and of a plain `git checkout` taking 9m57s
# under node-level I/O contention, so an undeclared timeout means one wedged job
# can hold a runner for six hours out of a pool of roughly 15-21.
#
# Only jobs with `runs-on` are checked: a job that calls a reusable workflow has
# no runner of its own and cannot declare a timeout.
#
# Usage: scripts/security/check_job_timeouts.sh
set -euo pipefail

cd "$(dirname "$0")/../.."

status=0

for file in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -e "$file" ] || continue

    awk -v file="$file" '
        function flush() {
            if (job != "" && has_runs_on && !has_timeout) {
                printf "%s:%d: job `%s` has runs-on but no timeout-minutes\n", file, job_line, job > "/dev/stderr"
                bad++
            }
            job = ""; has_runs_on = 0; has_timeout = 0
        }
        /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
        {
            line = $0
            sub(/[[:space:]]+$/, "", line)
            if (line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next

            # A non-indented key ends the jobs mapping.
            if (in_jobs && line !~ /^[[:space:]]/) { flush(); in_jobs = 0 }
            if (!in_jobs) next

            if (line ~ /^  [a-zA-Z0-9_-]+:[[:space:]]*$/) {
                flush()
                job = line
                sub(/^[[:space:]]*/, "", job)
                sub(/:.*$/, "", job)
                job_line = NR
                next
            }
            if (job == "") next
            if (line ~ /^    runs-on:/) has_runs_on = 1
            if (line ~ /^    timeout-minutes:/) has_timeout = 1
        }
        END { flush(); exit (bad > 0) }
    ' "$file" || status=1
done

if [ "$status" -ne 0 ]; then
    echo "" >&2
    echo "Add timeout-minutes to each job above. Rough budgets used in this repo:" >&2
    echo "  10  echo-only and guard-script jobs" >&2
    echo "  30  jobs that call the GitHub API, upload assets, or push over the network" >&2
    echo "  90+ anything using ./.github/actions/setup (cold cache restore alone is 11-21 min)" >&2
    exit 1
fi

echo "OK: every job with runs-on declares timeout-minutes"
