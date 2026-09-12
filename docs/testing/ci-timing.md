# CI timing samples

Collect a bounded sample of completed PR CI runs created in the last seven days with `python3 scripts/ci_timing_report.py --limit 30 --output /tmp/ci-timing.json`. This requires an authenticated `gh` CLI with Actions read access. The JSON retains run SHA, attempt, job and step timestamps so another reviewer can reproduce the summary with `--input /tmp/ci-timing.json --output /tmp/ci-timing-summary.json` without GitHub access.

The report separates job creation-to-start wait, job execution, and individual step durations. The successful-code sample excludes documentation-only runs and successful PR-closure cancellation handlers. It uses the selected attempt's start for reruns, includes parallel jobs in the runner-minute sum, and leaves missing timestamps unknown. It does not estimate compiler time from a combined build-and-test step or treat runner minutes as wall time or a bill.

A small recent sample may contain no complete successful code runs. In that case its median is absent, not zero. Keep cancelled and failed counts visible; do not replace the sample with only green runs when evaluating changes.

Before reducing a PR lane, compare a proposed path classifier in shadow mode against the existing full selection. Keep that lane required until equivalent nightly evidence is complete and current for the same source policy. Timing data alone does not establish functional coverage, escaped-regression rate or quarantine health. The 30–45 minute PR target in backlog #2483 remains an experiment to measure, not an acceptance result from this tool.
