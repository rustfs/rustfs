# schedule-failure-issue

Opens (or updates) a GitHub issue when a **scheduled** workflow run fails.
This is the single alerting mechanism for all timed pipelines
(rustfs/backlog#1149 ci-8, arbitration G3): scheduled workflows must consume
this action instead of inventing their own notification paths.

Issues are filed in **rustfs/backlog** (override with `target-repository`),
not the repository the workflow runs in, so scheduled-failure noise stays
out of the code repository's issue tracker.

## Behavior

- Issue title: `[scheduled-failure] <workflow name>` — the workflow name is
  the dedupe key.
- If an **open** issue with that exact title exists in the target
  repository, the failure is appended as a comment; otherwise a new issue is
  created (labeled `infrastructure` by default; the label must exist in the
  target repository). Closing the issue resets the cycle: the next failure
  opens a fresh one.
- The issue body / comment includes the source repository, run URL, run
  attempt, event, ref and the names of the failed (or timed-out/cancelled)
  jobs of the current run attempt.
- Requires `gh` and `jq` on the runner (both preinstalled on GitHub-hosted
  runners such as `ubuntu-latest`).

## Tokens

Two tokens are involved:

- `github-token` authenticates issue create/comment/search against the
  target repository. The workflow-scoped `secrets.GITHUB_TOKEN` is scoped to
  the code repository and **cannot** open issues in rustfs/backlog, so this
  must be a PAT or GitHub App token with `issues: write` on the target
  repository, stored as the `BACKLOG_ISSUE_TOKEN` repository secret.
- `source-token` (defaults to `${{ github.token }}`) reads the failed-job
  list from the reported run, which lives in the repository running the
  workflow.

## Usage

Add a final job to the workflow. `always()` is required — without it the job
is skipped when a needed job fails; `contains(needs.*.result, 'failure')`
makes it act only when something actually failed. Scope `issues: write` to
this job only; no other job may gain permissions.

```yaml
  alert-on-failure:
    name: Alert on scheduled failure
    needs: [<the jobs to watch>]
    if: always() && github.event_name == 'schedule' && contains(needs.*.result, 'failure')
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions:
      contents: read
      issues: write
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7
      - name: Open or update failure-tracking issue
        uses: ./.github/actions/schedule-failure-issue
        with:
          github-token: ${{ secrets.BACKLOG_ISSUE_TOKEN }}
```

Notes:

- Upstream jobs that are *cancelled* (e.g. a manually cancelled run) do not
  trigger the alert — a human was already looking. Job `timeout-minutes`
  expiry surfaces as `failure` and does alert.
- Manual `workflow_dispatch` runs never alert; a human is watching those.

## Current consumers

- `.github/workflows/e2e-s3tests.yml` (weekly full s3-tests sweep)
- `.github/workflows/mint.yml` (weekly multi-SDK mint run)
- `.github/workflows/fuzz.yml` (nightly fuzz corpus jobs)
- `.github/workflows/performance-ab.yml` (nightly warp A/B gate)
- **Pending:** the ci-7 e2e nightly-full workflow does not exist yet
  (backlog#1149 ci-7). When it lands, wire it up with the snippet above.

## Drill

`.github/workflows/schedule-failure-alert-drill.yml` is a manual-only
workflow that forces a job failure and runs this action through the exact
consumer wiring. Use it to verify the alert path end to end:

```bash
gh workflow run schedule-failure-alert-drill.yml -R rustfs/rustfs --ref <branch>
```

Run it twice to verify both paths (issue creation, then comment dedupe), and
close the `[scheduled-failure] Schedule Failure Alert Drill` issue afterwards.
