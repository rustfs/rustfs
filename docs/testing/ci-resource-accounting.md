# Actions resource accounting

**Use this when:** comparing CI changes across workflows and attempts, allocating observed work to PRs, or distinguishing runner occupancy from a provider bill.

The read-only collector is `scripts/ci/actions_cost_report.py`. The independent manual entry is `.github/workflows/ci-resource-accounting.yml`; it does not run on PRs. `scripts/ci_timing_report.py` continues to own the successful ordinary-code sample definition, including both the historical combined lane and the workspace/core archive consumers.

## Collect and reproduce

Use an authenticated GitHub CLI with Actions and pull-request read access. Start with a small, closed creation window. The collector deliberately rejects an inventory above GitHub's filtered-run limit rather than silently truncating it; split such a window and retain each report. Do not sum overlapping windows without deduplicating repository, run ID, attempt and job ID.

```bash
python3 scripts/ci/actions_cost_report.py \
  --repository rustfs/rustfs \
  --since 2026-10-01T00:00:00Z \
  --until 2026-10-01T12:00:00Z \
  --output /tmp/ci-accounting.json \
  --summary /tmp/ci-accounting.md

python3 scripts/ci/actions_cost_report.py \
  --input /tmp/ci-accounting.json \
  --output /tmp/ci-accounting-recomputed.json \
  --summary /tmp/ci-accounting-recomputed.md
```

The snapshot retains selected raw run, attempt, job and step fields alongside the summary. Every attempt from 1 to the observed latest attempt has its own Jobs API pagination. Failed and cancelled jobs remain in the sum. A missing attempt or a run that changes during collection marks the report incomplete and returns a failing exit status while retaining available evidence. Partial reports are useful for diagnosis; they are not a complete baseline.

GitHub's [run inventory](https://docs.github.com/en/rest/actions/workflow-runs#list-workflow-runs-for-a-repository) is filtered by original creation time. A rerun of an older run can occur inside the wall-clock interval without entering this inventory. Widen the creation window for lifecycle studies; API retention and unidentified downstream events still prevent a completeness claim. The [attempt-specific Jobs API](https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run-attempt) provides the job evidence; logs are not downloaded, so expired or mixed build/test logs are never interpreted as zero compiler time.

## Allocation and unknowns

Direct PR events use their recorded PR members. A push at a known merged PR's exact merge commit is allocated to that PR. The collector never guesses all merge-group members from the first PR in a synthetic branch, or assigns a downstream workflow solely because its SHA matches. Unresolved jobs stay in the unattributed bucket.

For reviewed merge-group membership or downstream provenance, supply an attribution file bound to the observed run ID and head SHA:

```json
{
  "schema": 1,
  "repository": "rustfs/rustfs",
  "runs": [
    {
      "run_id": 123456,
      "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "pull_requests": [123, 124],
      "evidence": "https://github.com/rustfs/rustfs/actions/runs/123456"
    }
  ],
  "pull_requests": {}
}
```

Pass it with `--attributions` when recomputing the snapshot. Its evidence URL is an operator-reviewed assertion, not an API proof of membership. Include the original event payload or another retained membership receipt in that review. Each job's execution and scheduler-wait minutes are divided equally among recorded members. Periodic work remains separate and cannot be reassigned with the manifest. The unallocated original total remains available to check conservation. PR creation and merge timestamps are observations, while `lifecycle_complete` stays false because retained Actions history cannot prove all historical work exists.

Known runner occupancy is the sum of completed-job start-to-end durations. The queue field measures creation-to-start wait; the API timestamps do not separately identify scheduler and dependency delay. Missing or inverted timestamps remain unknown. Explicitly skipped jobs consume zero execution minutes; queued, running or cancelled jobs with missing end times do not. The successful-code feedback sample uses the rerun attempt's start, excluding time before that attempt. That sample is separate from the accumulated PR cost across revisions and workflows.

Phase totals are available only for dedicated steps named `CI phase: setup`, `CI phase: compile`, `CI phase: test`, `CI phase: archive` or `CI phase: transfer`. Other step names and timestamps remain visible without inferring a compiler/test split. Labels classify standard hosted, fixed-capacity, metered and unknown runners according to the repository's existing placement policy. Per-job rounded minutes are only a comparison model. Unit prices, capacity invoices and actual billing remain unknown; reduced demand does not establish reduced spend.

## Comparisons and validation

Preserve the baseline snapshot and source revision. Compare the same event, source policy, runner image, toolchain, build configuration and cache conditions. Image and cache identity require the runner pilot or build receipts; they are not recoverable from a label. Report known totals and unknown coverage together, including failed and cancelled work. A faster isolated successful run cannot replace the lifecycle report or establish equivalent test coverage.

Run the accounting fixtures with `python3 -m unittest discover -s scripts/ci -p 'test_actions_cost_report.py'` and the shared timing tests with `python3 -m unittest discover -s scripts -p 'test_ci_timing_report.py'`. The fixtures cover pagination, partial retention, reruns, cancelled consumption, unknown timestamps, independent workflows, shared groups and allocation conservation. Disable the manual workflow to stop collection; no product gate or runner placement depends on it.
