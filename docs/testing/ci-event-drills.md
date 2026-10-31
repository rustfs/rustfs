# Event-class drills and retained evidence

**Use this when:** you need to confirm that a CI event class has been exercised end to end, locate its retained run evidence, or repeat the drill. **Source of truth:** the live `main` ruleset for required status; `.github/workflows/ci.yml` for event triggers; `scripts/ci_gate.py` for per-event selection and expected results; the run URLs below for observed outcomes.

Every event class the planner serves must have at least one reviewable drill: an actual run whose plan, job set and gate result can be re-inspected. A drill proves the wiring for that event, not product quality and not a cost saving. Static fixtures in `scripts/test_ci_plan.py` and `scripts/test_ci_merge_queue.py` cover the negative cases (missing, cancelled, mismatched, combined-candidate conflicts) and are not substitutes for the live drills listed here.

## Event classes and expected selection

| Event | Selection | Expected gate behavior |
|---|---|---|
| `pull_request` (ordinary code) | reverse-dependency closure + fixed core inventory; shared/unknown inputs expand to full | `Test and Lint` requires exactly the planned job results |
| `pull_request` (documentation only) | `docs` mode: no code jobs | Quick Checks and Typos run; expensive jobs are required **skipped** |
| `pull_request` (isolated E2E) | `e2e` mode: workspace + server build + E2E smoke + S3 lanes | Connect boundary, serial ILM, protocol/rio-v2, real io_uring required **skipped** |
| `pull_request` (closed) | `Cancel Closed PR Runs` only | in-flight validation for that PR is cancelled; no new validation is claimed |
| `pull_request` (merged) | entry into `merge_group` | combined candidate is validated as its own SHA/tree with the full matrix |
| `push` to `main` | full | complete validation; no reuse inferred from tree equality |
| `push` of a release tag | `build.yml` release matrix | build and publication path (not the PR planner) |
| `schedule` | registry-selected cadence plan | suites run under their resource locks; failures feed the health monitors |
| `workflow_dispatch` | full unless an explicit narrow input is passed | manual runs without a plan stay visible as unverified |
| `workflow_run` | monitor/filter through the responsibility registry | independent health layer reports expected SHA, attempt and evidence completeness |

## Retained evidence index

Run URLs are `https://github.com/rustfs/rustfs/actions/runs/<id>`. Re-open the run and its `Select CI scope` job log to read the planner's recorded mode, reasons and expected job results.

| Class | Drill | Run | Observed |
|---|---|---|---|
| Ordinary PR | `test(heal): align cleanup tests with removal-disabled heals` | [41153190859](https://github.com/rustfs/rustfs/actions/runs/41153190859) | full scope (12 real jobs: workspace build/tests, E2E, S3, core contract, io_uring); gate accepted exact planned results |
| PR closed | `test(heal): align cleanup tests…` closure handler | [41160735383](https://github.com/rustfs/rustfs/actions/runs/41160735383) | only `Cancel Closed PR Runs` executed; every validation job required **skipped** |
| `push` to `main` | `chore(deps): refresh workspace dependencies (#9080)` | [41160768241](https://github.com/rustfs/rustfs/actions/runs/41160768241) | full matrix on the merge commit; no selection shortcut |
| Release tag | `1.0.4` | [40198812318](https://github.com/rustfs/rustfs/actions/runs/40198812318) | `Build and Release` release matrix succeeded on the tag ref |
| `schedule` | `Functional chain health` | [41159991473](https://github.com/rustfs/rustfs/actions/runs/41159991473) | monitor refused to advance freshness on an incomplete chain attempt (failure is the expected answer while the chain is unhealthy) |
| `workflow_dispatch` | manual full validation | [41152382212](https://github.com/rustfs/rustfs/actions/runs/41152382212) | full suite set ran and the aggregate accepted the complete plan (85 min) |
| `workflow_run` | `Scheduled Validation Watchdog` | [41161431618](https://github.com/rustfs/rustfs/actions/runs/41161431618) | independent filter/monitor path executed against the registry |
| `merge_group` | first queued candidate after queue enablement | see below | live evidence attached after the first queue entry completes |
| Documentation-only PR | this document's own PR | see below | `docs` mode: expensive jobs required **skipped** |
| Isolated E2E PR | drill branch confined to `crates/e2e_test/src/` | see below | `e2e` mode: Connect/ILM/protocol lanes required **skipped** |

## Repeating a drill

Ordinary, docs and E2E classes: open a pull request containing only that class's paths and inspect `Select CI scope` → the planner's `mode` and `reasons`, then the aggregate's expected-vs-actual comparison. Close the PR without merging to leave no product change; the closure handler itself is the closed-event drill.

Merge-group class: queue a pull request through the enabled `merge_queue` rule (see [verification plans and artifacts](ci-verification-pipeline.md) for the switch record and rollback payload). The `merge_group` run validates the combined candidate's own SHA/tree. Two individually passing changes whose combination fails stay blocked until their combined candidate passes; that negative case is fixed in `scripts/test_ci_merge_queue.py`.

Schedule and `workflow_run` classes cannot be forced on demand without an event; use the retained runs above and the registry's own `python3 scripts/validation_registry.py check` output. Tag class requires a real tag push; use the retained release runs or the preview-tag rehearsal described in [CI gate matrix](ci-gates.md).

## Boundaries

A completed drill is evidence that the event wiring behaves as planned for that candidate. It is not release certification, not a latency or cost result (use [CI timing samples](ci-timing.md) and [resource accounting](ci-resource-accounting.md)), and not a substitute for the required checks' own product coverage. Do not edit this file to claim a drill that has no run URL; attach the run first.
