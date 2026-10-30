# Periodic validation orchestration

Use this reference to select packaged suites, interpret a health observation, or recover a quarantined lab environment. The machine-readable responsibility map is [`.github/scheduled-validations.json`](../../.github/scheduled-validations.json). Its `workflows` entries preserve every baseline workflow and distinguish new entries, independent maintenance, operator-disabled policies, alert writers, and duties without a periodic obligation. Its `suites` entries own selection, dependencies, environments, resource locks, requiredness, and evidence exits.

## Selection and scheduling

[The functional driver](../../.github/workflows/rustfs-functional-chain.yml) consumes the existing immutable nightly candidate and exact build attempt. Automatic nightly completion and manual runs select through the same registry function. The `validation_plan` input accepts `nightly`, `weekly`, or `monthly`; larger cadence plans include the shorter cadence duties. Existing workflow crons remain their execution entrypoints, and the registry check rejects event or cron drift. Listing an independent periodic duty in a plan does not dispatch it or change its budget.

The twelve existing packaged suites retain their nightly cadence. An empty `suites` input selects the complete cadence plan. A comma-separated list selects only those suites and declared dependencies. For example, `suites=s3` runs S3 compatibility alone. Explicit selection makes every selected suite required, including performance. A full plan may omit performance only when its registered optional policy and the preflight result both permit it; the complete evidence records the reason.

Inspect selection without fleet access:

```bash
python3 scripts/validation_registry.py select --cadence nightly --suites s3
python3 scripts/validation_registry.py select --cadence weekly --event schedule
python3 scripts/validation_registry.py check
```

Shared functional suites run serially, and an earlier test failure does not stop later selected suites from gathering results. Performance uses an isolated fleet and can run concurrently. The final aggregate rejects required failures, cancellation, unexpected skips, missing or foreign evidence, and execution outside the selected plan. A successful subset remains subset evidence; it cannot satisfy full-chain health.

The old `repository_dispatch` relay is retired. Each individual workflow remains a manual debugging entrypoint and stops after its own suite. Package-only builds, tools, read-only runner/fleet inspection, and interrupted evidence recovery remain separate driver modes. Their success does not represent suite completion. No workflow is enabled automatically by registration or monitoring.

## Shared environment ownership

Workflow concurrency holds the registered fleet lock across admission, setup, tests, cleanup, and evidence collection. Before any fleet mutation, the suite atomically records its run, attempt, suite, and lock identity in `~/.rustfs-functional-environment/active.json` on every node. This requires Python 3 on the lab nodes and a persistent SSH-user home directory. The receipt is retained as a `functional-environment-<suite>-<run>-<attempt>` artifact.

Only successful cleanup releases the markers. Cancellation, runner loss, partial admission, skipped cleanup, or cleanup failure leaves the fleet quarantined. A later suite fails before changing it. The fault-tolerance matrix observes the same policy, and canonical validation cases check for quarantine before executing. Read-only inspection and the existing evidence-based recovery entrypoints remain available.

For recovery, inspect the receipt and retained test evidence, ensure the original attempt has stopped, and clean every listed node while holding the same workflow resource lock. Then release only the original identity. For example, in an operator-controlled recovery step under that lock:

```bash
GITHUB_RUN_ID=123 GITHUB_RUN_ATTEMPT=2 \
RUSTFS_NODES='node1 node2 node3' RUSTFS_SSH_USER=azureuser \
python3 scripts/functional_environment.py recover --suite s3 --cleanup-outcome success
```

The `success` argument is an operator assertion that cleanup already completed; it does not perform cleanup. Recovery permits missing markers from partial admission but refuses to remove another attempt's marker. Keep the original evidence and recovery receipt. Do not delete quarantine files as a substitute for cleanup or run a second test against retained failed state.

## Independent health

The completion watchdog subscribes broadly and filters through the registry, so adding a monitored duty does not require a second workflow-name list. Existing in-workflow failure alerts remain their own writers; the watchdog handles cancellations and other incomplete endings, plus failures assigned to it. The freshness scan displays declared policy separately from live Actions state and recent attempts separately from successful runs. Disabled or paused entries are visible without a success claim or an enable operation.

[Functional-chain health](../../scripts/functional_chain_health.py) runs on `ubuntu-latest`, independently of both lab fleets. It reuses the candidate and suite evidence verifier. Health requires the expected source ref's current SHA, the correct run and attempt, the full selected responsibility set, and fresh completion evidence. A newly completed real attempt on an unchanged source SHA refreshes completion freshness even when its original package is older. Reusing old artifacts, verifying an older source SHA, or running only one suite does not.

A newer failed or cancelled full-plan attempt remains the latest attempt even when an earlier complete success is still fresh. Pagination gaps, API errors, identity mismatches, and source movement during inspection produce an incomplete observation. Previously published successes remain historical facts and cannot make that observation healthy. The dashboard keeps its existing publication and history format; this orchestration does not create a release certification path.

## Verification boundaries

The local registry, aggregation, freshness, and quarantine tests exercise selection and failure transitions with fixed fixtures; they do not claim a successful fleet run. Before treating deployment as accepted, retain one fixed-candidate full-plan run, one single-suite run, a cleanup-failure isolation exercise, and an independent-monitor alert/recovery exercise. Existing workflow disablement policy and private test-script revisions require their own authorized changes.
