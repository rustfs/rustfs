# Functional chain evidence

The functional-chain driver calls twelve reusable suite workflows sequentially in one Actions run. A suite failure does not suppress later suites. `complete-chain` requires every suite job and its evidence artifact to succeed. The root uses its own concurrency group so an active chain can finish; the suites retain the shared VM lock used by standalone tests.

## Candidate identity

The driver resolves one successful `nightly-gnu.yml` attempt from main. Automatic runs select the triggering scheduled attempt. Manual runs require an explicit build run ID and attempt. Resolution verifies the artifact name, run association, ZIP size and digest, single JSON member, and immutable package URL containing run, attempt and package checksum.

Schema 2 distinguishes the workflow SHA from the actual build SHA and source ref. A release build triggered by a main workflow remains release evidence. Legacy schema 1 is accepted only when its source SHA equals the producer workflow SHA. The consumer does not resolve the source branch again, so branch movement cannot silently select a different package.

All suites use the same candidate package and checksum. Installers with checksum support receive `PACKAGE_SHA256`, or `TO_SHA256` for upgrade, and verify the package before installation. Table and fault-tolerance installers have no checksum option, so `scripts/prepare_functional_package.py` first downloads and verifies the package on the runner. It transfers those bytes to a root-owned cache on each configured node, checks the SHA256 again before atomic publication, and supplies a `file://` URL to the existing installer. The cache is isolated by chain run and attempt and removed after the suite, including failed attempts. Fetch, hash, transfer, or cleanup failure prevents valid chain evidence.

The private test repository is checked out at `.config/functional-script-revision.txt`; change that pin only to a reviewed, merged revision. Standalone table and fault-tolerance runs retain their existing package inputs and report policy.

## Completion and reruns

The required order is upgrade, S3, KMS, tier, storage, heal, pool expansion, security, replication, fault tolerance, table, and performance. Fault-tolerance evidence requires a final summary matching its per-probe verdicts; its existing non-strict known-divergence verdicts are reported as unsupported cases.

Every suite records its chain run/attempt, workflow SHA, private pin, candidate identity, report hash and execution counts. A missing/empty report, zero passing executions, failed or unfinished case, failed test/report step, cancelled job, or mismatched private checkout invalidates the evidence. Uploading the suite proof requires successful proof generation.

The final job checks all twelve expected suite results and all twelve proof files against the same envelope. It emits `functional-chain-complete-<run>-<attempt>` only after those checks pass. Failed partial reruns cannot combine an old successful lane's proof with a new attempt. Use **Re-run all jobs** for a new complete acceptance attempt.

## Stalled suites and time limits

The nine non-performance reusable suite jobs, and the standalone table suite, have a **60-minute job limit**. The primary test step has a **45-minute limit** so a stalled test can fail before the hard job cancellation. Cleanup steps are limited to five minutes; report generation, dashboard/backlog operations and ordinary artifact uploads are limited to two minutes each. Separate installer and preflight steps are limited to five minutes. Performance is explicitly excluded from these limits.

The limits use GitHub Actions native timeouts. They are wall-clock limits, not log-idle detection: a process printing progress forever is still stopped. A step timeout is not a passing result. Existing `always()` finalizers attempt reporting and cleanup, and the chain driver's `always()` plus successful-prepare condition allows the next suite after a failed or cancelled suite. Missing or partial evidence still fails the complete-success gate; it cannot authorize closing historical issues.

The 15-minute difference between test and job limits is **headroom, not a reserved cleanup window**: checkout and setup also consume the 60-minute job budget, and several failing finalizers may exhaust it. Reaching the hard limit, losing a runner, or terminating an SSH connection does not guarantee remote processes have stopped or cleanup has completed. The next suite must retain its pre-test cleanup. Inspect the runner and remote VMs after a hard timeout before trusting subsequent results; do not interpret contaminated-environment failures as independent product regressions.

The performance workflow remains unchanged: its job limit is 900 minutes, the default duration is five minutes per round, and the external script retains its default sixty-second pauses. Its methods, sizes, manual overrides and step limits are not modified by the functional timeout policy. A long performance run can therefore still occupy the final lane and delay chain completion; it is not covered by the one-hour guarantee for non-performance suites.

Job timeouts start when execution starts; they do **not** bound runner or concurrency queue time. Runner preflight checks detect an already-offline runner but are not reservations. The reusable chain continues after a job timeout without cancelling the whole Actions run. The legacy `repository_dispatch` path relies on an in-job handoff and cannot guarantee continuation after hard cancellation; use the reusable driver for bounded nightly chains. A queue watchdog would need an external dispatcher and environment recovery, not cancellation of the whole parent run (which would also cancel the remaining suites).

## Health publication

`functional-chain-health.yml` inspects recent main-branch chain runs hourly. It validates complete evidence against the exact producer artifact again and checks the private pin from the chain's workflow commit. Its JSON separates the latest attempt from the last complete success for each source. A later failure preserves historical success without turning the new failure green.

Evidence expires 36 hours after the producer attempt started. The dashboard also treats collection older than two hours as stale. Workflow enablement, owner, source identities, evidence version and expiry are visible. An invalid legacy chain-driver success is not complete evidence, and release success cannot authorize moving main PR coverage to nightly.

The dashboard's `src/chain-health.json` must exist before enabling publication. Updates use the read blob SHA and reject unsupported, null or newer existing state. The companion dashboard view is required to display this data. Until a real same-candidate chain completes, these workflow and script checks do not satisfy backlog #2481 or unblock coverage migration in #2483.
