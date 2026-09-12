# Functional chain evidence

The functional-chain driver calls ten reusable suite workflows sequentially in one Actions run. A suite failure does not suppress later suites. `complete-chain` requires every suite job and its evidence artifact to succeed. The root uses its own concurrency group so an active chain can finish; the suites retain the shared VM lock used by standalone tests.

## Candidate identity

The driver resolves one successful `nightly-gnu.yml` attempt from main. Automatic runs select the triggering scheduled attempt. Manual runs require an explicit build run ID and attempt. Resolution verifies the artifact name, run association, ZIP size and digest, single JSON member, and immutable package URL containing run, attempt and package checksum.

Schema 2 distinguishes the workflow SHA from the actual build SHA and source ref. A release build triggered by a main workflow remains release evidence. Legacy schema 1 is accepted only when its source SHA equals the producer workflow SHA. The consumer does not resolve the source branch again, so branch movement cannot silently select a different package.

All suite installers receive the same package URL and checksum (`PACKAGE_SHA256`, or `TO_SHA256` for upgrade). Their existing package checks run before installation. The private test repository is checked out at `.config/functional-script-revision.txt`; change that pin only to a reviewed, merged revision.

## Completion and reruns

Every suite records its chain run/attempt, workflow SHA, private pin, candidate identity, report hash and execution counts. A missing/empty report, zero passing executions, failed or unfinished case, failed test/report step, cancelled job, or mismatched private checkout invalidates the evidence. Uploading the suite proof requires successful proof generation.

The final job checks all ten expected suite results and all ten proof files against the same envelope. It emits `functional-chain-complete-<run>-<attempt>` only after those checks pass. Failed partial reruns cannot combine an old successful lane's proof with a new attempt. Use **Re-run all jobs** for a new complete acceptance attempt.

## Health publication

`functional-chain-health.yml` inspects recent main-branch chain runs hourly. It validates complete evidence against the exact producer artifact again and checks the private pin from the chain's workflow commit. Its JSON separates the latest attempt from the last complete success for each source. A later failure preserves historical success without turning the new failure green.

Evidence expires 36 hours after the producer attempt started. The dashboard also treats collection older than two hours as stale. Workflow enablement, owner, source identities, evidence version and expiry are visible. An invalid legacy chain-driver success is not complete evidence, and release success cannot authorize moving main PR coverage to nightly.

The dashboard's `src/chain-health.json` must exist before enabling publication. Updates use the read blob SHA and reject unsupported, null or newer existing state. The companion dashboard view is required to display this data. Until a real same-candidate chain completes, these workflow and script checks do not satisfy backlog #2481 or unblock coverage migration in #2483.
