# RustFS Testing

> **Owner: backlog#1153 (infra-11).** This file is the authoritative home for
> test taxonomy, naming conventions, and serial/quarantine rules. It is
> currently a **skeleton** — infra-11 fills in the remaining sections. The
> event × budget × required matrix lives in `docs/testing/ci-gates.md`
> (ci-15); this file links to it rather than duplicating counts.

## Test taxonomy

_TODO (infra-11): unit / e2e-smoke / e2e-full / e2e-nightly / protocols /
s3-tests / fuzz / perf, with naming conventions._

## Serial groups & CI profiles

_TODO (infra-11): document the `[test-groups]` mechanism and the `default` vs
`ci` nextest profiles. See `.config/nextest.toml`._

## Flake policy

A flaky test is one that fails non-deterministically without a corresponding
code change. Flakes erode trust in the gate and block tightening required
checks, so they are handled on a strict, time-boxed loop.

**Retry semantics (source of truth: `.config/nextest.toml`):**

- The **local `default` profile never retries.** A red test on your machine is
  a real failure to investigate, not noise to paper over.
- The **CI `ci` profile runs with global `retries = 0`.** A new race must fail
  on its first occurrence so the first crime scene is never masked.
- Only tests on the **quarantine list** get `retries = 2`, and only under the
  `ci` profile. Each quarantine entry MUST link exactly one OPEN issue.
- **JUnit flaky markers are the observable.** A quarantined test that passes
  only after a retry is marked `flaky` in `target/nextest/ci/junit.xml`
  (uploaded as a CI artifact). That marker — not a green check — is how we see
  a flake is still live.

**Lifecycle of a flake:**

1. **Discover** — a test fails non-deterministically (CI or local), or shows a
   `flaky` marker in the JUnit report.
2. **Open an issue within 24h** — file/track an issue describing the flake
   (symptom, suspected cause, affected suite). No silent re-runs.
3. **Quarantine** — add the test to the quarantine override block in
   `.config/nextest.toml` with a comment linking that OPEN issue. This grants
   `retries = 2` under CI so the flake stops reddening unrelated PRs, while the
   `flaky` marker keeps it visible.
4. **Fix or delete within 30 days** — make the test robust (then remove the
   quarantine entry) or delete the test. A quarantine entry may not outlive its
   fix window; an entry without a live OPEN issue link is a policy violation.

First quarantine members: the two backlog#937 ecstore groups
(`concurrent_resend_same_part_commits_one_generation` and
`store::bucket::tests::bucket_delete_*`).
