# Candidate acceptance and release publication

RustFS builds the final product version before publication. A candidate is identified by its Actions run and `<version>-preview.<run_number>`; this identifier is not a Git tag or the executable's displayed version. Failed candidates consume no public version. A public tag is created only after the complete artifact manifest is approved.

## Entry points

| Workflow | Input | Result | Public writes |
|---|---|---|---|
| `release-candidate.yml` | Exact final version and full source SHA | Four Linux archives, native checks, upgrade results, Linux manifest | None |
| `release-complete.yml` | Successful candidate run, accepted Linux manifest SHA-256, backlog acceptance report | Desktop archives, DEB/RPM, OCI images, Helm chart, full certification and sealed manifest | None |
| `release-publish.yml` | Successful completion run and approved final manifest SHA-256 | Annotated tag, GitHub Release, R2 files, registry images, stable pointers and Helm index | Explicit publication |

The builder workflows (`build.yml`, `package.yml`, `docker.yml`, `helm-package.yml`) are reusable stages. Only `release-publish.yml` has release write credentials. Development builds retain their artifact names and download channel. The daily schedule selects Linux only; the Sunday 16:00 UTC schedule selects macOS and Windows. Manual dispatch can still select any platform list, and both desktop targets remain mandatory for a completed release candidate. Tag pushes no longer start a second compilation.

## Freeze and build

Select a reviewed source commit whose Cargo version equals the intended final version. Finish exact-source CI before dispatching the candidate: a successful `push` or `workflow_dispatch` run of `ci.yml` for that commit on `main` or the release branch whose `Quick Checks`, `Test and Lint` and `End-to-End Tests (full merge gate)` all succeeded. A commit that landed through the merge queue has a push run whose validation jobs were skipped as proven; `Quick Checks` and `Test and Lint` must still succeed there, and the evidence recorded is the successful `merge_group` run of that branch's queue whose head commit has the same tree, which must itself have passed all three jobs. `scripts/ci_merge_group_proof.py` defines that match for both the push plan and the candidate. The candidate workflow itself must run from upstream main. Unrelated main commits do not require rebuilding an accepted source.

```bash
gh workflow run release-candidate.yml --ref main \
  -f version='<version>' -f source_sha='<full-source-sha>'
```

The initial job records the source and workflow SHAs, release recipe, dependency lock, exact stable Rust version, Console tag/asset/SHA-256, CLI assets, previous stable release and validation plan. All platforms consume that record; none independently follows `latest`. The source commit must contain the same reviewed release recipe as the dispatcher. Relevant recipe changes require a new candidate; unrelated source changes on main do not invalidate the frozen record.

Linux is built first. Each archived executable must display the final version, expected target, source commit and compiler version, then start natively, serve the Console, and pass an authenticated S3 bucket/object round trip. The upgrade lane runs the existing pinned compatibility matrix plus the stable version frozen at candidate creation. It compiles the harness and runs the accepted executable; it does not rebuild a substitute server. A zero-test invocation fails.

The Linux manifest is partial evidence and cannot authorize publication. Download the exact artifacts for field acceptance. Record the source, manifest digest, run URL and actual tested binary digests in backlog. Required field acceptance includes Scanner/Heal and multi-pool behavior, major features, memory, high-concurrency 503s, performance against the agreed baseline, previous-version data integrity and upgrade recovery. The agreed stability duration is a release-specific requirement, not an implicit workflow default.

## Complete the accepted candidate

After the Linux report passes, dispatch `release-complete.yml` with the exact digest printed by the candidate run. The `release-linux-acceptance` environment is the operator gate. Configure its protection and allowed main branch in repository settings; a typed digest and report identify what is being accepted, while the workflow independently revalidates automated evidence.

```bash
gh workflow run release-complete.yml --ref main \
  -f candidate_run_id='<run-id>' \
  -f linux_manifest_sha256='<accepted-linux-manifest-sha256>' \
  -f acceptance_report='https://github.com/rustfs/backlog/issues/<issue>#issuecomment-<comment>'
```

macOS and Windows now build and run the same native checks. Packages and images wrap the accepted Linux binaries. DEB/RPM extraction must reproduce the recorded binary digest. Both OCI variants are checked on amd64 and arm64 and scanned before sealing. Vulnerability scans retain the repository's report-only policy; scanner execution errors still fail the stage. Image OS packages are resolved during packaging, then the actual resulting OCI bytes are accepted and preserved; this is not a promise of reproducible package-manager output.

The Helm chart is tested and packaged before sealing, with chart and application versions equal to the final product version. Full certification requires every platform and upgrade case from the original plan. The seal includes artifact hashes, raw evidence, source/input identities, acceptance report, checksums, SBOM and provenance. Successful Linux receipts remain valid while desktop results are added because each receipt binds its own exact archive and executable, the frozen plan, candidate identity and trusted producer.

Native smoke checks are not a substitute for Console UI and rc acceptance. Complete the release skill's UI/client checks against these exact artifacts, including desktop checks applicable to the release, before asking for final publication approval.

## Approve and publish

Review the complete run and final `release-manifest.json`. Human approval binds its SHA-256, source and version. A changed manifest requires renewed approval. No source version bump, compilation, package generation or image build occurs during publication.

```bash
gh workflow run release-publish.yml --ref main \
  -f complete_run_id='<successful-completion-run-id>' \
  -f manifest_sha256='<approved-final-manifest-sha256>'
```

The publisher downloads the artifact from the exact successful main workflow run, checks GitHub's artifact digest, recomputes file hashes and reruns complete certification. It creates an annotated tag whose message binds the source and manifest. An existing tag or asset with different bytes is a hard failure; tags are never moved and uploads never clobber assets.

Versioned GitHub assets, R2 objects and Docker Hub/GHCR/Quay manifests are uploaded and read back before the Release becomes public. OCI promotion uses `--preserve-digests`; registry conversion fails rather than silently accepting a new image. R2 creation uses atomic `If-None-Match: *`. The small evidence bundle persists with the public Release after Actions artifacts expire. Provenance is descriptive, not a claim of cryptographic attestation.

Publication uses a per-version lock and a shared channel lock, both without cancelling an active run. Stable channels advance only after versioned delivery succeeds. GitHub Latest acts as a monotonic marker during partial channel updates, so an older retry cannot downgrade Docker tags, R2 aliases or installer pointers. Prereleases do not advance stable channels. Helm publishes the already accepted chart after versioned images exist. GitHub's legacy `*-latest.zip` download names remain aliases of the accepted bytes; they become default downloads only when GitHub Latest advances.

Cross-registry publication is not atomic. A channel job can fail after some verified destinations advance. Resume the same run/seal: matching destinations are reused and conflicting ones stop the workflow. Retrying never recompiles. Do not claim complete publication until the channel receipt and all public endpoints are verified.

## Failure and retention

- Infrastructure failure: rerun failed jobs in the same run. Frozen inputs, successful platform/package/image artifacts and completed seals are reused. Missing or expired artifacts fail closed.
- Product or build-input change before tagging: create a new candidate run and repeat affected acceptance. Do not mutate the old candidate.
- Failure after tagging: resume publication of exactly that seal. A code fix requires a new patch version; never move the tag.
- New main commits: retain evidence for unchanged source, recipe and inputs. Recheck only affected boundaries; do not restart the entire matrix because an unrelated PR merged.
- Artifacts are retained for 30 days while awaiting acceptance. Public-repository Actions artifacts and staging identifiers are not confidential storage. Preserve important failure evidence in task-owned storage and backlog.

## Migration and workflow maintenance

Finish any in-flight legacy tag release, including its downstream Docker/package/Helm jobs, before enabling this cutover on main. Existing tags and Releases retain their identities. Do not feed a legacy public preview binary into the new promotion path: it embeds a different version and was not built from the frozen-input contract.

All workflow responsibilities, triggers and schedule ownership remain registered in `.github/scheduled-validations.json`. The migration does not re-enable workflows disabled in GitHub. PR checks, cache warming, nightly development builds, functional chains, lab tests and monitoring retain their distinct purposes; shared source or cache does not make different feature/platform tests redundant. The watchdog allocates a runner only for relevant unsuccessful scheduled/chained runs, while its registry still determines ownership.

Run `actionlint`, the release contract suite and the workflow registry validator after changes. The Security Audit workflow also exercises actual GitHub artifact transport, DEB/RPM package extraction, both multi-platform OCI builders, local registry promotion and archive scanning. Native packaging jobs run only for relevant changes or a manual audit. Their payloads are explicit transport fixtures, not product release acceptance. See the [release skill](../../.agents/skills/rustfs-release-publish/SKILL.md) for field acceptance, human confirmation and post-release follow-up.
