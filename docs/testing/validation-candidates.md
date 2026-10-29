# Immutable validation candidates

Canonical validation on the current auto-testing fleet consumes one exact
package, source commit and candidate-bound tool build. A successful package
build is provenance evidence, not a canonical test PASS.

The scheduled Functional Chain continues to resolve successful Nightly GNU
Build attempts from `main`. Optional manual validation can also resolve a
validation package built without publishing nightly aliases or release assets.

## Build a validation package

Dispatch **RustFS Functional Chain** from the desired driver revision with
only `validation_package_sha` set to the full 40-character RustFS source SHA.
Keep candidate, case, helper, tools and cleanup inputs empty or false. The
request validator rejects mixed package/fleet execution before building.

The package job calls `rustfs-validation-package.yml`, checks out the requested
source, confirms its Git SHA and builds the ordinary release binary and DEB.
This job uses build runners and does not acquire or mutate the Heal fleet.
The same workflow can be dispatched directly with its `source_sha` input when
it is available on the repository's default branch.

Publication requires complete R2 configuration, conditional `PutObject` with
`If-None-Match: *`, and a public download whose SHA-256 matches the local DEB.
Failure emits no candidate manifest. An existing object is never overwritten.
The only object path is:

```text
artifacts/rustfs/packages/validation/runs/<run>/<attempt>/<package-sha256>/rustfs.deb
```

The schema-3 artifact `validation-candidate-<run>-<attempt>` records the
workflow SHA independently of the exact source SHA and source ref, the
`validation` channel, build run/attempt, immutable URL and package checksum.
The resolver verifies the successful manual run, repository/workflow identity,
exact artifact name and digest, archive members, schema and immutable URL.
The ordinary scheduled chain does not enable validation package resolution.

## Validate the candidate

After the package run succeeds, dispatch Functional Chain with its exact
`build_run_id` and `build_run_attempt`, a canonical `validation_case`, and an
exact `auto_testing_sha`. Heal also requires a new tool build verified against
that source and helper revision. Reassess source-bound contracts and native
prerequisites when the source changes; an older tool artifact does not certify
a new candidate.

The fleet lane retains the shared environment lock, per-case evidence and
cleanup gate. Record new results separately. A package build, native test,
diagnostic replay, clean recovery or partial topology result cannot replace
complete original source assertions or rewrite a retained canonical FAIL.
