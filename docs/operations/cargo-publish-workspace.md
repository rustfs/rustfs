# Workspace Cargo Publish Runbook

This runbook covers crates.io publication for the RustFS Cargo workspace. It is separate from the GitHub tag, binary asset, Docker, Helm, and package release pipeline.

Use [`scripts/cargo_publish_workspace.sh`](../../scripts/cargo_publish_workspace.sh) as the source of truth for the live publish order. The script derives the order from `cargo metadata`, skips workspace members marked `publish = false`, fails on dependency cycles, and runs crates before their workspace dependents.

## Scope

- Publish workspace crates with `cargo publish -p <package>` in dependency order.
- Validate the package graph before any registry operation.
- Check whether each crate version already exists in the target registry before dry-run or publish work.
- Support an offline plan mode and an online dry-run mode before the final publish.
- Keep the publish operation on the exact reviewed commit.

Out of scope:

- RustFS version-file bumping.
- GitHub preview or final tag publication.
- GitHub Release asset verification.
- Docker, Helm, DEB, RPM, or latest-channel publication.

## Current Branch Snapshot

On this branch, `cargo metadata --format-version 1 --no-deps` reports 51 workspace members. None of the workspace package manifests set `publish = false`, so every member is eligible from Cargo's manifest perspective.

The script always filters out the test-only `e2e_test` package, leaving 50 packages in the default publish plan. If another test-only or internal crate should not be published, set `publish = false` in that crate's manifest before publication. The script also supports `--exclude <package>`, but it will fail if any selected publishable crate still depends on the excluded package.

The publish order below is the current topological order of workspace path dependencies:

1. `rustfs-checksums`
2. `rustfs-common`
3. `rustfs-config`
4. `rustfs-credentials`
5. `rustfs-crypto`
6. `rustfs-extension-schema`
7. `rustfs-heal-contracts`
8. `rustfs-license`
9. `rustfs-log-analyzer`
10. `rustfs-object-data-cache`
11. `rustfs-replication`
12. `rustfs-s3-types`
13. `rustfs-security-governance`
14. `rustfs-utils`
15. `rustfs-tls-runtime`
16. `rustfs-policy`
17. `rustfs-scanner-metrics`
18. `rustfs-s3-ops`
19. `rustfs-filemeta`
20. `rustfs-kms`
21. `rustfs-signer`
22. `rustfs-trusted-proxies`
23. `rustfs-targets`
24. `rustfs-keystone`
25. `rustfs-io-metrics`
26. `rustfs-data-usage`
27. `rustfs-storage-api`
28. `rustfs-madmin`
29. `rustfs-audit`
30. `rustfs-io-core`
31. `rustfs-lock`
32. `rustfs-object-capacity`
33. `rustfs-protos`
34. `rustfs-rio`
35. `rustfs-lifecycle`
36. `rustfs-concurrency`
37. `rustfs-rio-v2`
38. `rustfs-s3-client`
39. `rustfs-zip`
40. `rustfs-ecstore`
41. `rustfs-notify`
42. `rustfs-test-utils`
43. `rustfs-heal`
44. `rustfs-iam`
45. `rustfs-s3select-api`
46. `rustfs-scanner`
47. `rustfs-obs`
48. `rustfs-protocols`
49. `rustfs-s3select-query`
50. `rustfs`

Regenerate this order before a real publication:

```bash
scripts/cargo_publish_workspace.sh --mode plan
```

## Preflight

1. Start from the exact commit that should be published.
2. Confirm the worktree is clean:

```bash
git status --short
```

3. Confirm the branch contains the intended version numbers:

```bash
cargo metadata --format-version 1 --no-deps
```

4. Confirm crates.io authentication without printing tokens:

```bash
cargo login
```

5. Run the repository validation appropriate to the version change before publishing. At minimum, a real release should not rely only on `cargo publish --dry-run`; dry-run verifies packaging, not RustFS runtime acceptance.

## Dry Run

Run the complete ordered packaging check. The default registry is `crates-io`, and the script skips any crate version that already exists there. The script passes `--registry crates-io` explicitly so local source replacement does not redirect the publish command to a mirror.

```bash
scripts/cargo_publish_workspace.sh --mode dry-run
```

For an alternate registry:

```bash
scripts/cargo_publish_workspace.sh --mode dry-run --registry <registry-name>
```

Dry-run may need network access because the script checks whether versions already exist and Cargo validates registry dependency resolution. If the existence check cannot confirm the registry state, the script fails closed instead of publishing or dry-running that package. If dry-run fails for a workspace dependency, do not skip forward; fix the manifest/version state or publish the missing dependency first.

## Publish

The script requires an explicit environment confirmation for publish mode. The default registry is `crates-io`; crate versions that already exist in the registry are reported and skipped, not republished. Missing versions continue to `cargo publish --registry crates-io` by default.

```bash
RUSTFS_CARGO_PUBLISH_CONFIRM=publish \
  scripts/cargo_publish_workspace.sh --mode publish --wait-seconds 60
```

If the reviewed release/version bump is intentionally still uncommitted when packaging or publishing, pass `--allow-dirty`. The script then passes `--allow-dirty` through to `cargo publish`; without it, both the script and Cargo keep the dirty-worktree protection enabled.

For an alternate registry:

```bash
RUSTFS_CARGO_PUBLISH_CONFIRM=publish \
  scripts/cargo_publish_workspace.sh --mode publish --registry <registry-name> --wait-seconds 60
```

The wait between crates gives the registry index time to expose each newly published crate version before dependents are published. Increase `--wait-seconds` if dependents fail because the registry cannot resolve a just-published workspace crate.

## Failure Handling

- If a crate fails during dry-run, stop and fix the package before retrying from the beginning.
- If the script reports that a crate version already exists, it skips that crate and continues with the next selected package.
- If the registry existence check fails for network, service, authentication, or unexpected-response reasons, stop and rerun after the registry state can be verified.
- If a dependent crate cannot resolve a just-published internal dependency, wait for registry propagation and retry the same package.
- Do not bump versions, retag, or publish GitHub release assets as a workaround for a partial cargo publish. Treat that as a separate release decision.

## Post-Publish Checks

After the script finishes, spot-check the registry state:

```bash
cargo info rustfs
cargo info rustfs-ecstore
cargo info rustfs-utils
```

Record the final commit, crate version, script mode, registry, and any retry decisions in the release handoff.
