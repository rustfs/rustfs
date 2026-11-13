---
name: rustfs-release-version-bump
description: Prepare an exact RustFS Cargo version or align installation references after publication. Use for requested version bumps or the release workflow.
---
# Release Version Files

Require an exact target; ask if missing or ambiguous. Reject targets containing `-preview`: those identify candidates, not product versions or new public tags. Infer `post-release` only from an explicit request or parent workflow; otherwise use `prepare`. Delivery defaults to local edit/verify; commit, push, PR, and merge follow existing authorization and [Git rules](../../references/pull-requests.md).

| Stage | Files and constraints |
|---|---|
| `prepare` | Only `Cargo.toml` and `Cargo.lock`: workspace package, internal dependency, and workspace member versions become `<target>`. External dependencies stay unchanged. |
| `post-release` | Only `README.md`, `README_ZH.md`, `flake.nix`, `helm/rustfs/Chart.yaml`, and `rustfs.spec`. Cargo may already target the next release; leave it untouched. |

Before installation edits, verify the exact non-preview Release, downloadable assets/source archive, pullable `rustfs/rustfs:<target>` image manifest, and published Helm chart. Block on missing artifacts. Do not downgrade defaults if a newer deliverable superseded the target. Candidate preparation retains the previous published installation references; follow-up commits never replace validated tags or sealed artifacts.

Packaging policy:

- Docker image tags have no `v` prefix. Derive chart/app versions with `scripts/helm_chart_version.sh <target>`; both equal the final product SemVer, including prerelease suffixes. Helm CI uses the frozen candidate version before sealing, so no early installation chart bump is required. Previously published chart versions remain unchanged.
- RPM `Version` is numeric; `Release` is the prerelease suffix or `1` for stable. Expanded `Source0` and the unpack directory must resolve to the exact target including its suffix. Add the changelog entry using current git identity, date, and target; never copy an example identity/date.
- Changing these mappings requires explicit confirmation.

Verification follows root tiers: `prepare` validates Cargo metadata with the updated lockfile and checks every workspace/internal version with no unrelated lockfile updates. `post-release` renders Helm with the verified image, runs `scripts/test_helm_chart_version.sh` for chart version edits, and checks README, Nix, and RPM references. Run `git diff --check`; report unresolved required checks as `BLOCKED` without unrelated fixes.

For authorized commits, keep stages separate: `chore(release): prepare <target>` or `chore(release): align installation references for <target>`. Report target/stage, changed files, verification, material uncertainty, and actual commit/push/PR state.
