# Console Release Gate

Read during Phase 0, before changing RustFS version files or tags. Follow the parent skill's release scope and authorization rules.

### Console release gate

Complete this gate before changing any RustFS version file or creating any RustFS tag. RustFS `build.yml` downloads the asset returned by `repos/rustfs/console/releases/latest`, so a successful Console build alone is insufficient.

1. Read the latest published Console tag and compare it with Console `main`:

```bash
CONSOLE_REPO="rustfs/console"
CONSOLE_LATEST=$(gh api "repos/${CONSOLE_REPO}/releases/latest" --jq .tag_name)
gh api "repos/${CONSOLE_REPO}/compare/${CONSOLE_LATEST}...main" \
  --jq '{status, ahead_by, behind_by, commits: [.commits[] | {sha, message: .commit.message}]}'
```

- `ahead_by == 0`: no merged Console change is waiting for release. Still verify the current latest asset using step 4, then continue to Phase 1.
- `ahead_by > 0` and `behind_by == 0`: publish Console before continuing. Report the merged commits and select the next unused `vX.Y.Z` tag. Default to the next patch version when the changes are fixes or backward-compatible UI work; stop for confirmation if a minor/major bump is plausible.
- Any diverged history or `behind_by > 0`: stop and resolve the Console release baseline explicitly. Do not guess a range or publish RustFS.

2. Clone/fetch `rustfs/console` into a scratch directory and record its exact `main` commit. Before creating a tag, check for a `v*` tag or Release workflow already associated with that hash. If one is in progress, wait for it instead of creating another version:

```bash
CONSOLE_SCRATCH=$(mktemp -d)
gh repo clone "$CONSOLE_REPO" "$CONSOLE_SCRATCH/console"
git -C "$CONSOLE_SCRATCH/console" fetch origin main --tags
CONSOLE_HASH=$(git -C "$CONSOLE_SCRATCH/console" rev-parse origin/main)
git -C "$CONSOLE_SCRATCH/console" tag --points-at "$CONSOLE_HASH" 'v*'
gh run list -R "$CONSOLE_REPO" --workflow release.yml --commit "$CONSOLE_HASH" --limit 5
```

If no release exists or is running for `CONSOLE_HASH`, create the selected annotated tag at that exact hash and push it:

```bash
git -C "$CONSOLE_SCRATCH/console" tag -a "<console-tag>" -m "Release <console-tag>" "$CONSOLE_HASH"
git -C "$CONSOLE_SCRATCH/console" push origin "<console-tag>"
```

Console tags include the `v` prefix. Pushing the tag triggers `.github/workflows/release.yml` (`🚀 Release`). Remove `CONSOLE_SCRATCH` after the gate completes.

3. Find the exact tag run and wait for completion:

```bash
gh run list -R "$CONSOLE_REPO" --workflow release.yml --branch "<console-tag>" --limit 1
gh run watch -R "$CONSOLE_REPO" "<console-run-id>" --exit-status
```

4. Block until the published Release is non-draft, the latest endpoint returns the expected tag, and `rustfs-console-<console-tag>.zip` is uploaded, non-empty, and carries a `sha256:` digest:

```bash
gh release view -R "$CONSOLE_REPO" "<console-tag>" --json isDraft,isPrerelease,assets,url
test "$(gh api "repos/${CONSOLE_REPO}/releases/latest" --jq .tag_name)" = "<console-tag>"
test "$(gh api "repos/${CONSOLE_REPO}/releases/tags/<console-tag>" \
  --jq '[.assets[] | select(.name == "rustfs-console-<console-tag>.zip" and .state == "uploaded" and .size > 0 and (.digest | startswith("sha256:")))] | length')" -eq 1
```

Treat a missing/mismatched asset, digest, latest tag, or failed/cancelled workflow as BLOCKED. Do not start Phase 1 until the Console gate passes. Record `CONSOLE_TAG`, `CONSOLE_HASH`, Console run URL, and Release URL for the final report.
