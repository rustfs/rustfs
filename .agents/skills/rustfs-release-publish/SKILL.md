---
name: rustfs-release-publish
description: "Run the end-to-end RustFS console gate, version bump, preview validation, and final-tag publication pipeline. Use only when the user explicitly asks to release or publish a RustFS version (发版/发布)."
---
# RustFS Release Publish (preview-validated pipeline)

This skill orchestrates a full release. It wraps `rustfs-release-version-bump` (which only edits version files and opens the PR) with a mandatory preview-tag validation loop before the final tag is published.

Core design: **version files never carry a `-preview.N` suffix**. The preview suffix exists only in tag names. A preview tag creates a visible GitHub Release marked Prerelease and uploads versioned assets, but it never becomes GitHub Latest and never updates `*-latest`, `latest.json`, R2, Docker, or Helm channels. This works because the binary self-reports the git tag it was built from (`build::TAG` via shadow_rs, see `rustfs/src/config/cli.rs` `SHORT_VERSION`), and `build.yml` derives artifact names and preview classification from the tag name — Cargo.toml's version is only a no-tag fallback. Therefore the preview tag and the final tag can (and MUST) point at the exact same commit: what you validated is byte-for-byte the source that ships.

Pipeline shape:

```
check console main against its latest Release
  -> if ahead: publish console -> wait for Release asset + latest API
  -> bump RustFS version files to <target> (final version, ONE commit) -> merge
  -> tag <preview-tag> at that commit -> CI green
  -> verify preview Release assets -> run binary locally + console checks
  -> validate with latest rc client
  -> tag <target> at the SAME commit (zero delta) -> re-verify CI/release
```

On validation failure: fix lands on main via normal PR (version files are already at `<target>`, no new bump PR), then tag `<preview-tag N+1>` at the new main commit and restart from Phase 2.

## Required inputs

- Final target version, for example `1.0.0-beta.10`.
- Preview iteration `N` (default: next unused preview tag for that target; check with `git tag -l '<target>-preview.*'` after `git fetch --tags`).

If the target version is missing or ambiguous, stop and ask before doing anything (see the semver gate below).

## Semver gate — confirm the target version before touching anything

Versions follow [SemVer 2.0.0](https://semver.org/). Precedence reminder:

```
1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0 < 1.0.1 < 1.1.0 < 2.0.0
```

Numeric prerelease identifiers compare numerically (`beta.9 < beta.10`), not lexically — see [semver.org spec item 11](https://semver.org/#spec-item-11). Preview tags are internal validation tags layered on top of the target's prerelease channel — they are never themselves a deliverable version and never appear in version files.

Rules:

- A request like "发个版" / "release the next version" without an exact version string is ALWAYS ambiguous. Derive the current latest tag (`git tag --sort=-v:refname | head`), then ask the user to choose via AskUserQuestion with concrete candidates, e.g. from `1.0.0-beta.10`: next prerelease `1.0.0-beta.11`, promote to `1.0.0-rc.1`, promote to stable `1.0.0`. Never guess between these — they have very different meanings (channel promotion vs. iteration) and different CI classification consequences.
- After a stable `X.Y.Z` exists, the next version must state which component bumps: patch `X.Y.(Z+1)` for fixes only, minor `X.(Y+1).0` for backward-compatible features, major `(X+1).0.0` for breaking changes. If the user names a bump type but not a number, compute it from the latest stable tag and echo the exact resulting version back for confirmation.
- Echo the final confirmed version string verbatim in your first status report; every later phase must use exactly that string. If at any point the user's wording and the confirmed version diverge, stop and re-confirm.

## Preview tag naming

- Use `<target>-preview.N` for every target, e.g. `1.0.0-beta.10-preview.3` or `1.1.0-preview.1`.
- The canonical suffix is exactly `-preview.<digits>`. `build.yml` recognizes it before alpha/beta/rc classification and routes it to the preview-only path; any other tag containing `-preview` fails closed instead of being treated as a release.
- A preview Release MUST be published with `isPrerelease=true` and `isLatest=false`. Any `*-latest` preview asset or preview-triggered `latest.json`, R2, Docker, or Helm publication is a pipeline failure.

## Hard rules

- Version files (Cargo.toml, Cargo.lock, README, flake.nix, Chart.yaml, rustfs.spec) are bumped ONCE, directly to `<target>`. Never write a `-preview.N` suffix into any version file. If `rustfs-release-version-bump` is ever asked for a `-preview` version, that is a pipeline bug — stop.
- Preview Release assets are versioned and intentionally visible on the Releases page. Do not label them Latest or use them to update any latest distribution channel.
- Tags have no `v` prefix. Always annotated: `git tag -a <tag> -m "Release <tag>"`.
- The final tag MUST point at exactly `PREVIEW_HASH` — the commit the validated preview tag points at. Never tag current `main` HEAD (commits merged after validation are unvalidated), and never create an extra version-bump commit between preview and final.
- When a previous deliverable exists, GitHub Release notes for the preview and final tags MUST use it as their shared comparison baseline: the most recently published non-preview Release before the target. Internal `-preview.N` Releases are explicitly excluded from that selection, even when they point at the same commit as the final tag. If no previous deliverable exists, omit `previous_tag_name` and record that GitHub's default baseline fallback was used.
- Generated Release notes carry a workflow-management marker so retries can repair them. Before manually curating a generated body, remove that marker; unmarked non-placeholder notes are preserved by later workflow runs.
- Phases run in order; a failure in any phase blocks everything after it. After the fix lands on main, restart from Phase 2 with the next preview iteration against the new `origin/main` hash — do not resume mid-pipeline against a stale hash.
- If the release is abandoned after Phase 1 merged, main's version files claim a version that was never tagged. Either revert the bump PR or leave it to be overwritten by the next release — but tell the user explicitly and record the decision.
- User-facing status updates in Chinese; commits, PR titles/bodies, and tag messages in English. No hard-wrapping in commit messages, PR bodies, or documentation prose — one logical line per sentence/paragraph, let soft wrap handle display.

## Phase 0 — Preflight

- `git status --short` clean; `git fetch origin main --tags`.
- `gh auth status` works; confirm you can view `gh release list -L 3`.
- Confirm the exact final target version with the user if not explicit.

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

## Phase 1 — Version bump to the final target (once)

- If main's version files already read `<target>` (e.g. this is a restart after a failed preview), verify with `rg -n "<target>" Cargo.toml rustfs.spec helm/rustfs/Chart.yaml` and skip to Phase 2.
- Otherwise invoke the `rustfs-release-version-bump` skill with the final `<target>` (NOT a preview version), full GitHub flow (commit/push/PR).
- Get the PR merged into main. Record the resulting main commit:

```bash
git fetch origin main
PREVIEW_HASH=$(git rev-parse origin/main)   # must contain the bump PR
```

`PREVIEW_HASH` is the single source of truth for the rest of the pipeline — report it to the user and reuse it verbatim in Phases 2 and 6. Both the preview tag and the final tag will point at it.

## Phase 2 — Publish the preview tag

```bash
git tag -a "<preview-tag>" -m "Release <preview-tag>" "$PREVIEW_HASH"
git push origin "<preview-tag>"
```

Pushing the tag triggers `.github/workflows/build.yml` ("Build and Release"); `docker.yml` chains off it via `workflow_run`.

The preview run builds versioned artifacts and publishes them in a GitHub prerelease. Its latest-channel, R2, Docker, and Helm jobs must be skipped. Those publication paths run only after the final tag is pushed.

On a restart (N+1), refresh `PREVIEW_HASH=$(git rev-parse origin/main)` first — it must contain the fix — and re-report it.

## Phase 3 — CI and preview Release verification

- Find and watch the tag build: `gh run list --workflow build.yml --branch "<preview-tag>" --limit 1` then `gh run watch <run-id>`. Every build matrix target must succeed (linux x86_64/aarch64 × musl/gnu, macos-aarch64, windows-x86_64).
- Confirm the Release publication jobs (`create-release`, `upload-release-assets`, and `publish-release`) succeed while `update-latest-version` is skipped.
- Verify `gh release view "<preview-tag>" --json isPrerelease,assets,url`: `isPrerelease` must be `true`, and the Release must contain all 6 versioned platform zips, checksums, SBOM, and provenance with no `-latest` assets. Confirm `gh api repos/{owner}/{repo}/releases/latest --jq .tag_name` does not return `<preview-tag>`.
- Record `PREVIOUS_DELIVERABLE`, selected from published Releases by `publishedAt` after excluding the current tag and every `-preview.N` tag. Verify `gh release view "<preview-tag>" --json body --jq .body` contains `## What's Changed` and, when `PREVIOUS_DELIVERABLE` exists, `**Full Changelog**: https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<preview-tag>`. For a repository with no previous deliverable, verify a Full Changelog link exists and record the GitHub baseline fallback.
- Confirm preview-triggered Docker and Helm jobs are skipped. Preview validation covers the built RustFS binaries, embedded console, and rc compatibility; Docker image construction and Helm publication are deferred to the final tag because the Dockerfiles consume GitHub Release assets.

## Phase 4 — Run the artifact locally, verify the console

Work inside the session scratchpad directory; never leave stray data dirs.

```bash
gh release download "<preview-tag>" -p "rustfs-macos-aarch64-v<preview-tag>.zip" -D "$SCRATCH"
cd "$SCRATCH" && unzip -o rustfs-*.zip
./rustfs --version        # must report the PREVIEW TAG (build::TAG), not the Cargo.toml version, plus expected short SHA
mkdir -p data
RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin ./rustfs ./data
```

Defaults: S3 endpoint `:9000`, embedded console `:9001`.

Checks (all must pass):

- `./rustfs --version` reports the preview tag name and the short SHA of `PREVIEW_HASH`. Reporting `<target>` without the `-preview.N` suffix means the build did not embed the tag — treat as FAIL and investigate before proceeding.
- `curl -fsS http://localhost:9000/health/ready` returns ready.
- Startup log shows the embedded console being served (this was the regression that `fix(release): require embedded console assets` guards).
- Open `http://localhost:9001` in the browser: login with `rustfsadmin`/`rustfsadmin`; dashboard renders without JS console errors; create a bucket, upload a file, download it back (byte-identical), delete the object and bucket. Keep the server running for Phase 5.

## Phase 5 — Validate with the latest rc client

`rc` is the RustFS CLI client from <https://github.com/rustfs/cli>.

- Ensure the latest release is installed: compare `rc --version` against `gh api repos/rustfs/cli/releases/latest --jq .tag_name`; update via `brew upgrade rustfs/tap/rc` (or download the release binary).
- Point it at the preview server and run the command matrix, recording PASS/FAIL per command:

```bash
rc alias set preview http://localhost:9000 rustfsadmin rustfsadmin
rc ls preview/
rc mb preview/rel-check
rc cp <local-file> preview/rel-check/
rc stat preview/rel-check/<file>
rc cat preview/rel-check/<file>          # matches source
rc cp preview/rel-check/<file> ./out && cmp <local-file> ./out
rc cp -r <local-dir>/ preview/rel-check/dir/
rc find preview/rel-check --name "*"
rc share download preview/rel-check/<file> --expire 1h   # presigned URL fetchable via curl
rc rm preview/rel-check/<file> && rc rm -r --force preview/rel-check/dir
rc rb preview/rel-check
rc admin user list preview/
rc admin user add preview/ relcheckuser relchecksecret12
rc admin user remove preview/ relcheckuser
rc alias remove preview
```

- Any FAIL blocks the release. Afterwards stop the server and delete the scratch data directory.

## Phase 6 — Publish the final tag on the validated commit

No second version bump, no release branch. The final tag goes on the exact commit the preview validated:

```bash
git fetch origin --tags
git rev-parse "<preview-tag>^{commit}"          # must equal PREVIEW_HASH — abort if not
git tag -a "<target>" -m "Release <target>" "$PREVIEW_HASH"
git push origin "<target>"
```

- CI rebuilds from the same source; the only changed input is the tag name, so the binary now self-reports `<target>`.
- Verify the final tag's complete publication path: all matrix and release jobs green; `gh release view "<target>"` shows the full versioned and `-latest` asset set plus checksums, SBOM, and provenance; Docker and Helm workflows succeed; `latest.json` points to `<target>`. A stable target must have `isPrerelease=false` and `isLatest=true`. An alpha/beta/rc target must have `isPrerelease=true`; GitHub does not permit prereleases to be Latest, but the project `latest.json` still advances to the final non-preview target.
- Verify the final Release body contains `## What's Changed` and a Full Changelog link. When `PREVIOUS_DELIVERABLE` exists, the link MUST be `https://github.com/rustfs/rustfs/compare/<PREVIOUS_DELIVERABLE>...<target>` and the baseline MUST equal the preview Release baseline; for example, both `1.0.0-beta.12-preview.1` and `1.0.0-beta.12` compare from `1.0.0-beta.11`.
- Optionally spot-check `./rustfs --version` from a final-tag artifact — it must report `<target>`.

## Output contract

Always report:

- Console gate result: previous/latest Console tags, whether merged changes required a release, `CONSOLE_HASH`, and Console run/Release URLs when a release was published.
- Target version, preview tag(s) used, `PREVIEW_HASH` (which both tags point at).
- Per-phase result (PASS/FAIL/BLOCKED) with key evidence: preview and final Release URLs, preview `isPrerelease`/`isLatest` state, final latest-channel state, console check results, and the rc command matrix.
- Any deviation from this pipeline and why the user approved it.
