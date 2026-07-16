---
name: rustfs-release-publish
description: "End-to-end RustFS release pipeline: cut a preview tag first, verify the CI build and release artifacts, run the downloaded binary locally and exercise the console, validate the server with the latest rc client, then bump to the final version via rustfs-release-version-bump and publish the final tag from the validated preview hash — never from latest main. Use whenever the user wants to release/publish a RustFS version (发版/发布)."
---
# RustFS Release Publish (preview-validated pipeline)

This skill orchestrates a full release. It wraps `rustfs-release-version-bump` (which only edits version files and opens the PR) with a mandatory preview-tag validation loop before the final tag is published.

Pipeline shape:

```
bump to <target>-preview.N -> merge -> tag preview -> CI green
  -> verify release artifacts -> run binary locally + console checks
  -> validate with latest rc client
  -> bump to <target> ON TOP OF the validated preview hash -> CI green
  -> tag final version at that commit (NOT main HEAD)
```

## Required inputs

- Final target version, for example `1.0.0-beta.10`.
- Preview iteration `N` (default: next unused `-preview.N` for that target; check with `git tag -l '<target>-preview.*'` after `git fetch --tags`).

If the target version is missing or ambiguous, stop and ask before doing anything (see the semver gate below).

## Semver gate — confirm the target version before touching anything

Versions follow [SemVer 2.0.0](https://semver.org/). Precedence reminder:

```
1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0 < 1.0.1 < 1.1.0 < 2.0.0
```

Numeric prerelease identifiers compare numerically (`beta.9 < beta.10`), not lexically — see [semver.org spec item 11](https://semver.org/#spec-item-11). Preview tags (`<target>-preview.N`) are internal validation tags layered on top of the target's prerelease channel — they are never themselves a deliverable version.

Rules:

- A request like "发个版" / "release the next version" without an exact version string is ALWAYS ambiguous. Derive the current latest tag (`git tag --sort=-v:refname | head`), then ask the user to choose via AskUserQuestion with concrete candidates, e.g. from `1.0.0-beta.10`: next prerelease `1.0.0-beta.11`, promote to `1.0.0-rc.1`, promote to stable `1.0.0`. Never guess between these — they have very different meanings (channel promotion vs. iteration) and different CI classification consequences.
- After a stable `X.Y.Z` exists, the next version must state which component bumps: patch `X.Y.(Z+1)` for fixes only, minor `X.(Y+1).0` for backward-compatible features, major `(X+1).0.0` for breaking changes. If the user names a bump type but not a number, compute it from the latest stable tag and echo the exact resulting version back for confirmation.
- Echo the final confirmed version string verbatim in your first status report; every later phase must use exactly that string. If at any point the user's wording and the confirmed version diverge, stop and re-confirm.

## Hard rules

- **Prerelease classification is substring-based.** `build.yml` marks a tag as prerelease only if its name contains `alpha`, `beta`, or `rc`. A preview of a prerelease target (`1.0.0-beta.10-preview.3`) contains `beta` and is safe. For a **stable** target (e.g. `1.1.0`), NEVER tag `1.1.0-preview.N` — it would be treated as a stable release and overwrite `latest.json` as stable. Use `1.1.0-rc.N` as the preview tag instead.
- Tags have no `v` prefix. Always annotated: `git tag -a <tag> -m "Release <tag>"`.
- The final tag MUST point at a commit whose ancestry is exactly `validated preview hash + final version-bump commit(s)`. Never tag current `main` HEAD: commits merged after the preview was validated are unvalidated.
- Phases run in order; a failure in any phase blocks everything after it. After the fix lands on main, restart from Phase 1 with `N+1` — do not resume mid-pipeline against a stale hash.
- User-facing status updates in Chinese; commits, PR titles/bodies, and tag messages in English. No hard-wrapping in commit messages, PR bodies, or documentation prose — one logical line per sentence/paragraph, let soft wrap handle display.

## Phase 0 — Preflight

- `git status --short` clean; `git fetch origin main --tags`.
- `gh auth status` works; confirm you can view `gh release list -L 3`.
- Confirm the exact final target version with the user if not explicit.

## Phase 1 — Preview version bump

- Invoke the `rustfs-release-version-bump` skill with version `<target>-preview.N`, full GitHub flow (commit/push/PR).
- Get the PR merged into main. Record the resulting main commit:

```bash
git fetch origin main
PREVIEW_HASH=$(git rev-parse origin/main)   # must contain the bump PR
```

`PREVIEW_HASH` is the single source of truth for the rest of the pipeline — report it to the user and reuse it verbatim in Phases 2 and 6.

## Phase 2 — Publish the preview tag

```bash
git tag -a "<target>-preview.N" -m "Release <target>-preview.N" "$PREVIEW_HASH"
git push origin "<target>-preview.N"
```

Pushing the tag triggers `.github/workflows/build.yml` ("Build and Release"); `docker.yml` chains off it via `workflow_run`.

## Phase 3 — CI and artifact verification

- Watch the tag build: `gh run list --workflow build.yml --limit 5` then `gh run watch <run-id>`. Every matrix target must succeed (linux x86_64/aarch64 × musl/gnu, macos-aarch64, windows-x86_64) plus the release and latest.json jobs.
- Verify the GitHub release: `gh release view "<target>-preview.N" --json isPrerelease,assets`
  - `isPrerelease` must be `true`.
  - Assets must include all 6 platform zips in both versioned (`rustfs-<platform>-v<tag>.zip`) and `-latest` forms, plus `SHA256SUMS`, `SHA512SUMS`, `rustfs-<tag>.sbom.cdx.json`, `rustfs-<tag>.provenance.json`.
- Verify the chained Docker run succeeded: `gh run list --workflow docker.yml --limit 3`.
- Checksum spot-check for the platform you will run locally: download the zip and `SHA256SUMS`, verify with `shasum -a 256 -c` (grep to one line).

## Phase 4 — Run the artifact locally, verify the console

Work inside the session scratchpad directory; never leave stray data dirs.

```bash
gh release download "<target>-preview.N" -p "rustfs-macos-aarch64-v<tag>.zip" -D "$SCRATCH"
cd "$SCRATCH" && unzip -o rustfs-*.zip
./rustfs --version        # must report the preview version and expected short SHA
mkdir -p data
RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin ./rustfs ./data
```

Defaults: S3 endpoint `:9000`, embedded console `:9001`.

Checks (all must pass):

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

## Phase 6 — Final version bump from the validated hash

```bash
git checkout -b release/<target> "$PREVIEW_HASH"    # NOT origin/main
```

- Invoke `rustfs-release-version-bump` with the final `<target>` on this branch (commit/push/PR). Its verification gate (`make pre-commit` etc.) must pass; the PR's CI must be green before proceeding.
- Do not rebase this branch onto main and do not merge main into it — that would reintroduce unvalidated commits under the final tag.

## Phase 7 — Publish the final tag

```bash
FINAL_HASH=$(git rev-parse release/<target>)   # PREVIEW_HASH + bump commit(s) only
git merge-base --is-ancestor "$PREVIEW_HASH" "$FINAL_HASH"   # must succeed
git tag -a "<target>" -m "Release <target>" "$FINAL_HASH"
git push origin "<target>"
```

- Re-run the Phase 3 verification against the final tag (for a stable target, `isPrerelease` must now be `false`).
- Merge the bump PR into main afterwards so main records the version. With squash-merge, main's copy of the bump differs from the tagged commit — that is expected and fine.

## Output contract

Always report:

- Target version, preview tag(s) used, `PREVIEW_HASH`, `FINAL_HASH`.
- Per-phase result (PASS/FAIL/BLOCKED) with key evidence: CI run URLs, release URLs, console check results, the rc command matrix.
- Any deviation from this pipeline and why the user approved it.
