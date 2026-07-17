---
name: rustfs-release-publish
description: "End-to-end RustFS release pipeline: bump version files on main directly to the final target version, cut a preview tag on that commit, verify the CI build and release artifacts, run the downloaded binary locally and exercise the console, validate the server with the latest rc client, then publish the final tag on the SAME validated commit — never a new bump commit, never latest main. Use whenever the user wants to release/publish a RustFS version (发版/发布)."
---
# RustFS Release Publish (preview-validated pipeline)

This skill orchestrates a full release. It wraps `rustfs-release-version-bump` (which only edits version files and opens the PR) with a mandatory preview-tag validation loop before the final tag is published.

Core design: **version files never carry a `-preview.N` suffix**. The preview suffix exists only in tag names. This works because the binary self-reports the git tag it was built from (`build::TAG` via shadow_rs, see `rustfs/src/config/cli.rs` `SHORT_VERSION`), and `build.yml` derives artifact names and prerelease classification from the tag name — Cargo.toml's version is only a no-tag fallback. Therefore the preview tag and the final tag can (and MUST) point at the exact same commit: what you validated is byte-for-byte the source that ships.

Pipeline shape:

```
bump version files to <target> (final version, ONE commit) -> merge
  -> tag <preview-tag> at that commit -> CI green
  -> verify release artifacts -> run binary locally + console checks
  -> validate with latest rc client
  -> tag <target> at the SAME commit (zero delta) -> re-verify CI/release
```

On validation failure: fix lands on main via normal PR (version files are already at `<target>`, no new bump PR), then tag `<preview-tag N+1>` at the new main commit and restart from Phase 2.

## Required inputs

- Final target version, for example `1.0.0-beta.10`.
- Preview iteration `N` (default: next unused preview tag for that target; check with `git tag -l '<target>-preview.*'` — and for stable targets `git tag -l '<target>-rc.*'` — after `git fetch --tags`).

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

- Prerelease target (contains `alpha`/`beta`/`rc`): preview tag is `<target>-preview.N`, e.g. `1.0.0-beta.10-preview.3`. It contains `beta`, so `build.yml`'s substring-based classification marks it prerelease — safe.
- **Stable** target (e.g. `1.1.0`): NEVER tag `1.1.0-preview.N` — `build.yml` marks a tag prerelease only if its name contains `alpha`, `beta`, or `rc`, so `1.1.0-preview.N` would be treated as a stable release and overwrite `latest.json` as stable. Use `1.1.0-rc.N` as the preview tag instead.

## Hard rules

- Version files (Cargo.toml, Cargo.lock, README, flake.nix, Chart.yaml, rustfs.spec) are bumped ONCE, directly to `<target>`. Never write a `-preview.N` suffix into any version file. If `rustfs-release-version-bump` is ever asked for a `-preview` version, that is a pipeline bug — stop.
- Tags have no `v` prefix. Always annotated: `git tag -a <tag> -m "Release <tag>"`.
- The final tag MUST point at exactly `PREVIEW_HASH` — the commit the validated preview tag points at. Never tag current `main` HEAD (commits merged after validation are unvalidated), and never create an extra version-bump commit between preview and final.
- Phases run in order; a failure in any phase blocks everything after it. After the fix lands on main, restart from Phase 2 with the next preview iteration against the new `origin/main` hash — do not resume mid-pipeline against a stale hash.
- If the release is abandoned after Phase 1 merged, main's version files claim a version that was never tagged. Either revert the bump PR or leave it to be overwritten by the next release — but tell the user explicitly and record the decision.
- User-facing status updates in Chinese; commits, PR titles/bodies, and tag messages in English. No hard-wrapping in commit messages, PR bodies, or documentation prose — one logical line per sentence/paragraph, let soft wrap handle display.

## Phase 0 — Preflight

- `git status --short` clean; `git fetch origin main --tags`.
- `gh auth status` works; confirm you can view `gh release list -L 3`.
- Confirm the exact final target version with the user if not explicit.

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

On a restart (N+1), refresh `PREVIEW_HASH=$(git rev-parse origin/main)` first — it must contain the fix — and re-report it.

## Phase 3 — CI and artifact verification

- Watch the tag build: `gh run list --workflow build.yml --limit 5` then `gh run watch <run-id>`. Every matrix target must succeed (linux x86_64/aarch64 × musl/gnu, macos-aarch64, windows-x86_64) plus the release and latest.json jobs.
- Verify the GitHub release: `gh release view "<preview-tag>" --json isPrerelease,assets`
  - `isPrerelease` must be `true`.
  - Assets must include all 6 platform zips in both versioned (`rustfs-<platform>-v<tag>.zip`) and `-latest` forms, plus `SHA256SUMS`, `SHA512SUMS`, `rustfs-<tag>.sbom.cdx.json`, `rustfs-<tag>.provenance.json`.
- Verify the chained Docker run succeeded: `gh run list --workflow docker.yml --limit 3`.
- Checksum spot-check for the platform you will run locally: download the zip and `SHA256SUMS`, verify with `shasum -a 256 -c` (grep to one line).

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
- Re-run the Phase 3 verification against the final tag: all matrix jobs green; `gh release view "<target>"` shows the full asset set; for a prerelease target `isPrerelease` is `true`, for a stable target it must be `false` and `latest.json` must be updated.
- Optionally spot-check `./rustfs --version` from a final-tag artifact — it must report `<target>`.

## Output contract

Always report:

- Target version, preview tag(s) used, `PREVIEW_HASH` (which both tags point at).
- Per-phase result (PASS/FAIL/BLOCKED) with key evidence: CI run URLs, release URLs, console check results, the rc command matrix.
- Any deviation from this pipeline and why the user approved it.
