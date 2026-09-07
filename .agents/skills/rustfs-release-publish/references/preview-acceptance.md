# Preview Artifact Acceptance

Read after Phase 3 succeeds. Complete every check below before the parent skill's manual confirmation gate. These checks cover the downloaded artifact, embedded Console, and latest rc client.

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
