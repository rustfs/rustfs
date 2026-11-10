# Candidate Artifact Acceptance

Read after Phase 3 succeeds. Complete every check below before the parent skill's manual confirmation gate. These checks cover the downloaded artifact, embedded Console, and the rc release frozen in the candidate inputs.

## Phase 4 — Run the artifact locally, verify the console

Use a unique task-owned scratch directory. Select the versioned archive matching the acceptance host, and verify it against the sealed manifest before extraction. Before desktop completion, use the Linux acceptance environment rather than rebuilding for the local desktop. Do not substitute a different build when a matching artifact is unavailable; report that acceptance gap. The shell example below assumes the verified executable is at `RUSTFS_BINARY` (`rustfs.exe` on Windows), `SCRATCH` is the task directory, and `S3_PORT` and `CONSOLE_PORT` are distinct available ports. Use equivalent native commands when the shell differs.

Bind both listeners to loopback. Choose task-only credentials, set loopback proxy exclusions for the server/client processes, and record the server PID. A port probe does not reserve a port; retry startup with fresh ports on a bind conflict rather than connecting to an existing service.

```bash
"$RUSTFS_BINARY" --version
mkdir -p "$SCRATCH/data"
S3_ENDPOINT="http://127.0.0.1:${S3_PORT}"
CONSOLE_ENDPOINT="http://127.0.0.1:${CONSOLE_PORT}"
RUSTFS_ACCESS_KEY="$PREVIEW_ACCESS_KEY" RUSTFS_SECRET_KEY="$PREVIEW_SECRET_KEY" \
  "$RUSTFS_BINARY" server \
  --address "127.0.0.1:${S3_PORT}" \
  --console-enable \
  --console-address "127.0.0.1:${CONSOLE_PORT}" \
  "$SCRATCH/data" >"$SCRATCH/server.log" 2>&1 &
PREVIEW_SERVER_PID=$!
```

Checks (all must pass):

- `"$RUSTFS_BINARY" --version` reports the exact final `<target>` and full `CANDIDATE_SHA`. A preview suffix, another version or another source is a failure.
- Wait with a bounded readiness deadline while checking the recorded server process remains alive; `curl --noproxy '*' -fsS "$S3_ENDPOINT/health/ready"` must return ready from this process's listener.
- Startup log shows the embedded console being served (this was the regression that `fix(release): require embedded console assets` guards).
- Open `CONSOLE_ENDPOINT` in the browser and log in with the task credentials. The dashboard must render without JS console errors; create a bucket, upload a file, download it back (byte-identical), and delete the object and bucket. Keep the server running for Phase 5.

## Phase 5 — Validate with the pinned rc client

`rc` is the RustFS CLI client from <https://github.com/rustfs/cli>.

- Read the CLI release and asset digests from `inputs.json`; verify the local client's version or download a host-compatible asset from that exact release into the task directory. If the host is a desktop, resolve its asset within the same pinned CLI release and record the digest; do not independently follow `latest`. Set `RC_BIN` to that executable and record its version/digest. Avoid changing the user's installed client for this check.
- Inspect the client's current help for an isolated configuration option and use it when supported. Otherwise choose a unique `PREVIEW_ALIAS` and confirm it is absent before creating it; never overwrite an existing alias. Use task-owned bucket/user names and input/output files. The names below are safe only inside the newly started isolated server.
- Point the client at `S3_ENDPOINT` with loopback proxy exclusions and run the command matrix, recording PASS/FAIL per command:

```bash
"$RC_BIN" alias set "$PREVIEW_ALIAS" "$S3_ENDPOINT" "$PREVIEW_ACCESS_KEY" "$PREVIEW_SECRET_KEY"
"$RC_BIN" ls "$PREVIEW_ALIAS/"
"$RC_BIN" mb "$PREVIEW_ALIAS/rel-check"
"$RC_BIN" cp <local-file> "$PREVIEW_ALIAS/rel-check/"
"$RC_BIN" stat "$PREVIEW_ALIAS/rel-check/<file>"
"$RC_BIN" cat "$PREVIEW_ALIAS/rel-check/<file>"          # matches source
"$RC_BIN" cp "$PREVIEW_ALIAS/rel-check/<file>" "$SCRATCH/out"
cmp <local-file> "$SCRATCH/out"
"$RC_BIN" cp -r <local-dir>/ "$PREVIEW_ALIAS/rel-check/dir/"
"$RC_BIN" find "$PREVIEW_ALIAS/rel-check" --name "*"
"$RC_BIN" share download "$PREVIEW_ALIAS/rel-check/<file>" --expire 1h
# Fetch the presigned URL with curl --noproxy '*' and compare its body.
"$RC_BIN" rm "$PREVIEW_ALIAS/rel-check/<file>"
"$RC_BIN" rm -r --force "$PREVIEW_ALIAS/rel-check/dir"
"$RC_BIN" rb "$PREVIEW_ALIAS/rel-check"
"$RC_BIN" admin user list "$PREVIEW_ALIAS/"
"$RC_BIN" admin user add "$PREVIEW_ALIAS/" relcheckuser relchecksecret12
"$RC_BIN" admin user remove "$PREVIEW_ALIAS/" relcheckuser
"$RC_BIN" alias remove "$PREVIEW_ALIAS"
```

- Any FAIL blocks acceptance. On success or failure, stop only the recorded task process and remove only task-created aliases/data after retaining the needed failure evidence. Do not erase an existing client configuration or unrelated server data.

## Field acceptance and desktop completion

Before accepting the Linux manifest, record Scanner/Heal, multi-pool, memory, major features, high-concurrency 503, performance and previous-version integrity/upgrade results in backlog. Preserve the release-specific agreed limits and baseline; a PASS label without artifact identities or test results does not satisfy acceptance.

Dispatch `release-complete.yml` only after that report passes, using the exact Linux manifest digest and backlog URL. Require native macOS/Windows checks, package extraction, image startup/scans, chart tests and complete certification to pass. Complete any desktop Console/client checks applicable to the release against these same archives. Record the completion run and final manifest SHA-256 before entering the parent confirmation gate. Partial Linux acceptance does not authorize final publication.
