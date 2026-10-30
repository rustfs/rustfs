# Preview Artifact Acceptance

Read after Phase 3 succeeds. Complete every check below before the parent skill's manual confirmation gate. These checks cover the downloaded artifact, embedded Console, and latest rc client.

## Phase 4 — Run the artifact locally, verify the console

Use a unique task-owned scratch directory. Select the versioned archive matching the host OS, architecture, and runtime, download its checksum file, and verify the archive before extraction. Do not substitute a different build when a matching artifact is unavailable; report that acceptance gap. The shell example below assumes the verified executable is at `RUSTFS_BINARY` (`rustfs.exe` on Windows), `SCRATCH` is the task directory, and `S3_PORT` and `CONSOLE_PORT` are distinct available ports. Use equivalent native commands when the shell differs.

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

- `"$RUSTFS_BINARY" --version` reports the preview tag name and the short SHA of `PREVIEW_HASH`. Reporting `<target>` without the `-preview.N` suffix means the build did not embed the tag — treat as FAIL and investigate before proceeding.
- Wait with a bounded readiness deadline while checking the recorded server process remains alive; `curl --noproxy '*' -fsS "$S3_ENDPOINT/health/ready"` must return ready from this process's listener.
- Startup log shows the embedded console being served (this was the regression that `fix(release): require embedded console assets` guards).
- Open `CONSOLE_ENDPOINT` in the browser and log in with the task credentials. The dashboard must render without JS console errors; create a bucket, upload a file, download it back (byte-identical), and delete the object and bucket. Keep the server running for Phase 5.

## Phase 5 — Validate with the latest rc client

`rc` is the RustFS CLI client from <https://github.com/rustfs/cli>.

- Resolve the latest release with `gh api repos/rustfs/cli/releases/latest --jq .tag_name`; verify the local client's version or download and verify a host-compatible client into the task directory. Set `RC_BIN` to that executable and record its version/digest. Avoid changing the user's installed client for this check.
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
