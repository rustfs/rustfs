# SFTP Operations Guide

The guide covers enabling and operating the SFTP server in RustFS.
It is written for operators who need to expose buckets over SFTP, manage host
keys across platforms, size the server for large transfers, and diagnose
session cleanup behaviour.

## Enabling SFTP: Recommended Configuration

Two settings are required: the enable flag and the host-key directory. The
listen address has a default of `0.0.0.0:2222` and setting it explicitly is
recommended practice.

```bash
RUSTFS_SFTP_ENABLE=true
RUSTFS_SFTP_ADDRESS=0.0.0.0:2222
RUSTFS_SFTP_HOST_KEY_DIR=/etc/rustfs/sftp-keys
```

- `RUSTFS_SFTP_ENABLE` starts the SFTP listener at server startup. Off by
  default.
- `RUSTFS_SFTP_ADDRESS` is the listen address and port. `0.0.0.0` accepts
  connections on every interface. Port `2222` avoids the privileged port 22.
- `RUSTFS_SFTP_HOST_KEY_DIR` is the directory the SSH host keys are loaded
  from. No default, and startup fails when SFTP is enabled without it.

Generate a host key before first start. On Unix, `ssh-keygen` also writes a
world-readable `.pub` file into the directory, and the server requires every
file it considers in the host-key directory (regular, non-empty, at most
1 MiB) to be owner-only, so restrict or remove it:

```bash
ssh-keygen -t ed25519 -f /etc/rustfs/sftp-keys/ssh_host_ed25519_key -N ""
chmod 600 /etc/rustfs/sftp-keys/ssh_host_ed25519_key.pub
```

The private key itself is already written with owner-only permissions. The
server does not read the `.pub` file, so it can be removed instead. Keys must
be unencrypted (no passphrase).

On success the server logs `SFTP server listening` with the bound address. A
port of `0` in `RUSTFS_SFTP_ADDRESS` is resolved to a free port at startup
and the resolved port appears in that log line.

Every other setting has a tested default and should be left alone unless a
section below gives a concrete reason to change it.

## Path Model

The SFTP root directory is the account's bucket list. The first path
component names the bucket and the remainder is the object key:

```text
/reports/2026/q1.pdf

bucket:     reports
object key: 2026/q1.pdf
```

Files cannot be created at the root level. Creating or removing a top-level
directory creates or removes a bucket. The root listing shows at most 10000
buckets and logs a warning when truncated.

## Host Keys

`RUSTFS_SFTP_HOST_KEY_DIR` must name an existing directory containing at
least one decodable private key. Startup fails otherwise. There is no
generated fallback key.

- Any private key that russh can decode is accepted. Ed25519, ECDSA, and RSA
  are the expected formats. Passphrase-protected keys cannot be decoded and
  do not count. Keys are offered to clients in the order Ed25519, ECDSA,
  RSA, then anything else. Multiple keys of one algorithm all load, so during
  key rotation clients can be offered the new key as soon as it is added,
  not when the old one is removed.
- Empty files and files larger than 1 MiB are skipped entirely.
- On Unix, every other regular file in the directory must have no group or
  other permission bits set (owner-only, for example mode `0600`, `0400`, or
  `0700`). The check covers non-key files too: a world-readable README or
  `.pub` file fails startup with `host key file has insecure permissions`.
  Keep only owner-only files in the directory.
- On Windows there is no permission-bit check. The server logs a one-time
  warning at startup whose alertable first sentence reads exactly `SFTP host
  key file permission enforcement is not active on Windows`. Restrict the
  NTFS ACL on the host-key directory to the running rustfs service account,
  `NT AUTHORITY\SYSTEM`, and `BUILTIN\Administrators`. Default ProgramData
  inheritance grants `BUILTIN\Users` read access. Remove that grant on the
  host-key directory.
- Targets that are neither Unix nor Windows do not support SFTP and fail
  startup.

Host keys can be hot-reloaded without a restart by setting
`RUSTFS_SFTP_HOST_KEY_RELOAD_ENABLE=true`. The directory is rescanned every
`RUSTFS_SFTP_HOST_KEY_RELOAD_INTERVAL` seconds (default 30, silently raised
to 5 if set lower). A failed rescan, including a permission violation that
would be fatal at startup, keeps the previous keys and logs `SFTP host key
reload failed; keeping previous keys`. Reloaded keys affect new connections
only.

## Configuration Reference

Changing values beyond the recommended section is rarely necessary. The
defaults are the configuration the test suites and stress runs exercise.
Tuning values interact (for example part size multiplies against the handle
cap in worst-case memory), so change them deliberately and one at a time.

Invalid values fall into four classes:

1. Fatal at startup. The server refuses to start and names the problem.
2. Warn and use the default. The server starts and logs a warning naming
   the variable, the rejected value, and the bounds.
3. Silently clamped. The reload interval is the one variable raised to its
   floor without a warning.
4. Silent fallback. Non-numeric text in any numeric variable behaves as if
   the variable were unset. Invalid boolean text logs a one-time warning
   and uses the default.

| Variable | Default | Valid values | On invalid |
| -------- | ------- | ------------ | ---------- |
| `RUSTFS_SFTP_ENABLE` | `false` | boolean | warn, default |
| `RUSTFS_SFTP_ADDRESS` | `0.0.0.0:2222` | see binding note below | fatal |
| `RUSTFS_SFTP_HOST_KEY_DIR` | none | existing directory, required | fatal |
| `RUSTFS_SFTP_IDLE_TIMEOUT` | `600` | seconds, greater than zero | fatal |
| `RUSTFS_SFTP_PART_SIZE` | `16777216` | bytes, 5 MiB to 5 GiB | fatal |
| `RUSTFS_SFTP_READ_ONLY` | `false` | boolean | warn, default |
| `RUSTFS_SFTP_BANNER` | `SSH-2.0-RustFS` | must start with `SSH-2.0-` | fatal |
| `RUSTFS_SFTP_HANDLES_PER_SESSION` | `64` | `8` to `1024` | warn, default |
| `RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS` | `60` | `5` to `600` | warn, default |
| `RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES` | `4194304` | `0` disables, else 256 KiB to 64 MiB | warn, default |
| `RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES` | `268435456` | at least 16 MiB | warn, default |
| `RUSTFS_SFTP_HOST_KEY_RELOAD_ENABLE` | `false` | boolean | warn, default |
| `RUSTFS_SFTP_HOST_KEY_RELOAD_INTERVAL` | `30` | seconds, minimum 5 | clamped to 5 |

Notes on individual variables:

- `RUSTFS_SFTP_ADDRESS`: the host part must be a wildcard (`0.0.0.0` or
  `[::]`) or an address or hostname assigned to the host, otherwise startup
  fails. Use `[::]:2222` to listen on IPv6, which on Linux usually accepts
  IPv4 as well via dual-stack. Port `0` auto-assigns a free port.
- `RUSTFS_SFTP_BANNER` is the SSH protocol identification string sent on
  connect, not a free-text login banner. Values that do not start with
  `SSH-2.0-` fail startup.
- `RUSTFS_SFTP_IDLE_TIMEOUT` cannot be set to `0` to disable idle
  disconnects. See the sessions section for what closes idle sessions.
- `RUSTFS_SFTP_PART_SIZE` bounds follow S3 multipart limits. See the large
  files section before changing it.
- `RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES` set to exactly `0` disables read
  caching, which turns every client read request into one backend call.
- Worst-case buffered write memory per session is the handle cap times the
  part size: 64 handles at 16 MiB is 1 GiB per session. The server imposes
  no limit on concurrent sessions, so total worst case is that figure
  times however many clients connect. Only the read cache has a global
  cap. Enforce connection limits externally if that matters.

## Sessions and Cleanup

Several mechanisms close sessions:

- The server sends an SSH keepalive every 15 seconds and closes the
  connection after 3 consecutive unanswered keepalives, about 60 seconds
  after a client stops responding. Not configurable. The keepalive check
  cleans up clients that vanish without closing TCP.
- `RUSTFS_SFTP_IDLE_TIMEOUT` (default 600 seconds) sets the SSH inactivity
  timeout, but keepalive replies count as SSH traffic and reset it, so a
  client that answers keepalives is never disconnected by it.
- The session watchdog closes sessions that are silent at the SFTP request
  layer for 30 minutes, on every platform. Keepalives do not count as
  SFTP-layer activity. The watchdog is the mechanism that ends a healthy
  but idle session. On Linux the close logs `wedge watchdog cancelling
  session` with reason `fallback_silence`. On other platforms it logs
  `fallback watchdog cancelling session`.
- On Linux the watchdog additionally reads kernel TCP state for fast
  cleanup of wedged sessions: a socket sitting in `CLOSE_WAIT` across two
  consecutive 15-second checks while the SFTP layer has been silent for 30
  seconds is cancelled, typically 45 to 60 seconds after the client
  vanished. Two consecutive failed TCP-state probes are treated the same
  way, so a container that blocks `/proc/net/tcp` can see sessions
  cancelled on that schedule. The `reason` field of the log line names the
  trigger.
- If duplicating the connection socket fails at accept time (rare, usually
  file-descriptor exhaustion) the session runs with no watchdog at all and
  only the keepalive and idle mechanisms apply. Log line: `wedge watchdog:
  dup_socket failed`.
- Server shutdown cancels every live session immediately. Clients are
  disconnected mid-transfer. The server then waits up to 30 seconds for
  session cleanup, including aborting in-flight multipart uploads, before
  remaining tasks are dropped.

A session ended by any cancellation path logs `SFTP session cancelled
(watchdog or server shutdown)`.

## Authentication and Authorization

Authentication is by password only, verified against RustFS IAM users.
Public-key authentication is rejected. Anonymous access is not available.
Failed logins are rejected without delay and there is no lockout, so apply
rate limiting externally when the listener is exposed to untrusted
networks. Accepted logins log `SFTP auth accepted` at info level, rejections
log `SFTP auth rejected` at warn level.

Every SFTP operation is authorized against IAM policy before it reaches
storage. Policy condition keys (for example `aws:SourceIp`) are not
evaluated on the SFTP path. Only unconditional Allow and Deny statements
take effect.

The S3 actions a user needs:

| SFTP activity | Required S3 actions |
| ------------- | ------------------- |
| List the root directory (the bucket list) | `s3:ListAllMyBuckets` |
| List inside a bucket | `s3:ListBucket` |
| Download a file | `s3:GetObject` |
| Upload a file | `s3:PutObject` |
| Stat a file | `s3:GetObject` |
| Stat a bucket | `s3:ListBucket` |
| Delete a file | `s3:DeleteObject` |
| Rename (implemented as copy then delete) | `s3:GetObject` on the source, `s3:PutObject` on the destination, `s3:DeleteObject` on the source |
| Create or remove a top-level directory | `s3:CreateBucket` or `s3:DeleteBucket`, removal also needs `s3:ListBucket` for the emptiness check |
| Upload cleanup on disconnect | `s3:AbortMultipartUpload` (see the large files section) |

Upload-only users also need `s3:GetObject`, because SFTP clients stat files
as part of normal transfers.

With `RUSTFS_SFTP_READ_ONLY=true` the server rejects all mutating packets
at the protocol layer: opening a file for write, `WRITE`, `REMOVE`,
`MKDIR`, `RMDIR`, `RENAME`, `SETSTAT`, and `FSETSTAT`.

## Client Compatibility

The server speaks SFTP version 3 and maps onto object storage. Differences
from a filesystem-backed SFTP server:

- Uploads must be a single sequential stream from offset zero. Transfer
  resume, append mode, in-place edits (read-write opens), and segmented or
  multi-connection uploads of one file are rejected. Configure clients for
  whole-file, single-connection transfers.
- In normal (read-write) mode, `SETSTAT` and `FSETSTAT` are accepted and
  ignored: chmod, timestamp preservation, and ownership changes silently
  have no effect, and listed permissions are fixed server-generated values.
- Symlink operations (`SYMLINK`, `READLINK`) are not supported and return
  an unsupported-operation error. Object storage has no symlink equivalent.
- Renaming copies the object server-side and then deletes the source, so
  large-file renames are slow and not atomic: a failure after the copy can
  leave the file at both paths. Renaming a top-level directory (a bucket)
  is not supported.

## Large Files and Multipart Uploads

An upload smaller than `RUSTFS_SFTP_PART_SIZE` bytes is buffered in memory
and written with a single `PutObject` when the file is closed. At part size
or larger the handle switches to S3 multipart, flushing a part each time a
full part accumulates.

The server enforces the S3 limit of 10000 parts per upload, so the largest
single upload is part size times 10000: 156.25 GiB at the default 16 MiB
part size. Raise `RUSTFS_SFTP_PART_SIZE` until that product covers the
largest expected file. A larger part size raises per-session memory. A
write that would exceed the cap is rejected with log line `SFTP write would
exceed the S3 multipart parts limit`. The same cap can reject at file
close, logging `SFTP close rejected: trailing part would exceed S3
multipart parts limit`.

When a session ends mid-upload the server aborts the in-flight multipart
upload. Four situations leave an orphaned upload behind that the session
itself cannot clean up:

- the user lacks `s3:AbortMultipartUpload`,
- the abort itself fails or times out,
- a burst of simultaneous disconnects exhausts the global abort task pool
  (sized at twice the CPU parallelism, between 8 and 128),
- the server process is killed outright.

Orphaned uploads are not permanent. RustFS runs a background cleanup that
aborts stale incomplete multipart uploads (by default, uploads older than
24 hours, checked every 6 hours). An `AbortIncompleteMultipartUpload`
bucket lifecycle rule reclaims them as well and gives per-bucket control of
the window. The log lines when a session-drop abort is skipped or fails:

```text
skipped abort of orphaned multipart upload on session drop, principal lacks s3:AbortMultipartUpload, bucket lifecycle rules must reclaim parts
abort permit pool exhausted on session drop, bucket lifecycle rule must reclaim parts
failed to abort orphaned multipart upload
Drop abort of orphaned multipart upload timed out; bucket lifecycle rule must reclaim parts
```

Abort failures at file close log with different wording and are retried at
session drop, where the lines above appear.

## Log Lines Worth Alerting On

Every line below logs at warn level except `SFTP server listening` and
`SFTP auth accepted`, which log at info. The server's default log level is
error, so none of them are visible until the log level is raised to info
(for example `RUSTFS_OBS_LOGGER_LEVEL=info`, or warn to capture alerts
only).

| Message | Meaning |
| ------- | ------- |
| `SFTP server listening` | Startup complete, address bound |
| `SFTP auth rejected` | Failed login, no built-in lockout exists |
| `SFTP host key file permission enforcement is not active on Windows` | Expected once per start on Windows, verify the ACL guidance once |
| `host key file has insecure permissions` | Fatal at startup on Unix, fix file modes. During hot reload the same text appears inside the reload-failed warning and previous keys stay active |
| `SFTP host key reload failed; keeping previous keys` | Hot reload rescan failed, service unaffected, investigate the directory |
| `wedge watchdog cancelling session` | Linux only. `reason` field: `tcp_state_close_wait_confirmed` or `probe_failed_confirmed` mean a wedged or unprobeable socket, `fallback_silence` means routine 30-minute idle cleanup |
| `fallback watchdog cancelling session` | Non-Linux platforms: session silent at the SFTP layer for 30 minutes |
| `wedge watchdog: dup_socket failed` | Rare, that session runs without any watchdog |
| `SFTP session cancelled (watchdog or server shutdown)` | Session ended by cancellation rather than client close |
| `SFTP write would exceed the S3 multipart parts limit` | Client hit the per-upload size cap, raise the part size if legitimate. Also check the close-time variant below |
| `SFTP close rejected: trailing part would exceed S3 multipart parts limit` | Same cap hit at file close |
| `root READDIR truncated` | A principal can see more than 10000 buckets, listing was cut off |
| `skipped abort of orphaned multipart upload` | Orphaned parts on storage until background cleanup or lifecycle rule reclaims them |
| `abort permit pool exhausted on session drop` | Mass-disconnect burst, orphaned parts on storage until reclaimed |
| `RUSTFS_SFTP_` prefix in a warning | A tuning variable was rejected and its default applied |
