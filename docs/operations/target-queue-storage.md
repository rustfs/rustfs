# Target queue storage

## Queue directory location

A target's `queue_dir` is stored in the cluster-wide target configuration. The built-in defaults `/opt/rustfs/events` and `/opt/rustfs/audit` are logical defaults: each node resolves them to a local directory when it validates or opens a queue. The stock notify and audit default tables both persist `/opt/rustfs/events`; `/opt/rustfs/audit` applies only to an audit target whose configuration has no `queue_dir` key. Any other `queue_dir`, including an explicit operator path, is used as written and must be absolute.

| Node setting | `/opt/rustfs/events` resolves to | `/opt/rustfs/audit` resolves to |
|---|---|---|
| `RUSTFS_TARGET_STORE_DIR=<root>` (any platform) | `<root>/events` | `<root>/audit` |
| Windows, variable unset | `%ProgramData%\RustFS\events` | `%ProgramData%\RustFS\audit` |
| Linux/macOS, variable unset | `/opt/rustfs/events` | `/opt/rustfs/audit` |

`RUSTFS_TARGET_STORE_DIR` must be an absolute path; a relative value makes target validation fail with the queue-directory error. The queue store creates missing directories when it opens, so the resolved directory only needs to be creatable by the RustFS process. An admin request that names a default path explicitly is checked at its resolved location and, as for any explicit `queue_dir`, that directory must already exist.

The official container image links `/opt/rustfs/events` and `/opt/rustfs/audit` into `/data/.rustfs`, so its queues survive container recreation without this variable. Windows previously rejected targets that kept the default because the Unix literal is not an absolute Windows path; those targets now validate and queue under `%ProgramData%\RustFS`.

Setting or changing `RUSTFS_TARGET_STORE_DIR` on an existing node moves where its default queues are opened. Entries still pending in the previous directory are not migrated: let them drain first, or move the directory contents while RustFS is stopped. Rolling back to a version without this variable returns default queues to the previous location.

## Queue size limits

Disk queues for notification and audit targets enforce both their configured entry limit and a shared byte limit for each queue directory. `RUSTFS_TARGET_STORE_MAX_BYTES` sets that byte limit; the default is 1 GiB. The value must be a positive integer in bytes. Missing, zero, invalid, and overflowing values use the default.

The byte budget counts persisted live entries, failed entries, and reserved temporary writes. Compressed entries count their compressed file size. Payload fields are preserved. Concurrent clones of the same queue share reservations, and opening the queue reconstructs usage from disk. A queue directory must have a single owning process, as with the existing entry limit.

A write that cannot reserve space returns `StoreError::LimitExceeded`. Moving a live event to the failed store also needs space for its new representation before deleting the live entry; if that reservation fails, the live entry remains available for retry. Failed entries retain the existing count and expiry policies. Removal and expiry reclaim bytes. An I/O failure whose temporary file cannot be removed conservatively retains its reservation until cleanup or reopening succeeds.

Upgrading or reducing the limit preserves existing entries, including queues already above the limit. New writes remain rejected until delivery, expiry, or operator cleanup frees enough space. Reopening does not rewrite event payloads or change their filename format. Rolling back preserves readability but removes the byte limit on an older writer.
