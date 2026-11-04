# Target queue storage limits

Disk queues for notification and audit targets enforce both their configured entry limit and a shared byte limit for each queue directory. `RUSTFS_TARGET_STORE_MAX_BYTES` sets that byte limit; the default is 1 GiB. The value must be a positive integer in bytes. Missing, zero, invalid, and overflowing values use the default.

The byte budget counts persisted live entries, failed entries, and reserved temporary writes. Compressed entries count their compressed file size. Payload fields are preserved. Concurrent clones of the same queue share reservations, and opening the queue reconstructs usage from disk. A queue directory must have a single owning process, as with the existing entry limit.

A write that cannot reserve space returns `StoreError::LimitExceeded`. Moving a live event to the failed store also needs space for its new representation before deleting the live entry; if that reservation fails, the live entry remains available for retry. Failed entries retain the existing count and expiry policies. Removal and expiry reclaim bytes. An I/O failure whose temporary file cannot be removed conservatively retains its reservation until cleanup or reopening succeeds.

Upgrading or reducing the limit preserves existing entries, including queues already above the limit. New writes remain rejected until delivery, expiry, or operator cleanup frees enough space. Reopening does not rewrite event payloads or change their filename format. Rolling back preserves readability but removes the byte limit on an older writer.
