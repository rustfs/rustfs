# Native source reader

**Use this when:** exposing a live `NativeLogWriter` to a local publisher without reopening its redb database or granting event mutation authority.

## Ownership and API

`crates/capture-authority/src/source_reader.rs` owns the public native read protocol, server and client. Within an IO-enabled Tokio runtime, the actual authority owner calls `SourceReaderServer::bind(path, writer.clone())`, keeps the returned `Shutdown`, and spawns `server.serve()`. `Shutdown::shutdown()` stops new admissions; await the serve task to drain accepted reads and report socket cleanup errors. `NativeLogWriter::durable_identity()` reads and validates the identity persisted in META.

The supplied writer shares its existing Store and blocking worker boundary. The service reads durable identity and `created_events_page` under the same Store lock. It never opens another live database, rewrites an event, records an ACK, or advances a durable source cursor. Event delivery position does not remove EVENTS, including after checkpoint publication. All services bound from clones of one writer share the same admission budget.

`SourceReaderClient::new(path)` validates the private socket location. Its only operation is `page(expected_cluster, cursor, limit)`. Returned `ReadPageResponse` values contain the durable `NodeIdentity`, complete native `SourceEvent` records and `EventPage::next`. They describe local committed events and are not Controller receipts or consumer ACKs. The consumer retains its previous cursor until its independent durable acceptance policy permits advancement.

## Trust and transport

Support is limited to Linux and macOS. Other targets do not expose this module. The immediate directory must belong to the effective UID and forbid every group/other permission; a symlink directory is rejected. The service creates a socket with mode 0600 and refuses every existing path, including stale sockets and symlinks. The caller owns the pathname lifecycle exclusively while the service or its cleanup is active; do not externally unlink or rebind that path during this interval. Operators must investigate a stale path themselves; the service never takes it over.

Both server and client check connected peer credentials against the current effective UID. The client also validates directory/socket ownership and permissions before connecting, then checks their device/inode identity after connection. Effective UID changes while the service is running are unsupported. This is a shared same-UID trust domain: it does not isolate mutually hostile processes running as that same user. Do not put the socket in a directory exposed to another user or use it as a network service.

Frames contain a four-byte big-endian unsigned JSON byte length followed by compact UTF-8 JSON. `ReadPage`, `ReadPageReply` and all nested native types use strict field decoding; wire version 1 is required. Request size is at most 4096 bytes, response size at most 8 MiB, and page size at most `MAX_EVENT_PAGE`. The server rejects wrong clusters before reading event pages. Existing stored cursor/event checks remain authoritative. Native storage and checkpoint codecs are unchanged.

At most four connections per writer are admitted, including blocking reads from cancelled services. Request reads and response writes each have a five-second deadline; the client bounds the complete exchange at five seconds. Serialization uses a byte-limited writer. The blocking store worker retains its admission permit through reading and serialization even if a client disconnects or its caller is cancelled. Storage work is drained rather than forcibly interrupted. Graceful shutdown can therefore wait for an existing local storage operation beyond the network deadline. Dropping the serve future cancels network tasks, while already started blocking reads retain admission until they finish. A replacement service on the same writer cannot bypass those permits.

Cleanup checks the original socket inode and private directory before unlinking. Detected replacements are preserved and reported as cleanup failures. This check and unlink are separate filesystem operations, so the caller-exclusive pathname lifecycle is required. Cleanup disarms its ownership before its single attempt; dropping an old guard cannot retry against a socket rebound after that attempt. A cancelled serve task makes best-effort cleanup under those same checks. Socket removal is local lifecycle cleanup, not durable source-state deletion.

## Verification and scope

`cargo test -p rustfs-capture-authority source_reader -- --nocapture` exercises actual redb, committed native apply and Unix sockets owned by the test process. It covers live apply visibility, restart continuation, page boundaries, durable identity/cursor rejection, private and occupied paths, transport bounds, active-read shutdown/cancellation, immediate rebinding after cleanup, and checkpoint replay. The cluster-check mutation must fail `SOURCE_READER_CLUSTER` before a foreign page can be returned.

No production authority owner, RustFS main binary or automatic S3 capture is connected by this library change. Existing blocked/frozen source work stays separate. Rollback removes reader consumers and their local socket configuration; stored events, native log bytes and checkpoints remain compatible.
