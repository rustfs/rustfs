# Bucket heal recovery and bucket recreation

Explicit administrator bucket heals bind the bucket name to its authoritative,
non-nil incarnation UUID at admission. The same identity follows the task through
queueing, retries, shutdown persistence, and startup replay. Deleting a bucket and
creating another with the same name does not transfer the old task to the new bucket.

## Recovery records and stale tokens

Pending records use schema 3 in `.rustfs.sys/root-heal-<task-id>.json` on the existing
coordinator disk. A bucket record includes `bucket_incarnation_id`. Options, task ID,
and the remaining retry budget retain their existing recovery semantics.

A missing bucket, changed incarnation, or legacy bucket record without a usable
identity becomes a durable `Failed` terminal result containing
`stale_bucket_incarnation`. The original token remains queryable under the existing
terminal retention policy. Recovery never fills a legacy task with the identity of
the bucket currently using its name. Existing schema 1 cluster records and schema 2
non-bucket records keep their recovery behavior. Unsupported, corrupt, and oversized
records remain intact and receive a sibling
`quarantined-root-heal-{intent|terminal}-<task-id>.json` marker. The marker fences the
original task ID while allowing an independent compensation heal with a fresh ID.
Do not edit either file: a missing source, malformed marker, changed source digest,
duplicate owner, or transient metadata failure continues to defer recovery.

If a task is stale, submit a new bucket heal for the current bucket and use the new
token. Do not edit recovery files to replace the incarnation. A retained terminal
record takes precedence over an old pending copy of the same task.

## Execution and upgrades

Each bucket metadata or object repair acquires the existing bucket lifecycle read
fence and validates the admitted identity. The guard remains held through the
storage operation and its mutation tail. Cancellation stops waiting for the result;
an already admitted physical mutation retains its owner until it drains. Bucket
deletion and recreation must use the normal lifecycle write path.

Remote bucket repair, shard rename, metadata regeneration, version removal, and
path cleanup use dedicated `AtIncarnation` RPC methods. Their canonical body digest
binds the incarnation. A receiver validates it against its own storage instance;
missing or nil identities and unsigned bodies are rejected. Older nodes return
`Unimplemented`; senders do not retry through an unfenced method. Upgrade all
participating nodes before expecting these bucket heals to complete. Unscoped
RPC bodies retain their prior canonical encoding. Older coordinators do not
understand schema 3 pending records. Finish or cancel pending administrator tasks
before downgrading a coordinator; do not rewrite the schema to force replay.

The lifecycle fence remains mandatory even when `nolock` disables the optional
object namespace lock. Deployments with namespace locking disabled cannot perform
incarnation-bound bucket healing.

This contract covers explicit administrator **bucket** tasks. Cluster, prefix,
object, scanner, and replacement tasks retain their existing admission semantics.
It does not introduce a new object-generation protocol or claim rejection of every
already-dispatched native syscall after distributed lock loss; see the existing
[heal concurrency model](../architecture/heal-concurrency-model.md).
