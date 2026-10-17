# Bucket heal recovery and bucket recreation

**Use this when:** diagnosing incomplete administrator heal coverage, restoring a missing bucket directory, or replaying a bucket heal after restart.

Explicit administrator bucket heals bind the bucket name to its authoritative,
non-nil incarnation UUID at admission. The same identity follows the task through
queueing, retries, shutdown persistence, and startup replay. Deleting a bucket and
creating another with the same name does not transfer the old task to the new bucket.

## Incomplete bucket discovery

An administrator heal of the cluster or a selected pool/set enumerates bucket candidates from every selected disk, including directories below the ordinary namespace write quorum. Every selected disk must answer before discovery is complete. A pool/set task checks only its selected set; unavailable unrelated pools do not invalidate that scope. Candidate discovery grants no permission to recreate a bucket. Ordinary S3 and scanner listings retain their quorum rules.

Incomplete discovery uses the existing bounded task retry budget. Once exhausted, the canonical outcome is `execution.state=aborted`, `execution.reason=untraversable`, and `coverage=partial`; the legacy summary is `stopped`. Object counters can remain zero because traversal never started. A genuinely empty, completely enumerated namespace still finishes successfully. Dry runs use the same coverage checks.

Mutating cluster and pool/set tasks persist their resolved bucket scope only after complete discovery. Resumed checkpoints keep their original frozen scope, including a valid empty scope. A cluster task pins every bucket's incarnation before its first mutation and checkpoints its successful contiguous prefix. Checkpoints are batched into at most 32 intervals per traversal to bound inventory rewriting; an interrupted interval is replayed under the same generations. A fresh task is required to include buckets created after discovery. Cluster dry runs retain their observation-only behavior without requiring bucket incarnation metadata.

## Restoring a missing bucket directory

For a bucket that is still intended to exist but has lost its directory on one drive:

1. Check that the peer drives and authoritative bucket metadata remain available.
2. Start an authenticated administrator heal with `recursive=true`. For the v3 API, use `POST /rustfs/admin/v3/heal/` for the cluster or `POST /rustfs/admin/v3/heal/<bucket>` for one bucket. A deep scan (`scanMode=2`) additionally checks data integrity. Retain the returned `clientToken` and poll the same endpoint with that query parameter.
3. Inspect both execution and coverage in the terminal outcome. A readable object on a healthy peer alone does not establish that the missing copy was restored.
4. Verify recovered object contents and lengths against known hashes. In an isolated recovery test, stop the original healthy source and read through the repaired target to demonstrate independent recovery. Do not take a production source offline merely to inspect a heal result.

Default administrator bucket healing can reconstruct missing directories when every selected disk is reachable, each selected set retains a bucket directory, and persisted metadata proves the current generation was committed and has not been retired. The lifecycle fence remains held through reconstruction. Missing, corrupt, uncommitted, retired, or mismatched authority fails closed. Dry runs leave directories unchanged. `recreate=true` remains an explicit recovery option; it is not permission to revive a bucket deleted through S3. Bucket incarnation checks reject stale tasks after deletion and same-name recreation.

Legacy objects without independent storage commitments can be physically repaired while their canonical dispositions remain `unknown`. Do not infer a verified repair count from progress counters; see [heal terminal reports](../architecture/heal-terminal-reports.md).

## Recovery records and stale tokens

Pending records use schema 4 in `.rustfs.sys/root-heal-<task-id>.json` on the existing
coordinator disk. A bucket record includes `bucket_incarnation_id`. Options, task ID,
and the remaining retry budget retain their existing recovery semantics. A cluster
record additionally carries its frozen bucket generations and completed prefix.
Cluster records with this scope have a 16 MiB limit; other intents and legacy
terminal markers retain the 64 KiB limit. Initial cluster snapshots reserve 1 MiB
for later progress and bounded object outcomes. Oversized inventories fail before repair.

A missing bucket, changed incarnation, or legacy bucket record without a usable
identity becomes a durable `Failed` terminal result containing
`stale_bucket_incarnation`. The original token remains queryable under the existing
terminal retention policy. Recovery never fills a legacy task with the identity of
the bucket currently using its name. Legacy schema 1–3 cluster intents lack a frozen
generation inventory and require a fresh task after upgrade. Other supported legacy
intents and terminal reports remain readable. Unsupported, corrupt, and oversized
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
understand newer pending record schemas. Finish or cancel pending administrator tasks
before downgrading a coordinator; do not rewrite the schema to force replay.

The lifecycle fence remains mandatory even when `nolock` disables the optional
object namespace lock. Deployments with namespace locking disabled cannot perform
incarnation-bound bucket healing.

The generation-bound repair contract covers explicit administrator **bucket** tasks
and the frozen buckets of mutating administrator cluster tasks. Prefix, object,
scanner, and replacement tasks retain their existing admission semantics.
It does not introduce a new object-generation protocol or claim rejection of every
already-dispatched native syscall after distributed lock loss; see the existing
[heal concurrency model](../architecture/heal-concurrency-model.md).
