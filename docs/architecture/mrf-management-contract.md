# MRF management and integrity evidence

**Use this when:** changing cluster MRF observations, responsibility actions, or the relationship between legacy recovery and integrity audit evidence.

**Source of truth:** [MRF management handlers](../../rustfs/src/admin/handlers/heal/mrf_management.rs), [node operations](../../rustfs/src/storage/rpc/node_service/mrf.rs), [query admission and samples](../../crates/heal/src/heal/mrf_queue/management.rs), [responsibility ownership](../../crates/heal/src/heal/mrf_queue/partial_write.rs), and [integrity job execution](../../crates/ecstore/src/services/integrity/runner.rs).

## Separate authorities

Attempt execution, durable repair responsibility, integrity evidence, and operator disposition are separate facts. An idle attempt queue, a completed unverified Heal, an empty observation, and an accepted risk never establish that original object content was verified. Only the storage-owned responsibility lifecycle may retire its durable obligation under the existing identity and verification rules.

An integrity audit verifies the explicitly selected object version against eligible pre-existing SHA256 evidence. It does not clear MRF records or backfill historical proof. A protected migration creates a separately identified destination from the verified bytes and rereads that destination; the original object and its responsibilities remain intact. Protected-write activation retains its independent fleet and rollback qualification gates.

## Cluster observation contract

The authenticated `/rustfs/admin/v4/heal/mrf/cluster` family requires `admin:Heal` for summary, listing, single-responsibility lookup, and actions. The older node-local routes remain supported. Route selection resolves a node ID from the current configured topology; clients cannot supply an arbitrary peer URL or an actor identity.

Each response carries the topology fingerprint, observation start/end times, expected node count, individual node outcomes, `pageComplete`, and an optional `nextCursor`. Summary replies include the consumer's `sampledAtMs`. These are independently timed observations, not a frozen cluster snapshot. `pageComplete` requires at least one observed node and success for every node in that page; it does not mean every configured node has been observed. Consumers must follow `nextCursor` and retain partial failures.

Summary and responsibility cursors have distinct kinds and bind the topology fingerprint, node ID, node index, and optional responsibility progress. A topology change invalidates continuation and requires a new observation. Summary pages never carry responsibility progress. Timed-out nodes have been attempted; nodes that could not start before the deadline remain resumable. An empty or partial response must not become a zero-backlog or cluster-health assertion.

The legacy list contains parked responsibilities. A source-bound `active` responsibility is available through the single-ID lookup even when absent from that list. An absent lookup means only that the current process did not observe the ID; it is not a retirement receipt. Responsibility IDs and source bucket incarnations take precedence over matching bucket/object names.

## Bounded work and failure behavior

Summary reads copy the latest consumer-published sample without scanning responsibilities or sending a lifecycle command. List and ID queries share a small fixed admission budget and use non-waiting queue admission. Saturation returns `busy`; a stopped controller returns `unavailable`. Cancelling a caller keeps its permit attached to the queued command until that command is dequeued, and the consumer skips cancelled queries. Read-only commands do not republish metrics or flush checkpoints.

List selection retains only the bounded page, but currently scans the in-memory responsibility collection. ID lookup is also a scan. Admission limits bound concurrent work, not the cost of an individual scan. Local query timing baselines do not substitute for deployment latency or resource acceptance.

An unreadable checkpoint remains a visible error. Summary may report cached counts with `checkpointReadable=false`; that node has `state="error"` while retaining its summary reply, and `pageComplete` is false. Clients must preserve the summary details without treating those counts as complete. List, lookup, and actions retain the existing unreadable-owner refusal. Restart and checkpoint-recovery behavior is defined in the [MRF runbook](../operations/mrf-responsibilities.md).

## Peer and action boundaries

MRF peer RPC uses the authenticated canonical mutation envelope even for reads. The request binds protocol version, topology fingerprint, target node, operation bytes, and a fresh challenge. Response authentication binds the entire request and response body. The receiving node validates body size, authentication, protocol, current topology, and local-node identity before executing the operation. Older peers produce `unsupported`, never an empty successful result.

The coordinator additionally validates response kind, contract version, requested responsibility ID, list ordering/limits/cursor, and the action-specific state. Successful refresh, recheck, and risk acceptance all require `dataVerified=false` and `durableResponsibilityRetained=true`. Error codes remain structured and do not expose internal storage error strings.

The forwarding path does not retry ambiguous mutations. A timeout may occur after the owning node has durably committed a disposition. Reobserve the same node/responsibility before deciding whether another action is needed. Risk acceptance retains the operator, reason, reference, and stable request ID; it is an audited acknowledgement of unverified risk, not data verification.

## Audit and migration I/O

Audit streams source bytes through bounded buffering and SHA256 verification without opening a temporary payload file. Durable job checkpoints still require writable storage. The same size, source-identity, pause/cancel, timeout, and checksum checks remain in force.

Migration requires a temporary file because the exact bytes verified from the source must be the bytes submitted for create-only destination publication. It retains destination content reread and operation-identity recovery checks. Removing audit staging must not remove migration staging, permit a source change, or turn a failed digest into a successful job result.
