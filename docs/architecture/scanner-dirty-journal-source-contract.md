# Scanner dirty-journal metadata source contract

**Use this when:** adding storage-verified provenance to the node-owned scanner dirty-producer journal, its write completion, repair responsibilities, or replay.

**Status:** proposed implementation contract. The authority, mutation tickets and
receipts below are not implemented by the current journal. This document does
not enable a new writer, change a persisted format, migrate data, or authorize
repair. The existing reporting and quota decisions remain in
[scanner usage publication](scanner-usage-publication.md) and
[scanner usage authority](scanner-usage-authority-decision.md).

## Scope and current boundary

The scope is only the node-owned dirty-producer journal currently implemented in
`rustfs/src/storage/scanner_dirty_journal.rs`. Its configured endpoint selects an
owner hash and the owned JSON object. Mutation callbacks coalesce into bounded
state; the worker captures a state/revision under the mutex and releases it
before storage I/O. `confirm_flush` cannot confirm a newer in-memory revision.
Replay validates the stored owner and producer/generation fields. This is an
asynchronous restart hint, not a durability receipt for each S3 mutation.

The endpoint hash is a routing key, not a storage-verified namespace or writer
generation. A successful configuration save is not, by itself, proof that all
physical tail tasks completed. These facts must not be promoted into authority
for the old source-unknown MRF entries associated with internal JSON writes.

This contract introduces no generic internal-metadata authority. It does not
cover usage publication, bucket metadata, bucket-incarnation markers or
`pool.bin`. Pool metadata's transaction owner is not a dirty-journal owner.
Storage primitives may be reused only if they enforce this scope's own fences.

## Required identities

The names below describe storage roles, not exported Rust APIs.

| Identity | Required evidence | Meaning |
|---|---|---|
| Owner key | Domain-separated digest of the configured RPC authority | Selects the owned journal; never proves historical provenance |
| Namespace bootstrap | Immutable committed bytes naming this class, owner key and a non-nil namespace UUID, with an independent native transaction commit ID and digest | Establishes a new namespace only after storage verification; never labels legacy bytes |
| Writer epoch | A positive epoch activated by fenced CAS of the journal's active head | Excludes a replaced writer without treating process identity or restart as proof |
| Mutation ticket | Exact namespace/epoch/bootstrap binding, responsibility and mutation UUIDs, target pool/set/bucket/key, producer/trigger, payload digest and retained payload bytes or an immutable verified payload reference | Retains one new write's responsibility before dispatch |
| Completion receipt | The exact ticket, committed target revision/digest, terminal physical-tail evidence and read-back binding | Can settle only that new responsibility |

Namespace, bootstrap-commit, responsibility and mutation UUIDs must be non-nil.
The native target revision is an explicit observed-absent or observed-existing
CAS token, not an empty/default "current" value. Read failure, unsupported data
and proven absence are distinct; only proven absence permits conditional create.

The namespace UUID is generated only for an explicit new bootstrap transaction
and becomes usable only after that transaction is fully verified. Missing,
corrupt, conflicting, unsupported or unverified bootstrap data means unavailable
authority. It must not become a new current identity through a default value.
Unknown legacy entries retain their original identity, object/scope, status,
enqueue/held times and operator audit.

## Bootstrap and epoch activation

Bootstrap requires a storage-native conditional-create transaction with its own
durable recovery owner. It must retain indeterminate creation work independently
of the namespace being created: bootstrap cannot prove its own success by
decoding its JSON or by borrowing a current endpoint, process or pool owner.

The transaction creates immutable bootstrap bytes, drains native work, validates
the durability result and reads back the exact committed bytes. Only this
storage-verified result may construct the opaque bootstrap binding. Arbitrary
decoded records cannot construct it. Concurrent creation must select the same
committed record or fail closed; it cannot overwrite a competing namespace.

The active checkpoint is the single CAS surface for both the writer epoch and
the journal publication. An epoch transition cannot be a separate unguarded
object update followed by an ordinary PUT. Every physical mutation must carry a
storage-native parent/epoch condition effective at the final commit boundary.
The current generic configuration precondition callback is not assumed to
provide this capability. An implementation lacking the primitive cannot enable
source-bearing writes.

Epoch activation also retains all old in-flight work and debt. Every admitted
write needs a native terminal record; missing or indeterminate records remain
unresolved. A new process, expired lease or current registry observation cannot
stand in for proof that the old physical operations ended.

## Producer, binding and dispatch

Only the actual dirty-journal worker may use this class. Its target must be the
owner-selected journal object in the storage metadata namespace, with the exact
pool/set routing identity checked at admission and commit. The bootstrap object
and unrelated metadata objects are outside the ticket's repair scope.

User metadata, a decoded wire record or an authenticated RPC claim cannot
construct a verified binding. A receiving storage peer must validate the
bootstrap/ticket, target and native fence again before mutation; transport
authentication alone is not this scope's repair authorization.

The new head key is
`scanner/dirty-journal-source/v1/<owner>/checkpoint.bin`, where `<owner>` is the
canonical hash produced by `durable_dirty_usage_owner`. Immutable bootstrap
records live under that owner's distinct `bootstrap/` keyspace. This does not
reuse the shared or owner-only legacy JSON key. Mutation tickets and receipts
belong to the bounded, source-aware responsibility/checkpoint store; it must not
create an unbounded new object per callback or flush.

The producer obtains a verified bootstrap and active epoch outside the journal
state mutex. It captures the exact payload, revision and producer causes under
that mutex, then releases it. The dirty-state/namespace/storage locks must not
be hidden beneath the journal mutex while binding or writing.

Authority I/O belongs to bootstrap and the journal worker. S3 callbacks remain
bounded in-memory coalescing; unavailable authority retains dirty/unverified
state and does not turn this restart hint into an S3 admission veto.

Before dispatch, it durably admits the bounded mutation ticket and retains the
exact input required to retry that mutation after restart. Ticket admission
failure prevents dispatch and leaves the pending state unconfirmed. Digest-only
evidence is insufficient when the exact payload cannot otherwise be recovered.

After namespace locks are acquired, storage rechecks the bootstrap, active epoch,
target scope and parent revision. A changed identity rejects the candidate; it
does not rebind the old ticket to the current identity. The producer/trigger
distinguishes initial dispatch, minority/tail failure, cancellation and native
indeterminate completion. Missing trigger history remains unknown.
The metadata producer is the journal worker; the original object-mutation
producer labels remain payload hints. They do not confer metadata authority.

## Commit, tail and publication

A write-quorum ACK is not a completion receipt. Physical disk tasks retain the
mutation identity and their owner until terminal completion, including when an
async wrapper times out or its caller is cancelled.

Completion must honor the selected durability policy, including its file,
rename, parent and required-ancestor persistence boundaries. A mode that cannot
supply durable evidence cannot produce a durable source receipt. It retains
unverified state; this protocol neither silently forces extra fsync work in that
mode nor weakens the configured durability policy to obtain success.

System-metadata repair remains with this scoped owner. A user-bucket MRF proof
cannot authorize it. `TailDrainedWithOwnerRepair` is an available completion
boundary, not an authority token: a repair-required result must retain the
ticket and retry the exact mutation or remain unresolved. It cannot be converted
to success by a later ordinary read or a successful unrelated mutation.

Publication requires the current epoch/parent fence, the completed native
transaction and a read-back of the exact intended payload. The committed
checkpoint binds the bootstrap, ticket, target revision, payload digest and
native completion result. A torn checkpoint, stale epoch, conflicting bytes or
unknown tail status prevents confirmation and responsibility settlement.

`confirm_flush` additionally compares the captured revision with the current
in-memory revision. Completion of an older snapshot must not acknowledge newer
callbacks. Producer coalescing does not prove that a newer payload supersedes a
different pending ticket; automatic cross-mutation settlement is out of scope.

An empty verified state is a fenced tombstone publication, not an unguarded
physical delete of the active head. It cannot remove another owner's record or
any legacy/shared object. This keeps the epoch and completion binding available
for replay.

## Checkpoint, replay and exact settlement

Source-bearing readers are introduced before a source-bearing writer. They
validate the complete schema/version and byte limits, the immutable bootstrap
and native transaction evidence, the active checkpoint, and every ticket's exact
target and payload. A companion must bind the checkpoint owner, sequence, full
length and digest; absence or a mismatch cannot be read as current authority.

Bootstrap, ticket, checkpoint and receipt persistence retain their independent
storage-transaction recovery owners. Publishing a receipt cannot use the same
responsibility it settles as its sole evidence. A persistence failure leaves
the original responsibility and native recovery work unresolved; it does not
start a recursive chain of self-authorizing source claims.

Replay may resume an exact source-bearing ticket only with the same verified
namespace and mutation evidence. Reuse of an endpoint after restart does not
change the historical source. A new epoch retains old debt until the required
native terminal evidence and exact receipt exist.

Only a durably published receipt matching the complete responsibility identity
may settle it. Matching an object key, current owner, version, payload digest or
successful newer write alone is insufficient. Source-unknown entries, another
scope, another mutation, and legacy records are never automatically accepted,
rechecked, retired or physically cleaned by this protocol.

## Compatibility and bounded resources

The legacy shared JSON and current owner-only JSON remain retained as legacy
sources. They cannot be imported into a new authority namespace or overwritten
to manufacture provenance. A reader-first rollout needs distinct source-bearing
storage and an explicit minimum-reader/capability contract. An unsupported peer
must reject source-bearing admission before mutation, rather than silently
falling back to an owner-only write. Rollback cannot treat unknown fields as
source authority or automatically remove source-bearing material.

These are restrictions on the new protocol. A legacy process may still update
its own old-format key; that activity is not provenance for the new namespace
and must not be mistaken for byte-stable historical evidence.

This protocol does not turn scanner reports or restart hints into quota
admission authority. Existing full-scan, dirty-generation, peer-instance and
movement fences remain required. The existing user-bucket MRF journal and old
unknown responsibilities retain their current semantics.

The current journal's 64 KiB record, entry/scope limits and queue byte limits
remain constraints. Bootstrap bytes, tickets and retained payloads, companions,
checkpoints, native transaction records and receipts are all charged explicitly.
An implementation must not borrow an unrelated bucket's spare budget, omit
fields, evict identities, silently increase limits or accumulate unbounded
immutable objects. Exhaustion retains dirty state and blocks new admission.
No epoch/ticket garbage collection is authorized without a separate exact
ownership and durable-retention proof. This contract authorizes no cleanup of
the original residual data.

## Required conformance cases

These are implementation requirements, not tests claimed to have passed.

| Boundary | Required observation |
|---|---|
| Bootstrap absent, racing create, corrupt bytes or unknown native result | No authority token; retained creation/recovery evidence; legacy bytes unchanged |
| Endpoint reuse, writer replacement, movement or stale parent | Native fence rejects the stale mutation; old in-flight work/debt is not relabelled |
| Admission failure or crash before dispatch | No unrecorded write; exact retained ticket/payload can be read after restart |
| Quorum ACK while a native tail is blocked | No completion receipt, checkpoint settlement or premature owner release |
| Minority/tail failure, caller timeout/cancel or fence loss | Same ticket remains unresolved; no generic user-bucket repair authorization |
| Crash before/after native commit and checkpoint publication | Exact replay outcome or explicit indeterminate state, never a guessed success |
| Callback or dirty acknowledgement concurrent with I/O | Only the captured snapshot can complete; newer state remains pending |
| Partial/torn/misbound companion, payload or receipt | Fail closed without adopting a current namespace or deleting retained evidence |
| Exact success versus same-key/different-source/mutation success | Settle only the exact new ticket; all other and legacy unknown entries remain |
| Empty state, capacity exhaustion and unsupported reader | Fenced tombstone or retained failure; no broad delete, capacity relaxation or implicit downgrade |

The current owner-only journal, its tests and the pool metadata contract are
useful foundations, but none alone satisfies this source contract. Runtime
implementation and conformance acceptance are separate from this design. If the
contract enters a performance comparison, both arms must first use the same
accepted implementation and be fixed and registered again.
