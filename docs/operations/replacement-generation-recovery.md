# Replacement generation recovery

Automatic replacement intents use schema 7 and completion proofs use schema 2.
The external recovery status and peer RPC fields are unchanged. A persisted
`rebuilding` phase is reported as running only while the same generation has a
live local executor.

## Mount changes and restart

Each replacement executor takes exclusive locks on all descriptor-pinned target
roots in endpoint order. The permanent `.rustfs-replacement.lock` inode must not
be removed while a service or worker can access the disk. Cancellation waits for
issued storage operations; a dropped waiter does not release their execution
leases.

When a mount identity changes, startup and the disk scanner use the same recovery
decision. The predecessor records one successor UUID and the exact marker values
that may be transferred. A successor starts with an empty cursor and scans the
union of the previous bucket plan and currently listed buckets. It cannot enter
`rebuilding` before every marker has transferred. Interrupted transfers resume
with the recorded UUID and retain markers already transferred.

Unknown marker owners, an unavailable anchor, stale revisions, changed slots,
and exhausted retry budgets stop recovery. Inspect the durable error and retain
the source metadata. Do not delete `healing.bin` to bypass ownership checks.
Completion proof must bind the new target identities and lineage before its
markers can be cleared. Handoff authorities, migration receipts, original legacy
records, and completion proofs are retained for diagnosis; automatic retention
does not collect a referenced authority.

## Retry budget

`max_retries` counts rebuild attempts only. A rebuild attempt starts after the
target is formatted and the generation is `rebuilding`; a failure in that phase
consumes one attempt, and a pass that ends with failed or skipped versions
consumes one attempt when the target is ready. Nothing else consumes it:

- a format fence rejection or any other failure before `format.json` exists;
- a target that is unmounted, restarting, or otherwise not ready;
- a dangling-delete grace wait;
- a mount change and the handoff it causes, including the handoff preparation
  and marker transfer; a successor inherits its predecessor's attempt count
  unchanged;
- a generation or ownership conflict found while resolving the durable record.

Each of those keeps the durable error message for status reporting. Only the
handoff lineage bound (32 links) limits repeated interruptions.

## Retired generations and blocked entries

A generation that is `abandoned` with a committed handoff, or that was retired
by an approved legacy migration, has transferred its responsibility. It no
longer owns its set: a fresh generation for other target slots can be admitted
while the retired record is retained for diagnosis. Once its successor is
`verified`, `cleanup_pending`, or has published a completion proof, the retired
record is a retention candidate; GC never collects a predecessor whose
successor has not reached that point, and never collects an uncommitted one.
An `abandoned` record without a handoff keeps reserving its target slots until
every slot's `healing.bin` names another generation of the same set.

Discovery blocks one generation at a time, never a whole set. A record whose
durable copies diverge at the same revision, that cannot be loaded, that binds
to a different set than the disk it was found on, or that cannot be resolved is
recorded as blocked with its reason and the time it was first observed. Every
discovery pass re-reads the record; a block clears as soon as the record merges
and resolves cleanly, and is refreshed otherwise. Sibling generations of the
same set and all other sets keep recovering. Fresh admission for a set is
deferred only while a blocked generation of that set still reserves its
targets. The heal manager exposes the current blocked entries, with reasons,
to the admin status surface.

## Blocked checkpoints

`<task>_ahm_checkpoint.blocked` isolates a checkpoint snapshot that the reader
could not verify. The marker is a small JSON record with the reason, the time
it was written, and the earliest time of the next revalidation; older binaries
wrote the literal `blocked` and treat any content as blocked. The current
reader revalidates a blocked snapshot at most once per hour, at the moment a
resume selector or executor consults the marker. If the snapshot verifies with
the current reader (for example, a field order the previous reader could not
digest), the marker is removed and `unblocked_by_revalidation` is logged.
Tampered or genuinely corrupt snapshots stay blocked and the marker's next
revalidation time is advanced. A marker whose snapshot has been removed is
cleared on the next revalidation. Downgrading to an older binary keeps every
marker in place and never clears one.

## Root heal records

Administrator root heal records are inventoried one record at a time. A record
with two durable owners, an unreadable quarantine marker, or a quarantine
marker whose source changed is reported as conflicting with its reason; the
remaining accepted responsibilities are still replayed. A conflicting task ID
is refused for new admission, and its status query reports `failed` with the
same reason until an operator repairs the record.

## Ambiguous legacy delete residue

Delete recovery that finds a legacy residue it cannot classify no longer
reports request-rate pressure. The heal returns a dedicated storage error that
is not retried automatically; the object remains readable and the residue is
retained for operator inspection.

## Schema 5/6 maintenance migration

A legacy binary does not take the new execution lock. Ordinary startup therefore
refuses automatic takeover of schema 5/6 replacement intents. Plan a maintenance
window and stop **all old RustFS processes and every other writer that can access
the target disks or survivor anchor**. Stop restart supervisors as well. Verify
that condition operationally before creating an approval; the preparation script
does not prove remote process termination.

1. Preserve a filesystem snapshot or backup of the survivor metadata and target
   `healing.bin` files. Identify every orphan generation for the affected set and
   exact target slots. Do not combine generations from different sets or targets.
2. Generate a fresh successor UUID. Run the preparation command without
   `--write` to inspect its proposed JSON. Use the endpoint strings from the
   intents, including their configured URL/path spelling.
3. With writers still stopped, repeat the same command with
   `--write --stopped-all-writers`. The script publishes a digest-bound approval
   under the survivor's `.rustfs.sys/buckets/ahm-replacement/` directory and
   leaves all original intents and markers untouched.
4. Start the new binary with the existing storage configuration. Startup acquires
   target leases, verifies the approved bytes and marker owners, archives each
   source verbatim, publishes the reserved successor, and retires the source
   intents. It consumes the approval into a permanent receipt. No historical
   predecessor edge is inferred between formerly unrelated orphan generations.
5. Observe the successor through recovery status and verify physical shards and
   metadata on every target. Successful GETs alone do not demonstrate repaired
   redundancy. Preserve evidence for historical versions, explicit null
   versions, delete markers, and writes acknowledged during the outage.

Example (replace every placeholder with the recorded values):

```bash
python3 scripts/prepare_replacement_migration.py \
  --anchor /mnt/survivor \
  --source SOURCE_A_UUID --source SOURCE_B_UUID \
  --target 'CONFIGURED_ENDPOINT=/mnt/replacement' \
  --successor FRESH_SUCCESSOR_UUID
```

The runtime accepts only isolated schema 5/6 intent files. Flat legacy records,
different target scopes, unsupported schemas, altered source digests, and an
unlisted marker owner require investigation before an approval can be consumed.
A failed import is replayed on the next startup using the same approval and
successor UUID. If buckets change after successor publication but before import
completion, the unstarted successor extends its bucket plan while holding the target leases.
A successor that has already started retains its recorded scan plan.

Keep the new binary for an unfinished schema 7 recovery. An older binary rejects
that schema and cannot safely continue its ownership protocol. Finish and verify
recovery before any downgrade; never edit the schema number to force acceptance.

## Acceptance after a VM restart

Use an isolated fault-test deployment. Interrupt recovery during a page, restart
the VM so mount identity changes, and confirm that the recorded handoff completes
without another orphan generation. Repeat with a genuinely new disk and with a
second restart during marker transfer. Check target-local historical data, null
versions, delete markers, checksums, and erasure-set redundancy. Record the final
proof and marker removal for the successor. Production VM power-loss and physical
redundancy validation remain deployment acceptance requirements.
