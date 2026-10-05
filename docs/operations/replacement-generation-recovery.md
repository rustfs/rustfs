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
