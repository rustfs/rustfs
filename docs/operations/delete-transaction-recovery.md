# Delete transaction recovery

An interrupted object deletion can leave metadata backups, reserved shards, or
an unpublished target. Normal S3 listings cannot discover all of these files.
Recovery therefore inspects raw transaction records on every configured drive
in the owning erasure set under the bucket incarnation fence and exact-key
write lock. It does not modify GET or PUT behavior.

## Decisions and replay

New deletions assemble the backup, versioned prepared journal, and exact target
in the private system temporary area, then atomically publish the complete
transaction directory before modifying metadata. Interrupted preparation cannot
leave an undiscoverable target-only record or temporary file in the object key. After the metadata write quorum, the coordinator publishes durable
commit receipts to a write quorum before returning success. A failed or lost
publication response is indeterminate: recovery must inspect the persisted
records instead of immediately restoring the backup.

| Evidence | Action |
| --- | --- |
| New prepared transaction, no decision, intact original shards and matching generation | Restore and sync the original metadata before removing preparation evidence. |
| Structured commit receipt | Publish the certified target to matching returning participants and finish deletion. |
| Missing, unavailable, or unsupported participant | Preserve records and retry when every configured participant can be inspected. |
| Newer generation or partially overlapping version history | Preserve records and report blocked recovery. |
| Historical backup or disk-local marker without a structured set decision | Preserve records during automatic recovery. |

Rollback preserves native multipart metadata and integrity sidecars. Protected
parts require their declared proof generation; recovery verifies both the proof
records and actual shard payloads before restoring metadata or removing
recovery evidence. Missing or corrupt required proofs leave the transaction
blocked with its backup intact.

A disk-local `delete-data.<T>` marker does not prove an erasure-set decision.
Returning drives cannot restore stale data after a certified deletion. Commit
receipts remain until every participant and related transaction for the key
has converged. Cross-transaction null-version supersession also requires causal
predecessor evidence; timestamps alone cannot authorize it. Cleanup respects active snapshots and removes only validated
transaction files, with directory removal performed nonrecursively.

## Entry points

Unclean-shutdown erasure-set healing performs raw transaction recovery before
ordinary object listing. Administrator bucket, set, prefix, and object heal use the
same recovery core. Existing heal selection, lifecycle admission, namespace
locking, and movement guards still apply. Prefix recovery settles only matching
keys, including transactions hidden from listings. Dry-run inspects and reports without
settling transactions.

An unconditional DELETE of a missing exact key, or an administrator heal with
`remove` enabled, can authorize historical residue removal only when the
bucket is authoritatively never-versioned and has no Object Lock. Every
configured drive must be inspected, the current object must be absent, and all
residue must match the guarded legacy layout. Versioned/suspended buckets,
retained versions, conditions, unknown entries, symlinks, and prefix children
are not covered by that authorization. The same fresh intent can certify the
absence of an unprotected new prepared null-version transaction when every
configured drive has no current metadata. This is a new authorized decision;
it does not infer an old commit from missing causal evidence. Mixed or
unrecognized historical layouts remain blocked.

A missing specific-version DELETE first recovers hidden transactions and then
reads the current version again. It must not acknowledge an unresolved hidden
version as an idempotent no-op.

## Upgrade and failure handling

Upgrade all participating nodes before relying on the new protocol. Recovery
uses one authenticated additive RPC; unsupported peers cannot silently accept
new flags through the legacy delete endpoint. An online older participant
causes the modern operation to fail before decision publication. Modern nodes
also reject legacy forward transactional user-bucket deletes. A cluster still
controlled by an older majority retains the older behavior. Do not downgrade
while recovery records remain; old binaries cannot interpret the new protocol.

Keep unavailable drives and transaction directories intact. Bring the full
set online, run a scoped heal in dry-run mode, inspect its recovery result,
and then run the same scoped heal without dry-run. Use `remove` for ambiguous
historical residue only after verifying that the exact objects are intended
to be deleted and the bucket meets the restrictions above. Do not recursively
remove transaction directories by hand to make a bucket appear empty.

Recovery reports discovered, committed, rolled-back, reclaimed historical,
and blocked transaction counts. Unavailable participants return a retryable
quorum error; blocked evidence requires operator investigation. A successful
S3 delete response does not imply immediate physical cleanup while snapshots
or returning participants still retain necessary proof.

## Settlement deadline and durability

`RUSTFS_DELETE_RECOVERY_TIMEOUT_SECS` controls the settlement RPC deadline,
including full shard verification during rollback. Key and bucket lifecycle
ownership stay attached to late local filesystem operations after cancellation
or timeout. The default is 900 seconds;
valid values are 30 through 86400 seconds. Invalid values fail explicitly.
Increase it when intact large objects need more verification time. It does not
change normal object read or delete-apply deadlines. A timeout can conceal a
completed mutation, so rerun inspection and recovery rather than guessing the
outcome from the RPC error.

The records and restores follow the configured durability policy. Strict mode
syncs the required metadata and directory changes. Process SIGKILL testing
checks restart behavior; it does not establish power-loss behavior on every
filesystem or storage device. Mode `none` intentionally offers weaker sync
guarantees. Do not treat a throughput comparison as a durability substitute.

## Reproduce the Docker regression suite

Build the candidate with the test-only barriers enabled:

```sh
cargo build --locked --release -p rustfs --bin rustfs --no-default-features --features e2e-test-hooks
python3 scripts/test/delete_recovery_docker.py --help
```

Pass a native Linux binary with `--binary`, a Linux image containing Python and
curl with `--image`, and a unique Docker namespace. The optional
`--baseline-binary` enables the mixed-version case. The harness uses synthetic
S3 objects, localhost ports, labeled isolated volumes, and a non-root server.
It stops only its own containers and keeps JSON results and logs in the chosen
artifact directory. Production builds must not enable `e2e-test-hooks`.

Run the preactivation/prepare/apply/decision process-stop cases, interrupted restore,
returning and untouched peers, both generation orderings, version/retention
and prefix protection, administrator dry-run/removal, response-loss,
permission/corruption, and mixed-version cases. Preserve the source identity
of both binaries with the results. A passing process-stop matrix does not
cover hardware power cuts, Windows runtime behavior, or DELETE throughput.
