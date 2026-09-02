# S3 Tables Durable Backing Cutover Runbook

**Use this when:** moving a table-catalog warehouse from object-backed catalog state to the durable strong snapshot backing (`RUSTFS_TABLE_CATALOG_BACKING=durable-strong`), or rolling the strong snapshot format from version 1 to version 2.
**Source of truth:** the `{warehouse}/catalog/migration` routes registered in `rustfs/src/admin/handlers/table_catalog/routes.rs`; the env constants named below; claims and status labels in [docs/architecture/s3-tables-support-matrix.md](../architecture/s3-tables-support-matrix.md).

## Preconditions

| Requirement | Why |
|---|---|
| A principal with `GetTableCatalogAction` on each table bucket (preflight) and `admin:MigrateTableCatalog` (migration `POST` / `DELETE`). | The mutations are admin-gated. |
| Every catalog writer runs a release that recognizes the durable-backing migration fence. | An older writer does not see the persisted fence and can mutate the object-backed source after the snapshot inventory is captured. |
| An object-backed catalog backup, plus the current metadata pointer and version token for representative tables. | Recovery after a failed cutover is an operator-selected restore, not a restart against the stale pointer. |
| Every mutating object-only operation (maintenance workers, catalog recovery, export, diagnostics, external catalog bridge writes) is inventoried and confirmed supported in durable-strong mode. | Unsupported operations fail closed after cutover rather than continuing against object-backed state. |

## Cutover Procedure

1. Take the object-backed backup and record pointer and version token for representative tables.
2. Run the preflight for each warehouse and treat every `blockers` entry as fail-closed. Repair commit recovery state and backfill the warehouse prefix index before continuing. Requests are SigV4-signed with the catalog's REST signing name; the `/_iceberg/v1` alias accepts the same paths.

   ```text
   GET /iceberg/v1/{warehouse}/catalog/migration
   ```
3. Drain every catalog writer that predates the migration fence and restart it on a fence-aware release. Keep all writers on that release until cutover completes.
4. Quiesce mutating object-only operations (step 4 of Preconditions).
5. Run the migration `POST` with `admin:MigrateTableCatalog`. It acquires the exclusive migration fence to drain in-flight fence-aware mutations, persists the source fence while exclusivity is held, then copies catalog state and reports `ready_to_enable_durable_strong`.

   ```text
   POST /iceberg/v1/{warehouse}/catalog/migration
   ```

6. Repeat preflight and materialization for every table bucket. Do not proceed until the preflight reports `SNAPSHOT_MATERIALIZED`, no blockers, and `ready_to_enable_durable_strong: true` for all of them.
7. Restart with `RUSTFS_TABLE_CATALOG_BACKING=durable-strong`, then verify catalog config, table and view loads, commit idempotency, and table data-plane policy resolution before admitting writers.
8. Preserve the object-backed backup until durable strong backing has passed the operator's retention window.

## Cancelling Before Cutover

Before the restart in step 7, `DELETE` on the migration endpoint removes a migration-created target bucket snapshot and releases the source fence. It releases the bucket fence only while the target state has not advanced, and releases the registry fence after the last bucket is cancelled. Retries and `DELETE` may restore a known-absent initial target after an ambiguous first write, but fail closed if a previously existing or materialized global snapshot disappears.

```text
DELETE /iceberg/v1/{warehouse}/catalog/migration
```

After the durable-strong state advances, cancellation fails closed; recovery requires an operator-selected restore or reverse migration.

## Strong Snapshot Version 1 to Version 2

1. Keep snapshot writes on version 1 during a rolling binary upgrade. Current binaries read both versions.
2. After every catalog writer can read version 2, set both `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2=true` and `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED=true` and restart the catalog writers. Setting only one gate does not change the write format.
3. Perform a controlled catalog write or migration materialization and confirm the persisted snapshot is version 2 before serving table data-plane traffic. Once v2 is fleet-confirmed, data-plane resolution fails closed until the persisted snapshot is v2.
4. After any version 2 snapshot is persisted, do not roll writers back to a binary that only reads version 1. Current binaries preserve version 2 even when the gates are later disabled.

## Rollback And Collision Repair Rules

- A running process rejects restored version 1 content after observing version 2, but cannot distinguish an older snapshot with the same format version from a deliberate restore. The format high-water mark is process-local: restoring any older snapshot and restarting every writer is a privileged disaster-recovery rollback that must restore a compatible binary and an operator-selected snapshot together.
- Migration preflight rejects an active table/view identifier collision before writing a migration fence. A pre-existing version 1 strong snapshot with such a collision loads in cleanup-only quarantine: ambiguous reads fail closed, each cleanup mutation must reduce the collision set, and unrelated writes stay blocked until all collisions are removed. Drain writers that predate cleanup quarantine before starting the repair, and finish cleanup before the first version 2 write.

## Related

- [S3 Tables support matrix](../architecture/s3-tables-support-matrix.md)
- [Table catalog conformance scripts](../../scripts/table-catalog/README.md) (`failure_coverage.py --print-disaster-recovery-rehearsal` generates the rehearsal for this procedure)
