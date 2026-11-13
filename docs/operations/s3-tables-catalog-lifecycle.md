# S3 Tables Catalog Lifecycle

**Use this when:** disabling an object-backed table catalog, cleaning up metadata left by deleted empty tables, or diagnosing catalog registration reads.
**Source of truth:** `rustfs/src/admin/handlers/table_catalog/config.rs`, `rustfs/src/table_catalog/store/cleanup.rs`, and `rustfs/src/table_catalog/store/registration_cache.rs`.

## Disable A Table Catalog

Use `GET /iceberg/v1/buckets/{warehouse}` to inspect the table-bucket configuration. A signed `DELETE /iceberg/v1/buckets/{warehouse}` disables an empty catalog with `admin:SetTableBucket`; the `/_iceberg/v1` alias accepts the same request. Ordinary disablement and re-enablement preserve table metadata and object data.

Delete tables and namespaces through the catalog APIs before disabling. Active namespaces, retained catalog resources, active renames, and unfinished recovery operations block disablement. Do not manually delete internal metadata to bypass these checks. Review the bucket lifecycle configuration: disabling table protection allows existing object expiration rules to apply again.

The server persists an inactive catalog entry before clearing the bucket marker. If marker publication fails, retry the same DELETE; object protection remains until publication succeeds. A subsequent PUT explicitly enables the bucket again, while stale requests cannot reactivate an inactive entry. Re-enable disabled catalogs before rolling back to a version that does not recognize inactive entries.

## Clean Up Deleted Empty Tables

`DELETE /iceberg/v1/buckets/{warehouse}?cleanup=true` removes recognized metadata and internal records left by deleted empty tables, then disables the catalog. After removing an active table entry, ordinary table deletion reclaims its maintenance configuration, commit logs and idempotency receipts. It preserves Iceberg data and metadata files. Bucket cleanup can reclaim the same internal records left by older releases or interrupted drops, along with maintenance job reports for deleted tables. A retained table entry or a queued or running maintenance job blocks cleanup.

Complete the upgrade across all serving nodes and set `RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED=true` before cleanup. Delete the empty tables and namespaces through catalog APIs. The cleanup principal needs `admin:SetTableBucket` and `s3:GetObject`/`s3:DeleteObject` permissions for the target metadata objects. Omitting `cleanup` or setting it to `false` keeps ordinary disablement; repeated or invalid values are rejected.

Cleanup accepts only recognized empty-table metadata in the default managed warehouse. Active resources, snapshots and other data references, user objects, object versions, incomplete multipart uploads, corrupt or unknown metadata, and unfinished catalog operations block cleanup. The server validates the complete plan and all object permissions before deletion. Persisted restore barriers and legacy catalog control state also block cleanup until recovery resolves them.

If a storage operation fails, retry the same request. The server checks remaining objects again and retains table-bucket protection until cleanup and catalog disablement complete. It does not create a separate cleanup task file. After a successful response with `enabled: false`, delete the empty bucket through the normal S3 `DeleteBucket` operation.

## Registration Reads

Registration checks table-ID uniqueness and warehouse-directory overlap using existing catalog and warehouse-index records. Each registration obtains a complete authoritative inventory under the publication and catalog control guards. Inventory records must satisfy the same metadata commit checks as ordinary catalog reads; an incomplete or failed inventory rejects registration.

Each server keeps a bounded memory cache of validated ownership records. Reuse requires the same object identity and ETag in the current inventory. New or changed records are read and validated again; eviction and restarts cause additional reads. The cache is automatic, requires no configuration or new stored objects, and is isolated by application context.

Registration still enumerates namespace and warehouse-index prefixes, so enumeration grows with catalog size. Cache hits reduce object-body reads and JSON parsing. A hit reuses previously validated content without reading the current body again to prove physical readability. Conditional writes, rollback, and lock-loss fencing continue to control publication; the cache does not synchronize writers that bypass those guards.

Existing catalog, Iceberg metadata, and Console/client request formats remain unchanged. See the [S3 Tables support matrix](../architecture/s3-tables-support-matrix.md) for supported API boundaries.
