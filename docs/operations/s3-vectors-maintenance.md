# S3 Vectors maintenance

**Use this when:** configuring the background identity, investigating maintenance failures, or planning an upgrade that changes Vector Catalog records.

## Configuration and identity

The server configuration subsystem `s3vectors_maintenance` accepts the default target `_` with `enable` and `execution_account`. Its defaults are `enable=off` and an empty execution account. Changes require a server restart. Configuration parsing rejects unknown keys, duplicate keys, unsupported targets and invalid values. See `crates/ecstore/src/config/vector_maintenance.rs` and `VectorMaintenanceRuntimeConfig` in `rustfs/src/table_catalog/maintenance/config.rs`.

An enabled worker requires an existing ordinary IAM account. The account name and current user generation identify its work. Root credentials, STS sessions and service accounts cannot serve as this background identity. The worker checks the current account and policies when admitting new operations. Recreating an account with the same name does not give the replacement account ownership of the old generation's jobs.

Grant `s3vectors:GetVectors` for manifest and data reads, `s3vectors:PutVectors` for ANN and Compaction publications, and `s3vectors:DeleteVectors` for cleanup on the intended Index resources. IAM and bucket policies retain explicit-deny semantics. Internal file access uses a server-created capability restricted to the resource identity, incarnation, dataset prefix and operation. These operations do not require ordinary S3 object permissions.

Unencrypted resources require no KMS configuration. Encrypted reads and writes use the resource's effective SSE configuration and the execution account's current key authorization. Exact physical deletion and native deletion-journal recovery do not decrypt object contents. Ordinary S3 requests cannot directly access managed Vector files.

## Publications and cleanup

ANN and Compaction publish through the Catalog. Compaction records fragment reservation and data rewrite as separate publications. A completed reservation alone does not establish a completed rewrite. The current Catalog head selects the readable version; maintenance never selects a version by listing files.

Maintenance enrollment conditionally upgrades a Vector resource's record and preserves its exact manifest reference. Catalog maintenance records remain in the system bucket. Lance manifests, data files and index files remain in the managed Vector bucket. Table records retain their own scope and encoding.

Cleanup requires an authoritative retirement record containing the exact object identity and references. Listing discovers candidates but cannot authorize deletion. Current references, retained history and unresolved candidates remain protected. Replacements at an existing key, missing or recent modification times, incomplete reference proofs and exceeded proof budgets cause retention or rejection. An uncertain native outcome remains protected until the original owners and native outcome have been established.

Quota reconciliation follows actual native deletion or recovery evidence. Candidate upload consumes storage even if publication fails. A scheduled cleanup or an expired job lease does not establish physical deletion or release quota.

## Failure and shutdown handling

New work stops when the worker loses authorization, reaches its admission limit or receives cancellation. An operation already holding its owners retains them until its actual storage work completes. Its recorded result describes that completion even if permissions changed while it was running. A lease expiry cannot prove that an earlier operation has stopped.

Shutdown drains the original worker handles and preserves the original timeout result. Errors from nested workers propagate into the shutdown result. Investigate `vector_maintenance_admission` events and the corresponding Catalog job, publication and retirement records before retrying work. Preserve uncertain files and records; deleting them manually can remove recovery evidence.

The scheduler limits queue entries, discovery calls, concurrent work, managed buffers and I/O. These limits cover the operations accounted for by the implementation; they do not establish a process-wide memory or CPU ceiling. Runtime defaults and validation belong to `VectorMaintenanceRuntimeConfig`.

## Upgrade requirements

Keep maintenance disabled until every participating node supports the required Vector record formats, managed access and maintenance operations, and the deployment's consistency and recovery checks have passed. A version string alone does not prove these capabilities. Optional encryption requires the corresponding key services and permissions.

Upgraded readers retain supported older Vector records. Maintenance writes can introduce newer record versions; rollback therefore requires readers that understand all records already written. Disabling maintenance stops new background admissions after restart and does not downgrade existing records. Verify Table/Iceberg regressions and the deployment's recovery scenarios before enabling maintenance after an upgrade.
