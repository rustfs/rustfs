# S3 Tables Support Matrix

**Use this when:** writing a release note, README claim, or client-compatibility statement about RustFS S3 Tables / Iceberg REST Catalog, or deciding whether a feature is supported, preview, or not claimed.
**Source of truth:** the table-catalog handlers under `rustfs/src/admin/handlers/table_catalog/`; the conformance scripts and their README in `scripts/table-catalog/`; and the object catalog backup/restore contracts exercised by the focused server tests.

RustFS S3 Tables is an Iceberg REST Catalog and table-bucket implementation on top of the RustFS S3 data plane. It does not claim parity with the AWS S3 Tables control-plane API or with vendor-specific Iceberg catalog extensions.

## Status Labels

| Label | Meaning |
|---|---|
| Automated | Covered by a runnable RustFS script or server test. |
| Manual/live harness | RustFS generates pinned client inputs, commands, expected outputs, and CI opt-in gates for a live endpoint; the live run is not enabled by default in CI. |
| Generated harness | RustFS generates client configuration or probe input; live execution is not automated. |
| Supported | Implemented server-side and covered by focused RustFS tests. |
| Preview / controlled | Implemented behind explicit operator action or a run-once endpoint; no automatic background claim. |
| Documented, not automated | Behavior is documented; the live client run is not automated. |
| Reference only | Compatibility reference; live interoperability is not claimed. |
| Not claimed | Out of scope for the current implementation. |

## Endpoint And Profile Matrix

| Surface | Status | Notes |
|---|---|---|
| `/iceberg/v1` | Supported | Canonical REST Catalog prefix; default REST signing name `s3`. |
| `/_iceberg/v1` | Supported compatibility alias | MinIO AIStor-style alias; smoke profile defaults to signing name `s3tables`. |
| S3 object data plane | Supported | Data, metadata, manifest, and delete files are ordinary S3 objects with table-aware policy checks on warehouse paths. |
| S3 table-bucket listings | Supported | `ListObjects`, `ListObjectsV2`, `ListObjectVersions`, metadata-list extensions, and multipart-upload listings omit warehouse objects unless the caller has `admin:GetTableMetadata` for that table, and always omit internal `.rustfs-table` keys. Protected cursors are encrypted with a key derived from the cluster root credentials, so rotating those credentials invalidates cursors already issued. |
| Object-backed warehouse-index recovery | Supported with bounded request fallback | Data-plane requests repair or verify a missing warehouse-prefix index using authoritative table-entry pages. The scan selects `table-entry.json` below identifier-like directory components at any depth; namespace components named `tables` or `maintenance` remain valid. Noncandidate report names are filtered in directory batches before inventory retention and metadata reads, so report history does not consume ownership pages. Selected entries retain GET-equivalent quorum and corruption checks. More than 4,096 table entries fails closed with a retryable service error and requires operator backfill. An optional table-entry directory (see Maintenance Matrix) answers these lookups without the prefix listing. |
| Table bucket enablement | Supported | A regular bucket is enabled for catalog use and addressed as the REST catalog warehouse. |
| Catalog-vended table credentials | Automated when enabled | Disabled by default. LoadTable vends credentials only when `X-Iceberg-Access-Delegation` contains the exact `vended-credentials` token; the dedicated credentials endpoint uses the same issuer path. |
| AWS S3 Tables endpoint shape | Profile generator | Generates the AWS catalog URI and warehouse ARN shape for migration docs. API parity not claimed. |
| MinIO AIStor Tables profile | Profile generator plus alias smoke | Alias shape only; AIStor private extensions not claimed. |
| Cloudflare R2 Data Catalog profile | Profile generator | Catalog URI and warehouse-name shape only; live interop not claimed. |
| Alibaba OSS Tables profile | Profile generator | Endpoint, `acs:osstables` warehouse ARN, `osstables` signing name, and `https://oss-{region}.aliyuncs.com` S3FileIO endpoint shapes only; live interop not claimed. |

## Client And Engine Matrix

| Client or engine | Status | Claim |
|---|---|---|
| PyIceberg | Automated | Namespace and table create, append, exact-row reload/scan, metadata-location, refs, views, namespace properties, rename identity, maintenance, diagnostics, export, and credential negotiation against the object catalog. Optional vended credentials include an exact-prefix data-plane scope check. |
| Spark Iceberg REST catalog | Manual/live harness | Pinned package inputs, catalog properties, atomic CTAS SQL through staged create, expected `row_count=2`, and a CI opt-in gate for create/refresh/count/cleanup. Live execution and commit-conflict probing remain manual unless enabled in the runner. |
| Trino Iceberg REST catalog | Manual/live harness | Catalog properties and a read-only `SELECT COUNT(*)` against a PyIceberg- or Spark-created table. Write compatibility not claimed. |
| DuckDB Iceberg 1.5.5 | Automated | `duckdb_smoke.py` covers metadata-location read, single-table create/insert/update/delete/merge, schema evolution, snapshots, concurrent writers, drop, PyIceberg cross-read, and both signing profiles. Purge-on-drop and format v3 are verified fail-closed. Two-table mode runs without claiming cross-table atomicity. AWS `ENDPOINT_TYPE S3_TABLES`, staged-create client integration, and vended-credential integration are not claimed. |
| StarRocks Iceberg REST catalog | Documented, not automated | External catalog read-path reference only. |
| Databend | Manual/live harness | S3 stage read probe for table data files only; Iceberg REST integration not claimed. |
| Snowflake Open Catalog / Iceberg integrations | Generated harness | Operator-adapted external volume/catalog SQL template only; live interop not claimed. |

## Live Evidence And Operations Matrix

| Area | Status | Claim |
|---|---|---|
| Live conformance evidence | Automated for PyIceberg and DuckDB | `engine_compatibility.py --print-live-evidence-schema` defines the evidence schema and promotion boundaries; both smoke scripts write validated evidence records via `--live-evidence-output`. |
| Production operations guide | Generated harness | `engine_compatibility.py --print-operations-guide` records commands, evidence, pass criteria, and fail-closed signals for conformance, object catalog backup/restore, maintenance, recovery, permissions, credential vending, and claim governance. |
| Vendor compatibility gap audit | Generated harness | `engine_compatibility.py --print-vendor-audit` records provider URLs, path and warehouse shapes, auth models, validation categories, and not-claimed boundaries for the four vendor profiles. |
| Client claim promotion | Automated for scoped clients | PyIceberg and DuckDB claims are bounded by their smoke entrypoints and recorded versions; Spark needs recorded live evidence; Trino stays read-only; Snowflake and vendor profiles stay reference-only. |

## Catalog API Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Catalog config | Supported | `GET /v1/config` advertises defaults and only the supported OpenAPI REST paths in `endpoints`; RustFS extensions (administration, maintenance, backup/restore, diagnostics, refs, metadata-location) are not presented as standard endpoints. |
| Table bucket discovery | Supported | `PUT` / `GET /v1/buckets/{warehouse}` enable and inspect table bucket state. `DELETE` disables an empty catalog while preserving objects; discovery includes `disable-supported`. Namespaces, retained resources, staged metadata, and publication fences prevent disablement. |
| Namespaces | Supported | Create, list, load, exists, drop on both prefixes; `pageSize`/`pageToken` pagination with context-bound tokens; identifiers limited to 512 ASCII characters. |
| Tables | Supported | Direct and staged create, register, list, load, exists, rename, commit, metadata-location get/update, and drop on both prefixes. Staged create is stateless until an `assert-create` commit atomically publishes the initial metadata and table entry. Rename uses a bucket-scoped persistent fence, recoverable intent, and conditional publication; the source name is reusable only via an ETag-conditional tombstone replacement. Commit identifiers must match the URL; unknown requirements/updates fail as bad requests; register overwrite, purge-on-drop, and v3-only encryption-key updates return an explicit unsupported-operation response. |
| Commit CAS | Supported | Single-table commits validate base metadata, version token, referenced object existence, warehouse scope, and Iceberg requirements before advancing the pointer; external metadata transitions preserve monotonic assignment watermarks and immutable retained definitions. `idempotency-key-lifetime` is not advertised; mutation-wide `Idempotency-Key` semantics are unsupported. |
| Commit recovery | Supported | Commit log, idempotency lookup, diagnostics, and recovery routes expose and repair finalization gaps without moving the pointer. Object-backed recovery reports a staged request as SUPERSEDED only when a reachable finalized same-base winner proves it lost CAS and its idempotency binding is intact. The original staged receipt is retained; its old request still conflicts. The winning history must be retained with the losing receipt; missing or contradictory history requires manual review. |
| Snapshot refs | Supported | List, create/replace, delete via commits; `main` is protected; refs with explicit retention need forced delete. |
| Iceberg views | Supported | Create, list, load, replace, exists, drop with view-scoped authorization; only view format version 1. |
| LoadTable and table credentials endpoint | Supported | Vending only on negotiated `vended-credentials`; one temporary session scoped to the warehouse prefix and current metadata location; missing credential permission falls back to metadata-only with an explicit reason; vended storage sessions cannot authenticate Catalog requests; responses carry `Cache-Control: no-store, private`. |
| Catalog diagnostics and export | Supported | Object catalog state exposes recovery, consistency, commit-log, idempotency, maintenance, single-active-writer policy, and scale validation information. |
| Catalog backup and restore | Preview / controlled | `POST /iceberg/v1/{warehouse}/catalog/backup` creates object-catalog artifact v2 with catalog/current table metadata and an inventory of current/direct `metadata-log` history, current view metadata, manifest lists/manifests, live data/delete files, and table/partition statistics. References up to 16 MiB use size/SHA-256; larger references require the captured size/ETag. Proven superseded requests retain their original staged records and any idempotency indexes with their winning history; missing or contradictory proof blocks backup and restore. External payloads are not packaged. `POST /iceberg/v1/{warehouse}/catalog/restore` applies objects conditionally; keep the target closed to clients until restore and recovery verification finish. Per-object CAS does not establish bucket-wide atomic publication or crash isolation. |
| Catalog backup artifact transfer | Preview / controlled | `POST /iceberg/v1/{warehouse}/catalog/backup/export` takes strict `{"backup-id":"..."}` JSON and returns saved artifact bytes; `POST /iceberg/v1/{warehouse}/catalog/backup/import` accepts the raw artifact. Artifact transfer is limited to 64 MiB and does not publish catalog or prepare external objects. Existing object-catalog v1 bytes and their legacy catalog/current-table-metadata contract remain unchanged. |
| Catalog backup object preparation | Preview / controlled | `POST /iceberg/v1/{warehouse}/catalog/backup/prepare` takes strict `{"backup-id":"...","object":"...","data-base64":"..."}` with standard padded Base64, up to 16 MiB decoded and 24 MiB JSON. It conditionally creates one v2-listed object within its validated table/view scope, returning `PREPARED` or `ALREADY_PRESENT`; identical bytes are idempotent and different existing bytes are never overwritten. Preparation does not publish catalog state. |
| Catalog import and rollback | Supported | Import/register and online rollback go through validation and commit paths. Online rollback accepts only forward-safe targets; restoring an older target that lowers watermarks is an offline disaster-recovery operation with all writers stopped. |
| External catalog bridge | Supported operator path | Operator-supplied metadata pointer sync/import. Vendor SDK polling and policy mirroring not claimed. |
| Multi-table transactions | Not claimed | Single-table commit atomicity only. |

Backup extensions require a table-enabled warehouse and `admin:MigrateTableCatalog`; export, import, and prepare also accept `/_iceberg/v1`. Restore and disaster-recovery rollback require all writers and maintenance workers stopped and the target closed to clients until recovery verification finishes. Every serving/background node must understand persistent restore controls; otherwise keep those nodes/workers out of the target service until then. `RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED=true` confirms publication-fence deployment, not restore-control version compatibility; no restore isolation guarantee is claimed for a mixed old/new fleet.

## Data Plane And Credential Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Table-aware S3 policy bridge | Supported | Ordinary S3 actions on warehouse paths are checked through the table bridge; table policy cannot be bypassed by direct object access. |
| Warehouse location authorization | Supported | Caller-selected locations require `admin:ClaimTableLocation` on the normalized physical prefix without a trailing slash, for example `arn:aws:s3:::warehouse/shared`. To also allow claims on descendant prefixes, include `arn:aws:s3:::warehouse/shared/*`. This applies to explicit create locations, staged publication, register/import, external catalog registration, and warehouse relocation. Server-assigned create locations retain the namespace `admin:CreateTable` requirement. Existing table operations that retain the warehouse prefix keep their existing permissions. |
| Reserved catalog protection | Supported | Catalog-reserved prefixes are protected from ordinary object mutation. |
| Static S3 credentials | Automated | Default PyIceberg smoke path. |
| Catalog-vended credentials | Automated when enabled | `rustfs-vended-credentials` verifies the returned prefix, then checks Put/Head/Get/DeleteObject inside it and denies access outside it. |
| Credential lifetime | Supported | Server-side TTL clamped to a short-lived range. |
| No-long-term-data-credential bootstrap | Not claimed | Catalog setup still uses the configured principal before table-scoped credentials are requested. |

Granting `admin:ClaimTableLocation` authorizes changing the catalog ownership and
access requirements of all existing objects and versions under that prefix. It
does not grant S3 read or write access. Upgrade every server before adding the
action to policies; older servers do not recognize it. Applications using custom
locations, staged publication, or table registration need an explicit grant for
their warehouse prefixes.

## Maintenance Matrix

The object catalog supports the configuration, scheduler, worker, job
inspection, heartbeat, and quarantine endpoints. Recoverable job and
configuration records remain separate objects. The worker performs metadata and
unreferenced-object cleanup without changing the catalog pointer. The direct
`/maintenance/metadata` endpoint supports safe planning; snapshot expiration
and compaction commits remain explicit controlled operations. A configured
`background-enabled` table still requires explicit worker ticks. The standalone
server has opt-in, allowlisted periodic queue admission; see the
[scheduler runbook](../operations/s3-tables-maintenance-scheduler.md).

Renamed tables retain their original metadata paths. Object catalog scheduling,
planning, and cleanup fail closed with a conflict if another table shares a scanned
metadata or warehouse directory, including a recreated source name after rename.
The object backing rechecks this ownership inventory under the bucket publication
fence before every delete pass; the inventory reads every table entry in the bucket,
so maintenance cost grows with the table count. Such tables require disjoint cleanup
roots before maintenance can run; enabling deletion does not override this ownership
check.

Object catalog rename uses a persisted intent and publication fences. Recovery of an
interrupted rename and recovery of a partially applied catalog restore are separate
contracts; a successful rename test does not establish restore crash isolation.

Object-backed create, register, and commit publication share one preparation deadline, `RUSTFS_TABLE_CATALOG_PUBLICATION_PREPARATION_TIMEOUT_SECS` (default 30, never below the object lock-acquire timeout, capped at 86,400). Migration and bucket-lock queueing, reference HEAD validation, ownership inventories, metadata parsing, and snapshot preparation consume that same interval; repeated preparation does not renew it. A create writes its initial metadata file before registration without ending the deadline; when registration then fails before any catalog write may have begun, including on expiry, the handler removes that file, so the worst outcome of an expired create is a small orphan under a root no table owns, left by a crash or failed delete between the write and the removal. Expiry before a possible durable catalog write returns HTTP 503 and releases the preparation locks; a valid large operation that cannot finish within the interval keeps failing on retry until the interval is raised. Once a catalog write may have begun, this deadline no longer cancels that transaction. Individual PUT completion and persisted commit/rename recovery retain their existing cancellation guarantees. This is a preparation limit, not a hard bound on disk I/O or the durable tail. A rename that the client abandons mid-flight completes or recovers on a detached task, so the bucket's `active_rename_id` is always cleared.

Compaction commits hold inputs to a 512 MiB stored and decoded budget per rewrite group, validate the footer (including the row-group count the decoder would reserve for) and every page header before decoding a page (a recursive or over-deep Iceberg manifest Avro schema and a Parquet page that exceeds its budget are refused with HTTP 400), and run one compaction per node at a time; a concurrent request receives HTTP 503. The planner packs each rewrite group to the smaller of `max-rewrite-bytes-per-job` and that 512 MiB stored-input ceiling, so a larger configured value is accepted but yields several groups; a group whose stored inputs still exceed the ceiling at commit time (for example a plan produced by an older release) is refused with HTTP 400 naming the ceiling before any group's output is written (every group of the plan is checked first).

Object-backed commit publication identifies every catalog object it validated by the storage layer's write-unique body revision and rechecks only that identity under the bucket publication fence. Objects without such a revision (bytes the publication itself wrote before the fence, existence-only observations, and objects whose storage metadata reports no body revision) are "legacy" bytes: they are re-read and compared by SHA-256 under the fence, within a total budget of twice the maximum admin request body size (2 MiB) per publication. A publication whose legacy bytes exceed that budget fails with HTTP 503 and the message `legacy catalog objects exceed the publication validation budget; rewrite the referenced objects and retry`; rewriting the referenced metadata, manifest-list and manifest objects on the current release gives them a body revision, after which the same commit can be retried.

### Table-entry directory

An optional per-bucket directory of table entries under `.rustfs.sys` answers data-plane index-miss lookups without listing the catalog prefix. It is off by default: `RUSTFS_TABLE_CATALOG_DIRECTORY_INDEX=true` and `RUSTFS_TABLE_CATALOG_DIRECTORY_INDEX_FLEET_CONFIRMED=true` on every node, then `POST /iceberg/v1/{warehouse}/catalog/directory` (`admin:MigrateTableCatalog`) advances a backfill of 128 catalog objects per call until the directory reports `Ready`; `GET` reports the phase and `DELETE` rolls the directory back. Entry writers add a directory candidate only while the directory exists, and the backfill covers earlier entries. The directory state occupies the backing-migration fence object: nodes without this feature answer catalog mutations on that bucket with a parse error until they are upgraded, which is what the fleet-confirmed flag attests, and restores require disabling the directory first. Object catalog backups exclude the derived directory state and candidates. Re-enable the directory after a restore to rebuild it from the restored entries.

Fallback ownership scans, including the check before metadata cleanup, use the optional `identifier_tree_leaf` walk selector. An absent selector preserves ordinary listing behavior. Older peers ignore the additive field; the coordinator filters their noncandidate rows before metadata resolution and retains read-quorum checks for every selected entry. Old peers may do more I/O or fail on unrelated corrupt metadata, so a mixed-version scan can refuse work until those peers are upgraded. Such failures never establish owner absence. This selector changes no catalog or on-disk format and requires no fleet-confirmation flag.



| Capability | Status | Claim |
|---|---|---|
| Metadata retention dry-run | Supported | Reports retained metadata and deletion candidates without moving the pointer. |
| Metadata cleanup delete | Supported | Deletes only candidates passing the safety window and current-pointer checks. |
| Ordinary bucket lifecycle expiry | Disabled for table buckets | Table bucket objects are excluded from lifecycle expiration, including queued work; snapshot expiration and orphan cleanup stay inside catalog maintenance. |
| Snapshot expiration planning | Supported | Plans with retained and candidate snapshots. |
| Snapshot expiration commit | Preview / controlled | Manual commit through the catalog; stale plans fail closed. |
| Manifest/data/delete reachability cleanup | Supported | Reads manifest-list and manifest Avro references; deletes only unreferenced objects passing the safety window. |
| Maintenance scheduler run endpoint | Preview / controlled | Durably queues one job per table, reuses an active queued job, recovers expired leases. |
| Maintenance worker run endpoint | Preview / controlled | Claim, run-once, backpressure, retry deferral, lease expiry recovery, heartbeats. |
| Maintenance scheduler guardrails | Preview / controlled | Disabled/paused/ready state, handoff, backpressure, quarantine boundary, recommended actions, recent job audit timeline. |
| Maintenance audit events | Preview / controlled | Structured events for planning, worker transitions, heartbeats, lease recovery, quarantine mutations. |
| Maintenance quarantine operations | Preview / controlled | Inspect, release, retry, or abandon the quarantined job without moving the pointer. |
| Compaction planning | Preview / controlled | Partition-local and sort-order-local binpack candidates for Parquet; never mixes partitions or sort orders in one group. |
| Delete-file or row-level compaction planning | Preview / controlled | Position or equality delete files force machine-readable planning into manual review. |
| Compaction commit | Preview / controlled | Commits a safe partition-local Parquet rewrite preserving sort order IDs. |
| Built-in periodic scheduler | Preview / queue-only | Default off; allowlisted, paged admission with jitter, per-pass quotas, current-job checks, and cooperative drain. Workers and failed/expired-job recovery remain operator-triggered; no automatic execution or hard per-table I/O deadline. |
| Delete-file or row-level compaction execution | Not claimed | Manual-review maintenance item. |

## Recovery And Object Backing Matrix

| Area | Status | Claim |
|---|---|---|
| Single-table CAS | Supported | Pointer advances only through expected-token and expected-metadata-location validation. |
| Idempotent retry | Supported | Repeated commit IDs return the finalized result or surface recoverable finalization gaps. |
| Commit publication fencing | Supported with rolling-upgrade gate | Exact object guards protect referenced files during publication. Set `RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED=true` only after every serving node supports table and table-bucket fences. In scalable mode, warehouse prefixes must not overlap and enablement, first publication, drop, and relocation are serialized by the table-bucket fence. |
| Post-CAS finalization recovery | Supported | Repairs stale or missing idempotency indexes without changing the pointer. |
| Catalog export | Supported | Table state, current metadata pointer, commit recovery state, idempotency state, maintenance state, and object catalog manifest. |
| Disaster recovery rehearsal | Manual/live harness | `failure_coverage.py --print-disaster-recovery-rehearsal`; controlled object catalog backup/restore requires referenced table objects to remain available and does not provide cross-region object replication. |
| Scale and fault rehearsal | Manual/live harness | `failure_coverage.py --print-scale-fault-rehearsal`. |
| Single active writer region | Supported policy | Diagnostics publish single-active-writer semantics and read-only replica limits. |
| Active-active multi-region writes | Not claimed | A table must not accept independent concurrent writers in multiple regions. |

## Production Failure Coverage

Failure probes prove RustFS does not silently advance table state on failure. Tracked cases: stale commit token or base metadata returns a conflict without advancing the pointer; missing metadata, manifest, data, or delete objects fail closed before commit, backup, restore, or maintenance; concurrent writers produce one CAS winner and retryable conflicts; catalog and S3 permission denials prevent data-plane bypass; stale maintenance plans fail closed before deletion or commit; post-CAS finalization gaps are visible and safely recoverable; external catalog sync conflicts leave pointer, token, and generation unchanged; object backup/restore stays blocked until recovery and referenced-object validation are clean.

Do not promote a failure case from live probe or load test to an automated claim until the exact RustFS build, client version, and expected response shape are recorded.

## Unsupported Or Not Claimed

Full AWS S3 Tables control-plane parity; full MinIO AIStor private extensions; full Cloudflare R2 Data Catalog or Alibaba OSS Tables interoperability; automatic maintenance execution and failed/expired-job recovery; active-active multi-region writes; multi-table transactions; no-long-term-data-credential bootstrap; online vendor SDK polling; external catalog policy mirroring; delete-file rewrite or row-level compaction execution; built-in SQL execution; Delta Lake or Hudi; end-to-end SQL row-level DML validation through Spark or Trino. DuckDB single-table UPDATE/DELETE/MERGE has its own automated smoke coverage as listed above; this does not establish other engines' DML compatibility.

## Verification

Commands for updating this matrix, release notes, or client claims are maintained in [scripts/table-catalog/README.md](../../scripts/table-catalog/README.md); the unit tests are `scripts/table-catalog/test_*.py`.

## Release Claim Guidance

Acceptable: "RustFS includes a core Iceberg REST Catalog-based S3 Tables implementation with PyIceberg and DuckDB smoke coverage, table-aware S3 data-plane policy checks, controlled maintenance, catalog recovery diagnostics, manual conformance input for Spark, Trino, Databend, and Snowflake, production-failure probe harnesses, disaster-recovery and scale/fault rehearsal probes, and a machine-readable operations evidence guide."

Do not claim: "RustFS is fully compatible with AWS S3 Tables."

Any stronger vendor or engine claim needs a repeatable live harness, the exact client versions, and the expected response shapes recorded in the table-catalog inventories.

## Related

- [Table catalog conformance scripts](../../scripts/table-catalog/README.md)
- [Admin route action snapshot](admin-route-action-snapshot.md)
- [Runtime capability contracts](runtime-capability-contracts.md)
