# Storage metrics and observer selection

**Use this when:** configuring storage dashboards, migrating from replicated
per-drive metrics, or diagnosing stale and missing disk observations.

## Ownership and identity

The storage collectors have two distinct scopes:

| Scope | Metric families | Meaning |
|---|---|---|
| `collection_scope="local"` | `rustfs_system_drive_*`, `rustfs_node_disk_*` | Detailed metrics for drives owned by the reporting node; includes configured offline slots. Drive counts are local counts. |
| `collection_scope="cluster"` | Cluster capacity, health, objects/buckets, erasure sets, and `rustfs_cluster_drive_*` | One reporting node's observation of the whole cluster. |

`observer` identifies the reporting node. `server` identifies the drive owner;
`drive` alone is not a unique disk key. Keep `rustfs_cluster_id`, `server`, and
`drive` when grouping drives, and retain pool/set/drive indices for topology.
Drive counters also carry `disk_id`, so a physical replacement starts a separate
series. Unknown IDs are empty; `rustfs_system_drive_info` is emitted only when
the ID and topology are known. `rustfs_system_drive_present` preserves a configured
slot even when its disk is disconnected and its ID is unavailable.

Global drive metrics contain membership, runtime state, capacity and its source.
They do not duplicate remote API counters, error counters, or detailed I/O metrics.
An unreachable peer remains in the global inventory with the storage layer's
unknown/offline state and stale/missing capacity provenance. Missing capacity is
omitted from per-drive byte metrics. Cluster capacity may still include cached
observations: check `capacity_stale_drives` and `capacity_missing_drives` before
interpreting it. The existing cluster offline count includes drives not currently
observed online; use global per-drive runtime states to distinguish unknown.

## Configure the pipeline

Give every node of a deployment the same stable, unique resource attribute:

```bash
OTEL_RESOURCE_ATTRIBUTES=rustfs.cluster.id=production-a
```

Keep existing resource attributes in the comma-separated value. The example
Compose files accept `OTEL_RESOURCE_ATTRIBUTES` and default to the development
cluster ID `rustfs-dev`; set a distinct ID for each deployment. The Collector's
`resource_to_telemetry_conversion.enabled: true` promotes it to
`rustfs_cluster_id` on scraped samples. Prometheus `external_labels` are not a
substitute: they do not add a cluster label to the local time series.

Use the shipped Collector configuration with `send_timestamps: true` and keep
Prometheus's default `honor_timestamps: true`. Load
`.docker/observability/prometheus-rules/rustfs-storage.yml`. Use one ingestion
route per deployment/node; scraping replicas of the same Collector requires a
separate HA deduplication policy. Keep node and Prometheus clocks synchronized.

Storage export callbacks read the current snapshot from memory and stop exporting
removed series. They do not probe storage or run peer RPCs. A successful collection
updates `rustfs_storage_snapshot_last_success_timestamp_seconds`; OTLP export
alone does not update this value. The source age limit is three times the larger
of the collection interval and `RUSTFS_OBS_METER_INTERVAL` (with its normal default).
`rustfs_storage_snapshot_max_age_seconds` exports the remaining validity budget
after deducting collection time from that limit. A slow RPC or usage read therefore
cannot make an already expired observation fresh by completing. An unavailable
source leaves the last success unchanged; a stalled collector stops exporting its
old snapshot after the source age limit.

A Collector may cache a point after RustFS stops exporting it. The recording rules
therefore require both a fresh source and a raw point timestamp at least as new as
that source's last successful collection. This also removes cached optional fields
and old disk IDs when a new snapshot no longer contains them. The timestamp check
must run on the raw selector, before label rewriting or recording the value;
otherwise PromQL can substitute the query evaluation time for the original sample
time. The comparison uses Prometheus millisecond precision so co-published points
are not excluded by submillisecond rounding. Current recording rules preserve the
original name in `source_metric`.
When adding a storage metric, add its matching rule to that file.

Collection and export are asynchronous, not a transaction across instruments. A
snapshot published during an export can briefly mix adjacent observations or
withhold a value; the next complete export/collection converges. Source failure visibility is bounded
by the published maximum age plus the scrape/rule intervals. Cached capacity has
its own observation age and state; a fresh collection does not make that capacity
live. Bucket/object counts retain the existing scanner update delay.

## Queries and dashboards

The bundled dashboard requires the storage recording rules. Select one **Storage
cluster** and one fresh **Cluster observer**. Global panels use that observer's
complete view. If it expires, the panel shows no data; select another observer.
Do not sum replicated global totals or independently take maxima/minima of fields
from different observers: they may describe different moments or partitions.

For example, select the observed cluster raw capacity:

```promql
rustfs:storage:current{source_metric="rustfs_cluster_capacity_raw_total_bytes",rustfs_cluster_id="production-a",collection_scope="cluster",observer="node1:9000"}
```

For a per-drive API rate, select owner-scoped counters before `rate`, then require
a current observation. Sum the resulting rates only across the desired drives:

```promql
sum by (rustfs_cluster_id, api) (
  rate(rustfs_system_drive_api_calls_total{rustfs_cluster_id="production-a",collection_scope="local"}[5m])
  and ignoring(source_metric)
  rustfs:storage:current{source_metric="rustfs_system_drive_api_calls_total",rustfs_cluster_id="production-a",collection_scope="local"}
)
```

A missing series is unknown, not zero. Local totals include only fresh owners and
can decrease when a node becomes unreachable. Use a selected global observer for
configured topology and its explicit unknown/stale indicators.

## Rolling upgrades and validation

Install the rules, resource attribute, and dashboard together. During rolling
upgrades, old releases have no `collection_scope`; strict new-scope selectors
exclude their replicated series. Local panels initially contain only upgraded
owners. Global panels become available when an upgraded observer publishes a fresh
snapshot. Historical old-label series remain available for retrospective queries.
Do not combine old and new counter histories into one rate.

Run the PromQL regression fixtures from `.docker/observability/tests`:

```bash
promtool test rules storage-rules.test.yml
```

The native pipeline test in
`crates/e2e_test/src/storage_metric_ownership_test.rs` requires pinned Collector,
Prometheus, previous-release RustFS, and current RustFS executables. Set
`RUSTFS_OTELCOL_BINARY`, `RUSTFS_PROMETHEUS_BINARY`,
`RUSTFS_METRICS_BASELINE_BINARY`, and `CARGO_BIN_EXE_rustfs` to those files.
Optionally set `RUSTFS_METRICS_E2E_ARTIFACTS` to retain logs and Prometheus data.
Run only this external-tool test:

```bash
cargo test --locked -p e2e_test storage_metric_ownership_pipeline -- --ignored --nocapture
```

The test first reproduces duplicated global details with the previous release,
rolls four nodes forward, verifies owner and disk identity, and checks node loss
and recovery while the Collector remains running. SDK unit tests cover removed
series, replacements, counter resets, invalid updates, and a stalled collection;
PromQL fixtures cover shared paths across clusters/pools and stale cached fields.
