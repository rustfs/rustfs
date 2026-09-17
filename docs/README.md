# Documentation

Use the focused indexes rather than treating this directory as an unordered
collection:

- [Architecture knowledge base](architecture/README.md)
- [Testing references](testing/README.md)

## Operations

Operational runbooks live under [`operations/`](operations/). Replication
operators should start with:

| Runbook | Use it for |
|---|---|
| [Site replication operations](operations/site-replication-operations.md) | Health fields, pending operations, outage recovery, re-pair admission, IAM/SSE boundaries, and upgrades. |
| [Replication target check](operations/replication-check.md) | Validating an S3 destination and version fidelity before enabling replication. |
| [Replication object size limits](operations/replication-object-size-limits.md) | Multipart routing, large-object limits, and retry characteristics. |
| [Replication outbound transport](operations/replication-outbound-transport.md) | Integrity headers, generic target behavior, and transport knobs. |

For persisted administrator bucket tasks and bucket recreation, see
[Bucket heal recovery](operations/bucket-heal-recovery.md).

For disk replacement across VM restarts and schema 5/6 maintenance migration,
see [Replacement generation recovery](operations/replacement-generation-recovery.md).

For historical GET timeouts during PUT or Heal, see
[Object lock contention diagnostics](operations/object-lock-contention.md).

Other runbooks remain grouped by filename in [`operations/`](operations/);
architecture pages link to the relevant runbook where a cross-boundary
procedure is required.

For storage dashboards, see [Storage metrics and observer selection](operations/storage-metrics.md):
drive ownership, snapshot freshness, counter queries, and rolling upgrades.

For optional shard commitments, see [Independent shard integrity rollout](operations/shard-integrity-rollout.md):
activation, legacy repair results, multipart mode changes, and rollback limits.

For crates.io publication of workspace crates, see
[Workspace Cargo Publish](operations/cargo-publish-workspace.md): dependency
ordering, dry-run, publish, and failure handling.
