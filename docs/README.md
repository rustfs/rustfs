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

Other runbooks remain grouped by filename in [`operations/`](operations/);
architecture pages link to the relevant runbook where a cross-boundary
procedure is required.
