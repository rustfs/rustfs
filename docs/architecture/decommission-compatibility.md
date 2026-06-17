# Decommission Compatibility Scope

This note records the current RustFS decommission contract for admin/API
compatibility reviews.

## Current Contract

RustFS supports one target pool per decommission start request.

The admin handler accepts the request shape used by the MinIO-compatible admin
API, including comma-separated pool targets. The storage layer intentionally
rejects requests that resolve to more than one target pool with:

```text
failed to start decommission: decommission supports one target pool at a time
```

This is the supported RustFS behavior for the current remediation scope. It is
more conservative than MinIO's queued multi-pool decommission model, where
multiple pools can be submitted and processed serially.

## Out Of Scope

Queued multi-pool decommission is not part of the current implementation. Adding
it requires a separate product-approved design covering:

- persisted queue format and validation;
- restart recovery for active and queued pools;
- status semantics for active, queued, canceled, failed, and completed pools;
- one-active-pool-at-a-time worker scheduling;
- admin compatibility for partial queue failures and duplicate pool targets.

Until that design lands, clients should submit at most one decommission target
pool per request.

## Regression Guard

The single-pool contract is guarded by
`test_contextualized_decommission_start_request_rejects_multiple_target_pools`
in `crates/ecstore/src/pools.rs`.
