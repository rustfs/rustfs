# Tier Lifecycle Cleanup Stress Test

## Goal

Verify concurrent GET, ILM expiry, and tier free-version recovery do not produce `NoSuchVersion`, unbounded queue growth, or severe throughput regression.

## Setup

1. Start a RustFS cluster with a configured remote tier.
2. Upload at least 10,000 objects and transition them to the remote tier.
3. Configure lifecycle expiry for the transitioned prefix.
4. Enable lifecycle logs:

```bash
export RUST_LOG="info,rustfs_ecstore::bucket::lifecycle=debug"
```

## Workload

Run concurrent GET while triggering expiry:

```bash
seq 1 64 | xargs -I{} -P64 sh -c 'while true; do aws s3api get-object --bucket "$BUCKET" --key "prefix/obj-$((RANDOM % 10000))" /tmp/out.$$ >/dev/null 2>>get-errors.log; done'
```

## Required Checks

- `get-errors.log` contains no `NoSuchVersion`.
- Lifecycle queue pending count does not grow without bound.
- Missed free-version and tier journal counters may increase during injected failures, but later recovery drains persisted records.
- Recovery logs show cursor advancement across bucket and object markers.
- Duplicate recovery enqueue count stays bounded while remote tier delete is failing.
- Queue full or backpressure stops the current recovery pass instead of blocking normal lifecycle enqueue.
- Scan duration and scanned metadata entry count stay within the configured per-pass budget.
- p95 GET latency and lifecycle expiry throughput are recorded before and after the fix.

## Fault Injection

Repeat the workload while:

- stopping lifecycle workers
- forcing remote tier delete to fail
- restarting the RustFS process after local delete but before remote cleanup

After recovery, verify remote tier garbage does not remain for expired objects.
