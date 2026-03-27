# Rebalance Validation

This helper covers a practical rebalance smoke flow against a disposable multi-pool cluster that is already running and reachable via `mc`.

## What It Covers

- Admin rebalance start command flow
- Admin rebalance status polling
- Admin rebalance stop command flow
- Completion checks while buckets are still readable

## Prerequisites

- A disposable cluster with at least two pools
- An `mc` alias that points to the target cluster

If `mc` is not installed locally, set `MC_MODE=docker` and the helper will use `minio/mc` in a container with a persistent config directory.

## Recommended Flow

1. Start rebalance:
   `./scripts/test/rebalance_validation.sh start <alias>`
2. Wait for completion:
   `./scripts/test/rebalance_validation.sh wait <alias> 1800`
3. Inspect final status:
   `./scripts/test/rebalance_validation.sh status <alias>`

## Optional Stop Check

1. Start rebalance on a disposable cluster.
2. Run:
   `./scripts/test/rebalance_validation.sh stop <alias>`
3. Confirm the subsequent status output no longer reports `Started`.
