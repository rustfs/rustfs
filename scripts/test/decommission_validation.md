# Decommission Validation

This helper covers a practical decommission smoke flow against a disposable multi-pool cluster that is already running and reachable via `mc`.

## What It Covers

- Versioned object rewrite
- Multipart object rewrite
- Delete-marker preservation
- Object-lock bucket readability after decommission
- Admin decommission start/status/cancel command flow

## What It Does Not Cover Automatically

- Remote tier object migration
- Lifecycle-expired object skipping
- Replication target side-effects
- Cross-node failure injection during decommission

Those scenarios still need cluster-specific setup and should be verified manually after the smoke flow passes.

## Prerequisites

- `mc` installed and configured with an alias that points to the RustFS cluster
- A disposable cluster with at least two pools
- A pool cmdline string that matches the admin API input, for example:
  `http://server{5...8}/disk{1...4}`

## Recommended Flow

1. Prepare data:
   `./scripts/test/decommission_validation.sh prepare <alias>`
2. Start decommission:
   `./scripts/test/decommission_validation.sh start <alias> '<pool-cmdline>'`
3. Wait for completion:
   `./scripts/test/decommission_validation.sh wait <alias> '<pool-cmdline>' 900`
4. Verify readable data and version listings:
   `./scripts/test/decommission_validation.sh verify <alias>`

## Optional Cancel Check

1. Start decommission on a disposable pool.
2. Run:
   `./scripts/test/decommission_validation.sh cancel <alias> '<pool-cmdline>'`
3. Confirm status shows `canceled=true` and not `complete=true`.

## Manual Extension For Tiered Objects

If the cluster already has a configured remote tier and ILM transition rule:

1. Upload an object that matches the transition rule.
2. Wait until the object is transitioned remotely.
3. Run the decommission flow above.
4. Confirm:
   - the object is still readable after decommission
   - decommission completes without `failed=true`
   - no residual object/version remains on the source pool
