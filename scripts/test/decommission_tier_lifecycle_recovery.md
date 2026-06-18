# Decommission Tier Lifecycle Recovery

This manual E2E flow covers the Tier + ILM + Decommission interaction path
against a disposable local deployment.

## Goal

Validate that transitioned objects can be expired and recovered safely while
decommission also handles tiered objects without removing remote tier data
incorrectly.

## What It Covers

- RustFS tier configuration backed by a disposable MinIO service.
- Immediate transition of two object prefixes to the configured tier.
- Lifecycle expiry of transitioned objects with RustFS restart recovery.
- Concurrent GET traffic while expiry and cleanup are running.
- Decommission of a pool containing transitioned objects.
- Remote tier count checks before and after decommission.

## What It Does Not Cover Automatically

- True distributed node failure injection.
- Long-running high-cardinality stress with 10,000+ objects.
- Replication target side-effects.
- All lifecycle worker pause/failure modes.

Those scenarios should be added as nightly or cluster-specific E2E coverage
after this manual flow is stable.

## Prerequisites

- Docker with Compose support.
- `mc` configured or available on `PATH`.
- `awscurl` available on `PATH`.
- The local RustFS image used by `docker-compose.decommission.yml`.

If the RustFS image is not present, build it first:

```bash
./scripts/test/decommission_docker.sh build
```

## Quick Start

Run the full flow:

```bash
./scripts/test/decommission_tier_lifecycle_recovery.sh run
```

Clean up all generated containers, data, logs, and state:

```bash
./scripts/test/decommission_tier_lifecycle_recovery.sh reset
```

## Manual Flow

```bash
./scripts/test/decommission_tier_lifecycle_recovery.sh up
./scripts/test/decommission_tier_lifecycle_recovery.sh prepare
./scripts/test/decommission_tier_lifecycle_recovery.sh wait-transition
./scripts/test/decommission_tier_lifecycle_recovery.sh expire-recovery
./scripts/test/decommission_tier_lifecycle_recovery.sh decommission
./scripts/test/decommission_tier_lifecycle_recovery.sh verify
```

## Useful Overrides

- `OBJECT_COUNT=100` increases objects per prefix.
- `TRANSITION_DAYS=0` keeps transition immediate.
- `EXPIRY_DAYS=0` keeps expiry immediate for local validation.
- `WAIT_TIMEOUT_SECONDS=1800` gives slow machines more time.
- `GET_WORKERS=32` increases concurrent GET pressure.
- `GET_LOAD_SECONDS=300` keeps GET traffic running longer.

Example:

```bash
OBJECT_COUNT=100 GET_WORKERS=32 WAIT_TIMEOUT_SECONDS=1800 \
  ./scripts/test/decommission_tier_lifecycle_recovery.sh run
```

## Expected Output

The flow should end with:

- transitioned `expire/` objects no longer readable as current objects
- remote tier object count drained by at least the number of expired objects
- concurrent GET log without `NoSuchVersion` or invalid version-state errors
- transitioned `decom/` objects still readable after decommission
- remote tier object count not lower after decommission

The script stores evidence under `.tmp/decommission-tier-lifecycle/work`,
including scanner status samples, decommission status output, and concurrent
GET stderr.

Missed lifecycle or journal-style counters may increase during restart or queue
pressure, but recovery should leave no residual cleanup work for the expired
objects.

## Nightly Follow-Up

Before wiring this into nightly CI, run it repeatedly with larger `OBJECT_COUNT`
values and collect:

- scanner status samples
- tier backend object counts before and after expiry
- GET error logs
- decommission status JSON
- total runtime and flake rate
