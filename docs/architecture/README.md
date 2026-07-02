# Architecture Documentation

Durable architecture reference for RustFS: migration guardrails, runtime
contracts, boundary rules, and support matrices.

Two rules keep this directory healthy:

1. **Durable reference only.** One-shot implementation plans, task trackers,
   and PR templates live in [`docs/superpowers/plans/`](../superpowers/plans/)
   and are archived there when their work closes.
2. **No copies of other sources of truth.** Crate lists come from
   `Cargo.toml`, CI steps from `.github/workflows/ci.yml`, code structure from
   the code. `scripts/check_doc_paths.sh` fails the pre-commit gate when a
   doc here references a file path that no longer exists.

## Start here

- [overview.md](overview.md) — migration baseline, phase order, core principles

## CI-enforced core (required by `scripts/check_architecture_migration_rules.sh`)

- [crate-boundaries.md](crate-boundaries.md) — dependency direction, PR types, re-export contracts
- [runtime-lifecycle.md](runtime-lifecycle.md) — startup/shutdown sequencing, readiness guarantees
- [readiness-matrix.md](readiness-matrix.md) — request/dependency behavior, probe semantics
- [storage-control-data-plane.md](storage-control-data-plane.md) — storage API contracts, control-plane boundaries
- [global-state-crate-split-plan.md](global-state-crate-split-plan.md) — remaining global-state owners and split evaluation
- [ecstore-module-split-plan.md](ecstore-module-split-plan.md) — ECStore decomposition rules and facade contracts

## Contracts & invariants

- [placement-repair-invariants.md](placement-repair-invariants.md)
- [runtime-capability-contracts.md](runtime-capability-contracts.md)
- [workload-admission-contracts.md](workload-admission-contracts.md)
- [background-controller-contract.md](background-controller-contract.md)
- [config-model-boundary-adr.md](config-model-boundary-adr.md)
- [ecstore-layout-boundary.md](ecstore-layout-boundary.md)
- [decommission-compatibility.md](decommission-compatibility.md)

## Support matrices (release-facing, keep current)

- [s3-compatibility-matrix.md](s3-compatibility-matrix.md)
- [s3-tables-support-matrix.md](s3-tables-support-matrix.md)

## Inventories & baselines (snapshots that feed migration work)

- [global-state-inventory.md](global-state-inventory.md)
- [ecstore-api-facade-inventory.md](ecstore-api-facade-inventory.md)
- [ecstore-config-consumer-inventory.md](ecstore-config-consumer-inventory.md)
- [obs-ecstore-dependency-inventory.md](obs-ecstore-dependency-inventory.md)
- [background-services-inventory.md](background-services-inventory.md)
- [admin-route-action-snapshot.md](admin-route-action-snapshot.md)
- [startup-timeline.md](startup-timeline.md)
- [scheduler-baseline.md](scheduler-baseline.md)
- [profiling-numa-capability-inventory.md](profiling-numa-capability-inventory.md)
- [kms-development-defaults-inventory.md](kms-development-defaults-inventory.md)
- [compat-cleanup-register.md](compat-cleanup-register.md)

Historical plans and trackers (rebalance/decommission phases,
migration-progress ledger) were moved to
[`docs/superpowers/plans/`](../superpowers/plans/) in 2026-07.
