#!/usr/bin/env python3
"""Machine-readable production failure coverage helpers for RustFS S3 Tables."""

from __future__ import annotations

import argparse
import json
from io import StringIO
from typing import Any


def failure_case(
    case: str,
    coverage_status: str,
    expected_behavior: str,
    evidence: str,
) -> dict[str, str]:
    return {
        "case": case,
        "coverage_status": coverage_status,
        "expected_behavior": expected_behavior,
        "evidence": evidence,
    }


def production_failure_matrix() -> list[dict[str, str]]:
    return [
        failure_case(
            "commit-cas-conflict",
            "server-tests-plus-live-probe-required",
            "conflict-without-pointer-advance",
            "standard commit validation rejects stale expected token or base metadata before advancing the table pointer",
        ),
        failure_case(
            "post-cas-finalization-gap",
            "diagnostics-and-recovery-probe-required",
            "recoverable-diagnostics-and-idempotency-repair",
            "catalog diagnostics and recovery routes must report staged/finalization gaps and repair idempotency indexes without moving the pointer",
        ),
        failure_case(
            "missing-referenced-object",
            "server-tests-plus-live-probe-required",
            "fail-closed-before-pointer-advance",
            "commit validation and maintenance reachability must reject missing metadata, manifest, data, or delete objects",
        ),
        failure_case(
            "concurrent-writer-stress",
            "load-test-required",
            "single-winner-cas-and-retryable-conflicts",
            "multiple writers against one table should leave one committed pointer and retryable conflicts for stale writers",
        ),
        failure_case(
            "permission-negative",
            "server-tests-plus-live-probe-required",
            "deny-without-data-plane-bypass",
            "table catalog actions and ordinary S3 object actions must both enforce table-scoped permissions",
        ),
        failure_case(
            "maintenance-stale-plan",
            "server-tests-plus-live-probe-required",
            "manual-review-or-conflict-without-delete",
            "maintenance delete/commit requests must re-read current pointer and fail closed when the plan is stale",
        ),
        failure_case(
            "external-bridge-conflict",
            "server-tests-plus-live-probe-required",
            "sync-conflict-without-pointer-advance",
            "external catalog sync must not advance pointer/token/generation on UUID mismatch or stale external metadata token",
        ),
        failure_case(
            "backing-migration-blocked",
            "diagnostics-probe-required",
            "blocked-until-recovery-and-replay-are-clean",
            "catalog export and diagnostics must expose WAL/recovery blockers before any strong backing cutover",
        ),
    ]


def catalog_prefix(rest_path: str) -> str:
    stripped = rest_path.strip()
    if not stripped:
        raise ValueError("REST catalog path cannot be empty")
    if not stripped.startswith("/"):
        stripped = f"/{stripped}"
    stripped = stripped.rstrip("/")
    if not stripped.endswith("/v1"):
        stripped = f"{stripped}/v1"
    return stripped


def table_path(warehouse: str, namespace: str, table: str, suffix: str = "", rest_path: str = "/iceberg") -> str:
    base = f"{catalog_prefix(rest_path)}/{warehouse}/namespaces/{namespace}/tables/{table}"
    return f"{base}{suffix}"


def warehouse_path(warehouse: str, suffix: str = "", rest_path: str = "/iceberg") -> str:
    base = f"{catalog_prefix(rest_path)}/{warehouse}"
    return f"{base}{suffix}"


def probe_step(
    name: str,
    method: str,
    path: str,
    expected_status: str,
    expected_behavior: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    step: dict[str, Any] = {
        "name": name,
        "method": method,
        "path": path,
        "expected_status": expected_status,
        "expected_behavior": expected_behavior,
    }
    if body is not None:
        step["body"] = body
    return step


def failure_probe_plan(warehouse: str, namespace: str, table: str, rest_path: str = "/iceberg") -> list[dict[str, Any]]:
    table_endpoint = table_path(warehouse, namespace, table, rest_path=rest_path)
    return [
        probe_step(
            "stale-token-commit-conflict",
            "POST",
            table_endpoint,
            "409",
            "stale expected version token is rejected and current metadata pointer remains unchanged",
            {
                "identifier": {"namespace": [namespace], "name": table},
                "expected-version-token": "stale-token-from-previous-load",
                "expected-metadata-location": "current-metadata-location-from-load-table",
                "new-metadata-location": f"s3://{warehouse}/tables/table-id/metadata/conflict_probe.metadata.json",
                "requirements": [
                    {
                        "type": "assert-current-snapshot-id",
                        "snapshot-id": 0,
                    }
                ],
                "updates": [
                    {
                        "action": "set-current-schema",
                        "schema-id": 0,
                    }
                ],
            },
        ),
        probe_step(
            "missing-metadata-object-rejected",
            "POST",
            table_endpoint,
            "400",
            "new metadata object must exist before the catalog pointer can advance",
            {
                "identifier": {"namespace": [namespace], "name": table},
                "expected-version-token": "current-version-token-from-load-table",
                "expected-metadata-location": "current-metadata-location-from-load-table",
                "new-metadata-location": f"s3://{warehouse}/tables/table-id/metadata/does_not_exist.metadata.json",
            },
        ),
        probe_step(
            "diagnostics-after-finalization-gap",
            "GET",
            table_path(warehouse, namespace, table, "/catalog/diagnostics", rest_path),
            "200",
            "diagnostics should surface recoverable commit-log/idempotency gaps with operator actions",
        ),
        probe_step(
            "recovery-repairs-idempotency-index",
            "POST",
            table_path(warehouse, namespace, table, "/catalog/recovery", rest_path),
            "200",
            "recovery should repair stale or missing idempotency indexes without moving the table pointer",
            {
                "mode": "safe-repair",
            },
        ),
        probe_step(
            "maintenance-stale-plan-rejected",
            "POST",
            table_path(warehouse, namespace, table, "/maintenance/metadata", rest_path),
            "409",
            "stale maintenance plans must not delete or commit after the current pointer changes",
            {
                "dry-run": False,
                "expected-metadata-location": "stale-metadata-location",
            },
        ),
        probe_step(
            "external-sync-conflict",
            "POST",
            table_path(warehouse, namespace, table, "/catalog/import", rest_path),
            "409",
            "external catalog sync conflicts must leave pointer, token, and generation unchanged",
            {
                "external-version-token": "stale-external-token",
                "metadata-location": f"s3://{warehouse}/external/metadata/metadata.json",
            },
        ),
    ]


def rehearsal_phase(name: str, objective: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "name": name,
        "objective": objective,
        "steps": steps,
    }


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError("value must be a positive integer") from err
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def disaster_recovery_rehearsal_plan(
    *,
    warehouse: str,
    namespace: str,
    table: str,
    rest_path: str = "/iceberg",
    table_warehouse_location: str | None = None,
) -> dict[str, Any]:
    table_endpoint = table_path(warehouse, namespace, table, rest_path=rest_path)
    table_warehouse_location = table_warehouse_location or f"s3://{warehouse}/tables/table-id"
    return {
        "mode": "manual-or-ci-optional",
        "ci_gate": "RUSTFS_TABLE_CATALOG_DR_REHEARSAL=1",
        "preconditions": [
            "record the RustFS build and catalog backing mode before starting",
            "run against a disposable table or a backed-up table warehouse",
            "capture the current metadata location and version token from loadTable",
            "keep object-backed catalog state available until durable backing cutover is accepted",
        ],
        "expected_invariants": [
            "current metadata pointer remains recoverable or deliberately rolled back",
            "recovery repair does not move the table pointer",
            "rollback/import actions require explicit operator-selected metadata locations",
            "durable backing cutover remains blocked while migration blockers are present",
            "post-recovery loadTable and table data-plane policy checks still succeed",
        ],
        "phases": [
            rehearsal_phase(
                "capture-baseline",
                "Capture table and catalog state before injecting or repairing a failure.",
                [
                    probe_step(
                        "export-catalog-state",
                        "GET",
                        table_path(warehouse, namespace, table, "/catalog/export", rest_path),
                        "200",
                        "export includes table entry, current metadata location, commit recovery state, and backing manifest",
                    ),
                    probe_step(
                        "load-table-before-recovery",
                        "GET",
                        table_endpoint,
                        "200",
                        "baseline loadTable returns the current metadata location and version token",
                    ),
                ],
            ),
            rehearsal_phase(
                "diagnose-and-repair",
                "Inspect recovery state and run only safe repair actions.",
                [
                    probe_step(
                        "read-recovery-diagnostics",
                        "GET",
                        table_path(warehouse, namespace, table, "/catalog/diagnostics", rest_path),
                        "200",
                        "diagnostics expose commit recovery state, idempotency index state, recommended actions, and manual-review blockers",
                    ),
                    probe_step(
                        "safe-recovery-repair",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/recovery", rest_path),
                        "200",
                        "safe repair can finalize recoverable commit records or repair idempotency indexes without pointer movement",
                        {
                            "mode": "safe-repair",
                        },
                    ),
                    probe_step(
                        "diagnostics-after-repair",
                        "GET",
                        table_path(warehouse, namespace, table, "/catalog/diagnostics", rest_path),
                        "200",
                        "recovery state is clean or still reports manual-review blockers without advancing table state",
                    ),
                ],
            ),
            rehearsal_phase(
                "rollback-or-import",
                "Exercise explicit operator-selected rollback/import paths.",
                [
                    probe_step(
                        "rollback-to-known-metadata",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/rollback", rest_path),
                        "200-or-409",
                        "rollback commits only a validated operator-selected metadata location, or conflicts without pointer movement",
                        {
                            "metadata-location": "metadata-location-from-catalog-export-or-backup",
                            "version-token": "current-version-token-from-load-table",
                        },
                    ),
                    probe_step(
                        "import-known-metadata",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/import", rest_path),
                        "200-or-409",
                        "import/register validates metadata identity and conflicts without pointer/token/generation advancement when stale",
                        {
                            "metadata-location": "metadata-location-from-catalog-export-or-backup",
                            "properties": {
                                "recovery-source": "catalog-export-or-backup",
                            },
                        },
                    ),
                ],
            ),
            rehearsal_phase(
                "migration-preflight",
                "Verify durable backing cutover remains explainable and fail-closed.",
                [
                    probe_step(
                        "durable-backing-migration-dry-run",
                        "GET",
                        warehouse_path(warehouse, "/catalog/migration", rest_path),
                        "200",
                        "migration blockers must be empty before cutover",
                    ),
                ],
            ),
            rehearsal_phase(
                "post-recovery-validation",
                "Check the recovered table can still be loaded and data-plane policy still resolves to the table.",
                [
                    probe_step(
                        "load-table-after-recovery",
                        "GET",
                        table_endpoint,
                        "200",
                        "loadTable returns the intended current metadata location after repair, rollback, or import",
                    ),
                    probe_step(
                        "table-data-plane-policy-probe",
                        "S3-PROBE",
                        table_warehouse_location,
                        "inside-allowed-outside-denied",
                        "ordinary S3 object access still maps table warehouse objects to the table policy boundary",
                    ),
                    probe_step(
                        "diagnostics-after-recovery",
                        "GET",
                        table_path(warehouse, namespace, table, "/catalog/diagnostics", rest_path),
                        "200",
                        "diagnostics no longer report unexpected recovery blockers after the rehearsal",
                    ),
                ],
            ),
        ],
    }


def scale_fault_rehearsal_plan(
    *,
    warehouse: str,
    namespace: str,
    table: str,
    rest_path: str = "/iceberg",
    table_warehouse_location: str | None = None,
    writer_count: int = 8,
    maintenance_worker_count: int = 2,
    iteration_count: int = 50,
    catalog_backing: str = "object-backed-or-durable-strong",
) -> dict[str, Any]:
    table_endpoint = table_path(warehouse, namespace, table, rest_path=rest_path)
    table_warehouse_location = table_warehouse_location or f"s3://{warehouse}/tables/table-id"
    return {
        "mode": "manual-or-ci-optional",
        "ci_gate": "RUSTFS_TABLE_CATALOG_SCALE_FAULT_REHEARSAL=1",
        "parameters": {
            "writer_count": writer_count,
            "maintenance_worker_count": maintenance_worker_count,
            "iteration_count": iteration_count,
            "catalog_backing": catalog_backing,
        },
        "preconditions": [
            "run against a disposable table or a table restored from backup",
            "record the RustFS build, catalog backing mode, node count, and object backend before starting",
            "capture baseline loadTable metadata location, version token, generation, and warehouse prefix",
            "enable only opt-in clients and workers for this rehearsal run",
        ],
        "expected_invariants": [
            "current metadata generation is monotonic",
            "each conflicting writer cohort has one winner and retryable conflicts for stale writers",
            "maintenance jobs never delete reachable table objects",
            "scheduler and worker leases recover without duplicate active jobs for the same table",
            "durable backing migration stays blocked while recovery or replay blockers exist",
            "rollback/import under load either commits a validated metadata location or conflicts without pointer movement",
        ],
        "phases": [
            rehearsal_phase(
                "concurrent-commit-stress",
                "Stress single-table CAS behavior while preserving pointer and generation invariants.",
                [
                    probe_step(
                        "spawn-concurrent-writers",
                        "CLIENT-STRESS",
                        table_endpoint,
                        "single-winner-per-conflict-cohort",
                        "run concurrent append/commit attempts and record winner commit id, conflicts, final metadata location, token, and generation",
                        {
                            "writer-count": writer_count,
                            "iteration-count": iteration_count,
                        },
                    ),
                    probe_step(
                        "load-table-after-writer-cohort",
                        "GET",
                        table_endpoint,
                        "200",
                        "loadTable returns one current metadata location and a generation that advanced monotonically",
                    ),
                    probe_step(
                        "diagnostics-after-writer-cohort",
                        "GET",
                        table_path(warehouse, namespace, table, "/catalog/diagnostics", rest_path),
                        "200",
                        "diagnostics report no unexpected commit recovery blockers after the writer cohort",
                    ),
                ],
            ),
            rehearsal_phase(
                "maintenance-scheduler-failover",
                "Exercise queued maintenance, worker claim, expired lease recovery, and stale plan rejection.",
                [
                    probe_step(
                        "queue-maintenance-job",
                        "POST",
                        table_path(warehouse, namespace, table, "/maintenance/scheduler/run", rest_path),
                        "200",
                        "scheduler queues at most one active maintenance job for the table",
                        {
                            "scheduler-id": "scale-fault-rehearsal-scheduler",
                        },
                    ),
                    probe_step(
                        "claim-worker-job",
                        "POST",
                        table_path(warehouse, namespace, table, "/maintenance/worker/run", rest_path),
                        "200-or-409",
                        "one worker claims or executes the queued job while peer workers observe backpressure or no work",
                        {
                            "worker-count": maintenance_worker_count,
                        },
                    ),
                    probe_step(
                        "recover-expired-lease",
                        "POST",
                        table_path(warehouse, namespace, table, "/maintenance/scheduler/run", rest_path),
                        "200-or-409",
                        "expired scheduler or worker leases are recovered without creating duplicate active jobs",
                        {
                            "scheduler-id": "scale-fault-rehearsal-recovery",
                        },
                    ),
                    probe_step(
                        "stale-maintenance-plan-check",
                        "POST",
                        table_path(warehouse, namespace, table, "/maintenance/metadata", rest_path),
                        "409-or-manual-review",
                        "stale maintenance plans fail closed before deleting objects or committing metadata",
                        {
                            "dry-run": False,
                            "expected-metadata-location": "stale-metadata-location-from-before-writer-cohort",
                        },
                    ),
                ],
            ),
            rehearsal_phase(
                "durable-backing-cutover-under-load",
                "Verify durable backing cutover preflight remains explainable under recent write and maintenance churn.",
                [
                    probe_step(
                        "capture-cutover-backup",
                        "OPERATOR",
                        table_path(warehouse, namespace, table, "/catalog/export", rest_path),
                        "catalog-backup-recorded",
                        "record catalog-backup artifacts, current metadata location, idempotency indexes, and rollback mode before cutover",
                    ),
                    probe_step(
                        "migration-dry-run-before-cutover",
                        "GET",
                        warehouse_path(warehouse, "/catalog/migration", rest_path),
                        "200",
                        "migration dry-run reports inventory, replay blockers, idempotency blockers, and recommended actions",
                    ),
                ],
            ),
            rehearsal_phase(
                "recovery-rollback-import-under-load",
                "Exercise recovery and operator-selected rollback/import paths after stress activity.",
                [
                    probe_step(
                        "safe-recovery-repair-under-load",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/recovery", rest_path),
                        "200",
                        "safe repair handles recoverable idempotency gaps without moving the table pointer",
                        {
                            "mode": "safe-repair",
                        },
                    ),
                    probe_step(
                        "rollback-conflict-check",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/rollback", rest_path),
                        "200-or-409",
                        "rollback either commits the operator-selected metadata location or conflicts without pointer movement",
                        {
                            "metadata-location": "metadata-location-from-backup",
                            "version-token": "current-version-token-from-load-table",
                        },
                    ),
                    probe_step(
                        "import-conflict-check",
                        "POST",
                        table_path(warehouse, namespace, table, "/catalog/import", rest_path),
                        "200-or-409",
                        "import validates metadata identity and conflicts without pointer/token/generation movement when stale",
                        {
                            "metadata-location": "metadata-location-from-backup",
                            "properties": {
                                "rehearsal-source": "scale-fault-backup",
                            },
                        },
                    ),
                ],
            ),
            rehearsal_phase(
                "post-run-evidence",
                "Record the evidence needed before any scale or fault claim can be promoted.",
                [
                    probe_step(
                        "load-table-after-scale-fault-run",
                        "GET",
                        table_endpoint,
                        "200",
                        "loadTable returns the final intended metadata location, version token, and generation",
                    ),
                    probe_step(
                        "table-data-plane-policy-probe",
                        "S3-PROBE",
                        table_warehouse_location,
                        "inside-allowed-outside-denied",
                        "ordinary S3 object access still maps table warehouse objects to the table policy boundary",
                    ),
                    probe_step(
                        "capture-run-artifacts",
                        "EVIDENCE",
                        "operator-recorded-artifacts",
                        "recorded",
                        "record RustFS build, catalog backing, writer count, worker count, iteration count, final metadata location, observed conflicts, recovered leases, and failed-closed operations",
                    ),
                ],
            ),
        ],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print RustFS S3 Tables production failure coverage helpers.")
    parser.add_argument("--warehouse", default="rustfs-s3table-smoke")
    parser.add_argument("--namespace", default="smoke")
    parser.add_argument("--table", default="events")
    parser.add_argument("--rest-path", default="/iceberg")
    parser.add_argument("--table-warehouse-location")
    parser.add_argument("--writer-count", type=positive_int, default=8)
    parser.add_argument("--maintenance-worker-count", type=positive_int, default=2)
    parser.add_argument("--iteration-count", type=positive_int, default=50)
    parser.add_argument("--catalog-backing", default="object-backed-or-durable-strong")
    parser.add_argument("--print-failure-matrix", action="store_true")
    parser.add_argument("--print-failure-probes", action="store_true")
    parser.add_argument("--print-disaster-recovery-rehearsal", action="store_true")
    parser.add_argument("--print-scale-fault-rehearsal", action="store_true")
    return parser.parse_args(argv)


def print_json(document: Any, output: StringIO | None = None) -> None:
    text = json.dumps(document, indent=2, sort_keys=True)
    if output is None:
        print(text)
    else:
        output.write(f"{text}\n")


def cli_json(argv: list[str]) -> str:
    output = StringIO()
    run(parse_args(argv), output)
    return output.getvalue()


def run(args: argparse.Namespace, output: StringIO | None = None) -> None:
    printed = False
    if args.print_failure_matrix:
        print_json({"production_failure_coverage": production_failure_matrix()}, output)
        printed = True
    if args.print_failure_probes:
        print_json(
            {
                "failure_probe_plan": failure_probe_plan(
                    warehouse=args.warehouse,
                    namespace=args.namespace,
                    table=args.table,
                    rest_path=args.rest_path,
                )
            },
            output,
        )
        printed = True
    if args.print_disaster_recovery_rehearsal:
        print_json(
            {
                "disaster_recovery_rehearsal": disaster_recovery_rehearsal_plan(
                    warehouse=args.warehouse,
                    namespace=args.namespace,
                    table=args.table,
                    rest_path=args.rest_path,
                    table_warehouse_location=args.table_warehouse_location,
                )
            },
            output,
        )
        printed = True
    if args.print_scale_fault_rehearsal:
        print_json(
            {
                "scale_fault_rehearsal": scale_fault_rehearsal_plan(
                    warehouse=args.warehouse,
                    namespace=args.namespace,
                    table=args.table,
                    rest_path=args.rest_path,
                    table_warehouse_location=args.table_warehouse_location,
                    writer_count=args.writer_count,
                    maintenance_worker_count=args.maintenance_worker_count,
                    iteration_count=args.iteration_count,
                    catalog_backing=args.catalog_backing,
                )
            },
            output,
        )
        printed = True
    if not printed:
        print_json({"production_failure_coverage": production_failure_matrix()}, output)


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
