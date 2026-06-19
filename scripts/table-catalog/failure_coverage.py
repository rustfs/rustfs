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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print RustFS S3 Tables production failure coverage helpers.")
    parser.add_argument("--warehouse", default="rustfs-s3table-smoke")
    parser.add_argument("--namespace", default="smoke")
    parser.add_argument("--table", default="events")
    parser.add_argument("--rest-path", default="/iceberg")
    parser.add_argument("--print-failure-matrix", action="store_true")
    parser.add_argument("--print-failure-probes", action="store_true")
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
    if not printed:
        print_json({"production_failure_coverage": production_failure_matrix()}, output)


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
