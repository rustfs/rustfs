#!/usr/bin/env python3
"""Unit tests for the RustFS table catalog PyIceberg smoke helper."""

from __future__ import annotations

import json
import os
import re
import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from pathlib import Path
from unittest import mock

import pyiceberg_smoke


INTERNAL_ROADMAP_LABEL_RE = re.compile(r"\b(?:PR|PG|P)[0-9]+\b")
SCRIPT_DIR = Path(__file__).resolve().parent


class PyIcebergSmokeConfigTest(unittest.TestCase):
    def parse_with_args(self, argv: list[str]) -> object:
        env_keys = [
            key
            for key in os.environ
            if key.startswith("RUSTFS_") or key.startswith("AWS_")
        ]
        with mock.patch.object(sys, "argv", ["pyiceberg_smoke.py", *argv]):
            with mock.patch.dict(os.environ, {key: "" for key in env_keys}, clear=False):
                for key in env_keys:
                    os.environ.pop(key, None)
                return pyiceberg_smoke.parse_args()

    def test_default_profile_uses_canonical_rustfs_catalog_uri(self) -> None:
        args = self.parse_with_args(["--endpoint", "http://rustfs.local:9000", "--bucket", "lake"])

        self.assertEqual(args.profile, "rustfs")
        self.assertEqual(args.rest_path, "/iceberg")
        self.assertEqual(args.rest_signing_name, "s3")
        self.assertEqual(pyiceberg_smoke.catalog_properties(args)["uri"], "http://rustfs.local:9000/iceberg")
        self.assertEqual(pyiceberg_smoke.catalog_properties(args)["warehouse"], "lake")

    def test_compat_profile_uses_alias_catalog_and_s3tables_signing_name(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "rustfs-compat",
            "--endpoint",
            "https://rustfs.example",
            "--bucket",
            "warehouse",
        ])

        self.assertEqual(args.rest_path, "/_iceberg")
        self.assertEqual(args.rest_signing_name, "s3tables")
        self.assertEqual(pyiceberg_smoke.catalog_properties(args)["uri"], "https://rustfs.example/_iceberg")

    def test_vendor_profiles_cover_reference_catalogs(self) -> None:
        profiles = pyiceberg_smoke.vendor_profiles()

        self.assertIn("rustfs", profiles)
        self.assertIn("rustfs-compat", profiles)
        self.assertIn("rustfs-vended-credentials", profiles)
        self.assertIn("aws-s3tables", profiles)
        self.assertIn("minio-aistor", profiles)
        self.assertIn("cloudflare-r2-data-catalog", profiles)
        self.assertIn("oss-tables", profiles)
        self.assertEqual(
            profiles["rustfs-vended-credentials"]["credential_mode"],
            "catalog-vended-temporary-credentials",
        )
        self.assertEqual(profiles["minio-aistor"]["rest_signing_name"], "s3tables")
        self.assertEqual(profiles["cloudflare-r2-data-catalog"]["credential_mode"], "catalog-vended")

    def test_vendor_profiles_publish_migration_boundaries(self) -> None:
        profiles = pyiceberg_smoke.vendor_profiles()

        aws = profiles["aws-s3tables"]
        self.assertEqual(aws["compatibility_stage"], "reference-only")
        self.assertEqual(aws["warehouse_shape"], "arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}")
        self.assertEqual(aws["namespace_model"], "single-level")
        self.assertEqual(aws["pagination_model"], "vendor-specific")
        self.assertIn("full AWS S3 Tables API parity", aws["not_claimed"])

        cloudflare = profiles["cloudflare-r2-data-catalog"]
        self.assertEqual(cloudflare["catalog_uri_shape"], "{catalog_uri}")
        self.assertEqual(cloudflare["credential_mode"], "catalog-vended")
        self.assertIn("live RustFS interoperability", cloudflare["not_claimed"])

    def test_aws_reference_profile_formats_s3tables_warehouse_arn(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "aws-s3tables",
            "--endpoint",
            "https://s3tables.us-east-1.amazonaws.com",
            "--region",
            "us-east-1",
            "--account-id",
            "123456789012",
            "--table-bucket",
            "analytics",
        ])

        properties = pyiceberg_smoke.catalog_properties(args)

        self.assertEqual(properties["uri"], "https://s3tables.us-east-1.amazonaws.com/iceberg")
        self.assertEqual(properties["warehouse"], "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics")
        self.assertEqual(properties["rest.signing-name"], "s3tables")

    def test_cloudflare_reference_profile_uses_catalog_uri_and_warehouse_name(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "cloudflare-r2-data-catalog",
            "--catalog-uri",
            "https://catalog.example.com/iceberg",
            "--warehouse-name",
            "analytics",
        ])

        properties = pyiceberg_smoke.catalog_properties(args)

        self.assertEqual(properties["uri"], "https://catalog.example.com/iceberg")
        self.assertEqual(properties["warehouse"], "analytics")

    def test_vended_profile_requires_catalog_credentials_and_keeps_canonical_path(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "rustfs-vended-credentials",
            "--endpoint",
            "http://rustfs.local:9000",
            "--bucket",
            "lake",
        ])

        self.assertTrue(args.require_vended_credentials)
        self.assertEqual(args.rest_path, "/iceberg")
        self.assertEqual(args.rest_signing_name, "s3")
        self.assertEqual(pyiceberg_smoke.catalog_properties(args)["uri"], "http://rustfs.local:9000/iceberg")

    def test_credentials_endpoint_path_uses_encoded_table_identifier(self) -> None:
        args = self.parse_with_args([
            "--bucket",
            "lake bucket",
            "--namespace",
            "sales.analytics",
            "--table",
            "orders table",
        ])

        self.assertEqual(
            pyiceberg_smoke.credentials_endpoint_path(args),
            "/iceberg/v1/lake%20bucket/namespaces/sales.analytics/tables/orders%20table/credentials",
        )

    def test_table_catalog_endpoint_paths_encode_identifier_components(self) -> None:
        args = self.parse_with_args([
            "--bucket",
            "lake bucket",
            "--namespace",
            "sales.analytics",
            "--table",
            "orders table",
        ])

        self.assertEqual(
            pyiceberg_smoke.table_endpoint_path(args),
            "/iceberg/v1/lake%20bucket/namespaces/sales.analytics/tables/orders%20table",
        )
        self.assertEqual(
            pyiceberg_smoke.table_endpoint_path(args, "/metadata-location"),
            "/iceberg/v1/lake%20bucket/namespaces/sales.analytics/tables/orders%20table/metadata-location",
        )
        self.assertEqual(
            pyiceberg_smoke.table_ref_endpoint_path(args, "release/2026"),
            "/iceberg/v1/lake%20bucket/namespaces/sales.analytics/tables/orders%20table/refs/release%2F2026",
        )
        self.assertEqual(
            pyiceberg_smoke.view_endpoint_path(args, "orders view"),
            "/iceberg/v1/lake%20bucket/namespaces/sales.analytics/views/orders%20view",
        )

    def test_default_maintenance_config_is_safe_for_smoke_runs(self) -> None:
        config = pyiceberg_smoke.default_maintenance_config()

        self.assertEqual(config["version"], 1)
        self.assertFalse(config["delete-enabled"])
        self.assertFalse(config["background-enabled"])
        self.assertTrue(config["worker-paused"])
        self.assertEqual(config["max-retry-attempts"], 0)

    def test_safe_ref_segment_matches_rustfs_identifier_segment_rules(self) -> None:
        ref_name = pyiceberg_smoke.safe_ref_segment(
            "sales.analytics",
            "Orders With Spaces And A Very Long Name That Needs To Be Cut Down Safely",
        )

        self.assertLessEqual(len(ref_name), 64)
        self.assertNotIn(".", ref_name)
        self.assertRegex(ref_name, r"^[a-z0-9][a-z0-9_-]*[a-z0-9]$")
        self.assertTrue(ref_name.startswith("smoke-sales-analytics-orders"))

    def test_expected_error_helper_returns_matching_rest_error(self) -> None:
        args = self.parse_with_args([])
        expected = pyiceberg_smoke.RestRequestError("DELETE", "/path", 400, "bad request")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=expected):
            returned = pyiceberg_smoke.signed_rest_request_expect_error(
                args,
                mock.Mock(),
                "DELETE",
                "/path",
                expected_statuses={400},
            )

        self.assertIs(returned, expected)

    def test_expected_error_helper_rejects_wrong_status_or_success(self) -> None:
        args = self.parse_with_args([])
        wrong_status = pyiceberg_smoke.RestRequestError("DELETE", "/path", 404, "missing")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=wrong_status):
            with self.assertRaisesRegex(RuntimeError, "expected one of"):
                pyiceberg_smoke.signed_rest_request_expect_error(
                    args,
                    mock.Mock(),
                    "DELETE",
                    "/path",
                    expected_statuses={400},
                )

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", return_value={}):
            with self.assertRaisesRegex(RuntimeError, "unexpectedly succeeded"):
                pyiceberg_smoke.signed_rest_request_expect_error(args, mock.Mock(), "DELETE", "/path")

    def test_current_snapshot_id_is_read_from_rest_load_table_response(self) -> None:
        self.assertEqual(
            pyiceberg_smoke.current_snapshot_id_from_table_response({"metadata": {"current-snapshot-id": 42}}),
            42,
        )

        with self.assertRaisesRegex(RuntimeError, "metadata"):
            pyiceberg_smoke.current_snapshot_id_from_table_response({})
        with self.assertRaisesRegex(RuntimeError, "current snapshot id"):
            pyiceberg_smoke.current_snapshot_id_from_table_response({"metadata": {}})

    def test_metadata_location_probe_rejects_pointer_mismatch(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])

        with mock.patch.object(
            pyiceberg_smoke,
            "signed_rest_request",
            return_value={"metadata-location": "s3://lake/tables/id/metadata/v1.metadata.json", "version-token": "token-1"},
        ):
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                pyiceberg_smoke.run_metadata_location_probe(
                    args,
                    mock.Mock(),
                    {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json"},
                )

    def test_view_probe_drops_smoke_view_after_load_failure(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        view_path = pyiceberg_smoke.view_endpoint_path(args)
        calls: list[tuple[str, str, object]] = []

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if (method, path) == ("POST", view_path):
                return {}
            if (method, path) == ("GET", view_path):
                return {"identifiers": [{"name": "orders_smoke_view"}]}
            if (method, path) == ("GET", f"{view_path}/orders_smoke_view"):
                return {}
            if (method, path) == ("DELETE", f"{view_path}/orders_smoke_view"):
                return {}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with self.assertRaisesRegex(RuntimeError, "metadata-location"):
                pyiceberg_smoke.run_view_probe(args, mock.Mock())

        self.assertIn(("DELETE", f"{view_path}/orders_smoke_view", None), calls)

    def test_maintenance_probe_rejects_unknown_worker_status(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        config_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/config")
        maintenance_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/metadata")
        job_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1")
        quarantine_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1/quarantine")
        scheduler_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler")
        scheduler_run_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler/run")
        worker_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/worker/run")

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            if (method, path) == ("PUT", config_path):
                return {}
            if (method, path) == ("GET", config_path):
                return {"version": 1}
            if (method, path) == ("POST", maintenance_path):
                return {"job": {"job-id": "job-1"}, "audit-events": [{"action": "PLANNED"}]}
            if (method, path) == ("GET", job_path):
                return {"job": {"job-id": "job-1", "status": "SUCCESSFUL"}, "audit-events": [{"action": "PLANNED"}]}
            if (method, path) == ("POST", quarantine_path):
                return {"action": "INSPECT", "report": {"job": {"job-id": "job-1"}}}
            if (method, path) == ("GET", scheduler_path):
                return {"status": "DISABLED", "audit_timeline": [{"job_id": "job-1", "audit-events": [{"action": "PLANNED"}]}]}
            if (method, path) == ("POST", scheduler_run_path):
                return {
                    "report": {"job": {"job-id": "job-2", "status": "QUEUED", "scheduler-id": "pyiceberg-smoke-scheduler"}},
                    "scheduler": {"status": "QUEUED"},
                }
            if (method, path) == ("POST", worker_path):
                return {"job": {"status": "UNKNOWN"}, "audit-events": [{"action": "WORKER_CONTROL"}]}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with self.assertRaisesRegex(RuntimeError, "stable job status"):
                pyiceberg_smoke.run_maintenance_probe(args, mock.Mock())

    def test_catalog_api_probe_exercises_extended_rest_surfaces(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        deps = mock.Mock()
        calls: list[tuple[str, str, object]] = []
        table_path = pyiceberg_smoke.table_endpoint_path(args)
        metadata_location_path = pyiceberg_smoke.table_endpoint_path(args, "/metadata-location")
        refs_path = pyiceberg_smoke.table_ref_endpoint_path(args)
        ref_name = pyiceberg_smoke.safe_ref_segment(args.namespace, args.table)
        view_path = pyiceberg_smoke.view_endpoint_path(args)
        config_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/config")
        maintenance_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/metadata")
        quarantine_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1/quarantine")
        scheduler_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler")
        scheduler_run_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler/run")
        worker_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/worker/run")
        diagnostics_path = pyiceberg_smoke.table_endpoint_path(args, "/catalog/diagnostics")

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if (method, path) == ("GET", table_path):
                return {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "metadata": {"current-snapshot-id": 7}}
            if (method, path) == ("GET", metadata_location_path):
                return {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "version-token": "token-2"}
            if method == "PUT" and path.startswith(f"{refs_path}/"):
                return {}
            if (method, path) == ("GET", refs_path):
                if sum(1 for call in calls if call[:2] == ("GET", refs_path)) == 1:
                    return {"refs": {ref_name: {"snapshot-id": 7}}}
                return {"refs": {}}
            if method == "DELETE" and path.startswith(f"{refs_path}/"):
                return {}
            if (method, path) == ("POST", view_path):
                return {}
            if (method, path) == ("GET", view_path):
                return {"identifiers": [{"name": "orders_smoke_view"}]}
            if (method, path) == ("GET", f"{view_path}/orders_smoke_view"):
                return {"metadata-location": "s3://lake/views/orders_smoke_view/metadata/v1.json"}
            if (method, path) == ("DELETE", f"{view_path}/orders_smoke_view"):
                return {}
            if (method, path) == ("PUT", config_path):
                return {}
            if (method, path) == ("GET", config_path):
                return {"version": 1}
            if (method, path) == ("POST", maintenance_path):
                return {"job": {"job-id": "job-1"}, "audit-events": [{"action": "PLANNED"}]}
            if (method, path) == ("GET", pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1")):
                return {"job": {"job-id": "job-1", "status": "SUCCESSFUL"}, "audit-events": [{"action": "PLANNED"}]}
            if (method, path) == ("POST", quarantine_path):
                return {"action": "INSPECT", "report": {"job": {"job-id": "job-1"}}}
            if (method, path) == ("GET", scheduler_path):
                return {"status": "DISABLED", "audit_timeline": [{"job_id": "job-1", "audit-events": [{"action": "PLANNED"}]}]}
            if (method, path) == ("POST", scheduler_run_path):
                return {
                    "report": {"job": {"job-id": "job-2", "status": "QUEUED", "scheduler-id": "pyiceberg-smoke-scheduler"}},
                    "scheduler": {"status": "QUEUED"},
                }
            if (method, path) == ("POST", worker_path):
                return {"job": {"status": "PAUSED"}, "audit-events": [{"action": "WORKER_CONTROL"}]}
            if (method, path) == ("GET", diagnostics_path):
                return {"status": "ok"}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with mock.patch.object(
                pyiceberg_smoke,
                "signed_rest_request_expect_error",
                return_value=pyiceberg_smoke.RestRequestError(
                    "DELETE",
                    f"{refs_path}/{ref_name}",
                    400,
                    "snapshot ref has retention policy; force is required",
                ),
            ) as expect_error:
                pyiceberg_smoke.run_catalog_api_probes(args, deps)

        expect_error.assert_called_once_with(
            args,
            deps,
            "DELETE",
            f"{refs_path}/{ref_name}",
            {},
            expected_statuses={400},
        )
        self.assertIn(("GET", metadata_location_path, None), calls)
        self.assertIn(("GET", diagnostics_path, None), calls)
        self.assertIn(("GET", scheduler_path, None), calls)
        self.assertIn(("POST", scheduler_run_path, {"scheduler-id": "pyiceberg-smoke-scheduler"}), calls)
        self.assertIn(("POST", quarantine_path, {"action": "INSPECT"}), calls)
        self.assertIn(("POST", worker_path, {}), calls)

    def test_table_ref_probe_force_deletes_smoke_ref_after_validation_failure(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        refs_path = pyiceberg_smoke.table_ref_endpoint_path(args)
        calls: list[tuple[str, str, object]] = []

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if method == "PUT" and path.startswith(f"{refs_path}/"):
                return {}
            if (method, path) == ("GET", refs_path):
                return {"refs": {}}
            if method == "DELETE" and path.startswith(f"{refs_path}/"):
                return {}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with self.assertRaisesRegex(RuntimeError, "smoke tag"):
                pyiceberg_smoke.run_table_ref_probe(args, mock.Mock(), 7)

        self.assertIn(
            ("DELETE", f"{refs_path}/smoke-sales-orders", {"force": True}),
            calls,
        )

    def run_smoke_with_fakes(self, args: object, probe_calls: list[str]) -> list[str]:
        events: list[str] = []

        class FakeArrowTable:
            num_rows = 2

        class FakePyArrowTableFactory:
            @staticmethod
            def from_pylist(rows: list[dict[str, object]], *, schema: object) -> object:
                events.append(f"rows:{len(rows)}")
                return object()

        class FakePyArrow:
            Table = FakePyArrowTableFactory

            @staticmethod
            def int64() -> str:
                return "int64"

            @staticmethod
            def string() -> str:
                return "string"

            @staticmethod
            def field(name: str, field_type: str, *, nullable: bool) -> tuple[str, str, bool]:
                return (name, field_type, nullable)

            @staticmethod
            def schema(fields: list[tuple[str, str, bool]]) -> tuple[tuple[str, str, bool], ...]:
                return tuple(fields)

        class FakeScan:
            def to_arrow(self) -> FakeArrowTable:
                events.append("scan")
                return FakeArrowTable()

        class FakeTable:
            metadata = SimpleNamespace(location="s3://lake/tables/table-id")

            def append(self, rows: object) -> None:
                events.append("append")

            def scan(self) -> FakeScan:
                return FakeScan()

        class FakeCatalog:
            def create_table(self, identifier: tuple[str, str], *, schema: object) -> FakeTable:
                events.append(f"create:{'.'.join(identifier)}")
                return FakeTable()

            def load_table(self, identifier: tuple[str, str]) -> FakeTable:
                events.append(f"load:{'.'.join(identifier)}")
                return FakeTable()

        deps = SimpleNamespace(pyarrow=FakePyArrow(), load_catalog=lambda *_args, **_kwargs: FakeCatalog())
        storage_credential = pyiceberg_smoke.StorageCredential(
            prefix="s3://lake/tables/table-id/",
            config={
                "s3.access-key-id": "temp-access",
                "s3.secret-access-key": "temp-secret",
                "s3.session-token": "temp-token",
            },
        )

        with mock.patch.object(pyiceberg_smoke, "ensure_local_proxy_bypass"):
            with mock.patch.object(pyiceberg_smoke, "ensure_aws_env"):
                with mock.patch.object(pyiceberg_smoke, "ensure_bucket"):
                    with mock.patch.object(pyiceberg_smoke, "enable_table_bucket"):
                        with mock.patch.object(pyiceberg_smoke, "install_rustfs_rest_sigv4_adapter"):
                            with mock.patch.object(pyiceberg_smoke, "ensure_namespace"):
                                with mock.patch.object(
                                    pyiceberg_smoke,
                                    "load_table_storage_credential",
                                    side_effect=lambda *_args: (events.append("load-vended-credential"), storage_credential)[1],
                                ):
                                    with mock.patch.object(
                                        pyiceberg_smoke,
                                        "verify_vended_credential_data_plane_scope",
                                        side_effect=lambda *_args: events.append("verify-vended-scope"),
                                    ):
                                        with mock.patch.object(
                                            pyiceberg_smoke,
                                            "run_catalog_api_probes",
                                            side_effect=lambda *_args: (
                                                events.append("catalog-probes"),
                                                probe_calls.append("catalog-probes"),
                                            ),
                                        ):
                                            with redirect_stdout(StringIO()):
                                                pyiceberg_smoke.run_smoke(args, deps)
        return events

    def test_run_smoke_probes_extended_catalog_apis_by_default(self) -> None:
        args = self.parse_with_args(["--bucket", "lake", "--namespace", "sales", "--table", "orders"])
        probe_calls: list[str] = []

        events = self.run_smoke_with_fakes(args, probe_calls)

        self.assertEqual(probe_calls, ["catalog-probes"])
        self.assertLess(events.index("scan"), events.index("catalog-probes"))
        self.assertEqual(events[-1], "catalog-probes")

    def test_run_smoke_can_skip_extended_catalog_api_probes(self) -> None:
        args = self.parse_with_args([
            "--bucket",
            "lake",
            "--namespace",
            "sales",
            "--table",
            "orders",
            "--skip-catalog-api-probes",
        ])
        probe_calls: list[str] = []

        events = self.run_smoke_with_fakes(args, probe_calls)

        self.assertEqual(probe_calls, [])
        self.assertIn("append", events)
        self.assertIn("scan", events)

    def test_run_smoke_vended_credential_flow_still_runs_catalog_api_probes(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "rustfs-vended-credentials",
            "--bucket",
            "lake",
            "--namespace",
            "sales",
            "--table",
            "orders",
        ])
        probe_calls: list[str] = []

        events = self.run_smoke_with_fakes(args, probe_calls)

        self.assertEqual(probe_calls, ["catalog-probes"])
        self.assertLess(events.index("load-vended-credential"), events.index("verify-vended-scope"))
        self.assertLess(events.index("verify-vended-scope"), events.index("append"))
        self.assertLess(events.index("scan"), events.index("catalog-probes"))

    def test_smoke_view_request_uses_stable_iceberg_view_shape(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])

        request = pyiceberg_smoke.smoke_view_request(args, "orders_view", 7, "SELECT id FROM sales.orders")

        self.assertEqual(request["name"], "orders_view")
        self.assertEqual(request["schema"]["type"], "struct")
        self.assertEqual(request["view-version"]["version-id"], 7)
        self.assertEqual(request["view-version"]["representations"][0]["dialect"], "spark")
        self.assertEqual(request["properties"]["rustfs.smoke.table"], "orders")

    def test_catalog_properties_can_use_vended_storage_credentials(self) -> None:
        args = self.parse_with_args([
            "--access-key",
            "root-access",
            "--secret-key",
            "root-secret",
        ])
        credential = pyiceberg_smoke.storage_credential_from_response(
            {
                "storage-credentials": [
                    {
                        "prefix": "s3://lake/tables/table-id/",
                        "config": {
                            "s3.access-key-id": "temp-access",
                            "s3.secret-access-key": "temp-secret",
                            "s3.session-token": "temp-token",
                            "rustfs.credential-mode": "catalog-vended-temporary-credentials",
                        },
                    }
                ]
            }
        )

        properties = pyiceberg_smoke.catalog_properties(args, storage_credential=credential)

        self.assertEqual(properties["s3.access-key-id"], "temp-access")
        self.assertEqual(properties["s3.secret-access-key"], "temp-secret")
        self.assertEqual(properties["s3.session-token"], "temp-token")
        self.assertEqual(properties["rustfs.credential-mode"], "catalog-vended-temporary-credentials")

    def test_empty_vended_credentials_response_is_rejected(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "no storage credentials"):
            pyiceberg_smoke.storage_credential_from_response({"storage-credentials": []})

    def test_storage_credential_prefix_parses_bucket_and_key_prefix(self) -> None:
        bucket, key_prefix = pyiceberg_smoke.s3_scope_from_credential(
            pyiceberg_smoke.StorageCredential(
                prefix="s3://lake/tables/table-id/",
                config={
                    "s3.access-key-id": "temp-access",
                    "s3.secret-access-key": "temp-secret",
                    "s3.session-token": "temp-token",
                },
            )
        )

        self.assertEqual(bucket, "lake")
        self.assertEqual(key_prefix, "tables/table-id/")

    def test_s3_scope_from_uri_decodes_equivalent_prefix_encoding(self) -> None:
        bucket, key_prefix = pyiceberg_smoke.s3_scope_from_uri(
            "s3://lake/tables/table%2Did/",
            "storage credential prefix",
        )

        self.assertEqual(bucket, "lake")
        self.assertEqual(key_prefix, "tables/table-id/")

    def test_storage_credential_prefix_rejects_bucket_scope(self) -> None:
        credential = pyiceberg_smoke.StorageCredential(
            prefix="s3://lake",
            config={
                "s3.access-key-id": "temp-access",
                "s3.secret-access-key": "temp-secret",
                "s3.session-token": "temp-token",
            },
        )

        with self.assertRaisesRegex(RuntimeError, "object prefix"):
            pyiceberg_smoke.s3_scope_from_credential(credential)

    def test_table_warehouse_location_is_read_from_table_metadata(self) -> None:
        class FakeMetadata:
            location = "s3://lake/tables/table-id"

        class FakeTable:
            metadata = FakeMetadata()

        self.assertEqual(pyiceberg_smoke.table_warehouse_location(FakeTable()), "s3://lake/tables/table-id")

    def test_scope_probe_keys_are_inside_and_outside_the_vended_prefix(self) -> None:
        inside_key, denied_key = pyiceberg_smoke.scope_probe_keys("tables/table-id/", "namespace", "table")

        self.assertTrue(inside_key.startswith("tables/table-id/"))
        self.assertFalse(denied_key.startswith("tables/table-id/"))
        self.assertIn("namespace-table", inside_key)
        self.assertIn("namespace-table", denied_key)

    def test_vended_s3_client_uses_session_token(self) -> None:
        args = self.parse_with_args(["--endpoint", "http://rustfs.local:9000"])
        credential = pyiceberg_smoke.StorageCredential(
            prefix="s3://lake/tables/table-id/",
            config={
                "s3.access-key-id": "temp-access",
                "s3.secret-access-key": "temp-secret",
                "s3.session-token": "temp-token",
            },
        )
        boto3 = mock.Mock()
        deps = mock.Mock(boto3=boto3, botocore_config=mock.Mock())

        pyiceberg_smoke.vended_s3_client(args, deps, credential)

        boto3.client.assert_called_once()
        call_kwargs = boto3.client.call_args.kwargs
        self.assertEqual(call_kwargs["aws_access_key_id"], "temp-access")
        self.assertEqual(call_kwargs["aws_secret_access_key"], "temp-secret")
        self.assertEqual(call_kwargs["aws_session_token"], "temp-token")

    def test_data_plane_scope_probe_requires_inside_access_and_outside_denial(self) -> None:
        class FakeClientError(Exception):
            def __init__(self, status_code: int, code: str) -> None:
                self.response = {
                    "ResponseMetadata": {"HTTPStatusCode": status_code},
                    "Error": {"Code": code},
                }

        class FakeS3Client:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
                self.calls.append(("put", Key))
                if Key == "outside/probe":
                    raise FakeClientError(403, "AccessDenied")

            def head_object(self, *, Bucket: str, Key: str) -> None:
                self.calls.append(("head", Key))

            def get_object(self, *, Bucket: str, Key: str) -> dict[str, bytes]:
                self.calls.append(("get", Key))
                if Key == "outside/probe":
                    raise FakeClientError(403, "AccessDenied")
                return {"Body": b"rustfs table credential scope probe\n"}

            def delete_object(self, *, Bucket: str, Key: str) -> None:
                self.calls.append(("delete", Key))

        class FakeAdminS3Client:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
                self.calls.append(("put", Key))

            def delete_object(self, *, Bucket: str, Key: str) -> None:
                self.calls.append(("delete", Key))

        args = self.parse_with_args(["--bucket", "lake"])
        credential = pyiceberg_smoke.StorageCredential(
            prefix="s3://lake/tables/table-id/",
            config={
                "s3.access-key-id": "temp-access",
                "s3.secret-access-key": "temp-secret",
                "s3.session-token": "temp-token",
            },
        )
        fake_client = FakeS3Client()
        fake_admin_client = FakeAdminS3Client()
        deps = mock.Mock(botocore_client_error=FakeClientError)

        with mock.patch.object(pyiceberg_smoke, "vended_s3_client", return_value=fake_client):
            with mock.patch.object(pyiceberg_smoke, "configured_s3_client", return_value=fake_admin_client):
                with mock.patch.object(pyiceberg_smoke, "scope_probe_keys", return_value=("tables/table-id/probe", "outside/probe")):
                    pyiceberg_smoke.verify_vended_credential_data_plane_scope(args, deps, credential, "s3://lake/tables/table-id")

        self.assertEqual(
            fake_client.calls,
            [
                ("put", "tables/table-id/probe"),
                ("head", "tables/table-id/probe"),
                ("get", "tables/table-id/probe"),
                ("delete", "tables/table-id/probe"),
                ("put", "outside/probe"),
                ("get", "outside/probe"),
            ],
        )
        self.assertEqual(
            fake_admin_client.calls,
            [
                ("put", "outside/probe"),
                ("delete", "outside/probe"),
            ],
        )

    def test_data_plane_scope_probe_rejects_parent_prefix_before_s3_calls(self) -> None:
        args = self.parse_with_args(["--bucket", "lake"])
        credential = pyiceberg_smoke.StorageCredential(
            prefix="s3://lake/tables/",
            config={
                "s3.access-key-id": "temp-access",
                "s3.secret-access-key": "temp-secret",
                "s3.session-token": "temp-token",
            },
        )
        deps = mock.Mock()

        with mock.patch.object(pyiceberg_smoke, "vended_s3_client") as client_factory:
            with self.assertRaisesRegex(RuntimeError, "does not match table warehouse location"):
                pyiceberg_smoke.verify_vended_credential_data_plane_scope(
                    args,
                    deps,
                    credential,
                    "s3://lake/tables/table-id/",
                )

        client_factory.assert_not_called()

    def test_unsupported_inventory_names_stable_boundaries(self) -> None:
        inventory = pyiceberg_smoke.unsupported_inventory()
        capabilities = {entry["capability"] for entry in inventory}

        self.assertIn("credential-vending", capabilities)
        self.assertIn("row-level-delete-update-merge", capabilities)
        self.assertIn("background-maintenance-worker", capabilities)
        self.assertIn("external-catalog-bridge", capabilities)
        self.assertIn("multi-table-transactions", capabilities)
        external_bridge = next(entry for entry in inventory if entry["capability"] == "external-catalog-bridge")
        self.assertEqual(external_bridge["status"], "operator-sync-supported")
        for entry in inventory:
            self.assertIn("status", entry)
            self.assertIn("roadmap_area", entry)

    def test_production_readiness_inventory_tracks_catalog_backing(self) -> None:
        inventory = pyiceberg_smoke.production_readiness_inventory()
        capabilities = {entry["capability"] for entry in inventory}

        self.assertIn("strong-catalog-backing", capabilities)
        self.assertIn("single-active-writer-ha", capabilities)
        self.assertIn("scale-validation-matrix", capabilities)
        strong_backing = next(entry for entry in inventory if entry["capability"] == "strong-catalog-backing")
        self.assertEqual(strong_backing["status"], "migration-contract-supported")
        for entry in inventory:
            self.assertIn("status", entry)
            self.assertIn("validation", entry)

    def test_print_engine_compatibility_outputs_machine_readable_matrix(self) -> None:
        args = self.parse_with_args(["--print-engine-compatibility"])

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertTrue(pyiceberg_smoke.printed_metadata(args))

        document = json.loads(stdout.getvalue())
        self.assertIn("engine_compatibility", document)
        clients = {entry["client"] for entry in document["engine_compatibility"]}
        self.assertIn("PyIceberg", clients)
        self.assertIn("Spark Iceberg REST catalog", clients)

    def test_print_production_failure_coverage_outputs_machine_readable_matrix(self) -> None:
        args = self.parse_with_args(["--print-production-failure-coverage"])

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertTrue(pyiceberg_smoke.printed_metadata(args))

        document = json.loads(stdout.getvalue())
        self.assertIn("production_failure_coverage", document)
        cases = {entry["case"] for entry in document["production_failure_coverage"]}
        self.assertIn("commit-cas-conflict", cases)
        self.assertIn("post-cas-finalization-gap", cases)

    def test_print_vendor_profiles_outputs_migration_boundaries(self) -> None:
        args = self.parse_with_args(["--print-vendor-profiles"])

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertTrue(pyiceberg_smoke.printed_metadata(args))

        document = json.loads(stdout.getvalue())
        self.assertIn("vendor_profiles", document)
        aws = document["vendor_profiles"]["aws-s3tables"]
        self.assertEqual(aws["rest_signing_name"], "s3tables")
        self.assertEqual(aws["compatibility_stage"], "reference-only")
        self.assertIn("full AWS S3 Tables API parity", aws["not_claimed"])

        selected = document["selected_vendor_profile"]
        self.assertEqual(selected["name"], "rustfs")
        self.assertEqual(selected["catalog_uri"], "http://127.0.0.1:9000/iceberg")
        self.assertEqual(selected["warehouse"], "rustfs-s3table-smoke")

    def test_print_vendor_profiles_renders_selected_aws_profile(self) -> None:
        args = self.parse_with_args(
            [
                "--profile",
                "aws-s3tables",
                "--region",
                "us-east-1",
                "--account-id",
                "123456789012",
                "--table-bucket",
                "analytics",
                "--print-vendor-profiles",
            ]
        )

        stdout = StringIO()
        with redirect_stdout(stdout):
            self.assertTrue(pyiceberg_smoke.printed_metadata(args))

        document = json.loads(stdout.getvalue())
        selected = document["selected_vendor_profile"]
        self.assertEqual(selected["name"], "aws-s3tables")
        self.assertEqual(selected["catalog_uri"], "https://s3tables.us-east-1.amazonaws.com/iceberg")
        self.assertEqual(selected["warehouse"], "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics")
        self.assertEqual(selected["rest_signing_name"], "s3tables")

    def test_published_table_catalog_docs_do_not_use_internal_roadmap_labels(self) -> None:
        readme = (SCRIPT_DIR / "README.md").read_text(encoding="utf-8")

        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(readme))
        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(str(pyiceberg_smoke.unsupported_inventory())))
        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(str(pyiceberg_smoke.production_readiness_inventory())))
        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(str(pyiceberg_smoke.failure_coverage.production_failure_matrix())))


if __name__ == "__main__":
    unittest.main()
