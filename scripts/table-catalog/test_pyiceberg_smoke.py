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
        self.assertEqual(profiles["rustfs"]["pagination_model"], "iceberg-rest")
        self.assertEqual(
            profiles["rustfs-vended-credentials"]["credential_mode"],
            "catalog-vended-temporary-credentials",
        )
        self.assertEqual(profiles["minio-aistor"]["rest_signing_name"], "s3tables")
        self.assertEqual(profiles["cloudflare-r2-data-catalog"]["credential_mode"], "catalog-vended")
        self.assertEqual(profiles["oss-tables"]["rest_signing_name"], "osstables")

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

        oss = profiles["oss-tables"]
        self.assertEqual(oss["warehouse_shape"], "acs:osstables:{region}:{account_id}:bucket/{table_bucket}")
        self.assertIn("provider-error-code parity", oss["not_claimed"])

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

    def test_oss_tables_reference_profile_formats_warehouse_arn(self) -> None:
        args = self.parse_with_args([
            "--profile",
            "oss-tables",
            "--endpoint",
            "https://cn-hangzhou.oss-tables.aliyuncs.com",
            "--region",
            "cn-hangzhou",
            "--account-id",
            "123456789012",
            "--table-bucket",
            "analytics",
        ])

        properties = pyiceberg_smoke.catalog_properties(args)

        self.assertEqual(properties["uri"], "https://cn-hangzhou.oss-tables.aliyuncs.com/iceberg")
        self.assertEqual(properties["warehouse"], "acs:osstables:cn-hangzhou:123456789012:bucket/analytics")
        self.assertEqual(properties["rest.signing-name"], "osstables")

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

    @mock.patch.object(pyiceberg_smoke, "run_namespace_properties_probe")
    @mock.patch.object(pyiceberg_smoke, "run_encoded_namespace_probe")
    def test_view_probe_drops_smoke_view_after_load_failure(self, _encoded: object, _properties: object) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        probe_namespace = "smoke-sales-orders-views-12345678"
        namespace_path = pyiceberg_smoke.namespace_endpoint_path(args)
        view_path = pyiceberg_smoke.view_endpoint_path(args, namespace=probe_namespace)
        calls: list[tuple[str, str, object]] = []

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if (method, path) == ("POST", namespace_path):
                return {}
            if (method, path) == ("POST", view_path):
                return {}
            if (method, path) == ("GET", view_path):
                return {
                    "identifiers": [
                        {"name": "view-a"},
                        {"name": "view-b"},
                    ],
                    "next-page-token": None,
                }
            if (method, path) == ("GET", f"{view_path}/view-a"):
                return {}
            if method == "DELETE" and path.startswith(f"{view_path}/view-"):
                return {}
            if (method, path) == ("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace)):
                return {}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke.uuid, "uuid4", return_value=SimpleNamespace(hex="1234567890abcdef")):
            with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
                with mock.patch.object(pyiceberg_smoke, "paginated_identifier_names", return_value=["view-a", "view-b"]):
                    with self.assertRaisesRegex(RuntimeError, "metadata-location"):
                        pyiceberg_smoke.run_view_probe(args, mock.Mock())

        self.assertIn(("DELETE", f"{view_path}/view-a", None), calls)
        self.assertIn(("DELETE", f"{view_path}/view-b", None), calls)
        self.assertIn(("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace), None), calls)

    @mock.patch.object(pyiceberg_smoke, "run_namespace_properties_probe")
    @mock.patch.object(pyiceberg_smoke, "run_encoded_namespace_probe")
    def test_view_probe_cleans_candidate_after_create_timeout(self, _encoded: object, _properties: object) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        probe_namespace = "smoke-sales-orders-views-12345678"
        namespace_path = pyiceberg_smoke.namespace_endpoint_path(args)
        view_path = pyiceberg_smoke.view_endpoint_path(args, namespace=probe_namespace)
        calls: list[tuple[str, str, object]] = []

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if (method, path) == ("POST", namespace_path):
                return {}
            if (method, path) == ("POST", view_path):
                raise RuntimeError("timed out after commit")
            if (method, path) == ("DELETE", f"{view_path}/view-a"):
                return {}
            if (method, path) == ("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace)):
                return {}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke.uuid, "uuid4", return_value=SimpleNamespace(hex="1234567890abcdef")):
            with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
                with self.assertRaisesRegex(RuntimeError, "timed out after commit"):
                    pyiceberg_smoke.run_view_probe(args, mock.Mock())

        self.assertIn(("DELETE", f"{view_path}/view-a", None), calls)
        self.assertIn(("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace), None), calls)

    @mock.patch.object(pyiceberg_smoke, "run_namespace_properties_probe")
    @mock.patch.object(pyiceberg_smoke, "run_encoded_namespace_probe")
    def test_view_probe_continues_cleanup_after_delete_failure(self, _encoded: object, _properties: object) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        probe_namespace = "smoke-sales-orders-views-12345678"
        namespace_path = pyiceberg_smoke.namespace_endpoint_path(args)
        view_path = pyiceberg_smoke.view_endpoint_path(args, namespace=probe_namespace)
        calls: list[tuple[str, str, object]] = []

        def fake_signed_request(
            _args: object,
            _deps: object,
            method: str,
            path: str,
            body: object = None,
        ) -> dict[str, object]:
            calls.append((method, path, body))
            if (method, path) == ("POST", namespace_path) or (method, path) == ("POST", view_path):
                return {}
            if (method, path) == ("GET", view_path):
                return {"identifiers": [{"name": "view-a"}, {"name": "view-b"}], "next-page-token": None}
            if (method, path) == ("GET", f"{view_path}/view-a"):
                return {"metadata-location": "s3://lake/views/view-a/metadata/v1.json"}
            if (method, path) == ("DELETE", f"{view_path}/view-b"):
                raise pyiceberg_smoke.RestRequestError(method, path, 500, "delete failed")
            if (method, path) == ("DELETE", f"{view_path}/view-a"):
                return {}
            if (method, path) == ("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace)):
                return {}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke.uuid, "uuid4", return_value=SimpleNamespace(hex="1234567890abcdef")):
            with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
                with mock.patch.object(pyiceberg_smoke, "paginated_identifier_names", return_value=["view-a", "view-b"]):
                    with self.assertRaisesRegex(RuntimeError, "delete failed"):
                        pyiceberg_smoke.run_view_probe(args, mock.Mock())

        self.assertIn(("DELETE", f"{view_path}/view-b", None), calls)
        self.assertIn(("DELETE", f"{view_path}/view-a", None), calls)
        self.assertIn(("DELETE", pyiceberg_smoke.namespace_endpoint_path(args, probe_namespace), None), calls)

    def test_paginated_identifier_names_follows_tokens_until_null(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        view_path = pyiceberg_smoke.view_endpoint_path(args)
        responses = [
            {"identifiers": [], "next-page-token": "token-0"},
            {"identifiers": [{"name": "orders_a"}], "next-page-token": "token-1"},
            {"identifiers": [{"name": "orders_b"}], "next-page-token": None},
        ]

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=responses) as request:
            names = pyiceberg_smoke.paginated_identifier_names(args, mock.Mock(), view_path, page_size=1)

        self.assertEqual(names, ["orders_a", "orders_b"])
        self.assertEqual(request.call_count, 3)
        self.assertTrue(request.call_args_list[0].args[3].endswith("?pageSize=1"))
        self.assertNotIn("pageToken", request.call_args_list[0].args[3])
        self.assertIn("pageSize=1", request.call_args_list[1].args[3])
        self.assertIn("pageToken=token-0", request.call_args_list[1].args[3])
        self.assertIn("pageSize=1", request.call_args_list[2].args[3])
        self.assertIn("pageToken=token-1", request.call_args_list[2].args[3])

    def test_paginated_identifier_names_rejects_invalid_sequences(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        view_path = pyiceberg_smoke.view_endpoint_path(args)
        scenarios = [
            ([{"identifiers": [], "next-page-token": "token-1"}, {"identifiers": [], "next-page-token": "token-1"}], "invalid next-page-token"),
            ([{"identifiers": [{"name": "orders"}], "next-page-token": "token-1"}, {"identifiers": [{"name": "orders"}], "next-page-token": None}], "duplicate identifier"),
            ([{"identifiers": []}], "omitted next-page-token"),
            ([{"identifiers": [{"name": "orders_a"}, {"name": "orders_b"}], "next-page-token": None}], "exceeded pageSize"),
        ]

        for responses, message in scenarios:
            with self.subTest(message=message):
                with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=responses):
                    with self.assertRaisesRegex(RuntimeError, message):
                        pyiceberg_smoke.paginated_identifier_names(args, mock.Mock(), view_path, page_size=1)

    def maintenance_responses(self, backing="object", *, worker_changes=None, mutate_pointer=False):
        config = {}
        worker_ran = False
        pointer = {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "version-token": "token-2", "generation": 2}
        job = {"job_id": "job-2", "status": "QUEUED", "operation": "DRY_RUN"}
        audit = [{"action": "PLANNED"}]

        def request(_args, _deps, method, path, body=None):
            nonlocal config, worker_ran
            if (method, path.rsplit("/", 1)[-1]) == ("GET", "metadata-location"):
                return {**pointer, "generation": 3} if worker_ran and mutate_pointer else pointer.copy()
            if path.endswith("/maintenance/config"):
                if method == "PUT":
                    config = body.copy()
                return config.copy()
            if path.endswith("/maintenance/metadata"):
                if backing == "durable-strong":
                    envelope = {"error": {"code": 400, "type": "BadRequestException",
                                          "message": "operation is not supported with durable-strong table catalog backing"}}
                    raise pyiceberg_smoke.RestRequestError(method, path, 400, json.dumps(envelope))
                return {"job": {"job_id": "job-1"}, "audit-events": audit}
            if path.endswith("/maintenance/scheduler"):
                return {"status": "DISABLED", "audit_timeline": []}
            if path.endswith("/maintenance/scheduler/run"):
                return {"report": {"job": {**job, "scheduler-id": body["scheduler-id"]}},
                        "scheduler": {"status": "QUEUED"}}
            if path.endswith("/maintenance/jobs/job-2"):
                return {"job": job.copy(), "audit-events": audit}
            if path.endswith("/maintenance/jobs/job-2/quarantine"):
                return {"action": "INSPECT", "report": {"job": job.copy()}}
            if path.endswith("/maintenance/worker/run"):
                worker_ran = True
                completed = {**job, "status": "SUCCESSFUL", "deleted_metadata_file_count": 0, "deleted_object_count": 0}
                completed.update(worker_changes or {})
                return {"job": completed, "audit-events": audit}
            raise AssertionError(f"unexpected maintenance request: {method} {path}")

        return request

    def test_maintenance_dry_run_exercises_both_backings_and_leaves_worker_paused(self) -> None:
        args = self.parse_with_args([])
        for backing in ("object", "durable-strong"):
            with self.subTest(backing=backing), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=self.maintenance_responses(backing)
            ) as request:
                pyiceberg_smoke.run_maintenance_probe(args, mock.Mock(), backing)
            paths = [call.args[3] for call in request.call_args_list]
            for suffix in ("/maintenance/scheduler/run", "/maintenance/jobs/job-2",
                           "/maintenance/jobs/job-2/quarantine", "/maintenance/worker/run"):
                self.assertTrue(any(path.endswith(suffix) for path in paths), suffix)
            self.assertEqual(request.call_args.args[2], "PUT")
            self.assertEqual(request.call_args.args[4], pyiceberg_smoke.default_maintenance_config())
            for call in request.call_args_list:
                if call.args[2] == "PUT":
                    self.assertFalse(call.args[4]["delete-enabled"])

    def test_maintenance_failure_never_counts_as_success_and_restores_safe_config(self) -> None:
        args = self.parse_with_args([])
        failures = [
            ({"status": status}, "complete the dry-run")
            for status in ("FAILED", "PAUSED", "RUNNING", "QUEUED", "DISABLED", "UNKNOWN", None)
        ]
        failures.extend([
            ({"job_id": "other-job"}, "job identity"),
            ({"operation": "DELETE"}, "job identity"),
            ({"deleted_object_count": 1}, "zero deletions"),
            ({"deleted_metadata_file_count": 1}, "zero deletions"),
            ({"deleted_object_count": None}, "zero deletions"),
            ({"deleted_object_count": False}, "zero deletions"),
        ])
        for backing in ("object", "durable-strong"):
            for changes, message in failures:
                with self.subTest(backing=backing, changes=changes), mock.patch.object(
                    pyiceberg_smoke, "signed_rest_request",
                    side_effect=self.maintenance_responses(backing, worker_changes=changes)
                ) as request:
                    with self.assertRaisesRegex(RuntimeError, message):
                        pyiceberg_smoke.run_maintenance_probe(args, mock.Mock(), backing)
                self.assertEqual(request.call_args.args[4], pyiceberg_smoke.default_maintenance_config())

    def test_maintenance_detects_pointer_advance_and_pauses_after_transport_failure(self) -> None:
        args = self.parse_with_args([])
        for backing in ("object", "durable-strong"):
            for transport_failure in (False, True):
                base = self.maintenance_responses(backing, mutate_pointer=not transport_failure)

                def request(*args, **kwargs):
                    if transport_failure and args[3].endswith("/maintenance/worker/run"):
                        raise OSError("worker connection lost")
                    return base(*args, **kwargs)

                with self.subTest(backing=backing, transport_failure=transport_failure), mock.patch.object(
                    pyiceberg_smoke, "signed_rest_request", side_effect=request
                ) as calls:
                    expected = OSError if transport_failure else RuntimeError
                    with self.assertRaises(expected):
                        pyiceberg_smoke.run_maintenance_probe(args, mock.Mock(), backing)
                self.assertEqual(calls.call_args.args[4], pyiceberg_smoke.default_maintenance_config())

    def test_catalog_api_probe_exercises_extended_rest_surfaces(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        deps = mock.Mock()
        calls: list[tuple[str, str, object]] = []
        table_path = pyiceberg_smoke.table_endpoint_path(args)
        metadata_location_path = pyiceberg_smoke.table_endpoint_path(args, "/metadata-location")
        refs_path = pyiceberg_smoke.table_ref_endpoint_path(args)
        ref_name = pyiceberg_smoke.safe_ref_segment(args.namespace, args.table)
        quarantine_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1/quarantine")
        scheduler_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler")
        scheduler_run_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/scheduler/run")
        worker_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/worker/run")
        diagnostics_path = pyiceberg_smoke.table_endpoint_path(args, "/catalog/diagnostics")
        maintenance_request = self.maintenance_responses()

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
                return {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "version-token": "token-2", "generation": 2}
            if (method, path) == ("GET", pyiceberg_smoke.table_endpoint_path(args, "/catalog/export")):
                return {"table": {"metadata_location": "s3://lake/tables/id/metadata/v2.metadata.json", "version_token": "token-2", "generation": 2}}
            if method == "PUT" and path.startswith(f"{refs_path}/"):
                return {}
            if (method, path) == ("GET", refs_path):
                if sum(1 for call in calls if call[:2] == ("GET", refs_path)) == 1:
                    return {"refs": {ref_name: {"snapshot-id": 7}}}
                return {"refs": {}}
            if method == "DELETE" and path.startswith(f"{refs_path}/"):
                return {}
            if "/maintenance/" in path:
                return maintenance_request(_args, _deps, method, path, body)
            if (method, path) == ("GET", diagnostics_path):
                return {"status": "ok"}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with mock.patch.object(pyiceberg_smoke, "run_view_probe") as view_probe, mock.patch.object(
                pyiceberg_smoke, "run_table_rename_probe"
            ), mock.patch.object(pyiceberg_smoke, "run_load_table_delegation_probe"):
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
                    _, probes = pyiceberg_smoke.run_catalog_api_probes(args, deps, "object")

        self.assertEqual(probes["maintenance-worker-dry-run"], "pass")
        self.assertEqual(probes["diagnostics"], "pass")
        view_probe.assert_called_once_with(args, deps)
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
        self.assertTrue(any(method == "POST" and path == scheduler_run_path for method, path, _ in calls))
        self.assertIn(("POST", quarantine_path.replace("job-1", "job-2"), {"action": "INSPECT"}), calls)
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

    def run_smoke_with_fakes(self, args: object, probe_calls: list[str], scanned_rows: list[dict[str, object]] | None = None) -> list[str]:
        events: list[str] = []

        class FakeArrowTable:
            num_rows = 2

            def to_pylist(self) -> list[dict[str, object]]:
                return pyiceberg_smoke.SMOKE_ROWS if scanned_rows is None else scanned_rows

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

        with mock.patch.object(pyiceberg_smoke, "ensure_local_proxy_bypass"), mock.patch.object(
            pyiceberg_smoke, "discover_catalog_backing", return_value="object"
        ):
            with mock.patch.object(pyiceberg_smoke, "ensure_aws_env"):
                with mock.patch.object(pyiceberg_smoke, "ensure_bucket"):
                    with mock.patch.object(pyiceberg_smoke, "enable_table_bucket"):
                        with mock.patch.object(pyiceberg_smoke, "load_rest_catalog", side_effect=lambda *_args, **_kwargs: FakeCatalog()):
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
                                                ({}, {"direct-rest": "pass"}),
                                            )[-1],
                                        ):
                                            with redirect_stdout(StringIO()):
                                                pyiceberg_smoke.run_smoke(args, deps)
        return events

    def test_run_smoke_rejects_equal_row_count_with_different_data(self) -> None:
        args = self.parse_with_args(["--bucket", "lake", "--namespace", "sales", "--table", "orders"])
        calls: list[str] = []
        with self.assertRaisesRegex(RuntimeError, "different data"):
            self.run_smoke_with_fakes(args, calls, [{"id": 1, "payload": "wrong"}, {"id": 2, "payload": "beta"}])
        self.assertEqual(calls, [])

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
        self.assertEqual(request["view-version"]["default-namespace"], ["sales"])
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
        self.assertEqual(strong_backing["status"], "state-transfer-supported")
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

    def test_pyiceberg_live_evidence_record_uses_runtime_smoke_result(self) -> None:
        args = self.parse_with_args([
            "--endpoint",
            "http://127.0.0.1:9000",
            "--bucket",
            "lake",
            "--namespace",
            "smoke",
            "--table",
            "events",
            "--live-evidence-output",
            "/tmp/rustfs-live-evidence.json",
        ])
        result = pyiceberg_smoke.SmokeResult(
            metadata_location="s3://lake/tables/table-id/metadata/v2.metadata.json",
            row_count=2,
            cleanup_result="not-requested",
            table_warehouse_location="s3://lake/tables/table-id",
            catalog_backing="durable-strong",
            catalog_probes=pyiceberg_smoke.engine_compatibility.pyiceberg_catalog_probe_results("durable-strong", skipped=False, vended=False),
        )

        record = pyiceberg_smoke.pyiceberg_live_evidence_record(
            args,
            result,
            client_version="0.10.0",
            rustfs_build="rustfs-test",
            git_sha="abc123",
            catalog_backing="durable-strong",
            run_timestamp_utc="2026-07-09T00:00:00Z",
            operator="ci",
            command="python3 scripts/table-catalog/pyiceberg_smoke.py --live-evidence-output /tmp/rustfs-live-evidence.json",
        )

        self.assertEqual(record["client_name"], "PyIceberg")
        self.assertEqual(record["client_version"], "0.10.0")
        self.assertEqual(record["warehouse"], "lake")
        self.assertEqual(record["metadata_location"], "s3://lake/tables/table-id/metadata/v2.metadata.json")
        self.assertEqual(record["row_count"], 2)
        self.assertEqual(record["claim"], "automated-smoke")
        self.assertEqual(record["catalog_probes"]["metadata-maintenance"], "expected-unsupported")
        self.assertEqual(record["catalog_probes"]["maintenance-worker-dry-run"], "pass")
        self.assertEqual(record["rest_signing_name"], "s3")
        validate = pyiceberg_smoke.engine_compatibility.validate_live_conformance_evidence
        self.assertEqual(validate(record)["status"], "accepted")
        record["catalog_probes"]["metadata-maintenance"] = "pass"
        with self.assertRaisesRegex(ValueError, "capabilities"):
            validate(record)
        del record["catalog_probes"]
        with self.assertRaisesRegex(ValueError, "capabilities"):
            validate(record)

    def test_backing_discovery_uses_overrides_and_rejects_mismatches(self) -> None:
        args = self.parse_with_args(["--bucket", "lake", "--catalog-backing", "durable-strong"])
        config = {"defaults": {"rustfs.catalog-backing": "object"}, "overrides": {"rustfs.catalog-backing": "durable-strong"}}
        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", return_value=config) as request:
            self.assertEqual(pyiceberg_smoke.discover_catalog_backing(args, mock.Mock()), "durable-strong")
        self.assertEqual(request.call_args.args[2:4], ("GET", "/iceberg/v1/config?warehouse=lake"))
        for response in ({}, {"defaults": {}, "overrides": {}}, {"defaults": {"rustfs.catalog-backing": "unknown"}, "overrides": {}}):
            with self.subTest(response=response), mock.patch.object(pyiceberg_smoke, "signed_rest_request", return_value=response):
                with self.assertRaises(RuntimeError):
                    pyiceberg_smoke.discover_catalog_backing(args, mock.Mock())
        config["overrides"] = {}
        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", return_value=config):
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                pyiceberg_smoke.discover_catalog_backing(args, mock.Mock())
            args.catalog_backing = "object-backed"
            self.assertEqual(pyiceberg_smoke.discover_catalog_backing(args, mock.Mock()), "object")

    def test_client_version_is_observed_and_mismatch_aborts_before_setup(self) -> None:
        args = self.parse_with_args(["--client-version", "0.11.1"])
        with mock.patch.object(pyiceberg_smoke.importlib.metadata, "version", return_value="0.11.1"):
            self.assertEqual(pyiceberg_smoke.pyiceberg_client_version(args), "0.11.1")
        with mock.patch.object(pyiceberg_smoke.importlib.metadata, "version", return_value="0.10.0"), mock.patch.object(
            pyiceberg_smoke, "ensure_bucket"
        ) as bucket, mock.patch.object(pyiceberg_smoke, "discover_catalog_backing") as config:
            with self.assertRaisesRegex(RuntimeError, "expected PyIceberg 0.11.1, found 0.10.0"):
                pyiceberg_smoke.run_smoke(args, mock.Mock())
        bucket.assert_not_called()
        config.assert_not_called()

    def test_backing_mismatch_fails_before_creating_any_objects(self) -> None:
        args = self.parse_with_args([])
        with mock.patch.object(pyiceberg_smoke, "discover_catalog_backing", side_effect=RuntimeError("mismatch")), mock.patch.object(
            pyiceberg_smoke, "ensure_bucket"
        ) as bucket, mock.patch.object(pyiceberg_smoke, "ensure_aws_env"):
            with self.assertRaisesRegex(RuntimeError, "mismatch"):
                pyiceberg_smoke.run_smoke(args, mock.Mock())
        bucket.assert_not_called()

    def strong_probe_responses(self, *, status=400, error_type="BadRequestException", mutate=False):
        pointer = {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "version-token": "t2", "generation": 2}
        reads = 0

        def request(_args, _deps, method, path, body=None):
            nonlocal reads
            if path.endswith("/metadata-location"):
                reads += 1
                return {**pointer, "generation": 3} if mutate and reads > 1 else pointer.copy()
            self.assertTrue(path.endswith("/maintenance/metadata"), path)
            error = {"error": {"code": status, "type": error_type,
                               "message": "operation is not supported with durable-strong table catalog backing"}}
            raise pyiceberg_smoke.RestRequestError(method, path, status, json.dumps(error))

        return request

    def test_strong_metadata_boundary_checks_error_and_unchanged_pointer(self) -> None:
        args = self.parse_with_args([])
        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=self.strong_probe_responses()) as request:
            pyiceberg_smoke.run_strong_backing_boundaries(args, mock.Mock())
        self.assertEqual(request.call_count, 3)
        for kwargs, message in (({"status": 403}, "expected one of"), ({"status": 500}, "expected one of"),
                                ({"error_type": "RESTException"}, "error envelope"), ({"mutate": True}, "commit state")):
            with self.subTest(kwargs=kwargs), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=self.strong_probe_responses(**kwargs)
            ):
                with self.assertRaisesRegex(RuntimeError, message):
                    pyiceberg_smoke.run_strong_backing_boundaries(args, mock.Mock())

    def test_catalog_export_checks_pointer_token_and_generation_on_both_backings(self) -> None:
        args = self.parse_with_args(["--bucket", "lake"])
        location = "s3://lake/tables/id/metadata/v2.metadata.json"
        table = {"metadata-location": location, "metadata": {"current-snapshot-id": 7}}
        pointer = {"metadata-location": location, "version-token": "token-2", "generation": 2}
        entry = {"metadata_location": location, "version_token": "token-2", "generation": 2}
        for backing in ("object", "durable-strong"):
            for mismatch in (None, "metadata_location", "version_token", "generation"):
                exported = {**entry, mismatch: "stale"} if mismatch else entry
                with self.subTest(backing=backing, mismatch=mismatch), mock.patch.object(
                    pyiceberg_smoke, "signed_rest_request",
                    side_effect=[table, {"status": "HEALTHY"}, {"table": exported}, pointer, table]
                ), mock.patch.object(pyiceberg_smoke, "run_metadata_location_probe"), mock.patch.object(
                    pyiceberg_smoke, "run_table_ref_probe"
                ), mock.patch.object(pyiceberg_smoke, "run_view_probe"), mock.patch.object(
                    pyiceberg_smoke, "run_table_rename_probe"
                ), mock.patch.object(pyiceberg_smoke, "run_load_table_delegation_probe"), mock.patch.object(
                    pyiceberg_smoke, "run_maintenance_probe"
                ) as maintenance:
                    if mismatch:
                        with self.assertRaisesRegex(RuntimeError, "does not match"):
                            pyiceberg_smoke.run_catalog_api_probes(args, mock.Mock(), backing)
                    else:
                        _, probes = pyiceberg_smoke.run_catalog_api_probes(args, mock.Mock(), backing)
                        self.assertEqual(probes["diagnostics"], "pass")
                        self.assertEqual(probes["export"], "pass")
                        self.assertEqual(probes["maintenance-worker-dry-run"], "pass")
                        self.assertEqual(probes["metadata-maintenance"],
                                         "expected-unsupported" if backing == "durable-strong" else "pass")
                self.assertEqual(maintenance.call_args.args[2], backing)

    def test_namespace_properties_detect_partial_or_rejected_writes(self) -> None:
        args = self.parse_with_args([])
        before = {"properties": {"rustfs.smoke": "true", "owner": "unchanged"}}
        after = {"properties": {"rustfs.smoke.updated": "true", "owner": "unchanged"}}
        update = {"updated": ["rustfs.smoke.updated"], "removed": ["rustfs.smoke"]}
        error = pyiceberg_smoke.RestRequestError("POST", "/properties", 422, "overlapping keys")
        for final in (after, before):
            with self.subTest(final=final), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=[before, update, after, error, final]
            ):
                if final == after:
                    pyiceberg_smoke.run_namespace_properties_probe(args, mock.Mock(), "probe")
                else:
                    with self.assertRaisesRegex(RuntimeError, "rejected namespace"):
                        pyiceberg_smoke.run_namespace_properties_probe(args, mock.Mock(), "probe")

    def test_encoded_namespace_probe_rejects_aliasing_and_cleans_up(self) -> None:
        args = self.parse_with_args([])
        namespace = {"namespace": ["probe", "encoded"]}
        error = pyiceberg_smoke.RestRequestError("GET", "/namespace", 400, "invalid namespace")
        for double_encoded_response in (error, namespace):
            with self.subTest(response=double_encoded_response), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=[{}, namespace, double_encoded_response, namespace, {}]
            ) as request:
                if isinstance(double_encoded_response, Exception):
                    pyiceberg_smoke.run_encoded_namespace_probe(args, mock.Mock(), "probe")
                else:
                    with self.assertRaisesRegex(RuntimeError, "unexpectedly succeeded"):
                        pyiceberg_smoke.run_encoded_namespace_probe(args, mock.Mock(), "probe")
            self.assertTrue(request.call_args_list[1].args[3].endswith("probe%1Fencoded"))
            self.assertTrue(request.call_args_list[2].args[3].endswith("probe%251Fencoded"))
            self.assertEqual(request.call_args.args[2], "DELETE")

    def test_rename_probe_restores_name_even_when_metadata_changes(self) -> None:
        args = self.parse_with_args(["--bucket", "lake", "--namespace", "sales", "--table", "orders"])
        before = {"metadata-location": "s3://lake/tables/id/metadata/v2.metadata.json", "metadata": {"location": "s3://lake/tables/id", "table-uuid": "uuid"}}
        missing = pyiceberg_smoke.RestRequestError("GET", "/table", 404, "missing")
        for changed in (False, True):
            after = {**before, "metadata-location": "other"} if changed else before
            with self.subTest(changed=changed), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=[before, {}, missing, after, {}, before]
            ) as request:
                if changed:
                    with self.assertRaisesRegex(RuntimeError, "warehouse identity"):
                        pyiceberg_smoke.run_table_rename_probe(args, mock.Mock())
                else:
                    pyiceberg_smoke.run_table_rename_probe(args, mock.Mock())
                renames = [call.args[4] for call in request.call_args_list if call.args[2] == "POST"]
                self.assertEqual(renames[0]["source"], renames[1]["destination"])
                self.assertEqual(renames[0]["destination"], renames[1]["source"])

    def test_delegation_requires_exact_token_and_warehouse_scope(self) -> None:
        args = self.parse_with_args(["--require-vended-credentials"])
        credential = {"prefix": "s3://lake/tables/id/", "config": {key: "temporary" for key in pyiceberg_smoke.REQUIRED_STORAGE_CREDENTIAL_KEYS}}
        metadata_credential = {"prefix": "s3://lake/catalog/current.metadata.json", "config": credential["config"].copy()}
        response = {"metadata": {"location": "s3://lake/tables/id"}, "metadata-location": metadata_credential["prefix"], "storage-credentials": [credential, metadata_credential]}
        for prefix in ("s3://lake/tables/id/", "s3://lake/tables/other/"):
            credential["prefix"] = prefix
            with self.subTest(prefix=prefix), mock.patch.object(
                pyiceberg_smoke, "signed_rest_request", side_effect=[{"storage-credentials": []}] * 4 + [response]
            ) as request, mock.patch.object(pyiceberg_smoke, "verify_vended_credential_data_plane_scope") as scope:
                if prefix.endswith("/other/"):
                    with self.assertRaisesRegex(RuntimeError, "do not match"):
                        pyiceberg_smoke.run_load_table_delegation_probe(args, mock.Mock())
                    scope.assert_not_called()
                else:
                    pyiceberg_smoke.run_load_table_delegation_probe(args, mock.Mock())
                    scope.assert_called_once()
                    self.assertEqual(scope.call_args.args[-1], "s3://lake/tables/id")
                self.assertEqual(request.call_args.kwargs["access_delegation"], "remote-signing, vended-credentials")
        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "without exact"):
                pyiceberg_smoke.run_load_table_delegation_probe(args, mock.Mock())
        response["storage-credentials"] = [credential, metadata_credential, credential]
        with mock.patch.object(
            pyiceberg_smoke, "signed_rest_request", side_effect=[{}] * 4 + [response]
        ), mock.patch.object(pyiceberg_smoke, "verify_vended_credential_data_plane_scope") as scope:
            with self.assertRaisesRegex(RuntimeError, "exactly the warehouse"):
                pyiceberg_smoke.run_load_table_delegation_probe(args, mock.Mock())
        scope.assert_not_called()

        credential["prefix"] = "s3://lake/tables/id/"
        metadata_credential["config"]["s3.session-token"] = "different-session"
        response["storage-credentials"] = [credential, metadata_credential]
        with mock.patch.object(
            pyiceberg_smoke, "signed_rest_request", side_effect=[{}] * 4 + [response]
        ), mock.patch.object(pyiceberg_smoke, "verify_vended_credential_data_plane_scope") as scope:
            with self.assertRaisesRegex(RuntimeError, "same temporary session"):
                pyiceberg_smoke.run_load_table_delegation_probe(args, mock.Mock())
        scope.assert_not_called()

    def test_skipped_probes_are_not_recorded_as_full_rest_evidence(self) -> None:
        args = self.parse_with_args(["--skip-catalog-api-probes"])
        result = pyiceberg_smoke.SmokeResult("s3://lake/tables/id/metadata/v2.metadata.json", 2, "not-requested", "s3://lake/tables/id", "object", {"direct-rest": "skipped"})
        kwargs = dict(client_version="0.11.1", rustfs_build="test", git_sha="abc123", catalog_backing="object", run_timestamp_utc="2026-09-09T00:00:00Z", operator="test", command="smoke")
        record = pyiceberg_smoke.pyiceberg_live_evidence_record(args, result, **kwargs)
        self.assertEqual(record["scenario"], "create-append-reload-scan-core")
        self.assertEqual(record["catalog_probes"], {"direct-rest": "skipped"})
        kwargs["catalog_backing"] = "durable-strong"
        with self.assertRaisesRegex(ValueError, "observed server"):
            pyiceberg_smoke.pyiceberg_live_evidence_record(args, result, **kwargs)

    def test_live_evidence_command_redacts_cli_secrets(self) -> None:
        command = pyiceberg_smoke.redacted_command(
            [
                "pyiceberg_smoke.py",
                "--endpoint",
                "http://127.0.0.1:9000",
                "--access-key",
                "root-access",
                "--secret-key=root-secret",
                "--bucket",
                "lake",
            ]
        )

        self.assertIn("--access-key '<redacted>'", command)
        self.assertIn("--secret-key", command)
        self.assertIn("<redacted>", command)
        self.assertNotIn("root-access", command)
        self.assertNotIn("root-secret", command)
        self.assertIn("--bucket lake", command)

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
