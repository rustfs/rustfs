#!/usr/bin/env python3
"""Unit tests for the RustFS table catalog PyIceberg smoke helper."""

from __future__ import annotations

import os
import re
import sys
import unittest
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

    def test_catalog_api_probe_exercises_extended_rest_surfaces(self) -> None:
        args = self.parse_with_args(["--namespace", "sales", "--table", "orders"])
        deps = mock.Mock()
        calls: list[tuple[str, str, object]] = []
        table_path = pyiceberg_smoke.table_endpoint_path(args)
        metadata_location_path = pyiceberg_smoke.table_endpoint_path(args, "/metadata-location")
        refs_path = pyiceberg_smoke.table_ref_endpoint_path(args)
        view_path = pyiceberg_smoke.view_endpoint_path(args)
        config_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/config")
        maintenance_path = pyiceberg_smoke.table_endpoint_path(args, "/maintenance/metadata")
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
                    return {"refs": {"smoke-sales-orders": {"snapshot-id": 7}}}
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
                return {"job": {"job-id": "job-1"}}
            if (method, path) == ("GET", pyiceberg_smoke.table_endpoint_path(args, "/maintenance/jobs/job-1")):
                return {"job": {"job-id": "job-1", "status": "SUCCESSFUL"}}
            if (method, path) == ("POST", worker_path):
                return {"job": {"status": "PAUSED"}}
            if (method, path) == ("GET", diagnostics_path):
                return {"status": "ok"}
            raise AssertionError(f"unexpected REST request: {method} {path}")

        with mock.patch.object(pyiceberg_smoke, "signed_rest_request", side_effect=fake_signed_request):
            with mock.patch.object(
                pyiceberg_smoke,
                "signed_rest_request_expect_error",
                return_value=pyiceberg_smoke.RestRequestError("DELETE", f"{refs_path}/smoke-sales-orders", 400, "retained"),
            ) as expect_error:
                pyiceberg_smoke.run_catalog_api_probes(args, deps)

        expect_error.assert_called_once()
        self.assertIn(("GET", metadata_location_path, None), calls)
        self.assertIn(("GET", diagnostics_path, None), calls)
        self.assertIn(("POST", worker_path, {}), calls)

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
        for entry in inventory:
            self.assertIn("status", entry)
            self.assertIn("roadmap_area", entry)

    def test_published_table_catalog_docs_do_not_use_internal_roadmap_labels(self) -> None:
        readme = (SCRIPT_DIR / "README.md").read_text(encoding="utf-8")

        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(readme))
        self.assertIsNone(INTERNAL_ROADMAP_LABEL_RE.search(str(pyiceberg_smoke.unsupported_inventory())))


if __name__ == "__main__":
    unittest.main()
