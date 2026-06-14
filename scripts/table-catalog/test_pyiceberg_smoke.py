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

    def test_unsupported_inventory_names_stable_boundaries(self) -> None:
        inventory = pyiceberg_smoke.unsupported_inventory()
        capabilities = {entry["capability"] for entry in inventory}

        self.assertIn("credential-vending", capabilities)
        self.assertIn("iceberg-views", capabilities)
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
