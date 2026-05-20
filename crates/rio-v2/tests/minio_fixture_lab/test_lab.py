from __future__ import annotations

import importlib.util
import base64
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_lab_module():
    module_path = Path(__file__).with_name("lab.py")
    spec = importlib.util.spec_from_file_location("minio_fixture_lab_lab", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


lab = load_lab_module()


class DiscoverMinioLauncherTests(unittest.TestCase):
    def test_prefers_explicit_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            binary = Path(temp_dir) / "minio.exe"
            binary.write_text("", encoding="utf-8")

            launcher = lab.discover_minio_launcher(explicit_binary=binary, minio_root=None)

            self.assertEqual(launcher.command, [str(binary.resolve())])
            self.assertEqual(launcher.workdir, binary.resolve().parent)
            self.assertEqual(launcher.kind, "binary")

    def test_defaults_to_bundled_binary_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            binary = Path(temp_dir) / "minio.windows-amd64.RELEASE.2025-09-07T16-13-09Z.exe"
            binary.write_text("", encoding="utf-8")

            original_default_binary = lab.DEFAULT_MINIO_BINARY
            try:
                lab.DEFAULT_MINIO_BINARY = binary
                launcher = lab.discover_minio_launcher(explicit_binary=None, minio_root=None)
            finally:
                lab.DEFAULT_MINIO_BINARY = original_default_binary

            self.assertEqual(launcher.command, [str(binary.resolve())])
            self.assertEqual(launcher.workdir, binary.resolve().parent)
            self.assertEqual(launcher.kind, "binary")

    def test_rejects_source_checkout_without_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir) / "minio-master"
            source_root.mkdir()
            (source_root / "main.go").write_text("package main\n", encoding="utf-8")

            original_default_binary = lab.DEFAULT_MINIO_BINARY
            try:
                lab.DEFAULT_MINIO_BINARY = Path(temp_dir) / "missing-bundled-binary.exe"
                with mock.patch.object(lab.shutil, "which", return_value=None):
                    with self.assertRaises(FileNotFoundError):
                        lab.discover_minio_launcher(explicit_binary=None, minio_root=source_root)
            finally:
                lab.DEFAULT_MINIO_BINARY = original_default_binary

    def test_uses_binary_from_minio_root_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            binary_root = Path(temp_dir) / "minio-dist"
            binary_root.mkdir()
            binary = binary_root / "minio.exe"
            binary.write_text("", encoding="utf-8")

            launcher = lab.discover_minio_launcher(explicit_binary=None, minio_root=binary_root)

            self.assertEqual(launcher.command, [str(binary.resolve())])
            self.assertEqual(launcher.workdir, binary.resolve().parent)
            self.assertEqual(launcher.kind, "binary")


class FixtureMatrixTests(unittest.TestCase):
    def test_default_matrix_covers_sse_modes_and_shapes(self) -> None:
        cases = lab.build_default_cases()
        case_ids = {case.case_id for case in cases}

        self.assertEqual(len(cases), 6)
        self.assertIn("sse-s3-singlepart-64k", case_ids)
        self.assertIn("sse-kms-singlepart-64k", case_ids)
        self.assertIn("sse-c-singlepart-64k", case_ids)
        self.assertIn("sse-s3-multipart-8m", case_ids)
        self.assertIn("sse-kms-multipart-8m", case_ids)
        self.assertIn("sse-c-multipart-8m", case_ids)

    def test_default_matrix_uses_configured_kms_key_id(self) -> None:
        cases = lab.build_default_cases(kms_key_id="local-fixture-key")
        kms_cases = [case for case in cases if case.encryption == "SSE-KMS"]

        self.assertEqual(len(kms_cases), 2)
        self.assertTrue(all(case.kms_key_id == "local-fixture-key" for case in kms_cases))


class RequestRecordTests(unittest.TestCase):
    def test_sse_kms_request_record_includes_context_header(self) -> None:
        case = lab.FixtureCase(
            case_id="sse-kms-singlepart-64k",
            bucket="demo",
            object_name="dir/object.bin",
            encryption="SSE-KMS",
            size_bytes=64 * 1024,
            multipart=False,
            kms_key_id="minio-default-key",
            kms_context={"project": "rio-v2", "stage": "fixture"},
        )

        record = lab.build_request_record(case)

        self.assertEqual(record["headers"]["x-amz-server-side-encryption"], "aws:kms")
        self.assertEqual(
            record["headers"]["x-amz-server-side-encryption-aws-kms-key-id"],
            "minio-default-key",
        )
        expected_context = base64.b64encode(
            b'{"project":"rio-v2","stage":"fixture"}'
        ).decode("ascii")
        self.assertEqual(
            record["headers"]["x-amz-server-side-encryption-context"],
            expected_context,
        )

    def test_sse_s3_request_record_uses_aes256(self) -> None:
        case = lab.FixtureCase(
            case_id="sse-s3-singlepart-64k",
            bucket="demo",
            object_name="dir/object.bin",
            encryption="SSE-S3",
            size_bytes=64 * 1024,
            multipart=False,
        )

        record = lab.build_request_record(case)

        self.assertEqual(record["headers"], {"x-amz-server-side-encryption": "AES256"})

    def test_sse_c_request_record_includes_customer_key_headers(self) -> None:
        case = lab.FixtureCase(
            case_id="sse-c-singlepart-64k",
            bucket="demo",
            object_name="dir/object.bin",
            encryption="SSE-C",
            size_bytes=64 * 1024,
            multipart=False,
        )

        record = lab.build_request_record(case)

        self.assertEqual(
            record["headers"]["x-amz-server-side-encryption-customer-algorithm"],
            "AES256",
        )
        self.assertIn("x-amz-server-side-encryption-customer-key", record["headers"])
        self.assertIn("x-amz-server-side-encryption-customer-key-md5", record["headers"])


class MultipartManifestTests(unittest.TestCase):
    def test_complete_multipart_payload_is_xml(self) -> None:
        parts = [
            {"ETag": '"etag-1"', "PartNumber": 1},
            {"ETag": '"etag-2"', "PartNumber": 2},
        ]

        payload = lab.build_complete_multipart_xml(parts).decode("utf-8")

        self.assertIn("<CompleteMultipartUpload>", payload)
        self.assertIn("<PartNumber>1</PartNumber>", payload)
        self.assertIn("<ETag>\"etag-1\"</ETag>", payload)
        self.assertIn("<PartNumber>2</PartNumber>", payload)
        self.assertIn("<ETag>\"etag-2\"</ETag>", payload)


class KmsSecretKeyTests(unittest.TestCase):
    def test_parse_kms_secret_key_extracts_key_id(self) -> None:
        setting = lab.parse_kms_secret_key("local-fixture-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

        self.assertEqual(setting.secret_key, "local-fixture-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        self.assertEqual(setting.key_id, "local-fixture-key")

    def test_parse_kms_secret_key_rejects_missing_separator(self) -> None:
        with self.assertRaises(ValueError):
            lab.parse_kms_secret_key("invalid-secret-key")

    def test_minio_env_prefers_explicit_secret_key(self) -> None:
        env = lab.minio_env(
            kms_secret_key=lab.parse_kms_secret_key(
                "local-fixture-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            )
        )

        self.assertEqual(
            env["MINIO_KMS_SECRET_KEY"],
            "local-fixture-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        )


if __name__ == "__main__":
    unittest.main()
