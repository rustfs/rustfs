#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for scripts/check_kms_format_versions.py."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

import check_kms_format_versions as guard  # noqa: E402


SAMPLE_POLICY = """# KMS Format Version and Deprecation Policy

## Version Anchors

| Format | File | Constant | Value | Enforcement |
| --- | --- | --- | --- | --- |
| Backup manifest | `crates/kms/src/backup/manifest.rs` | `BackupManifest::FORMAT_VERSION` | `1` | Exact equality |
| Local KMS master key | `crates/kms/src/backends/local.rs` | `STORED_MASTER_KEY_FORMAT_VERSION` | `1` | Upper bound |
| DEK context binding | `crates/kms/src/encryption/dek.rs` | `CONTEXT_BINDING_AAD_V1` | `1` | Opt-in write |
| Local SSE DEK envelope | `crates/sse/src/kms/local.rs` | `LOCAL_SSE_DEK_FORMAT_VERSION` | `1` | Exact equality |
| Ciphertext segment nonce | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_SEGMENT_NONCE` | `0x03` | Opt-in write |

### Active Deprecation Guards

| Surface | Field | Test file | Regression guard |
| --- | --- | --- | --- |
| `ConfigureVaultKmsRequest` | `mount_path` | `crates/kms/src/api_types.rs` | `test_mount_path_accepted` |
"""


def write_fixture(root: Path, *, policy: str = SAMPLE_POLICY, manifest_ver: str = "1", seg_nonce: str = "0x03") -> None:
    files = {
        "docs/architecture/kms-format-version-policy.md": policy,
        "crates/kms/src/backup/manifest.rs": (
            f"pub struct BackupManifest;\nimpl BackupManifest {{\n    pub const FORMAT_VERSION: u32 = {manifest_ver};\n}}\n"
        ),
        "crates/kms/src/backends/local.rs": "pub(crate) const STORED_MASTER_KEY_FORMAT_VERSION: u32 = 1;\n",
        "crates/kms/src/encryption/dek.rs": "pub const CONTEXT_BINDING_AAD_V1: u8 = 1;\n",
        "crates/sse/src/kms/local.rs": "pub(crate) const LOCAL_SSE_DEK_FORMAT_VERSION: u8 = 1;\n",
        "crates/rio/src/encrypt_reader.rs": f"const FRAME_TYPE_SEGMENT_NONCE: u8 = {seg_nonce};\n",
        "crates/kms/src/api_types.rs": "#[test]\nfn test_mount_path_accepted() {}\n",
    }
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


class CheckKmsFormatVersionsTests(unittest.TestCase):
    def test_live_repository_passes(self) -> None:
        self.assertEqual(guard.check_repo(REPO_ROOT), [])

    def test_valid_fixture_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root)
            self.assertEqual(guard.check_repo(root), [])

    def test_tampered_rust_constant_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root, manifest_ver="2")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("BackupManifest::FORMAT_VERSION" in f and "2" in f for f in failures),
                f"expected version mismatch failure, got: {failures}",
            )

    def test_tampered_hex_constant_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root, seg_nonce="0x04")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("FRAME_TYPE_SEGMENT_NONCE" in f and "0x04" in f for f in failures),
                f"expected hex mismatch failure, got: {failures}",
            )

    def test_tampered_policy_value_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tampered_policy = SAMPLE_POLICY.replace("| `LOCAL_SSE_DEK_FORMAT_VERSION` | `1` |", "| `LOCAL_SSE_DEK_FORMAT_VERSION` | `2` |")
            write_fixture(root, policy=tampered_policy)
            failures = guard.check_repo(root)
            self.assertTrue(
                any("LOCAL_SSE_DEK_FORMAT_VERSION" in f for f in failures),
                f"expected policy value mismatch failure, got: {failures}",
            )

    def test_missing_constant_in_source_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root)
            (root / "crates/kms/src/encryption/dek.rs").write_text("// removed\n", encoding="utf-8")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("CONTEXT_BINDING_AAD_V1" in f and "not found" in f for f in failures),
                f"expected missing constant failure, got: {failures}",
            )

    def test_undocumented_kms_format_version_constant_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root)
            extra = root / "crates/kms/src/backends/new_format.rs"
            extra.write_text("pub const NEW_RECORD_FORMAT_VERSION: u32 = 1;\n", encoding="utf-8")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("NEW_RECORD_FORMAT_VERSION" in f and "undocumented" in f for f in failures),
                f"expected undocumented constant failure, got: {failures}",
            )

    def test_missing_deprecation_guard_test_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root)
            (root / "crates/kms/src/api_types.rs").write_text("// guard deleted\n", encoding="utf-8")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("test_mount_path_accepted" in f and "not found" in f for f in failures),
                f"expected missing deprecation guard failure, got: {failures}",
            )

    def test_missing_version_table_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_fixture(root, policy="# Empty policy\n")
            failures = guard.check_repo(root)
            self.assertTrue(
                any("## Version Anchors" in f for f in failures),
                f"expected missing table failure, got: {failures}",
            )


if __name__ == "__main__":
    unittest.main()
