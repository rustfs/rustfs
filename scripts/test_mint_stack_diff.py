#!/usr/bin/env python3
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contract tests for scripts/mint_stack_diff.py."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parent / "mint_stack_diff.py"
LEGACY_SHA = "5addae2f74756bf80104d0975e6347cda843bc2daffdfcd1f0cb80118b95cbd0"
GATEWAY_SHA = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"


def entry(name: str, function: str | None, status: str, **extra: object) -> str:
    payload: dict[str, object] = {"name": name, "status": status}
    if function is not None:
        payload["function"] = function
    payload.update(extra)
    return json.dumps(payload)


def write_leg(
    root: Path,
    leg: str,
    lines: list[str] | None,
    meta: dict | None = None,
) -> Path:
    leg_dir = root / leg
    (leg_dir / "mint-log").mkdir(parents=True, exist_ok=True)
    if lines is not None:
        (leg_dir / "mint-log" / "log.json").write_text("\n".join(lines) + "\n", encoding="utf-8")
    if meta is not None:
        (leg_dir / "target.json").write_text(json.dumps(meta) + "\n", encoding="utf-8")
    return leg_dir


def default_meta(leg: str) -> dict[str, str]:
    if leg == "legacy":
        return {
            "leg": "legacy",
            "binary_sha256": LEGACY_SHA,
            "expected_sha256": LEGACY_SHA,
            "version": "rustfs 1.0.5",
            "expected_version": "1.0.5",
        }
    return {
        "leg": "gateway",
        "binary_sha256": GATEWAY_SHA,
        "version": "rustfs 1.0.8-dev",
    }


def run_cli(
    legacy_lines: list[str] | None,
    gateway_lines: list[str] | None,
    *,
    legacy_meta: dict | None = None,
    gateway_meta: dict | None = None,
    require_meta: bool = True,
) -> tuple[int, str, str, str, dict]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        l_meta = default_meta("legacy") if (require_meta and legacy_meta is None) else legacy_meta
        g_meta = default_meta("gateway") if (require_meta and gateway_meta is None) else gateway_meta
        legacy_dir = write_leg(root, "legacy", legacy_lines, l_meta)
        gateway_dir = write_leg(root, "gateway", gateway_lines, g_meta)
        md_path = root / "out" / "diff.md"
        json_path = root / "out" / "diff.json"
        cmd = [
            sys.executable,
            str(SCRIPT),
            str(legacy_dir),
            str(gateway_dir),
            "--markdown",
            str(md_path),
            "--json",
            str(json_path),
        ]
        if require_meta:
            cmd.append("--require-meta")
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        md_text = md_path.read_text(encoding="utf-8")
        json_data = json.loads(json_path.read_text(encoding="utf-8"))
        return proc.returncode, proc.stdout, proc.stderr, md_text, json_data


BASE_LEGACY = [
    "Dependency validation complete",
    entry("awscli", "make_bucket\n", "PASS"),
    entry(
        "awscli",
        "aws --endpoint-url http://rustfs-mint:9000 s3api get-object --bucket awscli-mint-test-bucket-18384 --key datafile-1-kB /tmp/datafile-1-kB\n",
        "PASS",
    ),
    entry("healthcheck", "testPrometheusEndpoint", "FAIL"),
    entry("minio-java", "setBucketNotification()", "NA"),
    entry("minio-go: testFunctional", "MakeBucket(), PutObject(), RemoveBucket()", "PASS"),
    entry("minio-py:test_get_object", "", "PASS"),
    entry("minio-js", "copyObject(bucketName, objectName, srcObject, conditions, cb)", "PASS"),
    entry("minio-js", "copyObject(bucketName, objectName, srcObject, conditions, cb)", "FAIL"),
    entry("versioning", "/mint/build/versioning/legalhold.go:175: main.testLockingLegalhold", "FAIL"),
]

BASE_GATEWAY = [
    entry("awscli", "make_bucket\n", "PASS"),
    entry(
        "awscli",
        "aws --endpoint-url http://127.0.0.1:9000 s3api get-object --bucket awscli-mint-test-bucket-99102 --key datafile-1-kB /tmp/datafile-1-kB\n",
        "PASS",
    ),
    entry("healthcheck", "testPrometheusEndpoint", "FAIL"),
    entry("minio-java", "setBucketNotification()", "NA"),
    entry("minio-go: testFunctional", "MakeBucket(), PutObject(), RemoveBucket()", "PASS"),
    entry("minio-py:test_get_object", None, "PASS"),
    entry("minio-js", "copyObject(bucketName, objectName, srcObject, conditions, cb)", "PASS"),
    entry("minio-js", "copyObject(bucketName, objectName, srcObject, conditions, cb)", "FAIL"),
    entry("versioning", "/mint/build/versioning/legalhold.go:175: main.testLockingLegalhold", "FAIL"),
]


class MintStackDiffTests(unittest.TestCase):
    def test_identical_outcomes_with_ephemeral_awscli_noise_pass_with_empty_diff_table(self) -> None:
        code, stdout, stderr, md, data = run_cli(BASE_LEGACY, BASE_GATEWAY)
        self.assertEqual(code, 0, stderr)
        self.assertEqual(data["verdict"], "pass")
        self.assertEqual(data["differences"], [])
        self.assertIn("## Diff Table (0)", md)
        self.assertIn("_Empty:", md)
        self.assertEqual(stdout, md)

    def test_legacy_pass_gateway_fail_breaks_gate_and_populates_diff_table(self) -> None:
        mutated = [
            entry("awscli", "make_bucket\n", "FAIL") if '"make_bucket\\n"' in line else line
            for line in BASE_GATEWAY
        ]
        code, _, _, md, data = run_cli(BASE_LEGACY, mutated)
        self.assertEqual(code, 1)
        self.assertEqual(data["verdict"], "fail")
        self.assertEqual(len(data["differences"]), 1)
        diff = data["differences"][0]
        self.assertEqual((diff["sdk"], diff["test"], diff["legacy"], diff["gateway"]), ("awscli", "make_bucket", "PASS", "FAIL"))
        self.assertIn("| `awscli` | `make_bucket` | `PASS` | `FAIL` |", md)

    def test_legacy_fail_gateway_pass_also_fails_strict_empty_diff_table_gate(self) -> None:
        mutated = [
            entry("healthcheck", "testPrometheusEndpoint", "PASS")
            if "testPrometheusEndpoint" in line
            else line
            for line in BASE_GATEWAY
        ]
        code, _, _, md, data = run_cli(BASE_LEGACY, mutated)
        self.assertEqual(code, 1)
        self.assertEqual(data["verdict"], "fail")
        self.assertEqual(len(data["differences"]), 1)
        self.assertIn("| `healthcheck` | `testPrometheusEndpoint` | `FAIL` | `PASS` |", md)

    def test_missing_test_on_gateway_fails_with_missing_outcome(self) -> None:
        truncated = [line for line in BASE_GATEWAY if "test_get_object" not in line]
        code, _, _, md, data = run_cli(BASE_LEGACY, truncated)
        self.assertEqual(code, 1)
        self.assertEqual(len(data["differences"]), 1)
        diff = data["differences"][0]
        self.assertEqual(
            (diff["sdk"], diff["test"], diff["legacy"], diff["gateway"]),
            ("minio-py", "test_get_object", "PASS", "MISSING"),
        )
        self.assertIn("| `minio-py` | `test_get_object` | `PASS` | `MISSING` |", md)

    def test_extra_test_on_gateway_fails_with_legacy_missing(self) -> None:
        extra = BASE_GATEWAY + [entry("mc", "test_extra", "PASS")]
        code, _, _, md, data = run_cli(BASE_LEGACY, extra)
        self.assertEqual(code, 1)
        self.assertIn("| `mc` | `test_extra` | `MISSING` | `PASS` |", md)
        self.assertEqual(data["differences"][0]["legacy"], "MISSING")

    def test_minio_go_pass_vs_fail_aligns_to_single_diff_row(self) -> None:
        mutated = [
            entry("minio-go: testFunctional", "SetBucketPolicy(bucketName, policy)", "FAIL")
            if "minio-go: testFunctional" in line
            else line
            for line in BASE_GATEWAY
        ]
        code, _, _, md, data = run_cli(BASE_LEGACY, mutated)
        self.assertEqual(code, 1)
        self.assertEqual(len(data["differences"]), 1)
        diff = data["differences"][0]
        self.assertEqual(
            (diff["sdk"], diff["test"], diff["legacy"], diff["gateway"]),
            ("minio-go", "testFunctional", "PASS", "FAIL"),
        )
        self.assertIn("| `minio-go` | `testFunctional` | `PASS` | `FAIL` |", md)

    def test_versioning_file_line_prefix_matches_plain_passing_name(self) -> None:
        mutated = [
            entry("versioning", "testLockingLegalhold", "PASS")
            if "testLockingLegalhold" in line
            else line
            for line in BASE_GATEWAY
        ]
        code, _, _, md, data = run_cli(BASE_LEGACY, mutated)
        self.assertEqual(code, 1)
        self.assertEqual(len(data["differences"]), 1)
        self.assertIn("| `versioning` | `testLockingLegalhold` | `FAIL` | `PASS` |", md)

    def test_repeated_case_additional_failure_is_not_masked(self) -> None:
        # Both legs have at least one FAIL for minio-js copyObject, but gateway fails both invocations.
        mutated = [
            entry("minio-js", "copyObject(bucketName, objectName, srcObject, conditions, cb)", "FAIL")
            if "copyObject" in line
            else line
            for line in BASE_GATEWAY
        ]
        code, _, _, md, data = run_cli(BASE_LEGACY, mutated)
        self.assertEqual(code, 1)
        self.assertEqual(len(data["differences"]), 1)
        diff = data["differences"][0]
        self.assertEqual(diff["legacy"], "FAIL (1 PASS, 1 FAIL)")
        self.assertEqual(diff["gateway"], "FAIL (2)")
        self.assertIn("FAIL (1 PASS, 1 FAIL)", md)

    def test_missing_log_fails_closed_with_exit_2(self) -> None:
        code, _, stderr, md, data = run_cli(BASE_LEGACY, None)
        self.assertEqual(code, 2)
        self.assertEqual(data["verdict"], "error")
        self.assertIn("missing Mint log file", stderr)
        self.assertIn("missing Mint log file", md)

    def test_empty_or_progress_only_log_fails_closed_with_exit_2(self) -> None:
        for bad in ([""], ["Dependency validation complete"]):
            with self.subTest(bad=bad):
                code, _, stderr, md, data = run_cli(BASE_LEGACY, bad)
                self.assertEqual(code, 2)
                self.assertEqual(data["verdict"], "error")
                self.assertIn("zero test entries", stderr)
                self.assertIn("zero test entries", md)

    def test_corrupt_json_line_fails_closed_with_exit_2(self) -> None:
        bad = BASE_GATEWAY + ['{"name": "mc", "function": "ls", "sta']
        code, _, stderr, _, data = run_cli(BASE_LEGACY, bad)
        self.assertEqual(code, 2)
        self.assertEqual(data["verdict"], "error")
        self.assertIn("malformed JSON", stderr)

    def test_unknown_status_fails_closed_with_exit_2(self) -> None:
        bad = BASE_GATEWAY + [entry("mc", "test_bad", "ERROR")]
        code, _, stderr, _, data = run_cli(BASE_LEGACY, bad)
        self.assertEqual(code, 2)
        self.assertEqual(data["verdict"], "error")
        self.assertIn("unknown status", stderr)

    def test_missing_name_fails_closed_with_exit_2(self) -> None:
        bad = BASE_GATEWAY + [json.dumps({"function": "ls", "status": "PASS"})]
        code, _, stderr, _, _ = run_cli(BASE_LEGACY, bad)
        self.assertEqual(code, 2)
        self.assertIn("missing or non-string 'name'", stderr)

    def test_identical_binary_sha256_self_comparison_fails_closed(self) -> None:
        same_gateway = {**default_meta("gateway"), "binary_sha256": LEGACY_SHA}
        code, _, _, md, data = run_cli(BASE_LEGACY, BASE_GATEWAY, gateway_meta=same_gateway)
        self.assertEqual(code, 1)
        self.assertEqual(data["verdict"], "fail")
        self.assertIn("refusing self-comparison", md)

    def test_wrong_leg_or_mismatched_expected_sha256_fails_closed(self) -> None:
        wrong_sha = {**default_meta("legacy"), "binary_sha256": "0" * 64}
        code, _, _, md, _ = run_cli(BASE_LEGACY, BASE_GATEWAY, legacy_meta=wrong_sha)
        self.assertEqual(code, 1)
        self.assertIn("expected_sha256", md)

        wrong_leg = {**default_meta("gateway"), "leg": "legacy"}
        code, _, _, md, _ = run_cli(BASE_LEGACY, BASE_GATEWAY, gateway_meta=wrong_leg)
        self.assertEqual(code, 1)
        self.assertIn("declares leg='legacy'", md)

    def test_missing_required_provenance_metadata_fails_closed_with_exit_2(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy_dir = write_leg(root, "legacy", BASE_LEGACY, default_meta("legacy"))
            gateway_dir = write_leg(root, "gateway", BASE_GATEWAY, None)
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(legacy_dir), str(gateway_dir), "--require-meta"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 2)
            self.assertIn("missing provenance metadata file", proc.stderr)

    def test_direct_file_mode_without_meta_succeeds_when_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            leg_file = root / "legacy.json"
            gw_file = root / "gateway.json"
            leg_file.write_text("\n".join(BASE_LEGACY) + "\n", encoding="utf-8")
            gw_file.write_text("\n".join(BASE_GATEWAY) + "\n", encoding="utf-8")
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(leg_file), str(gw_file)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("PASS — diff table is empty", proc.stdout)


if __name__ == "__main__":
    unittest.main()
