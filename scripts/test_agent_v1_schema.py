from __future__ import annotations

import copy
import json
import re
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "protocol" / "agent" / "v1" / "job-envelope.schema.json"
RUST_JOB_PATH = ROOT / "rustfs" / "src" / "connect" / "diagnostics" / "job.rs"
FIXTURE = ROOT / "protocol" / "agent" / "v1" / "fixtures" / "diagnostic-scheduler" / "health-service-accept-vectors.json"


class JobEnvelopeSchemaRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(cls.schema)
        cls.validator = Draft202012Validator(cls.schema, format_checker=FormatChecker())
        vectors = json.loads(FIXTURE.read_text(encoding="utf-8"))["vectors"]
        cls.valid_job = next(v["value"] for v in vectors if v["schema"] == "job-envelope.schema.json")

    def assert_invalid(self, document, message: str):
        errors = list(self.validator.iter_errors(document))
        self.assertTrue(errors, message)

    def _job_branch(self, job_type: str):
        for branch in self.schema.get("allOf", []):
            actual = branch.get("if", {}).get("properties", {}).get("jobType", {}).get("const")
            if actual == job_type:
                return branch["then"]["properties"]
        self.fail(f"schema has no conditional branch for {job_type}")

    @staticmethod
    def _rust_u64_constant(name: str) -> int:
        source = RUST_JOB_PATH.read_text(encoding="utf-8")
        match = re.search(rf"^const\s+{re.escape(name)}\s*:\s*u64\s*=\s*([^;]+);", source, re.MULTILINE)
        if not match:
            raise AssertionError(f"Rust source constant {name} was not found")
        expression = match.group(1)
        # The protocol constants used here are integer literals / products only.
        factors = re.findall(r"\d[\d_]*", expression)
        if not factors or re.sub(r"[\d_\s*]", "", expression):
            raise AssertionError(f"unsupported Rust constant expression for {name}: {expression!r}")
        result = 1
        for factor in factors:
            result *= int(factor.replace("_", ""))
        return result

    def test_existing_job_fixture_remains_accepted(self):
        self.assertEqual([], list(self.validator.iter_errors(self.valid_job)))

    def test_drive_sample_period_is_exact(self):
        document = copy.deepcopy(self.valid_job)
        document["jobType"] = "performance.drive"
        document["requiredCapabilities"] = ["performance.drive@1"]
        document["parameters"].pop("maxEvidenceAgeSeconds", None)
        document["parameters"].update({
            "durationMillis": 5000,
            "samplePeriodMicros": 20000,
            "targetAlias": "drive-1",
            "scratchBytes": 524288,
            "blockBytes": 65536,
        })
        document["limits"].update({
            "timeoutSeconds": 30,
            "maxOutputBytes": 524288,
            "maxMemoryBytes": 67108864,
            "maxCpuMillis": 5000,
        })
        self.assert_invalid(document, "drive samplePeriodMicros != 10000 must be rejected")

    def test_drive_cpu_budget_is_exact(self):
        document = copy.deepcopy(self.valid_job)
        document["jobType"] = "performance.drive"
        document["requiredCapabilities"] = ["performance.drive@1"]
        document["parameters"].pop("maxEvidenceAgeSeconds", None)
        document["parameters"].update({
            "durationMillis": 5000,
            "samplePeriodMicros": 10000,
            "targetAlias": "drive-1",
            "scratchBytes": 524288,
            "blockBytes": 65536,
        })
        document["limits"].update({
            "timeoutSeconds": 30,
            "maxOutputBytes": 524288,
            "maxMemoryBytes": 67108864,
            "maxCpuMillis": 1000,
        })
        self.assert_invalid(document, "drive maxCpuMillis != 5000 must be rejected")

    def _low_memory_job(self, job_type: str, capability: str):
        document = copy.deepcopy(self.valid_job)
        document["jobType"] = job_type
        document["requiredCapabilities"] = [capability]
        document["parameters"].update({"durationMillis": 5000, "samplePeriodMicros": 10000})
        document["parameters"].pop("maxEvidenceAgeSeconds", None)
        document["parameters"].pop("targetAlias", None)
        document["parameters"].pop("scratchBytes", None)
        document["parameters"].pop("blockBytes", None)
        document["parameters"].pop("trafficBytes", None)
        document["limits"]["maxMemoryBytes"] = 1000
        document["limits"]["maxCpuMillis"] = 5000
        if job_type == "performance.network":
            document["parameters"]["trafficBytes"] = 1024
        return document


    def _valid_drive_job(self):
        document = copy.deepcopy(self.valid_job)
        document["jobType"] = "performance.drive"
        document["requiredCapabilities"] = ["performance.drive@1"]
        document["parameters"].pop("maxEvidenceAgeSeconds", None)
        document["parameters"].update({
            "durationMillis": 5000,
            "samplePeriodMicros": 10000,
            "targetAlias": "drive-1",
            "scratchBytes": 524288,
            "blockBytes": 65536,
        })
        document["limits"].update({
            "timeoutSeconds": 30,
            "maxOutputBytes": 524288,
            "maxMemoryBytes": 67108864,
            "maxCpuMillis": 5000,
        })
        return document

    def test_drive_timeout_is_fixed_at_30_seconds(self):
        document = self._valid_drive_job()
        document["limits"]["timeoutSeconds"] = 20
        self.assert_invalid(document, "drive timeoutSeconds != 30 must be rejected")

    def test_drive_output_budget_is_fixed_at_512_kib(self):
        document = self._valid_drive_job()
        document["limits"]["maxOutputBytes"] = 262144
        self.assert_invalid(document, "drive maxOutputBytes != 524288 must be rejected")

    def test_drive_memory_budget_is_fixed_at_64_mib(self):
        document = self._valid_drive_job()
        document["limits"]["maxMemoryBytes"] = 33554432
        self.assert_invalid(document, "drive maxMemoryBytes != 67108864 must be rejected")

    def test_network_minimum_memory_is_enforced(self):
        self.assert_invalid(self._low_memory_job("performance.network", "performance.network@1"), "network memory below 1 MiB")

    def test_top_api_minimum_memory_is_enforced(self):
        self.assert_invalid(self._low_memory_job("top.api", "top.api@1"), "top.api memory below 1 MiB")

    def test_top_rpc_minimum_memory_is_enforced(self):
        self.assert_invalid(self._low_memory_job("top.rpc", "top.rpc@1"), "top.rpc memory below 1 MiB")

    def test_top_locks_minimum_memory_is_enforced(self):
        self.assert_invalid(self._low_memory_job("top.locks", "top.locks@1"), "top.locks memory below 1 MiB")

    def test_ed25519_signature_encoding_is_exactly_86_chars(self):
        document = copy.deepcopy(self.valid_job)
        document["signature"]["value"] = "A" * 43
        self.assert_invalid(document, "43-character signature must be rejected")

    def test_key_id_must_be_64_lower_hex(self):
        document = copy.deepcopy(self.valid_job)
        document["signature"]["keyId"] = "k1"
        self.assert_invalid(document, "malformed keyId must be rejected")

    def test_nonce_must_have_canonical_base64url_tail_bits(self):
        document = copy.deepcopy(self.valid_job)
        nonce = document["nonce"]
        document["nonce"] = nonce[:-1] + "B"
        self.assert_invalid(document, "noncanonical 32-byte nonce encoding must be rejected")

    def test_signature_rejects_trailing_newline(self):
        document = copy.deepcopy(self.valid_job)
        document["signature"]["value"] += "\n"
        self.assert_invalid(document, "signature with trailing newline must be rejected")

    def test_key_id_rejects_trailing_newline(self):
        document = copy.deepcopy(self.valid_job)
        document["signature"]["keyId"] += "\n"
        self.assert_invalid(document, "keyId with trailing newline must be rejected")

    def test_nonce_rejects_trailing_newline(self):
        document = copy.deepcopy(self.valid_job)
        document["nonce"] += "\n"
        self.assert_invalid(document, "nonce with trailing newline must be rejected")

    def test_date_time_format_is_asserted(self):
        document = copy.deepcopy(self.valid_job)
        document["createTime"] = "not-a-date-time"
        self.assert_invalid(document, "invalid RFC3339 createTime must be rejected")

    def test_drive_schema_limits_match_rust_runtime_constants(self):
        drive = self._job_branch("performance.drive")
        limits = drive["limits"]["properties"]
        parameters = drive["parameters"]["properties"]
        expected = {
            "timeoutSeconds": 30,
            "maxOutputBytes": self._rust_u64_constant("MAX_OUTPUT_BYTES"),
            "maxMemoryBytes": self._rust_u64_constant("MAX_MEMORY_BYTES"),
            "maxCpuMillis": self._rust_u64_constant("DRIVE_DURATION_MILLIS"),
        }
        for field, value in expected.items():
            self.assertEqual(limits[field].get("const"), value, f"drive limits.{field} drifted from Rust")
        self.assertEqual(
            parameters["durationMillis"].get("const"),
            self._rust_u64_constant("DRIVE_DURATION_MILLIS"),
        )
        self.assertEqual(
            parameters["samplePeriodMicros"].get("const"),
            self._rust_u64_constant("DRIVE_SAMPLE_PERIOD_MICROS"),
        )

    def test_schema_memory_minimums_match_rust_runtime_constants(self):
        pairs = {
            "performance.network": "MIN_NETWORK_MEMORY_BYTES",
            "top.api": "MIN_TOP_API_MEMORY_BYTES",
            "top.rpc": "MIN_TOP_RPC_MEMORY_BYTES",
            "top.locks": "MIN_TOP_LOCKS_MEMORY_BYTES",
        }
        for job_type, constant in pairs.items():
            with self.subTest(job_type=job_type):
                limits = self._job_branch(job_type)["limits"]["properties"]
                self.assertEqual(
                    limits["maxMemoryBytes"].get("minimum"),
                    self._rust_u64_constant(constant),
                    f"{job_type} memory minimum drifted from Rust",
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
