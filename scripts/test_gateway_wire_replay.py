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
"""Regression tests for the release wire comparison and its verdict."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import gateway_wire_replay as wire


class WireTests(unittest.TestCase):
    def test_xml_canonicalization_and_changed_error_code(self):
        a = b"<Error>\n <Code>NoSuchKey</Code><RequestId>r1</RequestId> </Error>"
        b = b"<Error><Code>NoSuchKey</Code>\n<RequestId>r2</RequestId></Error>"
        def project(body):
            return wire.project("GetObject", "GET", 404, [("Content-Length", str(len(body)))], body)
        self.assertEqual(wire.differences(project(a), project(b)), [])
        self.assertEqual(wire.differences(project(a), project(b.replace(b"NoSuchKey", b"NoSuchBucket"))), ["error_code", "body"])

    def test_get_compares_object_bytes_even_for_xml_and_request_id(self):
        a = b"<doc><RequestId>one</RequestId></doc>"
        b = b"<doc><RequestId>two</RequestId></doc>"
        left = wire.project("GetObject", "GET", 200, [("ETag", "a")], a)
        right = wire.project("GetObject", "GET", 206, [("ETag", "b")], b)
        self.assertEqual(wire.differences(left, right), ["status", "headers", "body"])

    def test_header_presence_repetitions_and_head_length(self):
        fields = [("X-Amz-Meta-Key", "a"), ("x-amz-meta-key", "b")]
        def project(fields):
            return wire.project("HeadObject", "HEAD", 200, fields, b"")
        self.assertNotEqual(project(fields), project(fields[::-1]))
        self.assertNotEqual(project(fields), project(fields[:1]))
        self.assertNotEqual(project([("content-length", "16")]), project([("Content-Length", "32")]))
        self.assertEqual(project([("X-Amz-Request-Id", "foo")]), project([("Date", "other")]))

    def test_object_read_metadata_headers(self):
        fields = [
            ("Cache-Control", "max-age=60", "no-cache"),
            ("Content-Disposition", "inline", "attachment"),
            ("Content-Encoding", "identity", "gzip"),
            ("Content-Language", "en", "fr"),
            ("Expires", "Wed, 01 Jan 2031 00:00:00 GMT", "Thu, 02 Jan 2031 00:00:00 GMT"),
            ("X-Amz-Website-Redirect-Location", "/original", "/updated"),
            ("X-Amz-Object-Lock-Mode", "GOVERNANCE", "COMPLIANCE"),
            ("X-Amz-Object-Lock-Retain-Until-Date", "2031-01-01T00:00:00Z", "2031-01-02T00:00:00Z"),
            ("X-Amz-Object-Lock-Legal-Hold", "ON", "OFF"),
        ]
        for operation, method, body in [("GetObject", "GET", b"object"), ("HeadObject", "HEAD", b"")]:
            for name, value, changed in fields:
                reference = wire.project(operation, method, 200, [(name, value)], body)
                for scenario, headers, expected in [
                    ("missing", [], ["headers"]),
                    ("changed", [(name, changed)], ["headers"]),
                    ("same", [(name.lower(), value)], []),
                ]:
                    with self.subTest(operation=operation, header=name, scenario=scenario):
                        candidate = wire.project(operation, method, 200, headers, body)
                        self.assertEqual(wire.differences(reference, candidate), expected)

    def test_content_length_checked_before_wire_differences(self):
        with self.assertRaises(wire.ReplayError):
            wire.project("GetObject", "GET", 200, [("Content-Length", "1")], b"two")

    def test_executable_digest_rejects_changed_file(self):
        with tempfile.TemporaryDirectory() as temp:
            binary = Path(temp) / "rustfs"
            binary.write_bytes(b"reference fixture")
            pinned = hashlib.sha256(binary.read_bytes()).hexdigest()
            wire.verify_digest(binary, pinned)
            binary.write_bytes(b"changed")
            with self.assertRaises(wire.ReplayError):
                wire.verify_digest(binary, pinned)

    def test_gate_propagates_difference_missing_traffic_and_missing_control(self):
        with tempfile.TemporaryDirectory() as temp:
            summary = Path(temp) / "report.json"
            counts = dict.fromkeys(wire.replay_cases(), 1)
            ok = {"requests_compared": len(counts), "requests_by_case": counts, "differences": [],
                  "negative_controls": wire.EXPECTED_CONTROLS}
            for change, status in [({}, 0), ({"requests_compared": 0}, 1), ({"differences": [{"fields": ["body"]}]}, 1),
                                   ({"negative_controls": {}}, 1), ({"requests_by_case": {}}, 1)]:
                with self.subTest(change=change):
                    summary.write_text(json.dumps(dict(ok, **change)))
                    self.assertEqual(wire.main(["check", str(summary)]), status)


if __name__ == "__main__":
    unittest.main()
