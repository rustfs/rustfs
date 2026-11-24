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
from email.message import Message
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import socket
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import gateway_wire_replay as wire


class WireTests(unittest.TestCase):
    def test_compare_reports_header_direction_without_wire_data(self):
        date = "Tue, 24 Nov 2026 00:00:00 GMT"
        replay = wire.WireReplay(9000, 9001, parsedate_to_datetime(date))
        replay.case = "ceph-case"
        replay.operation = "CreateBucket"

        def compare(reference, candidate, status=200, method="PUT"):
            headers = Message()
            headers["Date"] = date
            for name, value in reference:
                headers[name] = value
            response = SimpleNamespace(status_code=200, raw=SimpleNamespace(headers=headers))
            request = (f"{method} /private-bucket HTTP/1.1\r\nHost: {replay.authority}\r\n"
                       "Authorization: Credential=rustfsadmin-ci/test\r\n\r\n").encode()
            client, server = socket.socketpair()
            with client, server, patch.object(wire.socket, "create_connection", return_value=client):
                reply = [f"HTTP/1.1 {status} Test", f"Date: {date}", *(f"{k}: {v}" for k, v in candidate)]
                server.sendall(("\r\n".join(reply) + "\r\n\r\n").encode())
                server.shutdown(socket.SHUT_WR)
                replay.compare(method, request, response, b"")

        location = [("Location", "/private-bucket")]
        compare([], location)
        self.assertEqual(replay.diffs[-1], {"case": "ceph-case", "request": 1, "operation": "CreateBucket",
                                           "fields": ["headers"], "header_differences": {"candidate_added": ["location"]}})
        compare(location, [])
        self.assertEqual(replay.diffs[-1]["header_differences"], {"candidate_missing": ["location"]})
        prefixes = ("x-amz-meta-", "x-amz-checksum-", "x-amz-server-side-encryption")
        compare([(p + "old-private", "same-value") for p in prefixes],
                [(p + "new-private", "same-value") for p in prefixes])
        families = sorted(p + "*" for p in prefixes)
        self.assertEqual(replay.diffs[-1]["header_differences"], {"candidate_missing": families, "candidate_added": families})
        repeated = [("ETag", "first-value"), ("etag", "second-value")]
        for candidate in (repeated[::-1], repeated[:1]):
            compare(repeated, candidate)
            self.assertEqual(replay.diffs[-1]["header_differences"], {"values_changed": ["etag"]})
        count = len(replay.diffs)
        compare(repeated, [(k.upper(), v) for k, v in repeated])
        self.assertEqual(len(replay.diffs), count)
        replay.operation = "HeadObject"
        compare([("Content-Length", "123")], [("Content-Length", "456")], method="HEAD")
        self.assertEqual(replay.diffs[-1]["header_differences"], {"values_changed": ["content-length"]})
        compare([], [], status=204)
        self.assertEqual(replay.diffs[-1]["fields"], ["status"])
        self.assertNotIn("header_differences", replay.diffs[-1])
        for private in ("private", "same-value", "first-value", "second-value", "123", "456"):
            self.assertNotIn(private, json.dumps(replay.diffs))

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

    def test_s3_response_headers(self):
        cases = [
            ("CreateBucket", "PUT", 200, "Location", "/example-bucket"),
            ("PutObject", "PUT", 200, "X-Amz-Expiration", 'expiry-date="Tue, 01 Jan 2030 00:00:00 GMT", rule-id="logs"'),
            ("CopyObject", "PUT", 200, "X-Amz-Copy-Source-Version-Id", "source-version"),
            ("HeadObject", "HEAD", 200, "X-Amz-Replication-Status", "COMPLETED"),
            ("GetObject", "GET", 206, "X-Amz-Mp-Parts-Count", "3"),
        ]
        for operation, method, status, name, value in cases:
            with self.subTest(operation=operation, header=name):
                response = wire.project(operation, method, status, [(name, value)], b"")
                self.assertEqual(response["headers"], [(name.lower(), value)])

    def test_post_redirect_compares_location_including_query(self):
        target = "https://example.com/upload/callback?origin=test&bucket=b&key=a%2Fb"

        def project(headers):
            return wire.project("PostObject", "POST", 303, headers, b"")

        reference = project([("Location", target)])
        self.assertEqual(wire.differences(reference, project([("location", target)])), [])
        self.assertEqual(wire.differences(reference, project([])), ["headers"])
        self.assertEqual(wire.differences(reference, project([("Location", target.split("?")[0])])), ["headers"])

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
                                   ({"differences": [{"fields": ["headers"], "header_differences": {"candidate_added": ["location"]}}]}, 1),
                                   ({"negative_controls": {}}, 1), ({"requests_by_case": {}}, 1)]:
                with self.subTest(change=change):
                    summary.write_text(json.dumps(dict(ok, **change)))
                    self.assertEqual(wire.main(["check", str(summary)]), status)


if __name__ == "__main__":
    unittest.main()
