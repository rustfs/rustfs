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
    def _compare(self, replay, date, reference, candidate, *, ref_status=200, cand_status=200, method="PUT",
                 target="/private-bucket", host=None, ref_body=b"", cand_body=b"", extra_request_headers=()):
        headers = Message()
        headers["Date"] = date
        for name, value in reference:
            headers[name] = value
        response = SimpleNamespace(status_code=ref_status, raw=SimpleNamespace(headers=headers))
        req_host = replay.authority if host is None else host
        req_lines = [f"{method} {target} HTTP/1.1", f"Host: {req_host}",
                     "Authorization: Credential=rustfsadmin-ci/test", *extra_request_headers]
        request = ("\r\n".join(req_lines) + "\r\n\r\n").encode()
        client, server = socket.socketpair()
        with client, server, patch.object(wire.socket, "create_connection", return_value=client):
            reply = [f"HTTP/1.1 {cand_status} Test", f"Date: {date}", *(f"{k}: {v}" for k, v in candidate)]
            server.sendall(("\r\n".join(reply) + "\r\n\r\n").encode() + cand_body)
            server.shutdown(socket.SHUT_WR)
            replay.compare(method, request, response, ref_body)

    def test_compare_reports_header_direction_without_wire_data(self):
        date = "Tue, 24 Nov 2026 00:00:00 GMT"
        replay = wire.WireReplay(9000, 9001, parsedate_to_datetime(date))
        replay.case = "ceph-case"
        replay.operation = "CreateBucket"

        def compare(reference, candidate, status=200, method="PUT"):
            self._compare(replay, date, reference, candidate, cand_status=status, method=method)

        location = [("Location", "/private-bucket")]
        compare([], location)
        self.assertEqual(replay.diffs, [])
        self.assertEqual(replay.accepted_diffs[-1], {
            "case": "ceph-case", "request": 1, "operation": "CreateBucket", "fields": ["headers"],
            "header_differences": {"candidate_added": ["location"]},
            "ruling": "accepted-change", "reference": wire.CREATE_BUCKET_LOCATION_RULING,
        })
        compare([], [("Location", "/other-private-bucket")])
        self.assertEqual(replay.diffs[-1], {"case": "ceph-case", "request": 2, "operation": "CreateBucket",
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
        for private in ("private", "other", "same-value", "first-value", "second-value", "123", "456"):
            self.assertNotIn(private, json.dumps(replay.diffs))
            self.assertNotIn(private, json.dumps(replay.accepted_diffs))

    def test_create_bucket_location_accepted_change_path_and_virtual_host(self):
        date = "Tue, 24 Nov 2026 00:00:00 GMT"
        frozen = parsedate_to_datetime(date)

        # Path-style CreateBucket (with and without trailing slash, and with matching shared headers).
        path_replay = wire.WireReplay(9000, 9001, frozen)
        path_replay.case = "test_bucket_create"
        path_replay.operation = "CreateBucket"
        self._compare(path_replay, date, [], [("Location", "/my-bucket")], target="/my-bucket")
        self._compare(path_replay, date, [], [("Location", "/my-bucket")], target="/my-bucket/")
        self._compare(path_replay, date, [("Content-Type", "application/xml")],
                      [("Content-Type", "application/xml"), ("Location", "/my-bucket")], target="/my-bucket")
        self.assertEqual(path_replay.diffs, [])
        self.assertEqual(len(path_replay.accepted_diffs), 3)
        for entry in path_replay.accepted_diffs:
            self.assertEqual(entry["ruling"], "accepted-change")
            self.assertEqual(entry["reference"], wire.CREATE_BUCKET_LOCATION_RULING)
            self.assertEqual(entry["header_differences"], {"candidate_added": ["location"]})

        # Virtual-hosted CreateBucket (without port, with port, dotted bucket, longest-domain match).
        vhost_replay = wire.WireReplay(
            9000, 9001, frozen,
            virtual_host_domains=("s3.example.com", "s3.us-east-1.example.com"),
        )
        vhost_replay.case = "test_bucket_create_vhost"
        vhost_replay.operation = "CreateBucket"
        for host, expected_loc in [
            ("my-bucket.s3.example.com", "/my-bucket"),
            ("my-bucket.s3.example.com:9000", "/my-bucket"),
            ("my.dotted.bucket.s3.example.com", "/my.dotted.bucket"),
            ("my-bucket.s3.us-east-1.example.com", "/my-bucket"),
        ]:
            with self.subTest(host=host):
                self._compare(vhost_replay, date, [], [("Location", expected_loc)], target="/", host=host)
        self.assertEqual(vhost_replay.diffs, [])
        self.assertEqual(len(vhost_replay.accepted_diffs), 4)

        # Configuring host directly also enables virtual-host domain extraction for that domain.
        host_replay = wire.WireReplay(9000, 9001, frozen, host="s3.example.com")
        host_replay.case = "test_bucket_create_host"
        host_replay.operation = "CreateBucket"
        self._compare(host_replay, date, [], [("Location", "/my-bucket")], target="/",
                      host="my-bucket.s3.example.com")
        self.assertEqual(host_replay.diffs, [])
        self.assertEqual(len(host_replay.accepted_diffs), 1)

    def test_create_bucket_location_exemption_rejects_drift_and_misuse(self):
        date = "Tue, 24 Nov 2026 00:00:00 GMT"
        frozen = parsedate_to_datetime(date)

        def run_case(*, operation="CreateBucket", method="PUT", target="/my-bucket", host=None,
                     ref_status=200, cand_status=200, ref_headers=(), cand_headers=(),
                     ref_body=b"", cand_body=b"", virtual_host_domains=()):
            replay = wire.WireReplay(9000, 9001, frozen, virtual_host_domains=virtual_host_domains)
            replay.case = "neg-case"
            replay.operation = operation
            self._compare(replay, date, ref_headers, cand_headers, ref_status=ref_status, cand_status=cand_status,
                          method=method, target=target, host=host, ref_body=ref_body, cand_body=cand_body)
            return replay

        negative_cases = [
            ("wrong_path_location", dict(cand_headers=[("Location", "/wrong-bucket")])),
            ("wrong_vhost_location", dict(target="/", host="my-bucket.s3.example.com",
                                          virtual_host_domains=("s3.example.com",),
                                          cand_headers=[("Location", "/wrong-bucket")])),
            ("vhost_wrong_domain_split", dict(target="/", host="my-bucket.s3.us-east-1.example.com",
                                              virtual_host_domains=("s3.example.com", "s3.us-east-1.example.com"),
                                              cand_headers=[("Location", "/my-bucket.s3.us-east-1")])),
            ("empty_location", dict(cand_headers=[("Location", "")])),
            ("trailing_slash_location", dict(cand_headers=[("Location", "/my-bucket/")])),
            ("duplicate_location", dict(cand_headers=[("Location", "/my-bucket"), ("Location", "/my-bucket")])),
            ("extra_etag_drift", dict(cand_headers=[("Location", "/my-bucket"), ("ETag", '"abc"')])),
            ("extra_content_type_drift", dict(ref_headers=[("Content-Type", "text/plain")],
                                              cand_headers=[("Content-Type", "application/xml"), ("Location", "/my-bucket")])),
            ("body_drift_with_location", dict(cand_headers=[("Location", "/my-bucket")], cand_body=b"unexpected")),
            ("non_200_409_adding_location", dict(ref_status=409, cand_status=409,
                                                 cand_headers=[("Location", "/my-bucket")])),
            ("status_mismatch", dict(ref_status=200, cand_status=201, cand_headers=[("Location", "/my-bucket")])),
            ("reference_has_location_candidate_omits", dict(ref_headers=[("Location", "/my-bucket")], cand_headers=[])),
            ("reference_location_changed", dict(ref_headers=[("Location", "/old-bucket")],
                                                cand_headers=[("Location", "/my-bucket")])),
            ("post_object_303_missing_location", dict(operation="PostObject", method="POST", ref_status=303, cand_status=303,
                                                      ref_headers=[("Location", "https://example.com/cb?bucket=my-bucket")],
                                                      cand_headers=[])),
            ("post_object_303_mismatched_location", dict(operation="PostObject", method="POST", ref_status=303, cand_status=303,
                                                         ref_headers=[("Location", "https://example.com/cb?a=1")],
                                                         cand_headers=[("Location", "https://example.com/cb?a=2")])),
            ("put_object_adding_location", dict(operation="PutObject", cand_headers=[("Location", "/my-bucket")])),
            ("non_put_method", dict(method="GET", cand_headers=[("Location", "/my-bucket")])),
            ("path_with_object_key", dict(target="/my-bucket/key", cand_headers=[("Location", "/my-bucket")])),
            ("path_with_query_subresource", dict(target="/my-bucket?acl", cand_headers=[("Location", "/my-bucket")])),
            ("path_style_root_without_bucket", dict(target="/", cand_headers=[("Location", "/")])),
            ("vhost_with_object_key", dict(target="/some-key", host="my-bucket.s3.example.com",
                                           virtual_host_domains=("s3.example.com",),
                                           cand_headers=[("Location", "/my-bucket")])),
            ("vhost_bare_domain_root", dict(target="/", host="s3.example.com",
                                            virtual_host_domains=("s3.example.com",),
                                            cand_headers=[("Location", "/s3")])),
        ]
        for label, kwargs in negative_cases:
            with self.subTest(case=label):
                replay = run_case(**kwargs)
                self.assertEqual(len(replay.diffs), 1, label)
                self.assertEqual(replay.accepted_diffs, [], label)

        # Authority guards still reject unauthorized host, wrong port, or duplicate Host headers.
        replay = wire.WireReplay(9000, 9001, frozen, virtual_host_domains=("s3.example.com",))
        replay.case = "auth-guard"
        replay.operation = "CreateBucket"
        for bad_host, extra in [
            ("evil.example.com", ()),
            ("my-bucket.s3.example.com:8888", ()),
            ("my-bucket.s3.example.com", ("Host: other.s3.example.com",)),
        ]:
            with self.subTest(bad_host=bad_host, extra=extra), self.assertRaises(wire.ReplayError):
                self._compare(replay, date, [], [("Location", "/my-bucket")], target="/",
                              host=bad_host, extra_request_headers=extra)

    def test_synthetic_93_create_bucket_and_post_redirect_batch(self):
        date = "Tue, 24 Nov 2026 00:00:00 GMT"
        frozen = parsedate_to_datetime(date)
        cases = wire.replay_cases()
        post_location = "https://example.com/upload/callback?bucket=post-bkt&key=obj"

        def run_batch(corrupt_index=None):
            replay = wire.WireReplay(9000, 9001, frozen, virtual_host_domains=("s3.example.com",))
            for idx in range(93):
                replay.case = cases[idx % len(cases)]
                replay.operation = "CreateBucket"
                bucket = f"batch-bucket-{idx:03d}"
                cand_loc = "/wrong-bucket" if idx == corrupt_index else f"/{bucket}"
                if idx % 2 == 0:
                    self._compare(replay, date, [], [("Location", cand_loc)], target=f"/{bucket}")
                else:
                    self._compare(replay, date, [], [("Location", cand_loc)], target="/",
                                  host=f"{bucket}.s3.example.com")
            for idx, case in enumerate(cases):
                replay.case = case
                replay.operation = "PostObject"
                self._compare(replay, date, [("Location", post_location)], [("Location", post_location)],
                              ref_status=303, cand_status=303, method="POST", target=f"/post-bkt-{idx}")
            return {
                "requests_compared": sum(replay.counts.values()),
                "requests_by_case": dict(replay.counts),
                "differences": replay.diffs,
                "accepted_differences": replay.accepted_diffs,
                "accepted_differences_count": len(replay.accepted_diffs),
                "negative_controls": wire.EXPECTED_CONTROLS,
            }

        with tempfile.TemporaryDirectory() as temp:
            summary = Path(temp) / "report.json"
            green = run_batch()
            self.assertEqual(green["differences"], [])
            self.assertEqual(green["accepted_differences_count"], 93)
            summary.write_text(json.dumps(green))
            self.assertEqual(wire.main(["check", str(summary)]), 0)

            red = run_batch(corrupt_index=42)
            self.assertEqual(len(red["differences"]), 1)
            self.assertEqual(red["differences"][0]["header_differences"], {"candidate_added": ["location"]})
            self.assertEqual(red["accepted_differences_count"], 92)
            summary.write_text(json.dumps(red))
            self.assertEqual(wire.main(["check", str(summary)]), 1)

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
            valid_accepted = {
                "case": next(iter(counts)), "request": 1, "operation": "CreateBucket", "fields": ["headers"],
                "header_differences": {"candidate_added": ["location"]},
                "ruling": "accepted-change", "reference": wire.CREATE_BUCKET_LOCATION_RULING,
            }
            for change, status in [({}, 0), ({"requests_compared": 0}, 1), ({"differences": [{"fields": ["body"]}]}, 1),
                                   ({"differences": [{"fields": ["headers"], "header_differences": {"candidate_added": ["location"]}}]}, 1),
                                   ({"accepted_differences": [valid_accepted], "accepted_differences_count": 1}, 0),
                                   ({"accepted_differences": [valid_accepted], "accepted_differences_count": 0}, 1),
                                   ({"accepted_differences": [dict(valid_accepted, operation="PostObject")],
                                     "accepted_differences_count": 1}, 1),
                                   ({"accepted_differences": [dict(valid_accepted, reference="unruled")],
                                     "accepted_differences_count": 1}, 1),
                                   ({"accepted_differences": [dict(valid_accepted, header_differences={"candidate_missing": ["location"]})],
                                     "accepted_differences_count": 1}, 1),
                                   ({"accepted_differences": [dict(valid_accepted, extra="wire-leak")],
                                     "accepted_differences_count": 1}, 1),
                                   ({"accepted_differences": [dict(valid_accepted, case="unknown-case")],
                                     "accepted_differences_count": 1}, 1),
                                   ({"negative_controls": {}}, 1), ({"requests_by_case": {}}, 1)]:
                with self.subTest(change=change):
                    summary.write_text(json.dumps(dict(ok, **change)))
                    self.assertEqual(wire.main(["check", str(summary)]), status)


if __name__ == "__main__":
    unittest.main()
