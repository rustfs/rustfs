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
"""Exercise the nightly capture gate without building a server."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import gateway_corpus_capture as capture


class CaptureGateTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.temp = Path(temp.name)
        self.summary = self.temp / "summary.json"
        self.args = ["record", "--ceph", ".", "--gateway", ".", "--tool", "./corpus", "--summary", str(self.summary)]

    def test_smoke_and_nightly_selections(self):
        smoke = capture.select_tests("smoke")
        nightly = capture.select_tests("nightly")
        for suite in ("ceph", "e2e"):
            self.assertLess(set(smoke[suite]), set(nightly[suite]))
        manifest = Path(capture.__file__).resolve().parent / "s3-tests/implemented_tests.txt"
        self.assertLessEqual(set(nightly["ceph"]), set(manifest.read_text().splitlines()))
        self.assertIn("test_multipart_upload_contents", smoke["ceph"])
        self.assertIn("test_object_copy_versioned_bucket", nightly["ceph"])
        self.assertTrue(any("copy_object_metadata_test" in name for name in nightly["e2e"]))

    def test_default_nightly_and_explicit_capture_modes(self):
        for extra, expected in [([], "nightly"), (["--suite", "nightly"], "nightly"), (["--suite", "smoke"], "smoke")]:
            with self.subTest(args=extra), patch.object(capture, "collect", return_value={}) as collect, patch.object(capture, "scan"):
                self.assertEqual(capture.main(self.args + extra), 0)
                self.assertEqual(collect.call_args.args[0].suite, expected)

    def test_nextest_discovery_requires_each_selected_case(self):
        cases = {"a": {"ignored": False, "filter-match": {"status": "matches"}},
                 "b": {"ignored": False, "filter-match": {"status": "matches"}}}
        listing = {"rust-suites": {"e2e_test": {"testcases": cases}}}
        capture.check_listing(listing, ["a", "b"])
        for field, value in [("ignored", True), ("filter-match", {"status": "mismatch"})]:
            with self.subTest(field=field), patch.dict(cases["b"], {field: value}), self.assertRaises(ValueError):
                capture.check_listing(listing, ["a", "b"])
        with self.assertRaises(ValueError):
            capture.check_listing(listing, ["a"])
        listing["rust-suites"]["other"] = {"testcases": {"a": cases["a"]}}
        with self.assertRaises(ValueError):
            capture.check_listing(listing, ["a", "b"])

    def test_virtual_run_requires_a_bucket_in_the_recorded_host(self):
        path = self.temp / "requests.jsonl"
        for host in ["127.0.0.1:9000", "s3.localhost:9000", "bucket.s3.localhost.example"]:
            path.write_text(json.dumps({"headers": [["Host", host]]}) + "\n")
            with self.subTest(host=host), self.assertRaises(ValueError):
                capture.check_virtual_capture(self.temp)
        path.write_text(json.dumps({"headers": [["host", "bucket.s3.localhost:9000"]]}) + "\n")
        capture.check_virtual_capture(self.temp)

    def test_missing_or_unexpected_source(self):
        path = self.temp / "raw.jsonl"
        sources = {"s3-tests@fixture", "rustfs-e2e@fixture"}
        for entries in [[], [{"src": "s3-tests@fixture", "sut": "rustfs-server"}],
                        [{"src": "other", "sut": "rustfs-server"}],
                        [{"src": "rustfs-e2e@fixture", "sut": "client"}]]:
            with self.subTest(entries=entries):
                path.write_text("".join(json.dumps(e) + "\n" for e in entries))
                with self.assertRaises(ValueError):
                    capture.count_entries([path], sources)

    def test_capture_or_scanner_failure_never_publishes_old_summary(self):
        self.summary.write_text("stale")
        failure = subprocess.CalledProcessError(2, ["scan"])
        with patch.object(capture, "collect", side_effect=failure), self.assertRaises(subprocess.CalledProcessError):
            capture.main(self.args)
        self.assertFalse(self.summary.exists())

    def test_report_scan_failure_blocks_the_artifact(self):
        failure = subprocess.CalledProcessError(1, ["scan"])
        with patch.object(capture, "collect", return_value={}), patch.object(capture, "scan", side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError):
                capture.main(self.args)
        self.assertFalse(self.summary.exists())

    def test_each_validation_failure_stops_before_publication(self):
        sources = {"s3-tests@fixture", "rustfs-e2e@fixture"}
        for stage in ("raw", "ingest", "verify", "checked"):
            self.summary.write_text("stale")

            def fail_stage(args, temp):
                raw = temp / "raw/corpus/file.jsonl"
                raw.parent.mkdir(parents=True)
                raw.write_text("".join(json.dumps({"src": src, "sut": "rustfs-server"}) + "\n" for src in sources))

                def run(command, **kwargs):
                    which = Path(kwargs["env"]["GATEWAY_CHECK_ROOT"]).name if command[0] == "bash" else command[1]
                    if which == stage:
                        self.assertTrue(kwargs["check"])
                        raise subprocess.CalledProcessError(1, command)

                with patch.object(capture.subprocess, "run", side_effect=run):
                    return capture.verify_capture(temp, args, sources)

            with self.subTest(stage=stage), patch.object(capture, "collect", side_effect=fail_stage):
                with self.assertRaises(subprocess.CalledProcessError):
                    capture.main(self.args)
                self.assertFalse(self.summary.exists())

    def test_duplicate_drop_controls_status_and_report(self):
        sources = {"s3-tests@fixture", "rustfs-e2e@fixture"}

        def write_entries(path, count):
            path.parent.mkdir(parents=True, exist_ok=True)
            rows = [{"src": src, "sut": "rustfs-server"} for src in sorted(sources)]
            path.write_text("".join(json.dumps(rows[index % 2]) + "\n" for index in range(count)))

        for retained, status in [(999, 1), (1000, 0), (2000, 0), (2001, 1)]:
            with self.subTest(retained=retained):
                def fake_collect(args, temp):
                    write_entries(temp / "raw/corpus/raw.jsonl", 2400)
                    calls = []

                    def tool(command, **kwargs):
                        self.assertTrue(kwargs["check"])
                        if command[0] == "bash":
                            stage = Path(kwargs["env"]["GATEWAY_CHECK_ROOT"]).name
                        else:
                            stage = command[1]
                        calls.append(stage)
                        if stage == "ingest":
                            write_entries(command[-1] / "bucket/GetObject.jsonl", retained)

                    with patch.object(capture.subprocess, "run", side_effect=tool):
                        result = capture.verify_capture(temp, args, sources)
                    self.assertEqual(calls, ["raw", "ingest", "verify", "checked"])
                    return result

                with patch.object(capture, "collect", side_effect=fake_collect), patch.object(capture.subprocess, "run") as report_scan:
                    self.assertEqual(capture.main(self.args), 0)
                    self.assertEqual(report_scan.call_count, 1)
                    self.assertEqual(Path(report_scan.call_args.kwargs["env"]["GATEWAY_CHECK_ROOT"]).name, "report")
                self.assertEqual(capture.main(["check-size", str(self.summary)]), status)
                report = json.loads(self.summary.read_text())
                self.assertEqual(report["retained_total"], retained)
                self.assertEqual(sum(report["raw_counts"].values()), 2400)
                self.assertFalse(report["full_suite_coverage"])
                self.assertEqual(report["official_reference_replay"], "see gateway-wire-differences report")


if __name__ == "__main__":
    unittest.main()
