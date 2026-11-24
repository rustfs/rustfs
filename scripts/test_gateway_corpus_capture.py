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

        for retained, status in [(999, 1), (1000, 0)]:
            with self.subTest(retained=retained):
                def fake_collect(args, temp):
                    write_entries(temp / "raw/corpus/raw.jsonl", 1500)
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

                with patch.object(capture, "collect", side_effect=fake_collect):
                    self.assertEqual(capture.main(self.args), 0)
                self.assertEqual(capture.main(["check-size", str(self.summary)]), status)
                report = json.loads(self.summary.read_text())
                self.assertEqual(report["retained_total"], retained)
                self.assertEqual(sum(report["raw_counts"].values()), 1500)
                self.assertFalse(report["full_suite_coverage"])
                self.assertEqual(report["official_reference_replay"], "not run")


if __name__ == "__main__":
    unittest.main()
