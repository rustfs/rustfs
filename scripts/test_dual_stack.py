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
"""Contract tests for dual_stack.py stack: which stack a leg's servers ran, proven from their own logs."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "dual_stack.py"
REFUSAL = "gateway stack is not assembled yet (rustfs/backlog#2734 T2.5)"


def run(*args):
    proc = subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def server_log(*events):
    return "\n".join(json.dumps(event) for event in events) + "\n"


def started(stack=None):
    event = {"level": "INFO", "event": "server_starting", "message": "Starting RustFS server"}
    if stack is not None:
        event["s3_stack"] = stack
    return event


class StackProofTests(unittest.TestCase):
    def prove(self, expect, files):
        with tempfile.TemporaryDirectory() as tmp:
            for name, text in files.items():
                (Path(tmp) / name).write_text(text)
            out = Path(tmp) / "evidence.json"
            code, _, err = run("stack", "--expect", expect, "--out", str(out), tmp)
            return code, json.loads(out.read_text()) if out.is_file() else None, err

    def test_a_server_of_the_other_stack_fails_the_proof(self):
        files = {"a.log": server_log(started("Legacy")), "b.log": server_log(started("Gateway"))}
        code, evidence, _ = self.prove("legacy", files)
        self.assertEqual(code, 1)
        self.assertEqual(evidence["foreign_starts"], 1)

    def test_the_other_direction_fails_too(self):
        code, evidence, _ = self.prove("gateway", {"a.log": server_log(started("Legacy"), started("Gateway"))})
        self.assertEqual((code, evidence["foreign_starts"]), (1, 1))

    def test_a_refused_start_proves_nothing_and_keeps_the_message(self):
        # The shape a refusing server logs: the cause is in `error`, `message` is generic.
        log = server_log({"level": "INFO", "message": "obs ready"},
                         {"level": "ERROR", "message": "Server runtime failed", "event": "server_runtime_failed", "error": REFUSAL})
        code, evidence, _ = self.prove("gateway", {"a.log": log})
        self.assertEqual(code, 1)
        self.assertEqual(evidence["not_started"], [{"message": REFUSAL, "logs": 1}])

    def test_unstamped_starts_alone_prove_nothing(self):
        code, evidence, _ = self.prove("legacy", {"old.log": server_log(started())})
        self.assertEqual((code, evidence["unstamped_starts"]), (1, 1))

    def test_empty_directory_is_an_input_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = run("stack", "--expect", "legacy", "--out", str(Path(tmp) / "e.json"), tmp)
            self.assertEqual(code, 2)
            self.assertIn("nothing proves", err)

    def test_missing_path_is_an_input_error(self):
        code, _, _ = run("stack", "--expect", "legacy", "--out", "/dev/null", "/nonexistent-dual-stack-logs")
        self.assertEqual(code, 2)

    def test_runner_output_without_log_events_is_not_a_server_log(self):
        files = {"a.log": server_log(started("Legacy")), "nextest.log": "PASS [ 1.0s] some::test\nSummary [ 2s] 1 test run\n"}
        code, evidence, _ = self.prove("legacy", files)
        self.assertEqual((code, evidence["logs_without_start"]), (0, 0))

    def test_a_plain_text_prefix_does_not_hide_the_event_or_split_the_message(self):
        line = "[FATAL] Server runtime failed: x" + json.dumps({"level": "ERROR", "error": REFUSAL}) + "\n"
        code, evidence, _ = self.prove("gateway", {"a.log": line, "b.log": line})
        self.assertEqual((code, evidence["not_started"]), (1, [{"message": REFUSAL, "logs": 2}]))

    def test_every_server_on_the_requested_stack_passes(self):
        files = {"a.log": server_log(started("Legacy")), "b.log": server_log(started("Legacy"), started("Legacy"))}
        code, evidence, _ = self.prove("legacy", files)
        self.assertEqual((code, evidence["started"]), (0, {"legacy": 3}))

    def test_older_release_servers_are_counted_not_failed(self):
        files = {"new.log": server_log(started("Gateway")), "old.log": server_log(started())}
        code, evidence, _ = self.prove("gateway", files)
        self.assertEqual((code, evidence["unstamped_starts"]), (0, 1))


if __name__ == "__main__":
    unittest.main()
