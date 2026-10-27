#!/usr/bin/env python3
"""Bounded ownership, inventory, memory and restart evidence tests; no live service."""

import copy
import io
import json
import os
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import uuid

import scanner_memory_acceptance as acceptance


class FakeClient:
    def __init__(self, pages, data):
        self.pages, self.data = iter(pages), data

    def list_objects_v2(self, **_):
        return next(self.pages)

    def get_object(self, **_):
        return {"Body": io.BytesIO(self.data)}


class AcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.root.chmod(0o700)
        data = self.root / "data"
        data.mkdir()
        run_id = str(uuid.uuid4())
        self.plan = {"run_id": run_id, "created_at": acceptance.utc(), "endpoint": "http://127.0.0.1:19000",
                     "source_sha": "a" * 40, "binary_sha256": "b" * 64, "layout": "wide", "objects": 7,
                     "object_bytes": 51200, "root_prefixes": 3, "memory_bytes": 4 * acceptance.GIB,
                     "data_root": str(data), "data_volume": None, "bucket": "scanner-memory-" + uuid.UUID(run_id).hex}
        self.manifest = {"schema": 1, "plan": self.plan, "plan_sha256": acceptance.digest(self.plan)}
        acceptance.atomic_json(self.root / "manifest.json", self.manifest)
        self.evidence = acceptance.Evidence(self.root)

    def api(self, entries, data=None, split=4):
        api = acceptance.S3.__new__(acceptance.S3)
        api.plan, api.evidence = self.plan, self.evidence
        api.owned = lambda: None
        pages = []
        for offset in range(0, len(entries), split):
            more = offset + split < len(entries)
            pages.append({"Contents": entries[offset:offset + split], "IsTruncated": more,
                          "NextContinuationToken": str(offset + split) if more else None})
        api.client = FakeClient(pages, acceptance.payload(self.plan) if data is None else data)
        return api

    def entries(self):
        return [{"Key": acceptance.OWNER_KEY, "Size": len(acceptance.owner_body(self.manifest))}] + [
            {"Key": acceptance.object_key(self.plan, index), "Size": self.plan["object_bytes"]}
            for index in range(self.plan["objects"])]

    def test_layout_roundtrip_and_root_width(self):
        for layout in ("wide", "nested"):
            self.plan["layout"] = layout
            keys = [acceptance.object_key(self.plan, index) for index in range(7)]
            self.assertEqual(len(set(keys)), 7)
            self.assertEqual([acceptance.key_index(self.plan, key) for key in keys], list(range(7)))
            self.assertEqual(len({key.split('/')[0] for key in keys}), 3 if layout == "wide" else 1)
        for key in ("photos/00/00/source:000001/00000000.webp", "foreign", "source:999999/00000000.webp"):
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.key_index(self.plan, key)

    def test_exact_inventory_payload_and_marker(self):
        result = self.api(self.entries()).verify(samples=3)
        self.assertEqual(result["objects"], 7)
        self.assertEqual(result["logical_bytes"], 7 * 51200)
        self.assertEqual(result["scanner_expected_objects"], 8)
        self.assertEqual(result["payload_samples"], 3)

    def test_inventory_rejects_missing_duplicate_foreign_and_wrong_size(self):
        entries = self.entries()
        variants = [entries[:-1], entries + [entries[1]], entries + [{"Key": "foreign", "Size": 51200}],
                    [*entries[:-1], {**entries[-1], "Size": 1}], entries[1:]]
        for variant in variants:
            with self.subTest(variant=variant), self.assertRaises(acceptance.AcceptanceError):
                self.api(variant).verify()
        with self.assertRaises(acceptance.AcceptanceError):
            self.api(entries, b"corrupt payload").verify()

    def test_inventory_timeout_is_not_success(self):
        with self.assertRaises(acceptance.AcceptanceError):
            self.api(self.entries()).verify(deadline=acceptance.time.monotonic() - 1)

    def test_manifest_and_resume_marker_fail_closed(self):
        changed = copy.deepcopy(self.manifest)
        changed["plan"]["objects"] = 8
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.validate_manifest(changed)
        for state in ({"plan_sha256": "foreign", "next_object": 0},
                      {"plan_sha256": self.manifest["plan_sha256"], "next_object": 8}):
            acceptance.atomic_json(self.root / "state.json", state)
            with self.assertRaises(acceptance.AcceptanceError):
                self.evidence.state()

    def test_runtime_requires_ownership_before_exec(self):
        runtime = acceptance.Runtime(self.evidence, container="foreign")
        raw = [{"Config": {"Labels": {acceptance.LABEL: str(uuid.uuid4())}}}]
        with patch.object(acceptance, "command", return_value=json.dumps(raw).encode()) as command:
            with self.assertRaises(acceptance.AcceptanceError):
                runtime.read_files()
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:2], ["docker", "inspect"])

    def test_owned_container_collects_real_fields_without_emitting_private_environment(self):
        runtime = acceptance.Runtime(self.evidence, container="owned")
        container = {"Id": "owned-id", "Config": {"Labels": {acceptance.LABEL: self.plan["run_id"]}},
                     "State": {"Running": True, "OOMKilled": False, "StartedAt": "2026-01-01T00:00:00Z"},
                     "HostConfig": {"Memory": self.plan["memory_bytes"]}, "RestartCount": 0,
                     "Mounts": [{"Type": "bind", "Source": self.plan["data_root"], "Destination": "/data", "RW": True}],
                     "NetworkSettings": {"Ports": {"9000/tcp": [{"HostPort": "19000", "HostIp": "127.0.0.1"}]}}}
        files = {"stat": "1 (rustfs) " + " ".join(["S"] + ["0"] * 18 + ["1234"]),
                 "status": "VmRSS: 20 kB\nRssAnon: 12 kB", "smaps_rollup": "Anonymous: 12 kB",
                 "cgroup": "0::/", "environ": "RUSTFS_SCANNER_ENABLED=false\0RUSTFS_SECRET_KEY=private-test-value\0",
                 "exe_inode": "1:5678", "memory.max": str(self.plan["memory_bytes"]), "memory.current": "40960",
                 "memory.peak": "50000", "memory.stat": "anon 12288\nfile 20000", "memory.events": "oom 0\noom_kill 0",
                 "storage": "disk 100 10 90 10% /data"}
        raw = "".join(f"\n__FILE_{name}__\n{value}\n" for name, value in files.items()).encode()
        def command(argv, **_):
            if argv[:2] == ["docker", "inspect"]:
                return json.dumps([container]).encode()
            if argv[3] == "sha256sum":
                return (self.plan["binary_sha256"] + "  /proc/1/exe\n").encode()
            return raw
        with patch.object(acceptance, "command", side_effect=command) as calls:
            sample = runtime.sample()
            runtime.sample()
        self.assertEqual(sum(call.args[0][3:4] == ["sha256sum"] for call in calls.call_args_list), 1)
        self.assertEqual(sample["storage_available_bytes"], 90 * 1024)
        self.assertEqual(sample["configuration"], {"RUSTFS_SCANNER_ENABLED": "false"})
        self.assertNotIn("private-test-value", json.dumps(sample))
        self.assertNotIn("SECRET", json.dumps(sample))
        container["NetworkSettings"]["Ports"] = {}
        with patch.object(acceptance, "command", side_effect=command), self.assertRaises(acceptance.AcceptanceError):
            runtime.sample()

    def test_memory_separates_rss_anonymous_page_cache_and_unknown_heap(self):
        runtime = acceptance.Runtime(self.evidence, pid=123)
        files = {"memory.max": str(4 * acceptance.GIB).encode(), "memory.current": b"40960", "memory.peak": b"50000",
                 "status": b"VmRSS:\t20 kB\nRssAnon:\t12 kB\n", "smaps_rollup": b"Anonymous: 12 kB\n",
                 "memory.events": b"low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
                 "memory.stat": b"anon 12288\nfile 20000\nslab 8672\n", "storage": b"disk 100 10 90 10% /data"}
        with patch.object(runtime, "read_files", return_value=({"pid": 123}, files, {"RUSTFS_SCANNER_ENABLED": "true"})):
            sample = runtime.sample()
        self.assertEqual(sample["rss_bytes"], 20480)
        self.assertEqual(sample["rss_anon_bytes"], 12288)
        self.assertEqual(sample["cgroup_stat"]["file"], 20000)
        self.assertIsNone(sample["active_heap_bytes"])
        files["memory.events"] = b"oom 1\noom_kill 1\n"
        with patch.object(runtime, "read_files", return_value=({"pid": 123}, files, {})), self.assertRaises(acceptance.AcceptanceError):
            runtime.sample()

    def test_process_stat_parentheses(self):
        raw = "123 (worker name ) tail) " + " ".join(["S"] + ["0"] * 18 + ["9876"])
        self.assertEqual(acceptance.parse_proc_stat(raw), 9876)

    def test_restart_requires_later_success_on_different_process(self):
        self.evidence.append("phase_result", result={"process_identity": "b", "complete": True, "completed_cycles": 2})
        self.evidence.append("phase_result", result={"process_identity": "a", "active_before_external_restart": True})
        self.assertFalse(acceptance.summarize(self.evidence)["restart_window_observed"])
        self.evidence.append("phase_result", result={"process_identity": "a", "complete": True, "completed_cycles": 2})
        self.assertFalse(acceptance.summarize(self.evidence)["restart_window_observed"])
        self.evidence.append("phase_result", result={"process_identity": "c", "complete": True, "completed_cycles": 2})
        self.assertTrue(acceptance.summarize(self.evidence)["restart_window_observed"])
        self.assertFalse(acceptance.summarize(self.evidence)["overall_acceptance_passed"])

    def test_probe_counts_new_success_cycles_and_requires_exact_usage(self):
        self.evidence.save({"plan_sha256": self.manifest["plan_sha256"], "next_object": 7, "load_complete": True})
        counter = iter(range(100, 1000))
        api = SimpleNamespace(owned=lambda: None,
                              status=lambda _: {"enabled": True, "metrics": {
                                  "last_cycle_end_unix_secs": next(counter), "last_cycle_result": "success"}},
                              verify=lambda **_: {"objects": 7},
                              usage=lambda _: {"matches_inventory": True, "converged": True})
        runtime = SimpleNamespace(sample=lambda: {"identity_sha256": "process-a"})
        args = SimpleNamespace(seconds=10, interval=0.01, label="cycles", stop_after_progress=None, min_completed_cycles=2)
        result = acceptance.run_probe(self.evidence, api, runtime, args)
        self.assertEqual(result["completed_cycles"], 2)
        self.assertTrue(result["complete"])
        self.assertIn("phase_id", result)

    def test_runtime_binding_checks_tcp_address_and_family(self):
        endpoint = self.plan["endpoint"]
        self.assertTrue(acceptance.docker_endpoint_matches({"9000/tcp": [{"HostPort": "19000", "HostIp": "0.0.0.0"}]}, endpoint))
        for protocol, ip in (("9000/tcp", "127.0.0.2"), ("9000/udp", "127.0.0.1"),
                             ("9000/tcp", "::"), ("9000/tcp", "")):
            with self.subTest(protocol=protocol, ip=ip):
                self.assertFalse(acceptance.docker_endpoint_matches({protocol: [{"HostPort": "19000", "HostIp": ip}]}, endpoint))
        self.assertTrue(acceptance.docker_endpoint_matches({"9000/tcp": [{"HostPort": "19000", "HostIp": "::"}]}, "http://[::1]:19000"))
        def table(ip, inode="123"):
            packed = acceptance.ipaddress.ip_address(ip).packed
            encoded = "".join(f"{int.from_bytes(packed[index:index+4], sys.byteorder):08X}" for index in range(0, len(packed), 4))
            return f"header\n0: {encoded}:4A38 00000000:0000 0A 0 0 0 0 0 {inode}\n"
        self.assertTrue(acceptance.native_endpoint_matches({"tcp": table("127.0.0.1")}, {"123"}, endpoint))
        self.assertFalse(acceptance.native_endpoint_matches({"tcp": table("127.0.0.2")}, {"123"}, endpoint))
        competing = table("127.0.0.1") + table("127.0.0.1", "456").split("\n", 1)[1]
        self.assertFalse(acceptance.native_endpoint_matches({"tcp": competing}, {"123"}, endpoint))
        self.assertTrue(acceptance.native_endpoint_matches({"tcp6": table("::1")}, {"123"}, "http://[::1]:19000"))
        self.assertFalse(acceptance.native_endpoint_matches({"tcp": table("127.0.0.1"), "tcp6": table("::", "456")}, {"123"}, endpoint))

    def test_evidence_symlink_and_hardlink_never_mutate_external_file(self):
        target = self.root / "external"
        target.write_bytes(b"unchanged external data")
        target.chmod(0o600)
        output = self.root / "observations.jsonl"
        for link in (lambda: output.symlink_to(target), lambda: os.link(target, output)):
            link()
            with self.assertRaises(acceptance.AcceptanceError):
                self.evidence.append("must-not-write")
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.summarize(self.evidence)
            self.assertEqual(target.read_bytes(), b"unchanged external data")
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            output.unlink()

    def test_evidence_fifo_rejection_is_bounded_before_type_check(self):
        fifo = self.root / "observations.jsonl"
        os.mkfifo(fifo, 0o600)
        program = """import sys
from pathlib import Path
import scanner_memory_acceptance as acceptance
try:
    with acceptance.open_owned(Path(sys.argv[1]), sys.argv[2]):
        pass
except acceptance.AcceptanceError:
    sys.exit(0)
sys.exit(1)
"""
        for mode in ("r", "a"):
            with self.subTest(mode=mode):
                result = subprocess.run([sys.executable, "-c", program, str(fifo), mode],
                                        cwd=Path(__file__).resolve().parent, capture_output=True, timeout=5, check=False)
                self.assertEqual(result.returncode, 0, "FIFO input must be rejected without waiting for another endpoint")

    def test_last_load_batch_cannot_succeed_after_deadline(self):
        clock = SimpleNamespace(now=0.0)
        last_key = acceptance.object_key(self.plan, 6)
        def put(**kwargs):
            if kwargs["Key"] == last_key:
                clock.now = 2.0
        api = SimpleNamespace(owned=lambda **_: None, status=lambda _: {"enabled": False, "metrics": {}},
                              client=SimpleNamespace(put_object=put),
                              verify=lambda **_: self.fail("expired phase must not start verification"))
        runtime = SimpleNamespace(sample=lambda: {"identity_sha256": "a", "storage_available_bytes": 1 << 50})
        args = SimpleNamespace(seconds=1, interval=300, workers=1, batch=7)
        with patch.object(acceptance.time, "monotonic", side_effect=lambda: clock.now), self.assertRaises(acceptance.AcceptanceError):
            acceptance.run_load(self.evidence, api, runtime, args)
        self.assertEqual(self.evidence.state()["next_object"], 7, "completed work remains resumable")
        self.assertNotIn("load_complete", self.evidence.state())

    def test_final_verification_cannot_switch_process_identity(self):
        identity = SimpleNamespace(value="old")
        runtime = SimpleNamespace(sample=lambda: {"identity_sha256": identity.value})
        api = SimpleNamespace(status=lambda _: {"enabled": True, "metrics": {}},
                              verify=lambda **_: setattr(identity, "value", "new"))
        args = SimpleNamespace(seconds=10, interval=300)
        with self.assertRaises(acceptance.AcceptanceError):
            with acceptance.Phase(self.evidence, runtime, api, "probe", True, args) as phase:
                phase.verify()

    def test_completed_burst_resume_reports_zero_new_requests(self):
        self.evidence.save({"plan_sha256": self.manifest["plan_sha256"], "next_object": 7, "load_complete": True,
                            "burst": {"id": str(uuid.uuid4()), "next": 7, "objects": 7}})
        runtime = SimpleNamespace(sample=lambda: {"identity_sha256": "a", "storage_available_bytes": 1 << 50})
        api = SimpleNamespace(owned=lambda: None, status=lambda _: {"enabled": False, "metrics": {}},
                              verify=lambda **_: {"objects": 7})
        args = SimpleNamespace(seconds=10, interval=300, workers=1, batch=7, burst_objects=7, idle_seconds=0.001)
        result = acceptance.run_burst(self.evidence, api, runtime, args)
        self.assertEqual(result["newly_completed_objects"], 0)
        self.assertEqual(result["new_logical_copy_head_delete_requests"], 0)
        self.assertEqual(result["cumulative_logical_copy_head_delete_requests"], 21)
        self.assertEqual(result["cursor_start"], result["cursor_end"])

    def test_summary_never_splices_retries_or_processes_into_one_memory_trend(self):
        for phase_id, process, rss in (("attempt-a", "process-a", 100), ("attempt-b", "process-b", 10)):
            self.evidence.append("sample", phase="burst", phase_id=phase_id, stage="idle",
                                 memory={"identity_sha256": process, "rss_bytes": rss, "cgroup_current_bytes": rss * 2,
                                         "cgroup_peak_bytes": rss * 3, "active_heap_bytes": None})
        groups = acceptance.summarize(self.evidence)["phases"]
        self.assertEqual(len(groups), 2)
        for group in groups.values():
            self.assertEqual(group["first_rss"], group["last_rss"])
            self.assertEqual(group["sampled_current_peak_bytes"], group["first_rss"] * 2)
            self.assertEqual(group["kernel_reported_peak_bytes"], group["first_rss"] * 3)

    def test_signed_observations_never_follow_redirects(self):
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://foreign.example")

    def test_admin_signer_includes_s3_payload_hash(self):
        from botocore.credentials import Credentials
        from types import SimpleNamespace
        api = acceptance.S3.__new__(acceptance.S3)
        api.plan = self.plan
        api.session = SimpleNamespace(get_credentials=lambda: Credentials("test-access", "test-secret"))
        observed = []
        def open_request(request, **_):
            observed.append({key.lower(): value for key, value in request.header_items()})
            return io.BytesIO(b"{}")
        opener = SimpleNamespace(open=open_request)
        with patch("urllib.request.build_opener", return_value=opener):
            self.assertEqual(api.admin("/rustfs/admin/v3/scanner/status"), {})
        self.assertEqual(observed[0]["x-amz-content-sha256"], acceptance.hashlib.sha256(b"").hexdigest())
        self.assertIn("x-amz-content-sha256", observed[0]["authorization"].lower())

    def test_capacity_guard_is_fail_closed(self):
        with patch.object(acceptance.shutil, "disk_usage", return_value=acceptance.shutil._ntuple_diskusage(10, 9, 1)):
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.free_space(self.plan, 1)

    def test_only_loopback_endpoints_without_embedded_credentials(self):
        for endpoint in ("http://localhost:9000", "http://remote.example:9000", "http://user:secret@localhost:9000", "http://localhost:9000/?token=secret"):
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance.local_url(endpoint)


if __name__ == "__main__":
    unittest.main()
