#!/usr/bin/env python3

import importlib.util
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_internode_msgpack_peer_canary.py")
SPEC = importlib.util.spec_from_file_location("msgpack_peer_canary", SCRIPT)
assert SPEC and SPEC.loader
canary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(canary)


def prometheus(rows):
    return {"status": "success", "data": {"resultType": "vector", "result": rows}}


def row(timestamp, value, **labels):
    return {"metric": labels, "value": [timestamp, str(value)]}


class MsgpackPeerCanaryTests(unittest.TestCase):
    def test_rpc_error_query_scopes_application_response_exclusion_to_read_version(self):
        cluster_id = "msgpack-" + "c" * 32
        query = canary.expected_queries(30, cluster_id)["rpc_errors"]
        self.assertIn(
            f'rustfs_cluster_id="{cluster_id}",backend="grpc",operation!="grpc_read_version"',
            query,
        )
        self.assertIn("internode_operation_classified_errors_total", query)
        self.assertIn(
            f'rustfs_cluster_id="{cluster_id}",backend="grpc",operation="grpc_read_version",'
            'classification!="remote_response"',
            query,
        )
        self.assertNotIn("http_status_other", query)

    def fixture(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        phases = []
        images = [
            {
                "Id": "sha256:" + char * 64,
                "Config": {"Labels": {"vcs-ref": char * 40}},
                "RevisionEvidence": "config-labels",
            }
            for char in ("a", "b")
        ]
        (root / "images.json").write_text(json.dumps(images))
        layouts = [
            ("mixed-old-peer", 100, 110, 3, 1, [row(110, 4, decision="msgpack_only", reason="peer_capable"),
                                                   row(110, 2, decision="dual_write", reason="peer_unknown")]),
            ("all-new", 120, 130, 4, 0, [row(130, 8, decision="msgpack_only", reason="peer_capable")]),
            ("rollback-old-peer", 140, 150, 3, 1, [row(150, 5, decision="msgpack_only", reason="peer_capable"),
                                                    row(150, 3, decision="dual_write", reason="peer_revoked")]),
        ]
        for name, start, end, new_nodes, old_nodes, decisions in layouts:
            nodes = {
                node: ("b" * 40 if node in canary.OLD_NODES[name] else "a" * 40)
                for node in ("node1", "node2", "node3", "node4")
            }
            running = {node: {"image_id": "sha256:" + revision[0] * 64, "revision": revision} for node, revision in nodes.items()}
            (root / name).mkdir(parents=True, exist_ok=True)
            (root / name / "nodes.json").write_text(json.dumps(running))
            artifacts = {}
            for kind, document in [
                ("decisions", prometheus(decisions)),
                ("fallback", prometheus([row(end, 0)])),
                ("decode_errors", prometheus([row(end, 0)])),
                ("rpc_errors", prometheus([row(end, 0)])),
            ]:
                relative = f"{name}/{kind}.json"
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(document), encoding="utf-8")
                artifacts[kind] = relative
            workload_relative = f"{name}/workload.json"
            summary_relative = f"{name}/workload-bench/summary.csv"
            summary_path = root / summary_relative
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_bytes = b"size,status,requests_per_sec,error_count\n4KiB,ok,10 req/s,0\n"
            summary_path.write_bytes(summary_bytes)
            readback_digest = "c" * 64 if name == "rollback-old-peer" else None
            (root / workload_relative).write_text(
                json.dumps({
                    "status": "success",
                    "requests": 100,
                    "rollback_readback": name == "rollback-old-peer",
                    "summary": summary_relative,
                    "summary_sha256": hashlib.sha256(summary_bytes).hexdigest(),
                    "readback_source_sha256": readback_digest,
                    "readback_download_sha256": readback_digest,
                }),
                encoding="utf-8",
            )
            phases.append({
                "name": name,
                "start_unix": start,
                "end_unix": end,
                "topology": {"new_nodes": new_nodes, "old_nodes": old_nodes, "nodes": nodes},
                "environment": {
                    "new_nodes": {"msgpack_only": True, "peer_capability": True, "fleet_confirmed": False},
                    "old_nodes": {"msgpack_only": False, "peer_capability": False, "fleet_confirmed": False},
                },
                "queries": canary.expected_queries(end - start, "msgpack-" + "c" * 32),
                "images": f"{name}/nodes.json",
                "workload": workload_relative,
                "artifacts": artifacts,
            })
        manifest = {
            "schema": 1,
            "scope": "internode-msgpack-peer-canary",
            "source_revision": "a" * 40,
            "baseline_revision": "b" * 40,
            "stable_connection_affinity": True,
            "cluster_id": "msgpack-" + "c" * 32,
            "images": "images.json",
            "max_metric_lag_secs": 30,
            "phases": phases,
        }
        return temp, root, manifest

    def test_valid_four_node_matrix_passes_with_receipts(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        result = canary.check(manifest, root)
        self.assertTrue(result["observed_pass"])
        self.assertEqual(result["violations"], [])
        self.assertEqual(result["cluster_id"], manifest["cluster_id"])
        self.assertEqual(
            result["image_revision_evidence"],
            {"candidate": "config-labels", "baseline": "config-labels"},
        )
        self.assertEqual(len(result["artifacts"]), 22)
        self.assertTrue(all(len(item["sha256"]) == 64 for item in result["artifacts"]))

    def test_nonzero_decode_error_is_valid_evidence_but_fails_gate(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        phase = manifest["phases"][1]
        path = root / phase["artifacts"]["decode_errors"]
        path.write_text(json.dumps(prometheus([row(phase["end_unix"], 1)])), encoding="utf-8")
        result = canary.check(manifest, root)
        self.assertFalse(result["observed_pass"])
        self.assertIn("all-new: nonzero decode_errors (1.0)", result["violations"])

    def test_old_peer_phase_requires_a_compatibility_decision(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        phase = manifest["phases"][0]
        path = root / phase["artifacts"]["decisions"]
        path.write_text(
            json.dumps(prometheus([row(phase["end_unix"], 4, decision="msgpack_only", reason="peer_capable")])),
            encoding="utf-8",
        )
        result = canary.check(manifest, root)
        self.assertFalse(result["observed_pass"])
        self.assertIn("mixed-old-peer: no old/unknown peer dual-write decision", result["violations"])

    def test_manual_fleet_and_not_requested_series_fail_peer_gate_canary(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        phase = manifest["phases"][1]
        path = root / phase["artifacts"]["decisions"]
        path.write_text(
            json.dumps(prometheus([
                row(phase["end_unix"], 4, decision="msgpack_only", reason="peer_capable"),
                row(phase["end_unix"], 1, decision="msgpack_only", reason="manual_fleet"),
                row(phase["end_unix"], 1, decision="dual_write", reason="not_requested"),
            ])),
            encoding="utf-8",
        )
        result = canary.check(manifest, root)
        self.assertFalse(result["observed_pass"])
        self.assertIn("all-new: manual fleet path was unexpectedly active", result["violations"])
        self.assertIn("all-new: peer gate was not active on an eligible new-node request", result["violations"])

    def test_invalid_affinity_path_and_revision_are_rejected(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        manifest["schema"] = True
        with self.assertRaisesRegex(canary.EvidenceError, "schema"):
            canary.check(manifest, root)
        manifest["schema"] = 1
        manifest["stable_connection_affinity"] = False
        with self.assertRaisesRegex(canary.EvidenceError, "affinity"):
            canary.check(manifest, root)
        manifest["stable_connection_affinity"] = True
        manifest["baseline_revision"] = manifest["source_revision"]
        with self.assertRaisesRegex(canary.EvidenceError, "must differ"):
            canary.check(manifest, root)
        manifest["baseline_revision"] = "b" * 40
        manifest["phases"][0]["topology"]["nodes"]["node4"] = "a" * 40
        with self.assertRaisesRegex(canary.EvidenceError, "node revisions"):
            canary.check(manifest, root)
        manifest["phases"][0]["topology"]["nodes"]["node4"] = "b" * 40
        manifest["phases"][0]["queries"]["decisions"] = "sum(unrelated_metric)"
        with self.assertRaisesRegex(canary.EvidenceError, "PromQL"):
            canary.check(manifest, root)
        manifest["phases"][0]["queries"] = canary.expected_queries(10, "msgpack-" + "c" * 32)
        manifest["phases"][0]["artifacts"]["decisions"] = "../escape.json"
        with self.assertRaisesRegex(canary.EvidenceError, "escapes"):
            canary.check(manifest, root)

    def test_duplicate_series_and_unbounded_labels_are_rejected(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        phase = manifest["phases"][2]
        path = root / phase["artifacts"]["decisions"]
        duplicate = row(phase["end_unix"], 1, decision="msgpack_only", reason="peer_capable")
        path.write_text(json.dumps(prometheus([duplicate, duplicate])), encoding="utf-8")
        with self.assertRaisesRegex(canary.EvidenceError, "duplicate decision series"):
            canary.check(manifest, root)
        path.write_text(
            json.dumps(prometheus([row(phase["end_unix"], 1, decision="msgpack_only", reason="peer-address")])),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(canary.EvidenceError, "unbounded label"):
            canary.check(manifest, root)

    def test_empty_error_vector_cannot_masquerade_as_zero(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        phase = manifest["phases"][1]
        path = root / phase["artifacts"]["rpc_errors"]
        path.write_text(json.dumps(prometheus([])), encoding="utf-8")
        with self.assertRaisesRegex(canary.EvidenceError, "exactly one row"):
            canary.check(manifest, root)

    def test_workload_summary_and_rollback_hashes_are_bound(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        mixed_workload_path = root / manifest["phases"][0]["workload"]
        mixed_workload = json.loads(mixed_workload_path.read_text(encoding="utf-8"))
        (root / mixed_workload["summary"]).write_text("changed\n", encoding="utf-8")
        with self.assertRaisesRegex(canary.EvidenceError, "summary hash mismatch"):
            canary.check(manifest, root)

        temp.cleanup()
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        rollback_path = root / manifest["phases"][2]["workload"]
        rollback = json.loads(rollback_path.read_text(encoding="utf-8"))
        rollback["readback_download_sha256"] = "d" * 64
        rollback_path.write_text(json.dumps(rollback), encoding="utf-8")
        with self.assertRaisesRegex(canary.EvidenceError, "readback digest mismatch"):
            canary.check(manifest, root)

    def test_foreign_queries_and_unverified_images_are_rejected(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        manifest["phases"][0]["queries"] = canary.expected_queries(10, "msgpack-" + "d" * 32)
        with self.assertRaisesRegex(canary.EvidenceError, "PromQL"):
            canary.check(manifest, root)
        manifest["phases"][0]["queries"] = canary.expected_queries(10, manifest["cluster_id"])
        images = json.loads((root / "images.json").read_text())
        images[1]["Id"] = images[0]["Id"]
        (root / "images.json").write_text(json.dumps(images))
        with self.assertRaisesRegex(canary.EvidenceError, "same image"):
            canary.check(manifest, root)
        images[1]["Id"] = "sha256:" + "b" * 64
        images[1]["Config"]["Labels"]["vcs-ref"] = "c" * 40
        (root / "images.json").write_text(json.dumps(images))
        with self.assertRaisesRegex(canary.EvidenceError, "revision mismatch"):
            canary.check(manifest, root)
        images[1]["Config"]["Labels"]["vcs-ref"] = "b" * 40
        images[1]["RevisionEvidence"] = "unverified"
        (root / "images.json").write_text(json.dumps(images))
        with self.assertRaisesRegex(canary.EvidenceError, "evidence source"):
            canary.check(manifest, root)

    def test_cli_exit_codes_distinguish_pass_gate_failure_and_invalid_input(self):
        temp, root, manifest = self.fixture()
        self.addCleanup(temp.cleanup)
        manifest_path = root / "manifest.json"
        out = root / "verification.json"

        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(manifest_path), "--out", str(out)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        verified = json.loads(out.read_text(encoding="utf-8"))
        self.assertTrue(verified["observed_pass"])
        self.assertEqual(len(verified["manifest_sha256"]), 64)

        raw_artifact = root / manifest["phases"][0]["workload"]
        overwrite = subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(manifest_path), "--out", str(raw_artifact)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(overwrite.returncode, 2)
        self.assertIn("overwrite raw canary evidence", overwrite.stderr)

        rollback_workload = root / manifest["phases"][2]["workload"]
        rollback_payload = json.loads(rollback_workload.read_text(encoding="utf-8"))
        rollback_payload["rollback_readback"] = False
        rollback_workload.write_text(json.dumps(rollback_payload), encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        failed = subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(manifest_path), "--out", str(out)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(failed.returncode, 1, failed.stderr)
        self.assertFalse(json.loads(out.read_text(encoding="utf-8"))["observed_pass"])

        manifest["scope"] = "command-transcript"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        invalid = subprocess.run(
            [sys.executable, str(SCRIPT), "--manifest", str(manifest_path), "--out", str(out)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("unexpected evidence scope", invalid.stderr)


if __name__ == "__main__":
    unittest.main()
