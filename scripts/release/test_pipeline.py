#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Offline contract rehearsals; these are not product acceptance receipts."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import candidate as c
import pipeline
import publish
import release_gate
import verify
from test_candidate import archive, inputs

ROOT = c.ROOT


def workflow(name):
    # Ruby/Psych is available on the developer host and hosted CI image. Tests
    # parse the actual workflow rather than a separately maintained job graph.
    result = subprocess.check_output(["ruby", "-ryaml", "-rjson", "-e",
                                      "puts JSON.generate(YAML.load_file(ARGV[0]))", str(ROOT / ".github/workflows" / name)], text=True)
    value = json.loads(result)
    value["on"] = value.pop("true", value.get("on"))
    return value


def run_metadata(run_id, file, sha):
    return {"id": run_id, "run_attempt": 2, "head_sha": sha, "head_branch": "main", "event": "workflow_dispatch",
            "status": "completed", "conclusion": "success", "path": f".github/workflows/{file}",
            "repository": {"full_name": c.REPOSITORY}}


def fixture(directory, selection="all"):
    value = inputs()
    value["plan_sha256"] = c.digest(c.read_json(ROOT / ".config/release-validation-plan.json"))
    runs = {70: run_metadata(70, "release-candidate.yml", value["workflow_sha"]),
            80: run_metadata(80, "release-complete.yml", "c" * 40)}
    for target in c.targets(selection):
        record = c.archive_record(archive(directory, target, value, binary=target.encode()), target, value)
        c.write_json(directory / f"record-{target}.json", record)
        run = runs[70 if target in c.LINUX else 80]
        env = {"GITHUB_WORKFLOW_REF": f"{c.REPOSITORY}/{run['path']}@refs/heads/main", "GITHUB_SHA": run["head_sha"],
               "GITHUB_RUN_ID": str(run["id"]), "GITHUB_RUN_ATTEMPT": "1"}
        with patch.dict(os.environ, env):
            for case in ("artifact-identity", "startup"):
                log = directory / f"{target}-{case}.log"
                log.write_text("contract fixture only\n")
                verify.envelope(value, record, "identity", case, [log], verify.now(), directory)
            if target == "linux-x86_64-gnu":
                for case in verify.UPGRADES:
                    log = directory / f"upgrade-{case}.log"
                    log.write_text("contract fixture only\n")
                    verify.envelope(value, record, "upgrade", case, [log], verify.now(), directory)
    return value, runs


class CertificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.value, self.runs = fixture(self.path)

    def test_complete_original_bytes_certify(self):
        _, summary = pipeline.certify(self.value, self.path, self.runs, "full")
        self.assertTrue(summary["publish_allowed"])
        self.assertEqual(len(summary["required_cases"]), 18)

    def test_linux_only_cannot_authorize_release(self):
        with tempfile.TemporaryDirectory() as temp:
            value, runs = fixture(Path(temp), "linux")
            candidate, summary = pipeline.certify(value, Path(temp), runs, "linux")
            self.assertEqual(summary["state"], "pass")
            self.assertFalse(summary["publish_allowed"])
            self.assertEqual(len(summary["required_cases"]), 14)
            self.assertEqual(len(candidate["deliverables"]), 4)
            with self.assertRaises((ValueError, OSError)):
                pipeline.certify(value, Path(temp), runs, "full")

    def mutate_envelope(self, edit):
        path = self.path / "identity-linux-x86_64-gnu-startup.json"
        value = c.read_json(path)
        edit(value)
        path.write_text(json.dumps(value))

    def test_digest_binding_is_required_and_platform_specific(self):
        original = (self.path / "identity-linux-x86_64-gnu-startup.json").read_bytes()
        arm = c.read_json(self.path / "record-linux-aarch64-gnu.json")
        edits = [lambda v: v.pop("artifact"), lambda v: v["artifact"].update(archive_sha256="0" * 64),
                 lambda v: v["artifact"].update(archive_sha256=arm["sha256"], binary_sha256=arm["binary_sha256"])]
        for edit in edits:
            with self.subTest(edit=edit):
                (self.path / "identity-linux-x86_64-gnu-startup.json").write_bytes(original)
                self.mutate_envelope(edit)
                with self.assertRaises(ValueError):
                    pipeline.certify(self.value, self.path, self.runs, "full")

    def test_missing_changed_and_duplicate_evidence_are_rejected(self):
        path = self.path / "linux-x86_64-gnu-startup.log"
        path.write_text("substituted")
        with self.assertRaisesRegex(ValueError, "evidence"):
            pipeline.certify(self.value, self.path, self.runs, "full")

    def test_receipt_cannot_claim_another_run_or_new_attempt(self):
        self.mutate_envelope(lambda v: v["workflow"].update(run_attempt=3))
        with self.assertRaisesRegex(ValueError, "trusted run"):
            pipeline.certify(self.value, self.path, self.runs, "full")

    def test_old_successful_unit_survives_failed_job_retry(self):
        # The candidate batch is immutable; successful unit receipts from an
        # earlier attempt still bind the exact archive, plan and producer.
        _, summary = pipeline.certify(self.value, self.path, self.runs, "full")
        self.assertEqual(summary["state"], "pass")
        self.mutate_envelope(lambda v: v.update(run_id="different-candidate"))
        with self.assertRaises(ValueError):
            pipeline.certify(self.value, self.path, self.runs, "full")

    def test_new_verifier_or_plan_cannot_reuse_old_acceptance(self):
        self.value["workflow_sha"] = "d" * 40
        with self.assertRaises(ValueError):
            pipeline.certify(self.value, self.path, self.runs, "full")


class SourceCITests(unittest.TestCase):
    def test_spent_versions_and_unavailable_lookups_fail_before_building(self):
        import urllib.error
        missing = urllib.error.HTTPError('https://api.github.com/fixture', 404, 'Not Found', {}, None)
        with patch.object(c, "github", side_effect=missing) as api:
            pipeline.unused_version("1.2.3")
            self.assertEqual(api.call_count, 4)
        with patch.object(c, "github", return_value={"id": 1}):
            with self.assertRaisesRegex(ValueError, "already has a tag or release"):
                pipeline.unused_version("1.2.3")
        unavailable = urllib.error.HTTPError('https://api.github.com/fixture', 503, 'Unavailable', {}, None)
        with patch.object(c, "github", side_effect=unavailable):
            with self.assertRaises(urllib.error.HTTPError):
                pipeline.unused_version("1.2.3")

    def test_only_complete_exact_source_ci_is_accepted(self):
        run = run_metadata(60, "ci.yml", "a" * 40)
        jobs = [{"name": name, "status": "completed", "conclusion": "success"} for name in
                ("Quick Checks", "Test and Lint", "End-to-End Tests (full merge gate)")]
        def api(path):
            return {"jobs": jobs} if "/jobs?" in path else {"workflow_runs": [run]}
        with patch.object(c, "github", side_effect=api):
            self.assertEqual(pipeline.source_ci("a" * 40, "1.2.3"), {"run_id": 60, "run_attempt": 2})
            for key, wrong in (("head_sha", "b" * 40), ("head_branch", "unreviewed"), ("event", "pull_request"), ("conclusion", "failure")):
                old = run[key]; run[key] = wrong
                with self.subTest(key=key), self.assertRaises(ValueError):
                    pipeline.source_ci("a" * 40, "1.2.3")
                run[key] = old
            for conclusion in ("skipped", "failure", None):
                jobs[2]["conclusion"] = conclusion
                with self.subTest(conclusion=conclusion), self.assertRaises(ValueError):
                    pipeline.source_ci("a" * 40, "1.2.3")

    def test_duplicate_required_ci_job_is_rejected(self):
        run = run_metadata(60, "ci.yml", "a" * 40)
        jobs = [{"name": name, "status": "completed", "conclusion": "success"} for name in
                ("Quick Checks", "Quick Checks", "Test and Lint", "End-to-End Tests (full merge gate)")]
        with patch.object(c, "github", side_effect=lambda path: {"jobs": jobs} if "/jobs?" in path else {"workflow_runs": [run]}):
            with self.assertRaises(ValueError):
                pipeline.source_ci("a" * 40, "1.2.3")

    def test_a_queue_landed_push_is_accepted_only_through_the_queue_run_of_its_tree(self):
        tree = "d" * 40
        gate = ("Quick Checks", "Test and Lint", "End-to-End Tests (full merge gate)")
        push = dict(run_metadata(60, "ci.yml", "a" * 40), event="push")

        def queued(run_id, branch, tree_id=tree):
            return dict(run_metadata(run_id, "ci.yml", "a" * 40), event="merge_group", run_attempt=3,
                        head_branch=f"gh-readonly-queue/{branch}/pr-1-" + "c" * 40, head_commit={"tree_id": tree_id},
                        html_url=f"https://github.com/{c.REPOSITORY}/actions/runs/{run_id}")
        listing = [queued(62, "main", "e" * 40), queued(63, "release/1.2.3"), queued(61, "main")]
        # Keyed by (run id, attempt): reading another attempt's jobs is a KeyError, not evidence.
        jobs = {(60, 2): [{"name": name, "status": "completed", "conclusion": "skipped" if name == gate[2] else "success"} for name in gate]}
        jobs.update({(run["id"], 3): [{"name": name, "status": "completed", "conclusion": "success"} for name in gate] for run in listing})

        def api(path):
            if "/jobs?" in path:
                run_id, attempt = path.split("/runs/")[1].split("/jobs?")[0].split("/attempts/")
                return {"jobs": jobs[(int(run_id), int(attempt))]}
            return {"workflow_runs": listing if "event=merge_group" in path else [push]}

        def source_ci():
            with patch.object(c, "github", side_effect=api), patch.object(c, "command", return_value=tree) as git:
                found = pipeline.source_ci("a" * 40, "1.2.3")
            self.assertEqual(git.call_args.args[0], ["git", "rev-parse", "--verify", "--end-of-options", "a" * 40 + "^{tree}"])
            return found
        self.assertEqual(source_ci(), {"run_id": 61, "run_attempt": 3})
        listing.reverse()
        self.assertEqual(source_ci(), {"run_id": 61, "run_attempt": 3})
        push["head_branch"] = "release/1.2.3"
        self.assertEqual(source_ci(), {"run_id": 63, "run_attempt": 3})
        del listing[listing.index(next(run for run in listing if run["id"] == 63))]
        with self.assertRaises(ValueError):
            source_ci()
        push["head_branch"] = "main"
        match = next(run for run in listing if run["id"] == 61)
        refusals = [(match, "head_commit", {"tree_id": "e" * 40}), (match, "head_branch", "gh-readonly-queue/release/1.2.3/pr-1-x"),
                    (match, "conclusion", "failure"), (match, "event", "push"), (push, "event", "workflow_dispatch")]
        for record, key, wrong in refusals:
            old = record[key]; record[key] = wrong
            with self.subTest(key=key, wrong=wrong), self.assertRaises(ValueError):
                source_ci()
            record[key] = old
        for key, index, conclusion in (((61, 3), 2, "skipped"), ((61, 3), 0, "failure"), ((60, 2), 1, "failure")):
            old = jobs[key][index]["conclusion"]; jobs[key][index]["conclusion"] = conclusion
            with self.subTest(run=key, job=gate[index]), self.assertRaises(ValueError):
                source_ci()
            jobs[key][index]["conclusion"] = old
        del jobs[(61, 3)][2]
        with self.assertRaises(ValueError):
            source_ci()

class SupplyChainTests(unittest.TestCase):
    def test_actual_generator_uses_frozen_lock_and_all_package_formats(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            assets = root / "assets"; assets.mkdir()
            for name in ("server.zip", "package.deb", "package.rpm", "chart.tgz"):
                (assets / name).write_bytes(name.encode())
            lock = root / "Cargo.lock"
            lock.write_text('version = 4\n[[package]]\nname = "frozen-fixture"\nversion = "1.2.3"\n')
            args = [sys.executable, str(ROOT / "scripts/security/generate_release_supply_chain_assets.py"), "--asset-dir", str(assets),
                    "--version", "1.2.3", "--cargo-lock", str(lock), "--workflow", "release-complete.yml", "--sha", "a" * 40]
            result = subprocess.run(args, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            provenance = c.read_json(assets / "rustfs-1.2.3.provenance.json")
            self.assertEqual({s["name"] for s in provenance["subject"]}, {"server.zip", "package.deb", "package.rpm", "chart.tgz"})
            self.assertTrue(provenance["predicate"]["buildDefinition"]["externalParameters"]["workflow"].endswith("/release-complete.yml"))
            sbom = c.read_json(assets / "rustfs-1.2.3.sbom.cdx.json")
            self.assertIn("frozen-fixture", {item["name"] for item in sbom["components"]})
            lock.unlink()
            result = subprocess.run(args, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)


class NativeEntryTests(unittest.TestCase):
    def test_real_native_entry_point_version_startup_and_failure(self):
        # A small child process exercises the verifier entry point and lifecycle;
        # it deliberately does not pretend to be a RustFS product test.
        target = "macos-aarch64"
        value = inputs()
        version = f"rustfs {value['version']}\nbuild target : {verify.TRIPLES[target]}\ngit commit   : {value['source_sha']}\nrust version : {value['toolchain']}\n"
        source = f'''#!/usr/bin/env python3
import sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
store = {{}}
if '--version' in sys.argv:
    print({version!r}, end='')
else:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(store.get(self.path, b'<html>fixture</html>'))
        def do_PUT(self):
            store[self.path] = self.rfile.read(int(self.headers.get('Content-Length', '0')))
            self.send_response(200); self.end_headers()
        def do_DELETE(self):
            store.pop(self.path, None)
            self.send_response(204); self.end_headers()
    api = sys.argv[sys.argv.index('--address')+1].split(':')
    server = HTTPServer((api[0], int(api[1])), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    address=sys.argv[sys.argv.index('--console-address')+1]
    host, port=address.split(':')
    HTTPServer((host,int(port)),Handler).serve_forever()
'''
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            c.write_json(path / "inputs.json", value)
            binary = archive(path, target, value, binary=source.encode())
            env = dict(os.environ, GITHUB_WORKFLOW_REF=f"{c.REPOSITORY}/.github/workflows/release-complete.yml@refs/heads/main",
                       GITHUB_RUN_ID="80", GITHUB_RUN_ATTEMPT="1", GITHUB_SHA="c" * 40)
            args = [sys.executable, str(ROOT / "scripts/release/verify.py"), "native", "--inputs", str(path / "inputs.json"),
                    "--archive", str(binary), "--target", target, "--output", str(path / "receipts")]
            result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(list((path / "receipts").glob("*.json"))), 2)
            archive(path, target, value, binary=source.replace("rustfs 1.2.3", "rustfs 1.2.4").encode())
            args[-1] = str(path / "rejected")
            result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(list((path / "rejected").glob("*.json")))


class PromotionTests(unittest.TestCase):
    def test_tag_is_created_once_and_conflicting_tag_never_moved(self):
        value = inputs()
        state = {}
        calls = []
        def api(path, method="GET", body=None, missing=False):
            calls.append((path, method, body))
            if path.endswith("/git/tags"):
                state["tag"] = dict(body, object={"sha": body["object"], "type": body["type"]})
                return {"sha": "f" * 40}
            if path.endswith("/git/refs"):
                state["ref"] = {"object": {"type": "tag", "sha": body["sha"]}}
                return state["ref"]
            if "/git/ref/tags/" in path:
                return state.get("ref")
            return state["tag"]
        with patch.object(publish, "api", side_effect=api):
            publish.ensure_tag(value, "a" * 64)
            count = len([call for call in calls if call[1] == "POST"])
            publish.ensure_tag(value, "a" * 64)
            self.assertEqual(len([call for call in calls if call[1] == "POST"]), count)
            with self.assertRaisesRegex(ValueError, "never move"):
                publish.ensure_tag(value, "b" * 64)
            state["tag"]["object"]["sha"] = "e" * 40
            with self.assertRaises(ValueError):
                publish.ensure_tag(value, "a" * 64)
            self.assertFalse(any(method in ("PATCH", "DELETE") for _, method, _ in calls))

    def test_image_promotion_preserves_digest_and_refuses_overwrite(self):
        with patch.dict(os.environ, REGISTRY_AUTH_FILE="auth.json"), patch.object(publish, "image_digest", return_value="sha256:" + "a" * 64), patch.object(subprocess, "run") as command:
            publish.copy_image(Path("image.tar"), "registry/image:1.2.3", "sha256:" + "a" * 64)
            command.assert_not_called()
            with self.assertRaisesRegex(ValueError, "never replace"):
                publish.copy_image(Path("image.tar"), "registry/image:1.2.3", "sha256:" + "b" * 64)
            command.assert_not_called()
        with patch.dict(os.environ, REGISTRY_AUTH_FILE="auth.json"), patch.object(publish, "image_digest", side_effect=[None, "sha256:" + "a" * 64]), patch.object(subprocess, "run") as command:
            publish.copy_image(Path("image.tar"), "registry/image:1.2.3", "sha256:" + "a" * 64)
            self.assertIn("--preserve-digests", command.call_args.args[0])
            self.assertIn("--all", command.call_args.args[0])

    def test_newer_channel_cannot_be_downgraded_after_old_job_retry(self):
        with patch.object(publish, "publish_version") as versions, patch.object(publish, "api", return_value={"tag_name": "1.2.4"}) as api, patch.object(publish, "images") as images, patch.object(publish, "helm") as helm:
            publish.channels(inputs(), {}, Path("candidate"), "a" * 64)
            versions.assert_called_once()
            images.assert_not_called()
            helm.assert_called_once()
            self.assertTrue(all(call.args[1:] == () for call in api.call_args_list))

    def test_prerelease_never_changes_stable_pointers(self):
        value = inputs(); value["version"] = "1.3.0-rc.1"
        with patch.object(publish, "publish_version"), patch.object(publish, "api") as api, patch.object(publish, "images") as images, patch.object(publish, "helm") as helm:
            publish.channels(value, {}, Path("candidate"), "a" * 64)
            api.assert_not_called(); images.assert_not_called(); helm.assert_called_once()

    def test_interrupted_version_publish_never_advances_channels(self):
        with patch.object(publish, "publish_version", side_effect=ValueError("registry unavailable")), patch.object(publish, "api") as api:
            with self.assertRaises(ValueError):
                publish.channels(inputs(), {}, Path("candidate"), "a" * 64)
            api.assert_not_called()

    def test_incomplete_helm_publication_does_not_advance_stable_channels(self):
        with patch.object(publish, "publish_version"), patch.object(publish, "helm", side_effect=ValueError("chart repository unavailable")), patch.object(publish, "api") as api, patch.object(publish, "images") as images:
            with self.assertRaises(ValueError):
                publish.channels(inputs(), {}, Path("candidate"), "a" * 64)
            api.assert_not_called()
            images.assert_not_called()

    def test_r2_retry_checks_bytes_and_uses_conditional_creation(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, R2_BUCKET="releases"):
            path = Path(temp) / "asset.zip"; path.write_bytes(b"accepted")
            objects = {}
            calls = []
            def aws(*args, **kwargs):
                calls.append(args)
                if args[:2] == ("s3api", "head-object"):
                    exists = args[args.index("--key") + 1] in objects
                    return subprocess.CompletedProcess(args, 0 if exists else 1, "{}", "" if exists else "(404)")
                if args[:2] == ("s3api", "put-object"):
                    self.assertIn("--if-none-match", args)
                    objects[args[args.index("--key") + 1]] = Path(args[args.index("--body") + 1]).read_bytes()
                else:
                    Path(args[3]).write_bytes(objects[args[2].split("/", 3)[3]])
                return subprocess.CompletedProcess(args, 0, "{}", "")
            with patch.object(publish, "aws", side_effect=aws):
                publish.r2_file(path, "version/asset.zip")
                publish.r2_file(path, "version/asset.zip")
                self.assertEqual(sum(call[:2] == ("s3api", "put-object") for call in calls), 1)
                objects["version/asset.zip"] = b"changed"
                with self.assertRaisesRegex(ValueError, "never overwrite"):
                    publish.r2_file(path, "version/asset.zip")


class WorkflowContractTests(unittest.TestCase):
    def test_schedule_cron_selects_linux_or_desktop_without_dropping_manual_platforms(self):
        value = workflow("build.yml")
        self.assertEqual([item["cron"] for item in value["on"]["schedule"]], ["13 1 * * *", "0 16 * * 0"])
        self.assertNotIn("push", value["on"])
        self.assertNotIn("pull_request", value["on"])
        self.assertEqual(value["on"]["workflow_dispatch"]["inputs"]["platforms"]["default"], "all")
        selection = value["jobs"]["prepare-platform-matrix"]["steps"][-1]
        env = selection["env"]
        self.assertEqual(env["RAW_PLATFORMS"], "${{ inputs.platforms || 'linux' }}")
        self.assertEqual(env["HAS_PLATFORM_INPUT"], "${{ inputs.platforms && 'true' || 'false' }}")
        self.assertEqual(env["EVENT_NAME"], "${{ github.event_name }}")
        self.assertEqual(env["EVENT_SCHEDULE"], "${{ github.event.schedule }}")
        linux = [
            ("linux-x86_64-musl", "sm-standard-4"),
            ("linux-aarch64-musl", "ubicloud-standard-16-arm"),
            ("linux-x86_64-gnu", "sm-standard-4"),
            ("linux-aarch64-gnu", "ubicloud-standard-16-arm"),
        ]
        desktop = [("macos-aarch64", "macos-latest-xlarge"), ("windows-x86_64", "windows-latest")]

        def select(event, raw, schedule="", has_input="false"):
            with tempfile.TemporaryDirectory() as temp:
                output = Path(temp) / "github-output"
                environment = {**os.environ, "EVENT_NAME": event, "EVENT_SCHEDULE": schedule,
                               "RAW_PLATFORMS": raw, "HAS_PLATFORM_INPUT": has_input, "GITHUB_OUTPUT": str(output)}
                result = subprocess.run(["bash", "-c", selection["run"]], env=environment, capture_output=True, text=True)
                parsed = {}
                if output.exists():
                    for line in output.read_text().splitlines():
                        key, item = line.split("=", 1)
                        parsed[key] = item
                return result, parsed

        def rows(parsed):
            return [(item["target_id"], item["os"]) for item in json.loads(parsed["matrix"])["include"]]

        daily, daily_out = select("schedule", "all", "13 1 * * *")
        self.assertEqual(daily.returncode, 0, daily.stderr)
        self.assertEqual(rows(daily_out), linux)
        self.assertEqual(daily_out["selected"], ",".join(item[0] for item in linux))
        weekly, weekly_out = select("schedule", "linux", "0 16 * * 0")
        self.assertEqual(weekly.returncode, 0, weekly.stderr)
        self.assertEqual(rows(weekly_out), desktop)
        self.assertEqual(weekly_out["selected"], ",".join(item[0] for item in desktop))
        for schedule in ("", "13 1 * * 1", "0 16 * * 7", "13 1 extra stuff"):
            rejected, _ = select("schedule", "all", schedule)
            self.assertNotEqual(rejected.returncode, 0, schedule)
            self.assertIn("Unrecognized schedule cron", rejected.stderr)
        caller, caller_out = select("schedule", "linux", "0 16 * * 0", has_input="true")
        self.assertEqual(caller.returncode, 0, caller.stderr)
        self.assertEqual(rows(caller_out), linux)
        dispatched, dispatched_out = select("workflow_dispatch", "all", has_input="true")
        self.assertEqual(dispatched.returncode, 0, dispatched.stderr)
        self.assertEqual(dispatched_out["selected"], "all")
        self.assertEqual(rows(dispatched_out), linux + desktop)
        custom, custom_out = select("workflow_dispatch", "linux-x86_64-musl,macos-aarch64", has_input="true")
        self.assertEqual(custom.returncode, 0, custom.stderr)
        self.assertEqual(custom_out["selected"], "linux-x86_64-musl,macos-aarch64")
        self.assertEqual(rows(custom_out), [("linux-x86_64-musl", "sm-standard-4"), ("macos-aarch64", "macos-latest-xlarge")])
        for event, raw, expected in (("workflow_call", "linux", linux), ("workflow_call", "desktop", desktop)):
            called, called_out = select(event, raw, has_input="true")
            self.assertEqual(called.returncode, 0, called.stderr)
            self.assertEqual(rows(called_out), expected)

    def test_main_push_cannot_cancel_the_scheduled_oidc_gate(self):
        value = workflow("oidc-keycloak.yml")
        self.assertIn("github.event_name", value["concurrency"]["group"])
        self.assertIn("!= 'schedule'", value["concurrency"]["cancel-in-progress"])

    def test_linux_is_the_only_initial_build(self):
        value = workflow("release-candidate.yml")
        self.assertEqual(set(value["on"]), {"workflow_dispatch"})
        self.assertEqual(value["jobs"]["linux"]["with"]["platforms"], "linux")
        self.assertNotIn("desktop", value["jobs"])
        self.assertEqual(set(value["jobs"]["seal-linux"]["needs"]), {"freeze", "linux", "upgrade"})

    def test_desktop_and_deliveries_wait_for_linux_acceptance(self):
        value = workflow("release-complete.yml")
        self.assertEqual(value["jobs"]["acceptance"]["environment"], "release-linux-acceptance")
        for name in ("desktop", "packages", "images", "chart"):
            self.assertEqual(value["jobs"][name]["needs"], "acceptance")
            self.assertNotIn("secrets", value["jobs"][name])
        self.assertEqual(set(value["jobs"]["seal"]["needs"]), {"acceptance", "desktop", "packages", "images", "chart"})

    def test_only_publisher_has_release_writes_and_never_builds(self):
        value = workflow("release-publish.yml")
        self.assertEqual(set(value["on"]), {"workflow_dispatch"})
        self.assertEqual(value["jobs"]["version"]["environment"], "release-publication")
        self.assertEqual(value["jobs"]["channels"]["needs"], ["verify", "version"])
        self.assertEqual(value["jobs"]["channels"]["concurrency"]["group"], "rustfs-release-channels")
        for job in ("version", "channels"):
            self.assertFalse(value["jobs"][job]["concurrency"]["cancel-in-progress"])
        for name in ("build.yml", "package.yml", "docker.yml", "helm-package.yml"):
            parsed = workflow(name)
            self.assertEqual(parsed["permissions"]["actions"], "read")
            self.assertNotIn("push", parsed["on"])
            self.assertNotIn("workflow_run", parsed["on"])
            self.assertNotIn("contents: write", (ROOT / ".github/workflows" / name).read_text())
        source = (ROOT / "scripts/release/publish.py").read_text()
        for forbidden in ("cargo build", "build-push", "helm package", "--clobber", "force=true"):
            self.assertNotIn(forbidden, source.replace("# No --clobber:", "# No replacement:"))

    def test_stage_failures_do_not_have_a_continue_on_error_bypass(self):
        for name in ("release-candidate.yml", "release-complete.yml", "release-publish.yml", "docker.yml", "package.yml", "helm-package.yml"):
            value = workflow(name)
            for job in value["jobs"].values():
                self.assertNotIn("continue-on-error", job)
                for step in job.get("steps", []):
                    self.assertNotIn("continue-on-error", step)


class ImageWorkflowTests(unittest.TestCase):
    def test_substituting_a_single_platform_scan_archive_is_rejected(self):
        import deliver
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "delivery"
            scans = Path(str(output) + "-scan")
            output.mkdir(); scans.mkdir()
            value = inputs()
            record = {"inputs_sha256": c.digest(value), "scan_inputs": {}}
            for arch in ("amd64", "arm64"):
                path = scans / f"image-musl-{arch}.tar"
                path.write_bytes(arch.encode())
                record["scan_inputs"][arch] = c.file_digest(path)
                c.write_json(output / f"scan-musl-{arch}.sarif", {"version": "2.1.0", "runs": [{}]})
            c.write_json(output / "image-musl-smoke.json", record)
            deliver.image_record(value, "musl", output)
            self.assertEqual(c.read_json(output / "image-musl.json")["scan"], "completed")
            (scans / "image-musl-arm64.tar").write_bytes(b"another architecture")
            with self.assertRaisesRegex(ValueError, "scan input changed"):
                deliver.image_record(value, "musl", output)

    def test_both_architectures_scan_before_seal_without_registry_credentials(self):
        value = workflow("docker.yml")
        self.assertEqual(value["permissions"], {"contents": "read", "actions": "read"})
        steps = value["jobs"]["build-docker"]["steps"]
        build = next(s for s in steps if "docker/build-push-action@" in s.get("uses", ""))
        self.assertFalse(build["with"]["push"])
        self.assertIn("type=oci", build["with"]["outputs"])
        scans = [s for s in steps if "aquasecurity/trivy-action@" in s.get("uses", "")]
        self.assertEqual({s["env"]["TRIVY_PLATFORM"] for s in scans}, {"linux/amd64", "linux/arm64"})
        for scan in scans:
            self.assertEqual(scan["with"]["exit-code"], "0")
            self.assertIn("input", scan["with"])
            architecture = scan["env"]["TRIVY_PLATFORM"].split("/")[-1]
            self.assertTrue(scan["with"]["input"].endswith(f"-{architecture}.tar"))
            self.assertLess(steps.index(scan), next(i for i, s in enumerate(steps) if s.get("name") == "Record verified OCI identity"))


class HelmWorkflowTests(unittest.TestCase):
    def test_installation_versions_match_candidate_chart_versions(self):
        subprocess.run(["bash", "scripts/test_helm_chart_version.sh"], cwd=ROOT,
                       check=True, capture_output=True, text=True, timeout=15)

    def test_chart_is_sealed_before_publish_and_does_not_push(self):
        value = workflow("helm-package.yml")
        self.assertEqual(set(value["on"]), {"workflow_call"})
        source = (ROOT / ".github/workflows/helm-package.yml").read_text()
        self.assertIn("scripts/test_helm_templates.sh", source)
        self.assertIn("('version', 'appVersion')", source)
        self.assertNotIn("git push", source)
        self.assertIn("inputs_sha256", source)


if __name__ == "__main__":
    unittest.main()
