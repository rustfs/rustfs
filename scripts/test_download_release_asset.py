#!/usr/bin/env python3
"""Exercise the release download entry point without fetching external tools."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / ".github/actions/setup/download_release_asset.py"


class ReleaseAssetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.body = b"verified tool fixture"
        self.sha256 = hashlib.sha256(self.body).hexdigest()
        self.asset = {"name": "tool.zip", "id": 17, "size": len(self.body), "digest": "sha256:" + self.sha256}
        self.metadata = {"tag_name": "v1.0", "assets": [self.asset]}
        self.output = self.root / "tool.zip"
        self.output.write_bytes(b"previous file")
        self.calls = self.root / "calls.jsonl"
        gh = self.root / "gh"
        gh.write_text(f"#!{sys.executable}\n" + '''import json,os,pathlib,sys
args=sys.argv[1:]
with pathlib.Path(os.environ['CALLS']).open('a') as out:out.write(json.dumps(args)+'\\n')
if '/releases/tags/' in args[-1]:
    if os.environ.get('FAIL_METADATA'):raise SystemExit(19)
    print(os.environ['METADATA'])
else:
    if os.environ.get('FAIL_DOWNLOAD'):raise SystemExit(23)
    sys.stdout.buffer.write(bytes.fromhex(os.environ['BODY']))
''')
        gh.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ["PATH"], CALLS=str(self.calls), GH_TOKEN="synthetic-do-not-log-token")

    def invoke(self, extra=(), **env):
        environment = dict(self.env, METADATA=json.dumps(self.metadata), BODY=self.body.hex(), **env)
        return subprocess.run([sys.executable, str(SCRIPT), "--repository", "owner/repo", "--tag", "v1.0", "--asset", "tool.zip", "--output", str(self.output), *extra], env=environment, capture_output=True, text=True)

    def assert_failed_atomically(self, result):
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_bytes(), b"previous file")
        self.assertEqual(list(self.root.glob(".release-asset-*")), [])
        self.assertNotIn(self.env["GH_TOKEN"], result.stdout + result.stderr)

    def test_valid_asset_uses_api_and_installs_only_verified_bytes(self):
        result = self.invoke(("--sha256", self.sha256))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output.read_bytes(), self.body)
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertEqual(calls[0], ["api", "--hostname", "github.com", "repos/owner/repo/releases/tags/v1.0"])
        self.assertEqual(calls[1], ["api", "--hostname", "github.com", "--header", "Accept: application/octet-stream", "repos/owner/repo/releases/assets/17"])
        self.assertNotIn(self.env["GH_TOKEN"], json.dumps(calls) + result.stdout + result.stderr)

    def test_download_failure_preserves_destination(self):
        self.assert_failed_atomically(self.invoke(FAIL_DOWNLOAD="1"))

    def test_metadata_failure_preserves_destination(self):
        self.assert_failed_atomically(self.invoke(FAIL_METADATA="1"))

    def test_corrupt_download_fails_digest_validation(self):
        self.body = b"modified tool fixture"
        self.assert_failed_atomically(self.invoke())

    def test_truncated_download_fails_size_validation(self):
        self.asset["size"] += 1
        self.assert_failed_atomically(self.invoke())

    def test_pinned_digest_cannot_be_replaced_by_release_metadata(self):
        self.assert_failed_atomically(self.invoke(("--sha256", "0" * 64)))
        self.assertEqual(len(self.calls.read_text().splitlines()), 1)

    def test_ambiguous_or_missing_assets_fail_before_download(self):
        for assets in ([], [self.asset, self.asset]):
            with self.subTest(assets=len(assets)):
                self.metadata["assets"] = assets
                self.assert_failed_atomically(self.invoke())

    def test_metadata_tag_must_match_requested_version(self):
        self.metadata["tag_name"] = "v2.0"
        self.assert_failed_atomically(self.invoke())

    def test_malformed_release_inventory_fails_before_download(self):
        for metadata in ([], {"tag_name": "v1.0", "assets": None}, {"tag_name": "v1.0", "assets": [None]}):
            with self.subTest(metadata=metadata):
                self.metadata = metadata
                self.assert_failed_atomically(self.invoke())

    def test_missing_digest_or_invalid_identity_fails_before_download(self):
        for field, value in [("digest", None), ("digest", "sha256:invalid"), ("id", True), ("id", 0), ("size", 0), ("size", 513 * 1024 * 1024)]:
            with self.subTest(field=field, value=value):
                self.asset[field] = value
                self.assert_failed_atomically(self.invoke())
                self.asset = {"name": "tool.zip", "id": 17, "size": len(self.body), "digest": "sha256:" + self.sha256}
                self.metadata["assets"] = [self.asset]


if __name__ == "__main__":
    unittest.main()
