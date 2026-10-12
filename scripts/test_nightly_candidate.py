#!/usr/bin/env python3
"""Exercise the nightly publication step without AWS, network or package builds."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from check_test_wiring import yaml_block
from resolve_functional_candidate import validate_manifest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/nightly-gnu.yml"


class NightlyCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.package = self.root / "rustfs-nightly-2026-09-06.deb"
        self.package.write_bytes(b"built package bytes\x00\xff")
        for command in (["git", "init", "-q"], ["git", "add", self.package.name],
                        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"]):
            subprocess.run(command, cwd=self.root, check=True, capture_output=True)
        self.sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        self.digest = hashlib.sha256(self.package.read_bytes()).hexdigest()
        self.output = self.root / "github-output"
        self.store = self.root / "store"
        self.shims = self.root / "fake-tools.sh"
        self.shims.write_text(r'''aws() {
  printf '%s\n' "$*" >> "$FAKE_AWS_LOG"
  if [[ "$1" == --version ]]; then printf 'aws-cli/1.44.79 fixture\n'; return; fi
  if [[ "$*" == *--generate-cli-skeleton* ]]; then
    if [[ "$FAKE_MODE" == broken-install || "$FAKE_MODE" =~ ^(old-cli|bootstrap-failure|install-failure)$ && ! -e "$FAKE_INSTALLED" ]]; then
      printf '{}\n'
    else
      printf '{"IfNoneMatch":""}\n'
    fi
    return
  fi
  if [[ "$1 $2" == 's3api put-object' ]]; then
    [[ "$FAKE_MODE" != upload-failure ]] || return 42
    shift 2
    local key="" body="" condition=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --key) key="$2";;
        --body) body="$2";;
        --if-none-match) condition="$2";;
      esac
      shift 2
    done
    [[ -z "$condition" || "$condition" == '*' ]] || return 43
    if [[ "$condition" == '*' && -e "$FAKE_STORE/$key" ]]; then return 44; fi
    mkdir -p "$(dirname "$FAKE_STORE/$key")"
    cp "$body" "$FAKE_STORE/$key"
  elif [[ "$1 $2" == 's3 cp' ]]; then
    [[ "$FAKE_MODE" != alias-failure ]] || return 45
    local destination="${4#s3://test-bucket/}"
    [[ "$destination" != */ ]] || destination+="$(basename "$3")"
    mkdir -p "$(dirname "$FAKE_STORE/$destination")"
    cp "$3" "$FAKE_STORE/$destination"
  else
    return 46
  fi
}
curl() {
  local url="${!#}"
  printf '%s\n' "$url" >> "$FAKE_CURL_LOG"
  [[ "$url" == https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/* ]] || return 22
  [[ "$FAKE_MODE" != missing-public-url ]] || return 22
  if [[ "$FAKE_MODE" == wrong-public-bytes ]]; then printf 'different package'; return; fi
  cat "$FAKE_STORE/${url#https://dl.rustfs.com/}" || return 22
  [[ "$FAKE_MODE" != incomplete-download ]] || return 47
}
sudo() {
  [[ "$*" == 'apt-get update' || "$*" == 'apt-get install -y -qq python3-venv' ]] || return 49
  [[ "$FAKE_MODE" != bootstrap-failure ]] || return 48
}
python3() {
  [[ "$1 $2" == '-m venv' ]] || return 50
  mkdir -p "$3/bin"
  cat > "$3/bin/python" <<'SH'
#!/usr/bin/env bash
[[ "$*" == '-m pip install --disable-pip-version-check awscli==1.44.79' ]] || exit 51
[[ "$FAKE_MODE" != install-failure ]] || exit 52
: > "$FAKE_INSTALLED"
SH
  printf '#!/usr/bin/env bash\naws "$@"\n' > "$3/bin/aws"
  chmod +x "$3/bin/python" "$3/bin/aws"
}
''')
        self.env = dict(os.environ, BASH_ENV=str(self.shims), DEB_FILE=self.package.name,
                        R2_ACCESS_KEY_ID="fake-access", R2_SECRET_ACCESS_KEY="fake-secret", R2_ENDPOINT="https://r2.example.invalid", R2_BUCKET="test-bucket",
                        RUNNER_TEMP=str(self.root), GITHUB_SHA=self.sha, NIGHTLY_BUILD_REF="main", GITHUB_RUN_ID="12345", GITHUB_RUN_ATTEMPT="1", GITHUB_OUTPUT=str(self.output),
                        FAKE_STORE=str(self.store), FAKE_AWS_LOG=str(self.root / "aws.log"), FAKE_CURL_LOG=str(self.root / "curl.log"), FAKE_INSTALLED=str(self.root / "installed"), FAKE_MODE="success")
        source = WORKFLOW.read_text()
        job = yaml_block(source.splitlines(), "build", 2)
        starts = [i for i, line in enumerate(job) if line.startswith("      - name: ")]
        self.steps = {
            job[start].split(": ", 1)[1]: job[start:end]
            for start, end in zip(starts, starts[1:] + [len(job)])
        }
        self.publish = self.steps["Upload DEB to Cloudflare R2"]
        start = self.publish.index("        run: |") + 1
        self.shell = "\n".join(line[10:] for line in self.publish[start:] if not line.strip() or line.startswith("          "))

    def run_publish(self, **overrides):
        self.output.unlink(missing_ok=True)
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.shell],
                              cwd=self.root, env=dict(self.env, **overrides), capture_output=True, text=True)

    def manifest(self):
        output = self.output.read_text().strip()
        self.assertTrue(output.startswith("candidate_file="), output)
        return json.loads(Path(output.split("=", 1)[1]).read_text())

    def test_success_binds_actual_package_checkout_and_attempt(self):
        result = self.run_publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = self.manifest()
        self.assertEqual(manifest, {"schema": 2, "workflow_sha": self.sha, "source_ref": "main", "source_sha": self.sha, "build_run_id": 12345, "build_run_attempt": 1,
                                   "package_sha256": self.digest, "package_url": f"https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/12345/1/{self.digest}/rustfs.deb"})
        for path in (f"runs/12345/1/{self.digest}/rustfs.deb", self.package.name, "rustfs-nightly-latest.deb"):
            self.assertEqual((self.store / "artifacts/rustfs/packages/nightly" / path).read_bytes(), self.package.read_bytes())
        self.assertEqual((self.root / "curl.log").read_text().strip(), manifest["package_url"])

    def test_missing_credentials_remain_artifact_only(self):
        for key in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET"):
            with self.subTest(missing=key):
                result = self.run_publish(**{key: ""})
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(self.output.exists())
                self.assertFalse((self.root / "aws.log").exists())
                self.assertEqual(list(self.root.glob("nightly-candidate-*.json")), [])

    def test_publication_failures_never_emit_a_candidate(self):
        for index, mode in enumerate(("upload-failure", "missing-public-url", "wrong-public-bytes", "incomplete-download", "alias-failure")):
            with self.subTest(mode=mode):
                result = self.run_publish(FAKE_MODE=mode, GITHUB_RUN_ID=str(20000 + index))
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertFalse(self.output.exists())
                self.assertEqual(list(self.root.glob("nightly-candidate-*.json")), [])

    def test_old_cli_is_upgraded_in_an_isolated_temporary_environment(self):
        result = self.run_publish(FAKE_MODE="old-cli")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "installed").exists())
        self.assertEqual(self.manifest()["package_sha256"], self.digest)
        self.assertEqual(list(self.root.glob("nightly-awscli.*")), [])

    def test_failed_cli_bootstrap_cannot_publish(self):
        for mode in ("bootstrap-failure", "install-failure", "broken-install"):
            with self.subTest(mode=mode):
                (self.root / "installed").unlink(missing_ok=True)
                result = self.run_publish(FAKE_MODE=mode)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.output.exists())
                self.assertFalse(self.store.exists())
                self.assertEqual(list(self.root.glob("nightly-awscli.*")), [])

    def test_manifest_advertises_checked_out_head_even_when_github_sha_differs(self):
        # With a ref override (NIGHTLY_BRANCH variable / dispatch `branch`
        # input) the checked-out HEAD intentionally differs from GITHUB_SHA;
        # the candidate manifest must record the tree that was built.
        result = self.run_publish(GITHUB_SHA="f" * 40, NIGHTLY_BUILD_REF="release")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.manifest()["source_sha"], self.sha)
        self.assertEqual(self.manifest()["workflow_sha"], "f" * 40)
        self.assertEqual(self.manifest()["source_ref"], "release")
        validate_manifest(self.manifest(), {"id": 12345, "run_attempt": 1, "head_sha": "f" * 40})

    def test_every_lane_uses_the_same_resolved_source(self):
        lines = WORKFLOW.read_text().splitlines()
        resolver = "\n".join(yaml_block(lines, "resolve-source", 2))
        self.assertIn("ref: ${{ env.NIGHTLY_BUILD_REF }}", resolver)
        self.assertIn('git rev-parse HEAD', resolver)
        for lane in ("build", "kms-vault-lane", "kms-vault-ha-failover"):
            with self.subTest(lane=lane):
                job = "\n".join(yaml_block(lines, lane, 2))
                self.assertIn("needs: resolve-source", job)
                self.assertIn("ref: ${{ needs.resolve-source.outputs.source_sha }}", job)
                self.assertNotIn("ref: ${{ env.NIGHTLY_BUILD_REF }}", job)

    def test_same_date_builds_and_reruns_keep_distinct_candidates(self):
        urls = []
        for run, attempt in (("12345", "1"), ("54321", "1"), ("12345", "2")):
            result = self.run_publish(GITHUB_RUN_ID=run, GITHUB_RUN_ATTEMPT=attempt)
            self.assertEqual(result.returncode, 0, result.stderr)
            urls.append(self.manifest()["package_url"])
        self.assertEqual(len(set(urls)), 3)
        self.assertEqual(len(list(self.root.glob("nightly-candidate-*.json"))), 3)

    def test_duplicate_key_is_not_overwritten_or_recertified(self):
        result = self.run_publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        key = self.manifest()["package_url"].removeprefix("https://dl.rustfs.com/")
        stored = self.store / key
        stored.write_bytes(b"preexisting conflicting object")
        (self.root / "nightly-candidate-12345-1.json").unlink()
        result = self.run_publish()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(stored.read_bytes(), b"preexisting conflicting object")
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob("nightly-candidate-*.json")), [])

    def test_manifest_upload_requires_publication_output(self):
        upload = self.steps["Upload nightly candidate manifest"]
        self.assertIn("        id: publish", self.publish)
        self.assertIn("          DEB_FILE: ${{ steps.deb.outputs.deb_file }}", self.publish)
        self.assertIn("        if: ${{ steps.publish.outputs.candidate_file != '' }}", upload)
        self.assertIn("          name: nightly-candidate-${{ github.run_id }}-${{ github.run_attempt }}", upload)
        self.assertIn("          path: ${{ steps.publish.outputs.candidate_file }}", upload)
        self.assertIn("          if-no-files-found: error", upload)
        self.assertNotIn("        continue-on-error: true", self.publish)
        self.assertNotIn("          overwrite: true", upload)


class ValidationPackageTests(unittest.TestCase):
    def setUp(self):
        # Exercise the real upload shell with the same conditional object-store
        # fixture used by the nightly publication tests.
        self.fixture = NightlyCandidateTests("test_success_binds_actual_package_checkout_and_attempt")
        self.fixture.setUp()
        self.addCleanup(self.fixture.temp.cleanup)
        self.workflow = ROOT / ".github/workflows/rustfs-validation-package.yml"
        job = yaml_block(self.workflow.read_text().splitlines(), "publish", 2)
        start = job.index("      - name: Upload validation package to immutable R2 path")
        end = job.index("      - name: Upload validation candidate manifest")
        upload = job[start:end]
        begin = upload.index("        run: |") + 1
        self.fixture.shell = "\n".join(line[10:] for line in upload[begin:] if not line.strip() or line.startswith("          "))
        self.fixture.shims.write_text(self.fixture.shims.read_text().replace("/packages/nightly/runs/*", "/packages/validation/runs/*"))
        self.fixture.env.update(PACKAGE_FILE=self.fixture.package.name, VALIDATION_SOURCE_SHA=self.fixture.sha,
                                VALIDATION_SOURCE_REF=self.fixture.sha, GITHUB_SHA="f" * 40, AWS_CLI="aws")
        def shell(name, end_name):
            step = job[job.index("      - name: " + name):job.index("      - name: " + end_name)]
            return "\n".join(line[10:] for line in step[step.index("        run: |") + 1:]
                             if not line.strip() or line.startswith("          "))
        self.stage_shell = shell("Validate and stage opaque package bytes", "Prepare trusted publication tools")
        self.tools_shell = shell("Prepare trusted publication tools", "Upload validation package to immutable R2 path")

    def stage(self):
        env = dict(os.environ, RUNNER_TEMP=str(self.fixture.root))
        env.pop("BASH_ENV", None)
        return subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.stage_shell],
                              cwd=self.fixture.root, env=env, capture_output=True, text=True)

    def test_success_uploads_only_one_immutable_object_and_exact_source(self):
        result = self.fixture.run_publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = self.fixture.manifest()
        self.assertEqual(manifest["schema"], 3)
        self.assertEqual(manifest["channel"], "validation")
        self.assertEqual(manifest["source_sha"], self.fixture.sha)
        self.assertEqual(manifest["source_ref"], self.fixture.sha)
        self.assertEqual(manifest["workflow_sha"], "f" * 40)
        validate_manifest(manifest, {"id": 12345, "run_attempt": 1, "head_sha": "f" * 40})
        objects = [path for path in self.fixture.store.rglob("*") if path.is_file()]
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].read_bytes(), self.fixture.package.read_bytes())
        self.assertEqual(str(objects[0].relative_to(self.fixture.store)),
                         f"artifacts/rustfs/packages/validation/runs/12345/1/{self.fixture.digest}/rustfs.deb")
        self.assertNotIn("s3 cp", (self.fixture.root / "aws.log").read_text())

    def test_failure_or_missing_credentials_cannot_certify_a_candidate(self):
        for index, mode in enumerate(("upload-failure", "missing-public-url", "wrong-public-bytes", "incomplete-download")):
            with self.subTest(mode=mode):
                result = self.fixture.run_publish(FAKE_MODE=mode, GITHUB_RUN_ID=str(20000 + index))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.fixture.output.exists())
                self.assertEqual(list(self.fixture.root.glob("validation-candidate-*.json")), [])
        for key in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET"):
            with self.subTest(key=key):
                self.assertNotEqual(self.fixture.run_publish(**{key: ""}).returncode, 0)
                self.assertFalse(self.fixture.output.exists())

    def test_stale_source_and_duplicate_objects_are_rejected(self):
        for source in ("not-an-exact-sha", "e" * 40):
            with self.subTest(source=source):
                result = self.fixture.run_publish(VALIDATION_SOURCE_SHA=source)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.fixture.store.exists())
        result = self.fixture.run_publish()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = self.fixture.manifest()
        key = manifest["package_url"].removeprefix("https://dl.rustfs.com/")
        stored = self.fixture.store / key
        stored.write_bytes(b"preexisting conflicting object")
        (self.fixture.root / "validation-candidate-12345-1.json").unlink()
        result = self.fixture.run_publish()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(stored.read_bytes(), b"preexisting conflicting object")
        self.assertFalse(self.fixture.output.exists())

    def test_exact_source_and_workflow_identity_are_preserved_separately(self):
        lines = self.workflow.read_text().splitlines()
        build = "\n".join(yaml_block(lines, "build", 2))
        publish = "\n".join(yaml_block(lines, "publish", 2))
        source = "\n".join(yaml_block(lines, "resolve-source", 2))
        self.assertIn('[[ "${REQUESTED_SHA}" =~ ^[0-9a-f]{40}$ ]]', source)
        self.assertIn('test "$(git -C "${source_directory}" rev-parse FETCH_HEAD)" = "${REQUESTED_SHA}"', source)
        self.assertIn("ref: ${{ needs.resolve-source.outputs.source_sha }}", build)
        self.assertIn('--arg source_sha "${VALIDATION_SOURCE_SHA}"', publish)
        self.assertIn('--arg workflow_sha "${GITHUB_SHA}"', publish)
        self.assertNotIn("PF_TESTING_GH_TOKEN", build)
        self.assertNotIn("publish_nightly_assets", build)

    def test_candidate_execution_and_publication_credentials_are_in_separate_jobs(self):
        lines = self.workflow.read_text().splitlines()
        build = "\n".join(yaml_block(lines, "build", 2))
        publish = "\n".join(yaml_block(lines, "publish", 2))
        self.assertIn("uses: ./.github/actions/setup", build)
        self.assertIn("cargo build", build)
        self.assertNotIn("secrets.", build)
        self.assertNotIn("id-token: write", build)
        self.assertIn("needs: [resolve-source, build]", publish)
        self.assertIn("runs-on: ubuntu-latest", publish)
        for forbidden in ("actions/checkout@", "actions/cache@", "uses: ./", "needs.build.outputs", "cargo ", "dpkg", "source "):
            with self.subTest(forbidden=forbidden): self.assertNotIn(forbidden, publish)
        name = "name: validation-package-bytes-${{ github.run_id }}-${{ github.run_attempt }}"
        self.assertIn(name, build)
        self.assertIn(name, publish)
        self.assertIn("VALIDATION_SOURCE_SHA: ${{ needs.resolve-source.outputs.source_sha }}", publish)
        self.assertIn("VALIDATION_SOURCE_REF: ${{ needs.resolve-source.outputs.source_ref }}", publish)
        prepare = publish.split("      - name: Upload validation package to immutable R2 path")[0]
        self.assertNotIn("secrets.", prepare)
        self.assertIn("secrets.R2_SECRET_ACCESS_KEY", publish)

    def test_staging_rejects_nonregular_empty_or_extra_artifact_entries(self):
        incoming = self.fixture.root / "validation-package-input"
        incoming.mkdir()
        package = incoming / "rustfs.deb"
        for kind in ("missing", "empty", "symlink", "directory", "fifo", "extra", "directory-symlink"):
            with self.subTest(kind=kind):
                if kind == "empty": package.touch()
                elif kind == "symlink": package.symlink_to(self.fixture.package)
                elif kind == "directory": package.mkdir()
                elif kind == "fifo": os.mkfifo(package)
                elif kind == "extra":
                    package.write_bytes(b"opaque")
                    (incoming / "BASH_ENV").write_text("exit 1")
                elif kind == "directory-symlink":
                    incoming.rmdir()
                    incoming.symlink_to(self.fixture.root, target_is_directory=True)
                self.assertNotEqual(self.stage().returncode, 0)
                self.assertFalse((self.fixture.root / "validation-publication").exists())
                if incoming.is_symlink():
                    incoming.unlink()
                    incoming.mkdir()
                elif package.is_dir(): package.rmdir()
                else: package.unlink(missing_ok=True)
                (incoming / "BASH_ENV").unlink(missing_ok=True)

    def test_staging_and_publication_never_execute_candidate_package_or_python_hooks(self):
        incoming = self.fixture.root / "validation-package-input"
        incoming.mkdir()
        marker = self.fixture.root / "candidate-executed"
        payload = f'#!/bin/bash\nprintf "%s" "$R2_SECRET_ACCESS_KEY" > "{marker}"\n'.encode()
        (incoming / "rustfs.deb").write_bytes(payload)
        (self.fixture.root / "sitecustomize.py").write_text(f"open({str(marker)!r}, 'w').write('python hook')")
        result = self.stage()
        self.assertEqual(result.returncode, 0, result.stderr)
        staged = self.fixture.root / "validation-publication/rustfs.deb"
        self.assertEqual(staged.read_bytes(), payload)
        result = self.fixture.run_publish(PACKAGE_FILE=str(staged))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(marker.exists())
        self.assertEqual(self.fixture.manifest()["package_sha256"], hashlib.sha256(payload).hexdigest())

    def test_cli_bootstrap_finishes_without_publication_secrets(self):
        for mode in ("success", "old-cli", "bootstrap-failure", "install-failure", "broken-install"):
            with self.subTest(mode=mode):
                (self.fixture.root / "installed").unlink(missing_ok=True)
                env = dict(self.fixture.env, FAKE_MODE=mode)
                for key in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT", "R2_BUCKET"):
                    env.pop(key)
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", self.tools_shell],
                                        cwd=self.fixture.root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, mode in ("success", "old-cli"), result.stderr)
                self.assertFalse(self.fixture.store.exists())
                self.assertEqual(list(self.fixture.root.glob("validation-candidate-*.json")), [])


if __name__ == "__main__":
    unittest.main()
