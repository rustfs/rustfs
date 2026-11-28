#!/usr/bin/env python3
"""Exercise S3 harness tool setup without installing packages or starting RustFS."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SOURCE = Path(__file__).with_name("run.sh").read_text()
TOX_SETUP = SOURCE[
    SOURCE.index("# Match the weekly compatibility workflow") : SOURCE.index("# Step 9: Run ceph s3-tests")
]
PLUGIN_SETUP = SOURCE[SOURCE.index('XDIST_ARGS=""') : SOURCE.index("# Resolve config path (absolute path for tox)")]
PATH_SETUP = SOURCE[SOURCE.index("# Ensure user-level Python scripts") : SOURCE.index("# Configuration")]
INSTALLER = SOURCE[SOURCE.index("ensure_python_pip() {") : SOURCE.index('if ! command -v awscurl')]
ALT_USER_PROVISIONING = SOURCE[
    SOURCE.index("# Provision alt user (required by suite)") : SOURCE.index("# Step 8: Prepare s3-tests")
]


class RunnerToolsTests(unittest.TestCase):
    def test_real_pip_installer_finds_new_user_binary_without_uv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "tools"
            tools.mkdir()
            python = tools / "python3"
            python.write_text(r'''#!/bin/bash
if [[ "$1" == -c ]]; then
    printf '3.12\n'
elif [[ "$*" == '-m pip --version' ]]; then
    printf 'pip 25.0\n'
elif [[ "$*" == *'tox==4.60.0'* ]]; then
    mkdir -p "$TOOL_TEST_HOME/.local/bin"
    printf '#!/bin/sh\nprintf "4.60.0 from user-site\\n"\n' > "$TOOL_TEST_HOME/.local/bin/tox"
    chmod +x "$TOOL_TEST_HOME/.local/bin/tox"
else
    exit 99
fi
''')
            python.chmod(0o755)
            awscurl = tools / "awscurl"
            awscurl.write_text("#!/bin/sh\nexit 0\n")
            awscurl.chmod(0o755)
            # Redirect only the extracted home paths into this test's sandbox.
            # Keep the real initialization and installer to exercise PATH ordering.
            script = (PATH_SETUP + INSTALLER + TOX_SETUP).replace("$HOME", "$TOOL_TEST_HOME")
            script = 'log_error() { printf "%s\\n" "$*" >&2; }\n' + script
            script += 'command -v tox\n'
            result = subprocess.run(
                ["/bin/bash", "-euo", "pipefail", "-c", script],
                env={**os.environ, "PATH": f"{tools}:/usr/bin:/bin", "TOOL_TEST_HOME": str(root)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), str(root / ".local/bin/tox"))

    def test_tox_version_is_enforced_before_collection(self) -> None:
        stubs = r'''
tox() {
    [[ "$TOOL_TEST_VERSION" != missing ]] || return 127
    printf '%s from /runner/tox\n' "$TOOL_TEST_VERSION"
}
install_python_package() {
    printf 'INSTALL %s\n' "$1"
    [[ "$TOOL_TEST_INSTALL" != fail ]] || return 1
    if [[ "$TOOL_TEST_INSTALL" != shadowed ]]; then
        TOOL_TEST_VERSION=4.60.0
    fi
}
log_error() { printf '%s\n' "$*" >&2; }
'''
        for version, install, expected, installs in (
            ("4.60.0", "ok", 0, 0),
            ("4.59.0", "ok", 0, 1),
            ("missing", "ok", 0, 1),
            ("4.59.0", "fail", 1, 1),
            ("4.59.0", "shadowed", 1, 1),
        ):
            with self.subTest(version=version, install=install):
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", stubs + TOX_SETUP + "printf 'COLLECT\n'"],
                    env={**os.environ, "TOOL_TEST_VERSION": version, "TOOL_TEST_INSTALL": install},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertEqual(result.stdout.count("INSTALL tox==4.60.0"), installs)
                self.assertEqual("COLLECT" in result.stdout, expected == 0)
                if install == "shadowed":
                    self.assertIn("Expected tox 4.60.0", result.stderr)

    def test_plugin_pins_preserve_serial_and_parallel_selection(self) -> None:
        for workers in ("0", "2"):
            with self.subTest(workers=workers), tempfile.TemporaryDirectory() as directory:
                requirements = Path(directory) / "requirements.txt"
                requirements.write_text("pytest\ntox\n")
                result = subprocess.run(
                    [
                        "bash", "-euo", "pipefail", "-c",
                        PLUGIN_SETUP + PLUGIN_SETUP + 'printf "%s" "$XDIST_ARGS"',
                    ],
                    cwd=directory,
                    env={**os.environ, "XDIST": workers},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                dependencies = requirements.read_text().splitlines()
                self.assertEqual(dependencies.count("tox==4.60.0"), 1)
                self.assertEqual(dependencies.count("pytest-timeout==2.4.0"), 1)
                self.assertEqual(dependencies.count("pytest-xdist==3.8.0"), int(workers != "0"))
                self.assertEqual(result.stdout, "" if workers == "0" else "-n 2 --dist=loadgroup")

    def test_alt_user_provisioning_encodes_credentials_and_stops_on_errors(self) -> None:
        stubs = r'''
log_info() { printf 'INFO %s\n' "$*"; }
log_error() { printf 'ERROR %s\n' "$*" >&2; }
awscurl() {
    printf 'CALL %s\n' "$*" >> "$PROVISION_LOG"
    if [[ "$FAIL_STAGE" == "add-user" && "$*" == *"/rustfs/admin/v3/add-user"* ]]; then
        printf '<Error xmlns="urn:s3"><Message>private-response</Message></Error>'
        return 0
    fi
    if [[ "$FAIL_STAGE" == "set-policy" && "$*" == *"/set-user-or-group-policy"* ]]; then
        printf '{"message":"private-response"}'
        printf 'private-diagnostic' >&2
        if [[ "$1" == "--fail-with-body" ]]; then return 22; fi
        return 0
    fi
    if [[ "$FAIL_STAGE" == "list-buckets" && "$*" == *"-X GET"* ]]; then
        printf '<Error><Code>InvalidAccessKeyId</Code><Message>missing</Message></Error>'
        return 0
    fi
    return 0
}
'''
        base_env = {
            **os.environ,
            "S3_HOST": "127.0.0.1",
            "S3_PORT": "9000",
            "S3_REGION": "us-east-1",
            "S3_ACCESS_KEY": "rustfsadmin",
            "S3_SECRET_KEY": "rustfsadmin",
            "S3_ALT_ACCESS_KEY": "alt+user&x",
            "S3_ALT_SECRET_KEY": r'alt"secret\tail',
        }
        with tempfile.TemporaryDirectory() as directory:
            call_log = Path(directory) / "calls.log"
            ok = subprocess.run(
                ["bash", "-euo", "pipefail", "-c", stubs + ALT_USER_PROVISIONING],
                env={**base_env, "PROVISION_LOG": str(call_log), "FAIL_STAGE": "none"},
                capture_output=True,
                text=True,
            )
            self.assertEqual(ok.returncode, 0, ok.stderr)
            self.assertIn("Alt user provisioned successfully", ok.stdout)
            calls = call_log.read_text().splitlines()
            self.assertEqual(len(calls), 3)
            self.assertIn('/rustfs/admin/v3/add-user?accessKey=alt%2Buser%26x', calls[0])
            payload, _ = json.JSONDecoder().raw_decode(calls[0].split(" -d ", 1)[1])
            self.assertEqual(payload, {"secretKey": base_env["S3_ALT_SECRET_KEY"], "status": "enabled"})
            self.assertIn('policyName=readwrite&userOrGroup=alt%2Buser%26x&isGroup=false', calls[1])

            for count, (stage, message) in enumerate((("add-user", "Failed to add alt user"),
                                                       ("set-policy", "Failed to set user policy"),
                                                       ("list-buckets", "Alt user cannot list buckets")), start=1):
                with self.subTest(stage=stage):
                    fail_log = Path(directory) / f"fail-{stage}.log"
                    res = subprocess.run(
                        ["bash", "-euo", "pipefail", "-c", stubs + ALT_USER_PROVISIONING],
                        env={**base_env, "PROVISION_LOG": str(fail_log), "FAIL_STAGE": stage},
                        capture_output=True,
                        text=True,
                    )
                    self.assertNotEqual(res.returncode, 0)
                    self.assertIn(message, res.stderr)
                    self.assertNotIn("private-response", res.stderr + res.stdout)
                    self.assertNotIn("private-diagnostic", res.stderr + res.stdout)
                    self.assertEqual(len(fail_log.read_text().splitlines()), count)
                    self.assertNotIn("Alt user provisioned successfully", res.stdout)


if __name__ == "__main__":
    unittest.main()
