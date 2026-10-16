#!/usr/bin/env python3
"""Exercise server-only release commands and strict acceptance archive readers."""

from __future__ import annotations

import io
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

from test_security_workflow import named_steps, shell_body, yaml_block

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / '.github/workflows/build.yml').read_text()
STEPS = named_steps(yaml_block(SOURCE.splitlines(), 'build-rustfs', 2))
MATRIX = json.loads(re.search(r"all='(\{.*?\})'", SOURCE, re.S)[1])['include']
CATALOG = ('rustfs.cpu-symbol-catalog.json', 'rustfs.cpu-symbol-catalog.sha256')
EXTRACTORS = (
    ('connect-diagnostic-schedule-acceptance.yml', 'diagnostic-schedule', 'Extract the verified official binary'),
    ('connect-health-service-acceptance.yml', 'health-service', 'Extract only the verified official service binary'),
    ('connect-profile-memory-acceptance.yml', 'profile-memory', 'Extract only the verified official service binary'),
    ('connect-top-disk-acceptance.yml', 'top-disk', 'Extract only the verified official service binary'),
)


def render(script, target, package='package.zip'):
    context = {f'matrix.{key}': str(value).lower() if isinstance(value, bool) else str(value) for key, value in target.items()}
    context['steps.package.outputs.package_file'] = package
    return re.sub(r'\$\{\{\s*(.*?)\s*\}\}', lambda match: context[match[1]], script)


def archive(path, members, binary):
    with zipfile.ZipFile(path, 'w') as package:
        for name in members:
            entry = zipfile.ZipInfo(name)
            entry.external_attr = (0o100755 if name == 'rustfs' else 0o100644) << 16
            package.writestr(entry, binary if name == 'rustfs' else b'fixture')


class ReleaseBuildTests(unittest.TestCase):
    def test_every_matrix_target_builds_only_server(self):
        mock_cargo = r'''
cargo() {
  printf '%q ' "$@" >> cargo-call
  printf '\n' >> cargo-call
  local file="target/$BUILD_TARGET/release/rustfs"
  if [[ "$BUILD_TARGET" == *-windows-msvc ]]; then file+='.exe'; fi
  mkdir -p "$(dirname "$file")"
  cat > "$file" <<'BIN'
#!/usr/bin/env bash
printf 'build target : %s\nbuild os     : %s\nrust channel : stable-%s\n' "$BUILD_TARGET" "$BUILD_OS" "$BUILD_TARGET"
BIN
  chmod +x "$file"
}
'''
        for target in MATRIX:
            with self.subTest(target=target['target_id']), tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                (path / 'rustfs').mkdir()
                (path / 'rustfs/build.rs').touch()
                (path / 'scripts/release').mkdir(parents=True)
                (path / 'scripts/release/check_glibc.py').write_bytes((ROOT / 'scripts/release/check_glibc.py').read_bytes())
                (path / 'tools').mkdir()
                readelf = path / 'tools/readelf'
                readelf.write_text("#!/bin/sh\nprintf '%s\\n' 'Version needs section:' 'Name: GLIBC_2.30'\n")
                readelf.chmod(0o755)
                script = mock_cargo + render(shell_body(STEPS['Build RustFS']), target)
                environment = {**os.environ, 'BUILD_TARGET': target['target'],
                               'BUILD_OS': target['platform'] + '-' + target['target'].split('-')[0],
                               'PATH': str(path / 'tools') + os.pathsep + os.environ['PATH']}
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script], cwd=path,
                                        env=environment, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (path / 'cargo-call').read_text().splitlines()
                self.assertEqual(len(calls), 1)
                args = shlex.split(calls[0])
                self.assertEqual(args[0], 'zigbuild' if target['cross'] else 'build')
                self.assertEqual(args[args.index('--bin') + 1], 'rustfs')
                self.assertNotIn('rustfs-cli', args)
                self.assertIn('--locked', args)
                expected_target = target['target'] + ('.' + target['glibc'] if target['glibc'] else '')
                self.assertEqual(args[args.index('--target') + 1], expected_target)
                if target['features']:
                    self.assertEqual(args[args.index('--features') + 1], target['features'])
                if target['glibc']:
                    self.assertEqual(args[args.index('--config') + 1], 'profile.release.strip="none"')

    def test_packaged_linux_server_and_catalog_members(self):
        binary = b'#!/bin/sh\necho rustfs-fixture\n'
        for libc in ('gnu', 'musl'):
            target = next(item for item in MATRIX if item['target_id'] == f'linux-x86_64-{libc}')
            expected = ['rustfs', *CATALOG] if libc == 'gnu' else ['rustfs']
            for members, valid in ((expected, True), (expected + ['rustfs-cli'], False)):
                with self.subTest(libc=libc, members=members), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory)
                    archive(path / 'package.zip', members, binary)
                    script = render(shell_body(STEPS['Verify packaged Linux binary']), target)
                    result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', script], cwd=path,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode == 0, valid, result.stderr)

    def test_actual_acceptance_extractors_keep_strict_members(self):
        source_sha = '0123456789abcdef' * 2 + '01234567'
        package_name = f'rustfs-linux-x86_64-gnu-dev-{source_sha[:7]}.zip'
        header = bytearray(20)
        header[:7] = b'\x7fELF\x02\x01\x01'
        header[18:20] = b'\x3e\x00'
        valid_members = ['rustfs', *CATALOG]
        cases = ((valid_members, True), (['rustfs'], False), (valid_members + ['rustfs-cli'], False),
                 (valid_members + ['../extra'], False))
        for workflow, job_name, step in EXTRACTORS:
            source = (ROOT / '.github/workflows' / workflow).read_text()
            job = yaml_block(source.splitlines(), job_name, 2)
            body = shell_body(named_steps(job)[step])
            python = body.split("python3 - <<'PY'\n", 1)[1].split('\nPY', 1)[0]
            for members, valid in cases:
                with self.subTest(workflow=workflow, members=members), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory)
                    archive(path / package_name, members, bytes(header))
                    with zipfile.ZipFile(path / 'official-artifact.zip', 'w') as outer:
                        outer.write(path / package_name, package_name)
                    result = subprocess.run([sys.executable, '-c', python], cwd=path,
                                            env={**os.environ, 'SOURCE_SHA': source_sha}, capture_output=True, text=True)
                    self.assertEqual(result.returncode == 0, valid, result.stderr)
                    if valid:
                        self.assertEqual((path / 'binary/rustfs').read_bytes(), bytes(header))


if __name__ == '__main__':
    unittest.main()
