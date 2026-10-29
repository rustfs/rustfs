#!/usr/bin/env python3
"""Retain self-hosted validation evidence outside Actions' temporary cleanup."""
import json
import os
from pathlib import Path
import re
import stat
import sys

MINIMUM_FREE_BYTES = 10 * 1024**3
MINIMUM_FREE_INODES = 50000


def initialize(environ=None):
    env = os.environ if environ is None else environ
    run, attempt = env['GITHUB_RUN_ID'], env['GITHUB_RUN_ATTEMPT']
    if not all(re.fullmatch(r'[1-9][0-9]*', value) for value in (run, attempt)):
        raise ValueError('Evidence requires numeric workflow run and attempt identities')
    tools_only = env['VALIDATION_TOOLS_ONLY']
    if tools_only not in {'true', 'false'}:
        raise ValueError('Evidence requires an explicit tools-only selection')
    temporary = Path(env['RUNNER_TEMP'])
    if not temporary.is_absolute() or temporary.resolve(strict=True) != temporary or not temporary.is_dir():
        raise ValueError('Runner temporary directory must be an existing canonical absolute directory')
    parent = temporary if tools_only == 'true' else temporary.parent / 'rustfs-validation-evidence'
    if tools_only == 'false':
        parent.mkdir(mode=0o700, exist_ok=True)
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError('Persistent evidence parent must be a private runner-owned directory')
    root = parent / f'rustfs-validation-{run}-{attempt}'
    root.mkdir(mode=0o700)
    observations = []
    for path in dict.fromkeys((temporary, parent)):
        usage = os.statvfs(path)
        observations.append({'path': str(path), 'availableBytes': usage.f_bavail * usage.f_frsize,
            'availableInodes': usage.f_favail, 'minimumFreeBytes': MINIMUM_FREE_BYTES,
            'minimumFreeInodes': MINIMUM_FREE_INODES})
    ready = all(item['availableBytes'] >= MINIMUM_FREE_BYTES and item['availableInodes'] >= MINIMUM_FREE_INODES
                for item in observations)
    report = {'schemaVersion': 1, 'runId': run, 'runAttempt': attempt, 'evidenceRoot': str(root),
              'persistentOutsideRunnerTemp': tools_only == 'false', 'storage': observations, 'ready': ready,
              'automaticEvidenceDeletion': False, 'canonicalPassClaimed': False}
    with (root / 'runner-storage-preflight.json').open('x') as stream:
        json.dump(report, stream, indent=2)
        stream.write(chr(10))
    with Path(env['GITHUB_ENV']).open('a') as stream:
        if any(character in str(root) for character in (chr(10), chr(13))):
            raise ValueError('Evidence path cannot contain environment-file delimiters')
        stream.write('VALIDATION_ROOT=' + str(root) + chr(10))
    print(json.dumps(report, sort_keys=True))
    if not ready:
        raise ValueError('Validation requires at least 10 GiB and 50000 free inodes on runner storage')
    return root


if __name__ == '__main__':
    try:
        initialize()
    except (OSError, ValueError, KeyError) as error:
        print(f'Validation evidence initialization failed: {error}', file=sys.stderr)
        sys.exit(1)
