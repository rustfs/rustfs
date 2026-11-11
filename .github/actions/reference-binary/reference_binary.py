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
"""Resolve and unpack the pinned reference RustFS release binary.

`resolve` reads the pin file and prints the selected platform's identity as
`key=value` lines (GitHub step-output format). `unpack` re-verifies the
downloaded archive digest against the pin and extracts exactly one executable.
Every missing or malformed input is a failure; nothing is skipped.
"""

import argparse
import hashlib
from pathlib import Path
import re
import sys
import tomllib
import zipfile

REPOSITORY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*")
TAG = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]*")
PLATFORM = re.compile(r"[a-z0-9][a-z0-9_-]*")
ASSET = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]*\.zip")
SHA256 = re.compile(r"[0-9a-f]{64}")
EXECUTABLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
MAX_EXECUTABLE_BYTES = 1024 * 1024 * 1024


class PinError(Exception):
    """A pin, archive or argument that cannot be trusted."""


def field(table, key, pattern, where):
    value = table.get(key)
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise PinError(f"{where}: missing or invalid '{key}'")
    return value


def resolve(pin_path, platform):
    if not PLATFORM.fullmatch(platform or ""):
        raise PinError("invalid platform name")
    try:
        data = tomllib.loads(Path(pin_path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PinError(f"{pin_path}: reference pin file is missing") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise PinError(f"{pin_path}: cannot read reference pin: {exc}") from exc
    if data.get("schema") != 1 or type(data.get("schema")) is not int:
        raise PinError(f"{pin_path}: unsupported pin schema")
    identity = {
        "repository": field(data, "repository", REPOSITORY, pin_path),
        "version": field(data, "version", TAG, pin_path),
        "tag": field(data, "tag", TAG, pin_path),
    }
    platforms = data.get("platforms")
    if not isinstance(platforms, dict) or not platforms:
        raise PinError(f"{pin_path}: no pinned platforms")
    entry = platforms.get(platform)
    if not isinstance(entry, dict):
        raise PinError(f"{pin_path}: no pin for platform '{platform}'")
    where = f"{pin_path} [platforms.{platform}]"
    identity["asset"] = field(entry, "asset", ASSET, where)
    identity["sha256"] = field(entry, "sha256", SHA256, where)
    identity["executable"] = field(entry, "executable", EXECUTABLE, where)
    if identity["version"] not in identity["asset"]:
        raise PinError(f"{where}: asset name does not carry version {identity['version']}")
    unknown = set(entry) - {"asset", "sha256", "executable"}
    if unknown:
        raise PinError(f"{where}: unknown keys {sorted(unknown)}")
    return identity


def file_sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def unpack(archive, sha256, executable, destination):
    if not SHA256.fullmatch(sha256 or "") or not EXECUTABLE.fullmatch(executable or ""):
        raise PinError("invalid expected digest or executable name")
    try:
        actual = file_sha256(archive)
    except OSError as exc:
        raise PinError(f"{archive}: reference archive is missing or unreadable") from exc
    if actual != sha256:
        raise PinError(f"{archive}: sha256 {actual} does not match the pinned digest")
    try:
        with zipfile.ZipFile(archive) as bundle:
            matches = [info for info in bundle.infolist() if info.filename == executable]
            if len(matches) != 1:
                raise PinError(f"{archive}: must contain exactly one top-level '{executable}'")
            info = matches[0]
            if info.is_dir() or not 0 < info.file_size <= MAX_EXECUTABLE_BYTES:
                raise PinError(f"{archive}: '{executable}' is not a plausible executable")
            destination = Path(destination)
            destination.mkdir(parents=True, exist_ok=True)
            target = destination / executable
            with bundle.open(info) as source, open(target, "wb") as sink:
                while chunk := source.read(1024 * 1024):
                    sink.write(chunk)
    except zipfile.BadZipFile as exc:
        raise PinError(f"{archive}: not a valid zip archive") from exc
    target.chmod(0o755)
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    res = sub.add_parser("resolve")
    res.add_argument("--pin", required=True)
    res.add_argument("--platform", required=True)
    unp = sub.add_parser("unpack")
    unp.add_argument("--archive", required=True)
    unp.add_argument("--sha256", required=True)
    unp.add_argument("--executable", required=True)
    unp.add_argument("--destination", required=True)
    args = parser.parse_args(argv)
    try:
        if args.mode == "resolve":
            for key, value in resolve(args.pin, args.platform).items():
                print(f"{key}={value}")
        else:
            print(f"path={unpack(args.archive, args.sha256, args.executable, args.destination)}")
    except PinError as exc:
        print(f"reference binary: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
