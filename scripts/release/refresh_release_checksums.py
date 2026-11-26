#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Keep a release checksum file aligned with Linux deb and rpm assets.

The legacy tag publisher writes SHA256SUMS and SHA512SUMS before packages
exist. This module preserves every unrelated line and replaces the package
lines with digests of the files just built. GitHub stores ``~`` in an asset
name as ``.``; checksum names use that stored form.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import re
import sys

ASSET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,200}")


def github_asset_name(filename: str) -> str:
    name = Path(filename).name.replace("~", ".")
    if ASSET_NAME.fullmatch(name) is None or name in {"SHA256SUMS", "SHA512SUMS"}:
        raise ValueError(f"refusing checksum name {filename!r}")
    return name


def parse_checksums(text: str, digest_length: int) -> list[tuple[str, str]]:
    if digest_length not in (64, 128):
        raise ValueError("checksum digest length must be 64 or 128")
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line_number, line in enumerate(text.splitlines(), 1):
        if line.strip() == "":
            continue
        digest, separator, name = line.partition("  ")
        if separator != "  " or len(digest) != digest_length or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"malformed checksum line {line_number}")
        if ASSET_NAME.fullmatch(name) is None or name in seen:
            raise ValueError(f"invalid checksum name on line {line_number}")
        seen.add(name)
        rows.append((digest, name))
    if not rows:
        raise ValueError("checksum file has no entries")
    return rows


def lookup(text: str, name: str, digest_length: int) -> str:
    asset = github_asset_name(name)
    matches = [digest for digest, entry in parse_checksums(text, digest_length) if entry == asset]
    if len(matches) != 1:
        raise ValueError(f"checksum file has no single entry for {asset}")
    return matches[0]


def refresh(text: str, packages: list[tuple[str, str]], digest_length: int) -> str:
    if not packages:
        raise ValueError("refusing to rewrite checksums with no packages")
    rows = parse_checksums(text, digest_length)
    replacements: dict[str, str] = {}
    for filename, digest in packages:
        asset = github_asset_name(filename)
        if len(digest) != digest_length or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"invalid digest for {asset}")
        if asset in replacements and replacements[asset] != digest:
            raise ValueError(f"conflicting digests for {asset}")
        replacements[asset] = digest
    # Keep every non-package line, including zip aliases, in its original
    # order. Package lines are replaced or appended.
    kept = [f"{digest}  {name}" for digest, name in rows if name not in replacements]
    if not kept:
        raise ValueError("refusing to drop every existing checksum entry")
    kept.extend(f"{digest}  {name}" for name, digest in sorted(replacements.items()))
    return "\n".join(kept) + "\n"


def file_digest(path: Path, algorithm: str) -> str:
    hasher = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--checksum", type=Path, required=True)
    verify.add_argument("--name", required=True)
    verify.add_argument("--digest", required=True)
    verify.add_argument("--digest-length", type=int, required=True)
    rewrite = sub.add_parser("refresh")
    rewrite.add_argument("--checksum", type=Path, required=True)
    rewrite.add_argument("--output", type=Path, required=True)
    rewrite.add_argument("--algorithm", choices=("sha256", "sha512"), required=True)
    rewrite.add_argument("--digest-length", type=int, required=True)
    rewrite.add_argument("packages", nargs="+", type=Path)
    args = parser.parse_args(argv)
    if args.mode == "verify":
        text = args.checksum.read_text(encoding="utf-8")
        found = lookup(text, args.name, args.digest_length)
        if found != args.digest:
            print(f"checksum mismatch for {args.name}", file=sys.stderr)
            return 1
        return 0
    length = args.digest_length
    if (args.algorithm, length) not in {("sha256", 64), ("sha512", 128)}:
        print("algorithm and digest length disagree", file=sys.stderr)
        return 1
    packages = [(github_asset_name(str(path)), file_digest(path, args.algorithm)) for path in args.packages]
    args.output.write_text(refresh(args.checksum.read_text(encoding="utf-8"), packages, length), encoding="utf-8")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
