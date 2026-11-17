#!/usr/bin/env python3
"""Download a named GitHub release asset through its API and verify its digest."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import quote


def download(repository, tag, name, output, expected_sha256=None):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", repository):
        raise ValueError("invalid release repository")
    if not tag or not name or "/" in name or "\\" in name:
        raise ValueError("invalid release tag or asset name")
    if expected_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("invalid pinned asset digest")
    result = subprocess.run(
        ["gh", "api", "--hostname", "github.com", f"repos/{repository}/releases/tags/{quote(tag, safe='')}"],
        check=True, capture_output=True, text=True, timeout=60,
    )
    release = json.loads(result.stdout)
    if not isinstance(release, dict) or release.get("tag_name") != tag:
        raise ValueError("release metadata does not match the requested tag")
    assets = release.get("assets")
    if not isinstance(assets, list) or any(not isinstance(asset, dict) for asset in assets):
        raise ValueError("invalid release asset inventory")
    matches = [asset for asset in assets if asset.get("name") == name]
    if len(matches) != 1:
        raise ValueError("release must contain exactly one named asset")
    asset = matches[0]
    asset_id, size, digest = asset.get("id"), asset.get("size"), asset.get("digest")
    if type(asset_id) is not int or asset_id <= 0 or type(size) is not int or not 0 < size <= 512 * 1024 * 1024:
        raise ValueError("invalid release asset identity or size")
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("release asset has no valid SHA-256 digest")
    sha256 = digest.removeprefix("sha256:")
    if expected_sha256 is not None and expected_sha256 != sha256:
        raise ValueError("release digest differs from the pinned digest")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output.parent, prefix=".release-asset-", delete=False) as stream:
            temporary = Path(stream.name)
            # gh authenticates to the API; its HTTP client strips credentials on
            # cross-host redirects. No browser URL or token is supplied in argv.
            subprocess.run(
                ["gh", "api", "--hostname", "github.com", "--header", "Accept: application/octet-stream", f"repos/{repository}/releases/assets/{asset_id}"],
                check=True, stdout=stream, timeout=300,
            )
        with temporary.open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if temporary.stat().st_size != size or actual != sha256:
            raise ValueError("downloaded release asset failed size or SHA-256 verification")
        temporary.replace(output)
        print(f"Verified release asset: {repository} {tag} {name} sha256={actual}")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--asset", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--sha256")
    args = parser.parse_args()
    try:
        download(args.repository, args.tag, args.asset, args.output, args.sha256)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError, subprocess.SubprocessError, OSError) as error:
        print(f"Release asset download failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
