#!/usr/bin/env python3
"""Stage one verified chain package for suites without an installer hash option."""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

from functional_chain_evidence import current_chain
from resolve_functional_candidate import positive, require, sha


def package_path(chain):
    digest = chain["candidate"]["manifest"]["package_sha256"]
    require(positive(chain["run_id"]) and positive(chain["attempt"]) and sha(digest, 64), "invalid package cache identity")
    return Path(f"/var/cache/rustfs-functional/{chain['run_id']}-{chain['attempt']}/{digest}.deb")


def targets():
    nodes = os.environ.get("RUSTFS_NODES", "").split() or ["vm000", "vm001", "vm002", "vm003"]
    user = os.environ.get("RUSTFS_SSH_USER") or "azureuser"
    require(bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", user)), "invalid SSH user")
    require(all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", node) for node in nodes), "invalid SSH node")
    require(len(set(nodes)) == len(nodes), "duplicate SSH node")
    return [f"{user}@{node}" for node in nodes]


def install_script(path, digest):
    # The cache parent is root-owned. Verify the transferred bytes before the
    # atomic rename exposes a readable package to the unprivileged installer.
    return f"""set -eu
install -d -m 0755 {shlex.quote(str(path.parent))}
temporary=$(mktemp {shlex.quote(str(path.parent / '.package.XXXXXX'))})
trap 'rm -f "$temporary"' EXIT HUP INT TERM
cat > "$temporary"
printf '%s  %s\\n' {shlex.quote(digest)} "$temporary" | sha256sum -c - >/dev/null
chmod 0644 "$temporary"
mv -f "$temporary" {shlex.quote(str(path))}
"""


def remote_command(script):
    quoted = shlex.quote(script)
    return f'if [ "$(id -u)" -eq 0 ]; then sh -c {quoted}; else sudo -n sh -c {quoted}; fi'


def ssh(target, script, **kwargs):
    subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", target, remote_command(script)],
                   check=True, **kwargs)


def prepare(chain):
    path = package_path(chain)
    peers = targets()
    manifest = chain["candidate"]["manifest"]
    with tempfile.TemporaryDirectory(prefix="rustfs-functional-package-") as temporary:
        package = Path(temporary) / "rustfs.deb"
        subprocess.run(["curl", "--fail", "--show-error", "--silent", "--location", "--retry", "3",
                        "--output", str(package), manifest["package_url"]], check=True)
        with package.open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        require(actual == manifest["package_sha256"], "candidate package SHA256 mismatch")
        script = install_script(path, actual)
        for peer in peers:
            with package.open("rb") as source:
                ssh(peer, script, stdin=source)
    return "file://" + str(path)


def cleanup(chain):
    path = package_path(chain)
    script = f"set -eu\nrm -f -- {shlex.quote(str(path))}\nrmdir -- {shlex.quote(str(path.parent))} 2>/dev/null || true\n"
    failures = []
    for peer in targets():
        try:
            ssh(peer, script)
        except subprocess.CalledProcessError:
            failures.append(peer)
    require(not failures, "package cache cleanup failed on: " + ", ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "cleanup"))
    args = parser.parse_args()
    chain = current_chain()
    if args.mode == "prepare":
        url = prepare(chain)
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("package_url=" + url + "\n")
    else:
        cleanup(chain)


if __name__ == "__main__":
    main()
