#!/usr/bin/env python3
# Copyright 2024 RustFS Team
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

"""Quantify RustFS S3 API surface coverage.

Compares the operations defined by the s3s `S3` trait (at the revision pinned
in the workspace Cargo.toml) against the methods RustFS overrides in
`impl S3 for FS` (rustfs/src/storage/ecfs.rs). Trait methods that are not
overridden fall back to the s3s default implementation, which returns
NotImplemented — so "overridden" is a faithful proxy for "implemented".

The s3s trait source is located automatically from the local cargo git
checkout cache; if absent (e.g. a fresh CI runner), it is fetched from the
pinned revision on GitHub.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
import tempfile

ECFS_PATH = "rustfs/src/storage/ecfs.rs"
TRAIT_RELPATH = "crates/s3s/src/s3_trait.rs"


def project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[2]


def parse_s3s_pin(cargo_toml: pathlib.Path) -> tuple[str, str]:
    """Return (git url, rev) of the s3s dependency from the workspace Cargo.toml."""
    for line in cargo_toml.read_text(encoding="utf-8").splitlines():
        if re.match(r"^s3s\s*=", line):
            git = re.search(r'git\s*=\s*"([^"]+)"', line)
            rev = re.search(r'rev\s*=\s*"([^"]+)"', line)
            if git and rev:
                return git.group(1), rev.group(1)
    raise RuntimeError(f"s3s git dependency with rev not found in {cargo_toml}")


def locate_trait_source(git_url: str, rev: str) -> pathlib.Path:
    """Find s3_trait.rs in the cargo git checkout cache, or fetch the pinned rev."""
    cargo_home = pathlib.Path.home() / ".cargo"
    for candidate in cargo_home.glob(f"git/checkouts/s3s-*/{rev[:7]}/{TRAIT_RELPATH}"):
        return candidate

    workdir = pathlib.Path(tempfile.mkdtemp(prefix="s3s-trait-"))
    subprocess.run(["git", "init", "-q", str(workdir)], check=True)
    subprocess.run(["git", "-C", str(workdir), "remote", "add", "origin", git_url], check=True)
    subprocess.run(["git", "-C", str(workdir), "fetch", "-q", "--depth", "1", "origin", rev], check=True)
    subprocess.run(["git", "-C", str(workdir), "checkout", "-qf", "--detach", rev], check=True)
    return workdir / TRAIT_RELPATH


def extract_block(src: str, start: int) -> str:
    """Return the brace-balanced block starting at the first '{' at/after start."""
    depth = 0
    begin = src.index("{", start)
    for i in range(begin, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[begin : i + 1]
    raise RuntimeError("unbalanced braces")


def trait_operations(trait_source: pathlib.Path) -> dict[str, str]:
    """Return {operation: default kind} for every trait method.

    Kind is "not_implemented" when the default body errors with NotImplemented,
    or "delegated" when it forwards to another operation (e.g. post_object
    delegates to put_object), in which case overriding is optional.
    """
    src = trait_source.read_text(encoding="utf-8")
    ops: dict[str, str] = {}
    for match in re.finditer(r"async fn (\w+)", src):
        body = extract_block(src, match.end())
        ops[match.group(1)] = "not_implemented" if "NotImplemented" in body else "delegated"
    return ops


def implemented_operations(ecfs_source: pathlib.Path) -> set[str]:
    src = ecfs_source.read_text(encoding="utf-8")
    marker = "impl S3 for FS {"
    start = src.find(marker)
    if start < 0:
        raise RuntimeError(f"`{marker}` not found in {ecfs_source}")
    block = extract_block(src, start)
    return set(re.findall(r"async fn (\w+)", block))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--s3s-trait",
        type=pathlib.Path,
        help=f"path to s3s {TRAIT_RELPATH} (default: auto-locate from Cargo.toml pin)",
    )
    parser.add_argument("--output", type=pathlib.Path, help="write the markdown report to this path")
    args = parser.parse_args()

    root = project_root()
    git_url, rev = parse_s3s_pin(root / "Cargo.toml")
    trait_source = args.s3s_trait or locate_trait_source(git_url, rev)

    all_ops = trait_operations(trait_source)
    impl_ops = implemented_operations(root / ECFS_PATH)

    stale = sorted(impl_ops - all_ops.keys())
    implemented = sorted(op for op in all_ops if op in impl_ops)
    delegated = sorted(op for op, kind in all_ops.items() if op not in impl_ops and kind == "delegated")
    missing = sorted(op for op, kind in all_ops.items() if op not in impl_ops and kind == "not_implemented")
    covered = len(implemented) + len(delegated)
    pct = 100.0 * covered / len(all_ops) if all_ops else 0.0

    lines = [
        "# S3 API surface coverage",
        "",
        f"s3s trait revision: `{rev}` — {len(all_ops)} operations.",
        "",
        f"**{covered}/{len(all_ops)} operations covered ({pct:.0f}%)** — "
        f"{len(implemented)} overridden by RustFS, {len(delegated)} served by s3s default delegation.",
        "",
        "Operations below fall back to s3s defaults that return NotImplemented.",
        "",
        f"## Missing operations ({len(missing)})",
        "",
    ]
    lines += [f"- `{op}`" for op in missing] or ["_none_"]
    lines.append("")
    if delegated:
        lines += [f"## Covered via s3s default delegation ({len(delegated)})", ""]
        lines += [f"- `{op}`" for op in delegated]
        lines.append("")
    lines += [f"## Implemented operations ({len(implemented)})", ""]
    lines += [f"- `{op}`" for op in implemented]
    lines.append("")
    if stale:
        lines += [f"## Overridden but absent from the trait ({len(stale)})", ""]
        lines += [f"- `{op}`" for op in stale]
        lines += ["", "_These suggest the s3s pin moved; re-run after `cargo update`._", ""]

    report = "\n".join(lines)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"[INFO] Report written to {args.output}")

    print(
        f"[INFO] {covered}/{len(all_ops)} S3 operations covered ({pct:.0f}%): "
        f"{len(implemented)} overridden, {len(delegated)} delegated, {len(missing)} missing"
    )
    for op in missing:
        print(f"[MISSING] {op}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
