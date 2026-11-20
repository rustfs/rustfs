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

Compares the reference S3 trait with the operation rows installed by the RustFS gateway.
Only registered operations count toward coverage.

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

HANDLERS = pathlib.Path("rustfs/src/server/gateway_handlers")
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


def trait_operations(trait_source: pathlib.Path) -> set[str]:
    source = trait_source.read_text(encoding="utf-8")
    names = set(re.findall(r"async fn (\w+)", source))
    if not names:
        raise ValueError(f"no S3 operations found in {trait_source}")
    return names


def registered_items(source: pathlib.Path, name: str) -> list[str]:
    text = source.read_text(encoding="utf-8")
    matches = re.findall(rf"\b{name}: [^=;]+?=\s*&\[([^;]*?)\];", text)
    if len(matches) != 1:
        raise ValueError(f"expected one {name} registration table in {source}")
    entries = re.sub(r"//[^\n]*", "", matches[0]).strip().rstrip(",")
    return [item.strip() for item in entries.split(",")] if entries else []


def implemented_operations(root: pathlib.Path) -> set[str]:
    directory = root / HANDLERS
    operations = set()
    for family in registered_items(directory / "mod.rs", "OPERATIONS"):
        module = re.fullmatch(r"(\w+)::ROWS", family)
        if not module:
            raise ValueError(f"unknown operation family: {family}")
        for item in registered_items(directory / f"{module[1]}.rs", "ROWS"):
            if direct := re.fullmatch(r"row::<dto::(\w+)>\(\)", item):
                operation = direct[1]
            elif external := re.fullmatch(r"super::(\w+)::(\w+)", item):
                source = (directory / f"{external[1]}.rs").read_text(encoding="utf-8")
                names = re.findall(rf"\bconst {external[2]}: OpRow = OpRow \{{\s*name: dto::(\w+)::NAME,", source)
                if len(names) != 1:
                    raise ValueError(f"missing or ambiguous operation name for {item}")
                operation = names[0]
            else:
                raise ValueError(f"unknown registration in {family}: {item}")
            name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", operation).lower()
            if name in operations:
                raise ValueError(f"duplicate registered operation: {name}")
            operations.add(name)
    return operations


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
    impl_ops = implemented_operations(root)

    stale = sorted(impl_ops - all_ops)
    implemented = sorted(op for op in all_ops if op in impl_ops)
    missing = sorted(all_ops - impl_ops)
    covered = len(implemented)
    pct = 100.0 * covered / len(all_ops) if all_ops else 0.0

    lines = [
        "# S3 API surface coverage",
        "",
        f"s3s trait revision: `{rev}` — {len(all_ops)} operations.",
        "",
        f"**{covered}/{len(all_ops)} reference operations registered ({pct:.0f}%)**",
        "",
        f"## Missing operations ({len(missing)})",
        "",
    ]
    lines += [f"- `{op}`" for op in missing] or ["_none_"]
    lines.append("")
    lines += [f"## Implemented operations ({len(implemented)})", ""]
    lines += [f"- `{op}`" for op in implemented]
    lines.append("")
    if stale:
        lines += [f"## Registered operations outside the reference trait ({len(stale)})", ""]
        lines += [f"- `{op}`" for op in stale]
        lines.append("")

    report = "\n".join(lines)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"[INFO] Report written to {args.output}")

    print(
        f"[INFO] {covered}/{len(all_ops)} S3 operations covered ({pct:.0f}%): "
        f"{len(implemented)} registered, {len(missing)} missing"
    )
    for op in missing:
        print(f"[MISSING] {op}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
