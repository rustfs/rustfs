#!/usr/bin/env python3
"""Reject GNU artifacts whose ELF version needs exceed the package ABI floor."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path


def version(value: str) -> tuple[int, ...]:
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", value):
        raise ValueError(f"invalid glibc version: {value}")
    parts = tuple(map(int, value.split(".")))
    return parts + (0,) * max(0, 3 - len(parts))


def check(binary: Path, baseline: str) -> str:
    floor = version(baseline)
    result = subprocess.run(
        ["readelf", "--version-info", "--wide", str(binary)],
        check=True, capture_output=True, text=True, env={**os.environ, "LC_ALL": "C"},
    )
    # Only loader requirements matter. Definitions and arbitrary strings in
    # the executable do not describe the libc that it needs to run.
    needs = result.stdout.partition("Version needs section")[2]
    symbols = re.findall(r"\bName:\s+(GLIBC_\S+)", needs)
    if not symbols:
        raise ValueError(f"{binary}: no GLIBC version needs found in GNU artifact")
    highest = "GLIBC_0.0"
    for symbol in symbols:
        required = version(symbol[len("GLIBC_"):])
        if required > floor:
            raise ValueError(f"{binary}: requires {symbol}, above GLIBC_{baseline}")
        if required > version(highest[len("GLIBC_"):]):
            highest = symbol
    return highest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline")
    parser.add_argument("binaries", type=Path, nargs="+")
    args = parser.parse_args()
    try:
        for binary in args.binaries:
            print(f"{binary}: highest required symbol {check(binary, args.baseline)} <= GLIBC_{args.baseline}")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
