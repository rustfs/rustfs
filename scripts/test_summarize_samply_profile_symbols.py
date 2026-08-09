#!/usr/bin/env python3

from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def write_json_gz(path: Path, value: object) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as target:
        json.dump(value, target)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        profile_path = tmp_path / "profile.json.gz"
        symbols_path = tmp_path / "profile.syms.json"

        profile = {
            "libs": [
                {"debugName": "rustfs", "codeId": "abc"},
                {"debugName": "libc.so.6", "codeId": "def"},
            ],
            "threads": [
                {
                    "name": "rustfs-worker",
                    "stringArray": ["0x1004", "0x2004"],
                    "resourceTable": {"length": 2, "lib": [0, 1], "name": [0, 1], "host": [None, None], "type": [1, 1]},
                    "funcTable": {"length": 2, "name": [0, 1], "resource": [0, 1]},
                    "frameTable": {"length": 2, "address": [0x1004, 0x2004], "func": [0, 1]},
                    "stackTable": {"length": 2, "prefix": [None, 0], "frame": [0, 1]},
                    "samples": {"stack": [0, 1]},
                },
                {
                    "name": "tokio-runtime-worker",
                    "stringArray": ["0x1008"],
                    "resourceTable": {"length": 1, "lib": [0], "name": [0], "host": [None], "type": [1]},
                    "funcTable": {"length": 1, "name": [0], "resource": [0]},
                    "frameTable": {"length": 1, "address": [0x1008], "func": [0]},
                    "stackTable": {"length": 1, "prefix": [None], "frame": [0]},
                    "samples": {"stack": [0]},
                },
            ],
        }
        symbols = {
            "string_table": [
                "rustfs_ecstore::set_disk::read_all_data",
                "libc::writev",
                "rustfs_ecstore::set_disk::read_all_inline_data",
            ],
            "data": {
                "rustfs": {
                    "code_id": "abc",
                    "symbol_table": [
                        {"rva": 0x1000, "size": 0x10, "symbol": 0},
                    ],
                },
                "libc.so.6": {
                    "code_id": "def",
                    "symbol_table": [
                        {"rva": 0x1000, "size": 0x10, "symbol": 2},
                        {"rva": 0x2000, "size": 0x10, "symbol": 1},
                    ],
                },
            },
        }
        write_json_gz(profile_path, profile)
        symbols_path.write_text(json.dumps(symbols), encoding="utf-8")

        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "summarize_samply_profile_symbols.py"),
                "--profile",
                str(profile_path),
                "--symbols",
                str(symbols_path),
                "--thread",
                "rustfs-worker",
                "--format",
                "json",
                "--limit",
                "5",
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        summary = json.loads(result.stdout)

        assert summary["total_samples"] == 2
        assert summary["resolved_samples"] == 2
        assert summary["unresolved_samples"] == 0
        assert summary["threads"] == {"rustfs-worker": 2}
        leaf_names = [row["function"] for row in summary["leaf"]]
        assert "rustfs_ecstore::set_disk::read_all_data [rustfs]" in leaf_names
        assert "libc::writev [libc.so.6]" in leaf_names
        inclusive_names = [row["function"] for row in summary["inclusive"]]
        assert "rustfs_ecstore::set_disk::read_all_data [rustfs]" in inclusive_names

    print("test_summarize_samply_profile_symbols: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
