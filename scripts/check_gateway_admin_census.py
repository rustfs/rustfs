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
"""Compare the pinned gateway native inventory with the routes its collector finds in RustFS."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


NATIVE_INVENTORY = Path("crates/goldens/src/migration_inventory/rustfs_admin_routes_native.json")
COLLECTOR = Path("scripts/gen_rustfs_admin_route_inventory.py")
FORMAT = "rustfs-admin-route-inventory/2"


def route_keys(document, label):
    if not isinstance(document, dict) or document.get("format") != FORMAT:
        raise ValueError(f"{label} must have format {FORMAT}")
    rows = document.get("routes")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{label} has no route list")
    keys = set()
    for row in rows:
        if not isinstance(row, dict) or not all(isinstance(row.get(k), str) and row[k] for k in ("method", "path")):
            raise ValueError(f"{label} contains a route without a method and path")
        key = (row["method"], row["path"])
        if key in keys:
            raise ValueError(f"{label} repeats {key[0]} {key[1]}")
        keys.add(key)
    return keys


def compare(host, native):
    differences = []
    for label, routes in (("host only", host - native), ("native inventory only", native - host)):
        differences.extend(f"{label}: {method} {path}" for method, path in sorted(routes))
    if differences:
        raise ValueError("admin method/path census differs:\n" + "\n".join(differences))


def inventory(path, label):
    raw = path.read_bytes()
    keys = route_keys(json.loads(raw), label)
    print(f"{label}: {len(keys)} routes, sha256={hashlib.sha256(raw).hexdigest()}", flush=True)
    return keys


def check(gateway):
    native_path = gateway / NATIVE_INVENTORY
    if not native_path.is_file():
        raise ValueError(f"pinned gateway is missing the native v2 inventory: {NATIVE_INVENTORY}")
    native = inventory(native_path, "pinned gateway native inventory")
    with tempfile.TemporaryDirectory() as temp:
        actual = Path(temp) / "actual.json"
        subprocess.run(
            [sys.executable, "-B", gateway / COLLECTOR, "--rustfs", Path.cwd(), "--out", actual],
            check=True,
        )
        compare(inventory(actual, "RustFS collector output"), native)


class CensusTests(unittest.TestCase):
    def test_compare_checks_both_directions_even_at_equal_counts(self):
        host = {("GET", "/bucket"), ("POST", "/item")}
        compare(host, set(host))
        with self.assertRaisesRegex(ValueError, "host only: POST /item"):
            compare(host, {("GET", "/bucket")})
        with self.assertRaisesRegex(ValueError, "native inventory only: PUT /item"):
            compare(host, {("GET", "/bucket"), ("PUT", "/item")})

    def test_inventory_requires_distinct_native_rows(self):
        row = {"method": "GET", "path": "/bucket"}
        with self.assertRaisesRegex(ValueError, "repeats GET /bucket"):
            route_keys({"format": FORMAT, "routes": [row, row]}, "fixture")
        with self.assertRaisesRegex(ValueError, "format"):
            route_keys({"format": "rustfs-admin-route-inventory/1", "routes": [row]}, "fixture")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--gateway", type=Path, help="the gateway checkout at the RustFS Cargo pin")
    mode.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        unittest.main(argv=[sys.argv[0]])
    try:
        check(args.gateway.resolve())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(f"gateway admin census: {error}")
