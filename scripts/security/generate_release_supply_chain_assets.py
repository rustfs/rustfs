#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import tomllib
import uuid
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate release SBOM and provenance files for RustFS assets.",
    )
    parser.add_argument("--asset-dir", required=True, help="Directory containing release assets.")
    parser.add_argument("--version", required=True, help="Release version.")
    parser.add_argument("--tag", default="", help="Release tag.")
    parser.add_argument("--build-type", default="", help="Build type, such as release or prerelease.")
    parser.add_argument("--repository", default=os.getenv("GITHUB_REPOSITORY", "rustfs/rustfs"))
    parser.add_argument("--ref", default=os.getenv("GITHUB_REF", ""))
    parser.add_argument("--sha", default=os.getenv("GITHUB_SHA", ""))
    parser.add_argument("--run-id", default=os.getenv("GITHUB_RUN_ID", ""))
    parser.add_argument("--run-attempt", default=os.getenv("GITHUB_RUN_ATTEMPT", ""))
    parser.add_argument("--cargo-lock", default="Cargo.lock", help="Cargo.lock path.")
    parser.add_argument("--metadata-file", help="Use an existing cargo metadata JSON file.")
    return parser.parse_args()


def load_packages(metadata_file: str | None, cargo_lock: str) -> list[dict]:
    if metadata_file:
        with open(metadata_file, "r", encoding="utf-8") as handle:
            return json.load(handle).get("packages", [])

    with open(cargo_lock, "rb") as handle:
        return tomllib.load(handle).get("package", [])


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_version(version: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", version).strip("-") or "unknown"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_type(package: dict) -> str:
    for target in package.get("targets", []):
        if "bin" in target.get("kind", []) and target.get("name") == "rustfs":
            return "application"
    return "library"


def component_from_package(package: dict) -> dict:
    component = {
        "type": package_type(package),
        "name": package["name"],
        "version": package["version"],
        "purl": f"pkg:cargo/{package['name']}@{package['version']}",
    }

    license_expr = package.get("license")
    if license_expr:
        component["licenses"] = [{"expression": license_expr}]

    source = package.get("source")
    if source:
        component["externalReferences"] = [{"type": "distribution", "url": source}]

    return component


def build_sbom(packages: list[dict], args: argparse.Namespace, timestamp: str) -> dict:
    components = [component_from_package(package) for package in packages]
    components.sort(key=lambda item: (item["name"], item.get("version", "")))

    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": timestamp,
            "tools": {
                "components": [
                    {
                        "type": "application",
                        "name": "Cargo.lock",
                        "version": "version 4",
                    },
                    {
                        "type": "application",
                        "name": "generate_release_supply_chain_assets.py",
                        "version": "1",
                    },
                ]
            },
            "component": {
                "type": "application",
                "name": "rustfs",
                "version": args.version,
                "purl": f"pkg:cargo/rustfs@{args.version.lstrip('v')}",
            },
        },
        "components": components,
    }


def build_provenance(asset_dir: Path, args: argparse.Namespace, timestamp: str) -> dict:
    subjects = []
    for asset in sorted(asset_dir.glob("*.zip")):
        subjects.append(
            {
                "name": asset.name,
                "digest": {"sha256": sha256_file(asset)},
            }
        )

    if not subjects:
        raise RuntimeError(f"no .zip release assets found in {asset_dir}")

    workflow_ref = f"{args.repository}/.github/workflows/build.yml"
    invocation = args.run_id
    if args.run_attempt:
        invocation = f"{invocation}/attempts/{args.run_attempt}" if invocation else args.run_attempt

    return {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": subjects,
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": "https://github.com/ActionsWorkflow@v1",
                "externalParameters": {
                    "workflow": workflow_ref,
                    "ref": args.ref,
                    "tag": args.tag,
                    "version": args.version,
                    "buildType": args.build_type,
                },
                "internalParameters": {
                    "sha": args.sha,
                    "repository": args.repository,
                },
                "resolvedDependencies": [
                    {
                        "uri": f"git+https://github.com/{args.repository}.git",
                        "digest": {"gitCommit": args.sha},
                    }
                ],
            },
            "runDetails": {
                "builder": {"id": f"https://github.com/{args.repository}/actions"},
                "metadata": {
                    "invocationId": invocation,
                    "startedOn": timestamp,
                    "finishedOn": timestamp,
                },
            },
        },
    }


def write_json(path: Path, payload: dict) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> int:
    args = parse_args()
    asset_dir = Path(args.asset_dir)
    if not asset_dir.is_dir():
        raise RuntimeError(f"asset directory does not exist: {asset_dir}")

    timestamp = utc_now()
    packages = load_packages(args.metadata_file, args.cargo_lock)
    version = safe_version(args.version)

    sbom_path = asset_dir / f"rustfs-{version}.sbom.cdx.json"
    provenance_path = asset_dir / f"rustfs-{version}.provenance.json"

    write_json(sbom_path, build_sbom(packages, args, timestamp))
    write_json(provenance_path, build_provenance(asset_dir, args, timestamp))

    print(f"Generated {sbom_path}")
    print(f"Generated {provenance_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
