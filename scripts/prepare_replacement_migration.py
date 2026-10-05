#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Prepare a digest-bound maintenance approval for legacy replacement intents."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import uuid

INTENT_SUFFIX = "_ahm_replacement_intent.json"
APPROVAL_SUFFIX = "_legacy_replacement_approval.json"


def canonical_uuid(value):
    parsed = str(uuid.UUID(value))
    if value != parsed:
        raise ValueError("generation IDs must be canonical UUIDs")
    return parsed


def metadata_directory(root):
    root = Path(root).resolve(strict=True)
    path = root
    for part in [".rustfs.sys", "buckets", "ahm-replacement"]:
        path = path / part
        if path.is_symlink() or not path.is_dir():
            raise ValueError(f"expected a real metadata directory: {path}")
    return path


def prepare(anchor, source_ids, target_roots, successor):
    directory = metadata_directory(anchor)
    successor = canonical_uuid(successor)
    if not source_ids or len(source_ids) > 32 or len(set(source_ids)) != len(source_ids):
        raise ValueError("specify between 1 and 32 distinct source generations")
    sources, states = [], []
    for task_id in source_ids:
        canonical_uuid(task_id)
        if task_id == successor:
            raise ValueError("the successor must be a fresh generation")
        path = directory / (task_id + INTENT_SUFFIX)
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"expected an isolated legacy intent: {path}")
        raw = path.read_bytes()
        state = json.loads(raw)
        if (
            state.get("schema_version") not in (5, 6)
            or state.get("task_id") != task_id
            or state.get("replacement_generation") != task_id
            or not state.get("replacement_targets")
        ):
            raise ValueError(f"source is not a schema 5/6 replacement intent: {task_id}")
        sources.append({"task_id": task_id, "sha256": list(hashlib.sha256(raw).digest())})
        states.append(state)
    first = states[0]
    targets = first["replacement_targets"]
    if targets != sorted(set(targets)) or set(target_roots) != set(targets):
        raise ValueError("provide exactly one --target endpoint=directory for every source target slot")
    if any(state["replacement_targets"] != targets or state["set_disk_id"] != first["set_disk_id"] for state in states):
        raise ValueError("source generations must have exactly the same set and target slots")
    owners = {f'{first["set_disk_id"]}:{source}' for source in source_ids}
    markers = []
    for endpoint in targets:
        root = Path(target_roots[endpoint]).resolve(strict=True)
        metadata = root / ".rustfs.sys"
        marker = metadata / "healing.bin"
        if metadata.is_symlink() or marker.is_symlink():
            raise ValueError("target metadata and marker must not be symlinks")
        value = marker.read_text(encoding="utf-8") if marker.exists() else None
        if value is not None and value not in owners:
            raise ValueError(f"target has an unapproved marker owner: {endpoint}")
        markers.append(value)
    return directory / (successor + APPROVAL_SUFFIX), {
        "schema_version": 1,
        "successor": successor,
        "set_disk_id": first["set_disk_id"],
        "targets": targets,
        "expected_markers": markers,
        "sources": sources,
        "maintenance_assertion": "all-writers-stopped-before-upgrade",
    }


def publish(path, approval):
    # Link a fully synced temporary file without replacing an existing approval.
    temporary = path.with_name(f".{uuid.uuid4()}.migration.tmp")
    try:
        with temporary.open("xb") as output:
            os.chmod(temporary, 0o600)
            output.write((json.dumps(approval, indent=2) + "\n").encode())
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anchor", required=True, help="survivor disk root containing the isolated intents")
    parser.add_argument("--source", action="append", required=True, help="source generation UUID; repeat for every orphan")
    parser.add_argument("--target", action="append", required=True, help="endpoint=mounted-directory; repeat for every target")
    parser.add_argument("--successor", required=True, help="fresh UUID reserved for the migration")
    parser.add_argument("--write", action="store_true", help="publish the approval; default only prints the proposed JSON")
    parser.add_argument("--stopped-all-writers", action="store_true", help="assert all old binaries and other writers have stopped")
    args = parser.parse_args()
    try:
        pairs = [value.split("=", 1) for value in args.target]
        if any(len(pair) != 2 or not all(pair) for pair in pairs):
            raise ValueError("--target must be endpoint=mounted-directory")
        targets = dict(pairs)
        if len(targets) != len(pairs):
            raise ValueError("duplicate target endpoint")
        path, approval = prepare(args.anchor, args.source, targets, args.successor)
        if args.write:
            if not args.stopped_all_writers:
                raise ValueError("--write requires --stopped-all-writers; the new lease cannot fence an old binary")
            publish(path, approval)
            print(path)
        else:
            print(json.dumps(approval, indent=2))
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
