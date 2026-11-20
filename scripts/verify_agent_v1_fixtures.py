#!/usr/bin/env python3
"""Verify Agent v1 fixture manifests and schema accept/reject vectors.

Reject vectors are validated against the schema declared by their base Accept
vector. A schema-accepted Reject vector is a failure unless the exact vector ID
is listed in an explicit semantic-gap allowlist. Unknown bases, stale allowlist
entries, malformed vectors, unsafe paths, and validation errors fail closed.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any

try:
    from jsonschema import Draft202012Validator, FormatChecker
except ImportError:  # pragma: no cover - exercised by CLI users without deps
    print("Missing dependency: python -m pip install -r protocol/agent/v1/requirements.txt", file=sys.stderr)
    raise SystemExit(2)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_relative_file(root: Path, name: str) -> Path | None:
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts:
        return None
    candidate = root.joinpath(*relative.parts).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate


def parse_manifest_line(raw: str, manifest: Path, line_no: int) -> tuple[str, str] | None:
    line = raw.strip()
    if not line:
        return None
    parts = line.split(None, 1)
    if len(parts) != 2 or len(parts[0]) != 64:
        raise ValueError(f"{manifest}:{line_no}: malformed manifest entry")
    checksum, filename = parts[0].lower(), parts[1].strip()
    if any(char not in "0123456789abcdef" for char in checksum):
        raise ValueError(f"{manifest}:{line_no}: invalid SHA-256 digest")
    return checksum, filename


def verify_manifests(root: Path) -> bool:
    ok = bad = 0
    nested = sorted(root.glob("*/MANIFEST.sha256"))
    if not nested:
        print(f"NO MANIFESTS under {root}")
        return False

    for manifest in nested:
        for line_no, raw in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
            try:
                parsed = parse_manifest_line(raw, manifest, line_no)
                if parsed is None:
                    continue
                expected, name = parsed
                target = safe_relative_file(manifest.parent, name.lstrip("*"))
                if target is None or not target.is_file():
                    raise ValueError(f"{manifest.parent / name}: missing or unsafe path")
                actual = sha256_file(target)
                if actual != expected:
                    raise ValueError(f"{target}: digest mismatch (expected {expected}, got {actual})")
                ok += 1
            except (OSError, ValueError) as exc:
                print(f"FAIL {exc}")
                bad += 1

    top = root / "manifest.sha256"
    declared: list[str] = []
    if not top.is_file():
        print(f"FAIL {top}: required top-level manifest is missing")
        bad += 1
    else:
        for line_no, raw in enumerate(top.read_text(encoding="utf-8").splitlines(), 1):
            try:
                parsed = parse_manifest_line(raw, top, line_no)
                if parsed is None:
                    continue
                expected, set_name = parsed
                if "/" in set_name or "\\" in set_name or set_name in {".", ".."}:
                    raise ValueError(f"{top}:{line_no}: unsafe fixture-set name {set_name!r}")
                if set_name in declared:
                    raise ValueError(f"{top}:{line_no}: duplicate fixture-set entry {set_name!r}")
                declared.append(set_name)
                nested_manifest = root / set_name / "MANIFEST.sha256"
                if not nested_manifest.is_file():
                    raise ValueError(f"{nested_manifest}: missing fixture-set manifest")
                actual = sha256_file(nested_manifest)
                if actual != expected:
                    raise ValueError(f"{nested_manifest}: digest mismatch (expected {expected}, got {actual})")
                ok += 1
            except (OSError, ValueError) as exc:
                print(f"FAIL {exc}")
                bad += 1

        # Every fixture set on disk must be listed exactly once at the root,
        # and every root entry must resolve to a fixture set. An empty root
        # manifest used to pass as long as the nested manifests were valid.
        discovered = {manifest.parent.name for manifest in nested}
        declared_set = set(declared)
        if not declared_set:
            print(f"FAIL {top}: top-level manifest declares no fixture sets")
            bad += 1
        for name in sorted(discovered - declared_set):
            print(f"FAIL {root / name}: fixture set on disk is not listed in {top.name}")
            bad += 1
        for name in sorted(declared_set - discovered):
            print(f"FAIL {root / name}: fixture set listed in {top.name} has no nested manifest")
            bad += 1

    print(f"manifest entries ok={ok} bad={bad}")
    return bad == 0


def pointer_tokens(pointer: str) -> list[str]:
    """Decode an RFC 6901 JSON Pointer, rejecting invalid escape sequences."""
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise ValueError(f"JSON Pointer must start with '/': {pointer!r}")
    tokens: list[str] = []
    for raw in pointer[1:].split("/"):
        token: list[str] = []
        index = 0
        while index < len(raw):
            char = raw[index]
            if char != "~":
                token.append(char)
                index += 1
                continue
            if index + 1 >= len(raw) or raw[index + 1] not in "01":
                raise ValueError(f"invalid JSON Pointer escape in token {raw!r}")
            token.append("/" if raw[index + 1] == "1" else "~")
            index += 2
        tokens.append("".join(token))
    return tokens


_ARRAY_INDEX = re.compile(r"^(?:0|[1-9][0-9]*)$")


def array_index(token: str) -> int:
    """Return a JSON Pointer array index for an existing element.

    RFC 6901 array tokens are unsigned decimal integers without leading zeros.
    Python's int() also accepts "-1" and "01", which would silently address a
    different element than the vector names.
    """
    if not _ARRAY_INDEX.fullmatch(token):
        raise ValueError(f"invalid JSON Pointer array index {token!r}")
    return int(token)


def apply_mutation(value: Any, vector: dict[str, Any]) -> Any:
    if not isinstance(vector, dict):
        raise ValueError("reject vector must be an object")
    document = copy.deepcopy(value)
    tokens = pointer_tokens(vector.get("path"))
    if not tokens:
        raise ValueError("mutation of document root is not supported")
    cursor = document
    for token in tokens[:-1]:
        if isinstance(cursor, list):
            cursor = cursor[array_index(token)]
        elif isinstance(cursor, dict):
            cursor = cursor[token]
        else:
            raise TypeError(f"cannot traverse JSON Pointer token {token!r}")
    last = tokens[-1]
    if vector.get("remove") is True:
        if isinstance(cursor, list):
            del cursor[array_index(last)]
        elif isinstance(cursor, dict):
            if last not in cursor:
                raise KeyError(f"cannot remove missing property {last!r}")
            del cursor[last]
        else:
            raise TypeError("JSON Pointer parent is not an object or array")
    else:
        if "replacement" not in vector:
            raise KeyError(f"reject vector {vector.get('id')!r} lacks replacement")
        if isinstance(cursor, list):
            cursor[array_index(last)] = vector["replacement"]
        elif isinstance(cursor, dict):
            cursor[last] = vector["replacement"]
        else:
            raise TypeError("JSON Pointer parent is not an object or array")
    return document


def load_validator(schemas_dir: Path, name: str, cache: dict[str, Draft202012Validator]) -> Draft202012Validator:
    if not isinstance(name, str) or Path(name).name != name or not name.endswith(".schema.json"):
        raise ValueError(f"unsafe/invalid schema filename: {name!r}")
    if name not in cache:
        schema_path = schemas_dir / name
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        cache[name] = Draft202012Validator(schema, format_checker=FormatChecker())
    return cache[name]


def load_allowlist(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("allowlist root must be an object")
    entries = data.get("allow_schema_gaps", [])
    if not isinstance(entries, list):
        raise ValueError("allow_schema_gaps must be an array")
    result: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError(f"invalid allowlist entry: {entry!r}")
        ident, reason = entry.get("id"), entry.get("reason")
        if not isinstance(ident, str) or not ident or not isinstance(reason, str) or not reason.strip():
            raise ValueError(f"invalid allowlist entry: {entry!r}")
        if ident in result:
            raise ValueError(f"duplicate allowlist id: {ident}")
        result[ident] = reason.strip()
    return result


def _load_vectors(path: Path, label: str) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    vectors = data.get("vectors")
    if not isinstance(vectors, list):
        raise ValueError(f"{label} file must contain a vectors array: {path}")
    return vectors


def verify_vectors(
    schemas_dir: Path,
    accept_file: Path,
    reject_file: Path | None,
    allowlist_file: Path | None,
) -> bool:
    validators: dict[str, Draft202012Validator] = {}
    bases: dict[str, tuple[str, Any]] = {}
    seen_accepts: set[str] = set()
    errors = 0

    accept_vectors = _load_vectors(accept_file, "accept")
    reject_vectors = _load_vectors(reject_file, "reject") if reject_file else []

    print("-- accept --")
    for vector in accept_vectors:
        if not isinstance(vector, dict):
            print(f"  FAIL accept vector must be an object: {vector!r}")
            errors += 1
            continue
        ident = vector.get("id")
        schema_name = vector.get("schema")
        if not isinstance(ident, str) or not ident or ident in seen_accepts:
            print(f"  FAIL duplicate/missing accept id: {ident!r}")
            errors += 1
            continue
        seen_accepts.add(ident)
        try:
            validator = load_validator(schemas_dir, schema_name, validators)
            value = vector["value"]
            validation_errors = list(validator.iter_errors(value))
        except Exception as exc:  # malformed vector/schema must fail closed
            print(f"  FAIL {ident}: cannot validate: {exc}")
            errors += 1
            continue
        bases[ident] = (schema_name, value)
        if validation_errors:
            detail = "; ".join(f"{list(error.path)}: {error.message[:100]}" for error in validation_errors[:3])
            print(f"  FAIL {ident} [{schema_name}] -> {detail}")
            errors += 1
        else:
            print(f"  PASS {ident} [{schema_name}]")

    allowlist = load_allowlist(allowlist_file)
    seen_allowed: set[str] = set()
    seen_rejects: set[str] = set()
    print("-- reject --")
    for vector in reject_vectors:
        if not isinstance(vector, dict):
            print(f"  FAIL reject vector must be an object: {vector!r}")
            errors += 1
            continue
        ident = vector.get("id")
        base_id = vector.get("base")
        if not isinstance(ident, str) or not ident or ident in seen_rejects:
            print(f"  FAIL duplicate/missing reject id: {ident!r}")
            errors += 1
            continue
        seen_rejects.add(ident)
        if not isinstance(base_id, str) or base_id not in bases:
            print(f"  FAIL {ident}: unknown base {base_id!r}")
            errors += 1
            continue
        schema_name, base_value = bases[base_id]
        try:
            validator = load_validator(schemas_dir, schema_name, validators)
            mutated = apply_mutation(base_value, vector)
            validation_errors = list(validator.iter_errors(mutated))
        except Exception as exc:
            print(f"  FAIL {ident}: cannot apply/validate: {exc}")
            errors += 1
            continue

        if validation_errors:
            if ident in allowlist:
                print(f"  FAIL {ident}: stale allowlist entry; schema now rejects this vector")
                errors += 1
            else:
                print(f"  PASS {ident} [{schema_name}]")
        elif ident in allowlist:
            seen_allowed.add(ident)
            print(f"  EXPECTED-SEMANTIC-GAP {ident}: {allowlist[ident]}")
        else:
            print(f"  FAIL {ident}: schema accepts a reject vector (unapproved GAP)")
            errors += 1

    for ident in sorted(set(allowlist) - seen_allowed):
        print(f"  FAIL unused allowlist entry: {ident}")
        errors += 1

    print(
        "vector validation errors={}; accepted vectors={}; reject vectors={}; approved semantic gaps={}".format(
            errors, len(accept_vectors), len(reject_vectors), len(seen_allowed)
        )
    )
    return errors == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, help="fixture root for MANIFEST.sha256 verification")
    parser.add_argument("--schemas", type=Path, help="directory containing JSON Schemas")
    parser.add_argument("--accept", type=Path, help="accept vector JSON")
    parser.add_argument("--reject", type=Path, help="reject vector JSON")
    parser.add_argument("--allow-schema-gaps", type=Path, help="explicit allowlist for runtime-only semantic rejects")
    args = parser.parse_args()

    if not (args.fixtures or (args.schemas and args.accept)):
        parser.error("provide --fixtures and/or --schemas + --accept")
    if args.schemas and not args.accept:
        parser.error("--schemas requires --accept")
    if args.reject and not (args.schemas and args.accept):
        parser.error("--reject requires --schemas and --accept")
    if args.allow_schema_gaps and not (args.schemas and args.accept):
        parser.error("--allow-schema-gaps requires --schemas and --accept")

    ok = True
    try:
        if args.fixtures:
            ok = verify_manifests(args.fixtures) and ok
        if args.schemas and args.accept:
            ok = verify_vectors(args.schemas, args.accept, args.reject, args.allow_schema_gaps) and ok
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
