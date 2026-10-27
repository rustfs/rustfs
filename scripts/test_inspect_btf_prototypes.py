#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Prototype/CLI contracts plus an optional independent bpftool reference."""

import hashlib
import json
import os
import re
import selectors
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import inspect_btf_prototypes as reader

HERE = Path(__file__).parent

STRINGS = b"\0demo\0value\0u64\0Root\0Alias\0"


def typ(kind, third, name="", count=0, kflag=False):
    offset = STRINGS.index(name.encode() + b"\0") if name else 0
    return struct.pack("<III", offset, (kind << 24) | count | (int(kflag) << 31), third)


def blob(types):
    raw = b"".join(types)
    header = struct.pack("<HBBIIIII", 0xEB9F, 1, 0, 24, 0, len(raw), len(raw), len(STRINGS))
    return header + raw + STRINGS


def integer():
    return typ(1, 8, "u64") + struct.pack("<I", 64)


def function(types, result, args):
    proto_id = len(types) + 1
    param_name = STRINGS.index(b"value\0")
    params = b"".join(struct.pack("<II", param_name, arg) for arg in args)
    return blob(types + [typ(13, result, count=len(args)) + params, typ(12, proto_id, "demo")])


def structure(types, field, size=8):
    member = struct.pack("<III", STRINGS.index(b"value\0"), field, 0)
    return blob(types + [typ(4, size, "Root", count=1) + member])


class VoidBoundaryTests(unittest.TestCase):
    def test_scalar_and_existing_pointer_declarations_are_retained(self):
        result = reader._BTF(function([integer(), typ(2, 1)], 1, [2])).prototype("demo")
        self.assertEqual(result["prototype"]["size_or_type"], 1)
        self.assertEqual(result["args"][0]["type_ID"], 2)
        self.assertEqual(result["args"][0]["type"]["kind"], 2)
        self.assertEqual(result["args"][0]["pointee"]["ID"], 1)
        self.assertEqual(result["return"]["size_or_type"], 8)

    def test_void_pointer_argument_has_explicit_void_pointee(self):
        raw = function([typ(2, 0)], 0, [1])
        result = reader._BTF(raw).prototype("demo")
        arg = result["args"][0]
        self.assertEqual(arg["type"]["kind"], 2)
        self.assertEqual(arg["type"]["size_or_type"], 0)
        self.assertEqual(arg["pointee"], {"ID": 0, "name": "void", "kind": 0})
        self.assertEqual(arg["pointee_chain"], [])
        self.assertEqual(result["return"], {"ID": 0, "name": "void", "kind": 0})

    def test_qualified_void_pointer_keeps_declared_chain(self):
        raw = function([typ(10, 0), typ(2, 1)], 0, [2])
        arg = reader._BTF(raw).prototype("demo")["args"][0]
        self.assertEqual(arg["pointee"]["kind"], 0)
        self.assertEqual([t["ID"] for t in arg["pointee_chain"]], [1])
        self.assertEqual(arg["pointee_chain"][0]["kind"], 10)

    def test_qualified_void_return_is_explicit(self):
        raw = function([typ(8, 0, "Alias")], 1, [])
        result = reader._BTF(raw).prototype("demo")
        self.assertEqual(result["return"], {"ID": 0, "name": "void", "kind": 0})
        self.assertEqual([t["ID"] for t in result["return_chain"]], [1])

    def test_resolved_missing_nonzero_ids_are_not_void(self):
        fixtures = [
            function([], 0, [999]),
            function([typ(2, 999)], 0, [1]),
            function([typ(8, 999, "Alias"), typ(2, 1)], 0, [2]),
            function([], 999, []),
            function([typ(2, 999)], 1, []),
            function([typ(2, 999), typ(2, 1)], 0, [2]),
        ]
        for raw in fixtures:
            with self.subTest(raw=raw.hex()):
                with self.assertRaisesRegex(ValueError, "actual_nonvoid_type_ID"):
                    reader._BTF(raw).prototype("demo")

    def test_named_direct_void_parameter_remains_unsupported(self):
        with self.assertRaisesRegex(ValueError, "actual_nonvoid_type_ID"):
            reader._BTF(function([], 0, [0])).prototype("demo")

    def test_void_is_not_a_stored_type_record(self):
        with self.assertRaisesRegex(ValueError, "unknown_BTF_kind_not_guessed"):
            reader._BTF(blob([typ(0, 0)]))

    def test_qualifier_cycle_is_rejected(self):
        raw = function([typ(10, 2), typ(8, 1, "Alias")], 0, [1])
        with self.assertRaisesRegex(ValueError, "acyclic_bounded_qualifier_chain"):
            reader._BTF(raw).prototype("demo")

    def test_qualifier_record_limit_is_preserved(self):
        for count, accepted in [(31, True), (32, False)]:
            types = [integer()]
            for _ in range(count):
                types.append(typ(10, len(types)))
            raw = function(types, 0, [len(types)])
            with self.subTest(scalar_qualifiers=count):
                if accepted:
                    self.assertEqual(reader._BTF(raw).prototype("demo")["args"][0]["type"]["kind"], 1)
                else:
                    with self.assertRaisesRegex(ValueError, "acyclic_bounded_qualifier_chain"):
                        reader._BTF(raw).prototype("demo")
        for count, accepted in [(32, True), (33, False)]:
            types = [typ(10, index) for index in range(count)]
            types.append(typ(2, count))
            raw = function(types, 0, [len(types)])
            with self.subTest(void_qualifiers=count):
                if accepted:
                    result = reader._BTF(raw).prototype("demo")["args"][0]
                    self.assertEqual(len(result["pointee_chain"]), 32)
                else:
                    with self.assertRaisesRegex(ValueError, "acyclic_bounded_qualifier_chain"):
                        reader._BTF(raw).prototype("demo")

    def test_void_descriptor_does_not_infer_layout_or_record(self):
        result = reader._BTF(function([typ(2, 0)], 0, [1])).prototype("demo")
        void = result["args"][0]["pointee"]
        self.assertEqual(set(void), {"ID", "name", "kind"})
        self.assertNotIn("offset", void)
        self.assertNotIn("size_or_type", void)
        self.assertNotIn("pointer_width", result["args"][0])

    def test_malformed_resolved_pointer_headers_are_rejected(self):
        fixtures = [typ(2, 0, count=1), typ(2, 0, kflag=True), typ(2, 0, "value")]
        for pointer in fixtures:
            for args, ret in [([1], 0), ([], 1)]:
                with self.subTest(pointer=pointer.hex(), args=args):
                    with self.assertRaisesRegex(ValueError, "actual_pointer_header"):
                        reader._BTF(function([pointer], ret, args)).prototype("demo")

    def test_malformed_resolved_qualifier_headers_are_rejected(self):
        for qualifier in [typ(10, 0, count=1), typ(10, 0, kflag=True), typ(10, 0, "value")]:
            raw = function([qualifier, typ(2, 1)], 0, [2])
            with self.subTest(qualifier=qualifier.hex()):
                with self.assertRaisesRegex(ValueError, "actual_qualifier_header|unnamed_CVR_qualifier"):
                    reader._BTF(raw).prototype("demo")

    def test_incomplete_source_is_rejected(self):
        raw = function([integer()], 1, [1])
        for truncated in [raw[:23], raw[:-1]]:
            with self.subTest(length=len(truncated)):
                with self.assertRaises(ValueError):
                    reader._BTF(truncated)

    def test_default_record_and_unwrap_interfaces_stay_nonvoid(self):
        btf = reader._BTF(function([], 0, []))
        for operation in [lambda: btf.type(0), lambda: btf.unwrap(0), lambda: btf.unwrap(False), lambda: btf.unwrap(999)]:
            with self.assertRaisesRegex(ValueError, "actual_nonvoid_type_ID"):
                operation()


class CliTests(unittest.TestCase):
    def run_cli(self, data, *names):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.btf"
            path.write_bytes(data)
            command = [sys.executable, "-B", str(HERE / "inspect_btf_prototypes.py"), "--btf", str(path)]
            for name in names:
                command.extend(["--function", name])
            result = subprocess.run(command, capture_output=True, timeout=10)
            self.assertEqual(path.read_bytes(), data)
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["input.btf"])
            return result

    def test_complete_json_has_observed_source_digest(self):
        raw = function([typ(2, 0)], 0, [1])
        result = self.run_cli(raw, "demo")
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["schema"], 1)
        self.assertEqual(value["source"]["bytes"], len(raw))
        self.assertEqual(value["source"]["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(value["prototypes"][0]["args"][0]["pointee"]["ID"], 0)

    def test_multiple_functions_keep_complete_request_order(self):
        raw = blob([typ(13, 0), typ(12, 1, "demo"), typ(12, 1, "value")])
        result = self.run_cli(raw, "value", "demo")
        self.assertEqual(result.returncode, 0, result.stderr)
        document = json.loads(result.stdout)
        self.assertEqual([p["function"]["name"] for p in document["prototypes"]], ["value", "demo"])

    def test_later_missing_function_emits_no_partial_report(self):
        result = self.run_cli(function([], 0, []), "demo", "missing")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertIn(b"unique_actual_named_type_or_function", result.stderr)

    def test_corrupt_input_is_an_error_not_an_empty_success(self):
        result = self.run_cli(b"not a BTF source", "demo")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")

    def test_duplicate_names_are_rejected(self):
        result = self.run_cli(function([], 0, []), "demo", "demo")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")

    def test_absent_input_is_rejected_without_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, "-B", str(HERE / "inspect_btf_prototypes.py"),
                 "--btf", str(Path(directory) / "missing"), "--function", "demo"],
                capture_output=True, timeout=10,
            )
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertNotIn(b"Traceback", result.stderr)

    def test_source_limit_is_enforced_before_parsing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversized"
            with path.open("wb") as output:
                output.seek(reader.MAX_BTF_BYTES)
                output.write(b"x")
            with self.assertRaisesRegex(ValueError, "BTF_source_exceeds_32_MiB"):
                reader.inspect_prototypes(path, ["demo"])

    def test_report_limit_rejects_before_output(self):
        with self.assertRaisesRegex(ValueError, "prototype_report_exceeds_1_MiB"):
            reader.encode_document({"large": "x" * reader.MAX_JSON_BYTES})

    def test_large_valid_prototype_report_has_empty_stdout_on_rejection(self):
        strings = STRINGS + b"Q" * 4095 + b"\0"
        types = [integer()]
        for _ in range(31):
            types.append(struct.pack("<III", len(STRINGS), 8 << 24, len(types)))
        params = struct.pack("<II", STRINGS.index(b"value\0"), 32) * 16
        types.extend([typ(13, 0, count=16) + params, typ(12, 33, "demo")])
        records = b"".join(types)
        raw = struct.pack("<HBBIIIII", 0xEB9F, 1, 0, 24, 0, len(records),
                          len(records), len(strings)) + records + strings
        result = self.run_cli(raw, "demo")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertIn(b"prototype_report_exceeds_1_MiB", result.stderr)

    def test_function_count_is_bounded_before_file_read(self):
        with self.assertRaisesRegex(ValueError, "one_to_sixteen"):
            reader.inspect_prototypes("not-read", [str(n) for n in range(17)])

    def test_selector_shapes_are_rejected_before_file_read(self):
        for names in [None, "demo", [[]], [1], [""], ["a\0b"]]:
            with self.subTest(names=names):
                with self.assertRaisesRegex(ValueError, "one_to_sixteen"):
                    reader.inspect_prototypes("not-read", names)

    def test_symlink_and_fifo_are_rejected_without_blocking(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.write_bytes(function([], 0, []))
            link = Path(directory) / "link"
            link.symlink_to(source)
            fifo = Path(directory) / "fifo"
            os.mkfifo(fifo)
            for path in [link, fifo]:
                with self.subTest(path=path.name):
                    with self.assertRaisesRegex(ValueError, "regular_non_symlink"):
                        reader.inspect_prototypes(path, ["demo"])

    def test_ambiguous_function_identity_is_rejected(self):
        raw = blob([typ(13, 0), typ(12, 1, "demo"), typ(12, 1, "demo")])
        result = self.run_cli(raw, "demo")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")


def bounded_reference_command(command, timeout=30):
    """Bound both pipe bytes and child lifetime, including a silent child."""
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": reader.MAX_JSON_BYTES, "stderr": 64 * 1024}
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                ready = selector.select(max(0, remaining))
                if remaining <= 0 or not ready:
                    raise ValueError("native_reference_timeout")
                for key, _ in ready:
                    chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffers[key.data].extend(chunk)
                    if len(buffers[key.data]) > limits[key.data]:
                        raise ValueError("native_reference_" + key.data + "_exceeded_its_bound")
        code = process.wait(timeout=max(0.01, deadline - time.monotonic()))
        if code != 0:
            raise ValueError("native_reference_failed returncode=" + str(code))
        if buffers["stderr"]:
            print("Native reference diagnostics: " + repr(bytes(buffers["stderr"])), file=sys.stderr)
        return bytes(buffers["stdout"])
    except (ValueError, subprocess.TimeoutExpired) as error:
        raise ValueError(str(error) + "; captured_stderr=" + repr(bytes(buffers["stderr"]))) from error
    finally:
        process.stdout.close()
        process.stderr.close()
        if process.poll() is None:
            process.kill()
            process.wait()


class NativeCaptureTests(unittest.TestCase):
    def test_native_stderr_and_exact_failure_status_are_retained(self):
        command = [sys.executable, "-c", "import sys; sys.stderr.write('reference marker'); sys.exit(7)"]
        with self.assertRaisesRegex(ValueError, "returncode=7.*reference marker"):
            bounded_reference_command(command)

    def test_native_success_returns_complete_stdout(self):
        self.assertEqual(bounded_reference_command([sys.executable, "-c", "print('complete')"]), b"complete\n")

    def test_native_output_overflow_is_refused(self):
        command = [sys.executable, "-c", "import os; os.write(1, b'x' * (1024 * 1024 + 1))"]
        with self.assertRaisesRegex(ValueError, "stdout_exceeded_its_bound"):
            bounded_reference_command(command)

    def test_silent_native_child_cannot_outlive_the_deadline(self):
        with self.assertRaisesRegex(ValueError, "native_reference_timeout"):
            bounded_reference_command([sys.executable, "-c", "import threading; threading.Event().wait()"], timeout=0.1)


def native_reference(path):
    """Compare a small compiler-produced fixture with bpftool, not this decoder."""
    path = Path(path)
    with path.open("rb") as source:
        raw = source.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise ValueError("native_reference_requires_the_small_compiler_fixture")
    native = bounded_reference_command(
        [os.environ.get("BPFTOOL", "bpftool"), "btf", "dump", "file", str(path), "format", "raw"])
    records, current = {}, None
    for line in native.decode("utf-8").splitlines():
        match = re.fullmatch(r"\[(\d+)\] (\w+) '(.*)'(?: (.*))?", line)
        if match:
            ident, kind, name, extra = match.groups()
            current = {"kind": kind, "name": name, "extra": extra or "", "args": []}
            records[int(ident)] = current
        elif current is not None and current["kind"] == "FUNC_PROTO":
            argument = re.fullmatch(r"\s*'(.*)' type_id=(\d+)", line)
            if not argument:
                raise ValueError("unexpected_native_prototype_row")
            current["args"].append((argument.group(1), int(argument.group(2))))
    names = ["rustfs_btf_probe", "rustfs_btf_void"]
    command = [sys.executable, "-B", str(HERE / "inspect_btf_prototypes.py"), "--btf", str(path)]
    for name in names:
        command.extend(["--function", name])
    result = subprocess.run(command, capture_output=True, timeout=30, check=True)
    report = json.loads(result.stdout)
    if report["source"]["sha256"] != hashlib.sha256(raw).hexdigest() or path.read_bytes() != raw:
        raise ValueError("native_reference_source_changed")
    if (report["schema"] != 1 or report["source"]["bytes"] != len(raw)
            or [p["function"]["name"] for p in report["prototypes"]] != names):
        raise ValueError("native_reference_membership_mismatch")
    kinds = {"INT": 1, "PTR": 2, "ARRAY": 3, "STRUCT": 4, "UNION": 5,
             "ENUM": 6, "FWD": 7, "TYPEDEF": 8, "VOLATILE": 9, "CONST": 10,
             "RESTRICT": 11, "FUNC": 12, "FUNC_PROTO": 13, "VAR": 14,
             "DATASEC": 15, "FLOAT": 16, "DECL_TAG": 17, "TYPE_TAG": 18, "ENUM64": 19}

    def check_record(actual):
        if actual["ID"] == 0:
            if actual != {"ID": 0, "name": "void", "kind": 0} or 0 in records:
                raise ValueError("native_reference_void_mismatch")
            return
        expected = records[actual["ID"]]
        name = "" if expected["name"] == "(anon)" else expected["name"]
        if actual["kind"] != kinds[expected["kind"]] or actual["name"] != name:
            raise ValueError("native_reference_record_mismatch")
        declared = re.search(r"(?:\btype_id|\bret_type_id|\bsize)=(\d+)", expected["extra"])
        if declared and actual["size_or_type"] != int(declared.group(1)):
            raise ValueError("native_reference_type_or_size_mismatch")
        count = re.search(r"\bvlen=(\d+)", expected["extra"])
        if count and actual["count"] != int(count.group(1)):
            raise ValueError("native_reference_parameter_count_mismatch")

    def check_chain(start, chain, leaf):
        current = start
        for index, record in enumerate(chain):
            if record["ID"] != current:
                raise ValueError("native_reference_chain_disconnected")
            check_record(record)
            if record["kind"] in (8, 9, 10, 11, 18):
                current = record["size_or_type"]
            elif index != len(chain) - 1 or record != leaf:
                raise ValueError("native_reference_chain_leaf_mismatch")
        check_record(leaf)
        if current != leaf["ID"] or (leaf["ID"] != 0 and (not chain or chain[-1] != leaf)):
            raise ValueError("native_reference_incomplete_chain")

    for name, actual in zip(names, report["prototypes"]):
        matches = [(ident, value) for ident, value in records.items()
                   if value["kind"] == "FUNC" and value["name"] == name]
        if len(matches) != 1:
            raise ValueError("native_reference_function_is_not_unique")
        ident, function = matches[0]
        proto_id = int(re.search(r"\btype_id=(\d+)", function["extra"]).group(1))
        expected = records[proto_id]
        return_id = int(re.search(r"\bret_type_id=(\d+)", expected["extra"]).group(1))
        args = [(arg["name"], arg["type_ID"]) for arg in actual["args"]]
        if (actual["function"]["ID"] != ident or actual["prototype"]["ID"] != proto_id
                or actual["prototype"]["size_or_type"] != return_id or args != expected["args"]):
            raise ValueError("native_reference_declaration_mismatch")
        for record in [actual["function"], actual["prototype"], actual["return"], *actual["return_chain"]]:
            check_record(record)
        check_chain(return_id, actual["return_chain"], actual["return"])
        for arg in actual["args"]:
            check_chain(arg["type_ID"], arg["chain"], arg["type"])
            for record in [arg["type"], *arg["chain"], *arg.get("pointee_chain", [])]:
                check_record(record)
            if "pointee" in arg:
                check_record(arg["pointee"])
            if arg["type"]["kind"] == 2:
                pointer = records[arg["type"]["ID"]]
                target = int(re.search(r"\btype_id=(\d+)", pointer["extra"]).group(1))
                if pointer["kind"] != "PTR" or target != arg["type"]["size_or_type"]:
                    raise ValueError("native_reference_pointer_mismatch")
                check_chain(target, arg["pointee_chain"], arg["pointee"])
                for qualifier in arg["pointee_chain"]:
                    record = records[qualifier["ID"]]
                    if int(re.search(r"\btype_id=(\d+)", record["extra"]).group(1)) != qualifier["size_or_type"]:
                        raise ValueError("native_reference_qualifier_mismatch")
    print("Validated both compiler-produced prototypes against independent bpftool output")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--native-reference":
        native_reference(sys.argv[2])
    else:
        unittest.main()
