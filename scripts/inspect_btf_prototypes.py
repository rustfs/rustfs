#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Bounded BTF v1 reader; no inferred offsets, pointer values or runtime semantics."""
import argparse
import array
import hashlib
import json
import os
import stat
import struct
import sys
from pathlib import Path

MAX_BTF_BYTES = 32 * 1024 * 1024
MAX_FUNCTIONS = 16
MAX_JSON_BYTES = 1024 * 1024


def need(x, label):
    if not x:
        raise ValueError(label)


class _BTF:
    def __init__(self, raw):
        need(isinstance(raw, bytes) and 24 <= len(raw) <= 32 << 20, 'bounded_complete_BTF_blob')
        magic, version, flags, header, toff, tlen, soff, slen = struct.unpack_from('<HBBIIIII', raw)
        need((magic, version, flags) == (0xeb9f, 1, 0) and 24 <= header <= len(raw), 'supported_actual_BTF_v1_LE_header')
        self.raw, self.tb, self.te = raw, header + toff, header + toff + tlen
        self.sb, self.se = header + soff, header + soff + slen
        need(header <= self.tb <= self.te <= self.sb < self.se <= len(raw) and self.se == len(raw), 'complete_nonoverlapping_declared_sections')
        need(raw[self.sb] == 0 and raw[self.se - 1] == 0, 'valid_BTF_string_table_sentinels')
        self.offsets = array.array('I', [0])
        offset = self.tb
        while offset < self.te:
            need(offset + 12 <= self.te, 'complete_common_type')
            _, info, _ = struct.unpack_from('<III', raw, offset)
            kind, count = (info >> 24) & 31, info & 65535
            need(info & 0x60ff0000 == 0, 'reserved_type_info_bits_zero')
            fixed = {1: 4, 2: 0, 3: 12, 7: 0, 8: 0, 9: 0, 10: 0, 11: 0, 12: 0, 14: 4, 16: 0, 17: 4, 18: 0}
            if kind in fixed:
                size = fixed[kind]
            elif kind in (4, 5, 15, 19):
                size = count * 12
            elif kind in (6, 13):
                size = count * 8
            else:
                raise ValueError('unknown_BTF_kind_not_guessed')
            need(offset + 12 + size <= self.te and len(self.offsets) <= 500000, 'complete_bounded_type_payload')
            self.offsets.append(offset)
            offset += 12 + size
        need(offset == self.te, 'all_type_bytes_consumed')

    def string(self, at):
        need(type(at) is int and 0 <= at < self.se - self.sb, 'string_offset_in_actual_table')
        end = self.raw.find(b'\0', self.sb + at, self.se)
        need(end >= 0 and end - self.sb - at <= 4096, 'bounded_terminated_name')
        return self.raw[self.sb + at:end].decode('utf-8', errors='strict')

    def type(self, ident):
        need(type(ident) is int and 0 < ident < len(self.offsets), 'actual_nonvoid_type_ID')
        offset = self.offsets[ident]
        name, info, size = struct.unpack_from('<III', self.raw, offset)
        kind, count = (info >> 24) & 31, info & 65535
        return {'ID': ident, 'offset': offset, 'name': self.string(name), 'kind': kind, 'count': count,
                'kflag': bool(info >> 31), 'size_or_type': size}

    def find(self, name, kind):
        result = []
        encoded = name.encode() + b'\0'
        for i in range(1, len(self.offsets)):
            off = self.offsets[i]
            nm, info, _ = struct.unpack_from('<III', self.raw, off)
            if (info >> 24) & 31 == kind:
                need(nm < self.se - self.sb, 'name_offset_in_string_section')
                if self.raw.startswith(encoded, self.sb + nm):
                    result.append(self.type(i))
        need(len(result) == 1, 'unique_actual_named_type_or_function')
        return result[0]

    def unwrap(self, ident, *, allow_void=False):
        chain, seen = [], set()
        while True:
            # Void has no record. Only explicit prototype boundaries opt in.
            if allow_void and type(ident) is int and ident == 0:
                return {'ID': 0, 'name': 'void', 'kind': 0}, chain
            need(ident not in seen and len(chain) < 32, 'acyclic_bounded_qualifier_chain')
            seen.add(ident)
            t = self.type(ident)
            chain.append(t)
            if t['kind'] == 2:
                name = struct.unpack_from('<I', self.raw, t['offset'])[0]
                need(name == 0 and t['count'] == 0 and not t['kflag'], 'actual_pointer_header')
                if t['size_or_type'] != 0:
                    self.type(t['size_or_type'])
            if t['kind'] not in (8, 9, 10, 11, 18):
                return t, chain
            need(t['count'] == 0 and not t['kflag'], 'actual_qualifier_header')
            if t['kind'] in (9, 10, 11):
                name = struct.unpack_from('<I', self.raw, t['offset'])[0]
                need(name == 0, 'unnamed_CVR_qualifier')
            ident = t['size_or_type']

    def prototype(self, name):
        fn = self.find(name, 12)
        p = self.type(fn['size_or_type'])
        need(p['kind'] == 13 and p['count'] <= 16, 'actual_bounded_function_prototype')
        args = []
        for i in range(p['count']):
            nm, ty = struct.unpack_from('<II', self.raw, p['offset'] + 12 + i * 8)
            t, chain = self.unwrap(ty)
            arg = {'name': self.string(nm), 'type_ID': ty, 'type': t, 'chain': chain}
            if t['kind'] == 2:
                target, target_chain = self.unwrap(t['size_or_type'], allow_void=True)
                arg.update({'pointee': target, 'pointee_chain': target_chain})
            args.append(arg)
        result, chain = self.unwrap(p['size_or_type'], allow_void=True)
        return {'function': fn, 'prototype': p, 'args': args, 'return': result, 'return_chain': chain}


def inspect_prototypes(path, names):
    """Read bounded source bytes and report declared prototypes, never runtime ABI."""
    if (not isinstance(names, (list, tuple)) or not names or len(names) > MAX_FUNCTIONS
            or any(not isinstance(name, str) or not name or "\0" in name
                   or len(name.encode("utf-8")) > 4096 for name in names)
            or len(set(names)) != len(names)):
        raise ValueError("one_to_sixteen_unique_function_names_required")
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("BTF_input_must_be_a_regular_non_symlink_file")

    def opener(name, flags):
        return os.open(name, flags | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0))

    with open(path, "rb", opener=opener) as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("BTF_input_must_be_a_regular_non_symlink_file")
        raw = source.read(MAX_BTF_BYTES + 1)
    if len(raw) > MAX_BTF_BYTES:
        raise ValueError("BTF_source_exceeds_32_MiB")
    btf = _BTF(raw)
    return {"schema": 1,
            "source": {"format": "BTF-v1-little-endian", "bytes": len(raw),
                       "sha256": hashlib.sha256(raw).hexdigest()},
            "prototypes": [btf.prototype(name) for name in names]}


def encode_document(document):
    """Reject an oversized report before emitting any JSON prefix."""
    pieces, length = [], 0
    for chunk in json.JSONEncoder(ensure_ascii=True, sort_keys=True).iterencode(document):
        encoded = chunk.encode("utf-8")
        length += len(encoded)
        if length + 1 > MAX_JSON_BYTES:
            raise ValueError("prototype_report_exceeds_1_MiB")
        pieces.append(encoded)
    return b"".join(pieces) + b"\n"


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only inspection of declared BTF function prototypes; no probe loading or runtime ABI claims.")
    parser.add_argument("--btf", required=True, type=Path, help="raw BTF v1 little-endian file (at most 32 MiB)")
    parser.add_argument("--function", required=True, action="append", dest="functions",
                        help="exact, unique function name; repeat at most 16 times")
    args = parser.parse_args(argv)
    try:
        output = encode_document(inspect_prototypes(args.btf, args.functions))
    except OSError:
        parser.exit(2, "BTF input could not be read\n")
    except (ValueError, UnicodeError) as error:
        parser.exit(2, "BTF inspection rejected: " + str(error) + "\n")
    sys.stdout.buffer.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
