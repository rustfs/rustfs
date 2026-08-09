#!/usr/bin/env python3
"""Summarize a samply/Firefox profile with a samply .syms.json sidecar."""

from __future__ import annotations

import argparse
import bisect
import collections
import gzip
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]+$")


@dataclass(frozen=True)
class Symbol:
    rva: int
    size: int
    name: str
    library: str
    code_id: str | None

    @property
    def end(self) -> int:
        return self.rva + max(self.size, 1)


class IntervalIndex:
    def __init__(self, symbols: list[Symbol]) -> None:
        self._symbols = sorted(symbols, key=lambda symbol: symbol.rva)
        self._starts = [symbol.rva for symbol in self._symbols]

    def lookup(self, address: int) -> Symbol | None:
        pos = bisect.bisect_right(self._starts, address)
        for symbol in reversed(self._symbols[max(0, pos - 8) : pos]):
            if symbol.rva <= address < symbol.end:
                return symbol
        return None


class SymbolIndex:
    def __init__(self, symbols: list[Symbol]) -> None:
        self._all = IntervalIndex(symbols)
        by_code_id: dict[str, list[Symbol]] = collections.defaultdict(list)
        for symbol in symbols:
            if symbol.code_id:
                by_code_id[symbol.code_id.lower()].append(symbol)
        self._by_code_id = {code_id: IntervalIndex(items) for code_id, items in by_code_id.items()}

    def lookup(self, address: int, code_id: str | None) -> Symbol | None:
        if code_id:
            index = self._by_code_id.get(code_id.lower())
            if index:
                symbol = index.lookup(address)
                if symbol:
                    return symbol
        return self._all.lookup(address)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join a samply profile.json.gz and .syms.json sidecar into function-level hotpath counters.",
    )
    parser.add_argument("--profile", required=True, type=Path, help="samply Firefox profile JSON or JSON.GZ")
    parser.add_argument("--symbols", required=True, type=Path, help="samply .syms.json sidecar")
    parser.add_argument("--limit", type=int, default=20, help="number of functions to show per section")
    parser.add_argument("--thread", help="regular expression used to include matching thread names only")
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="output format; markdown is issue-comment friendly",
    )
    parser.add_argument("--max-name-len", type=int, default=160, help="truncate long function names in markdown output")
    return parser.parse_args()


def load_json(path: Path) -> Any:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    with path.open("r", encoding="utf-8") as source:
        return json.load(source)


def string_at(strings: list[Any], value: Any) -> str | None:
    if isinstance(value, int) and 0 <= value < len(strings):
        return str(strings[value])
    if isinstance(value, str):
        return value
    return None


def parse_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 0)
        except ValueError:
            return None
    return None


def iter_symbol_libraries(symbols_json: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    data = symbols_json.get("data", {})
    if isinstance(data, dict):
        return [(str(name), library) for name, library in data.items() if isinstance(library, dict)]
    if isinstance(data, list):
        result = []
        for index, library in enumerate(data):
            if isinstance(library, dict):
                name = library.get("debug_name") or library.get("name") or f"library-{index}"
                result.append((str(name), library))
        return result
    return []


def symbol_name(entry: dict[str, Any], strings: list[Any]) -> str | None:
    symbol = string_at(strings, entry.get("symbol"))
    if symbol:
        return symbol

    frames = entry.get("frames")
    if isinstance(frames, list):
        for frame in reversed(frames):
            if isinstance(frame, dict):
                name = string_at(strings, frame.get("function"))
                if name:
                    return name
    return None


def load_symbols(path: Path) -> SymbolIndex:
    symbols_json = load_json(path)
    if not isinstance(symbols_json, dict):
        raise ValueError("symbol sidecar does not look like samply .syms.json")
    strings = symbols_json.get("string_table", [])
    if not isinstance(strings, list):
        raise ValueError("symbol sidecar does not look like samply .syms.json")

    symbols: list[Symbol] = []
    for library_name, library in iter_symbol_libraries(symbols_json):
        table = library.get("symbol_table", [])
        if not isinstance(table, list):
            continue
        code_id = library.get("code_id") or library.get("codeId")
        for entry in table:
            if not isinstance(entry, dict):
                continue
            rva = parse_int(entry.get("rva"))
            size = parse_int(entry.get("size")) or 1
            name = symbol_name(entry, strings)
            if rva is None or not name:
                continue
            symbols.append(Symbol(rva=rva, size=size, name=name, library=library_name, code_id=str(code_id) if code_id else None))

    if not symbols:
        raise ValueError("symbol sidecar did not contain any usable symbols")
    return SymbolIndex(symbols)


def table_get(table: dict[str, Any], column: str, index: int) -> Any:
    values = table.get(column, [])
    if isinstance(values, list) and 0 <= index < len(values):
        return values[index]
    return None


def sample_stacks(samples: dict[str, Any]) -> list[Any]:
    if isinstance(samples.get("stack"), list):
        return samples["stack"]
    data = samples.get("data")
    schema = samples.get("schema", {})
    stack_column = schema.get("stack")
    if isinstance(data, list) and isinstance(stack_column, int):
        return [row[stack_column] if isinstance(row, list) and stack_column < len(row) else None for row in data]
    return []


def frame_code_id(profile_libs: list[Any], thread: dict[str, Any], func_index: int | None) -> str | None:
    if func_index is None:
        return None
    resource_index = table_get(thread.get("funcTable", {}), "resource", func_index)
    if not isinstance(resource_index, int):
        return None
    lib_index = table_get(thread.get("resourceTable", {}), "lib", resource_index)
    if not isinstance(lib_index, int) or not (0 <= lib_index < len(profile_libs)):
        return None
    library = profile_libs[lib_index]
    if not isinstance(library, dict):
        return None
    code_id = library.get("codeId") or library.get("code_id")
    return str(code_id) if code_id else None


def frame_name(profile_libs: list[Any], thread: dict[str, Any], frame_index: int, symbols: SymbolIndex) -> tuple[str, bool]:
    frame_table = thread.get("frameTable", {})
    func_table = thread.get("funcTable", {})
    strings = thread.get("stringArray", [])
    address = parse_int(table_get(frame_table, "address", frame_index))
    func_index = table_get(frame_table, "func", frame_index)
    typed_func_index = func_index if isinstance(func_index, int) else None
    if address is not None:
        symbol = symbols.lookup(address, frame_code_id(profile_libs, thread, typed_func_index))
        if symbol:
            return f"{symbol.name} [{symbol.library}]", True

    if typed_func_index is not None:
        name_index = table_get(func_table, "name", typed_func_index)
        name = string_at(strings, name_index)
        if name and not ADDRESS_RE.match(name):
            return name, False

    if address is not None:
        return f"0x{address:x}", False
    return "<unknown>", False


def stack_frames(thread: dict[str, Any], stack_index: Any) -> list[int]:
    stack_table = thread.get("stackTable", {})
    if not isinstance(stack_index, int):
        return []

    frames: list[int] = []
    seen: set[int] = set()
    current: int | None = stack_index
    while current is not None and current not in seen:
        seen.add(current)
        frame = table_get(stack_table, "frame", current)
        if isinstance(frame, int):
            frames.append(frame)
        prefix = table_get(stack_table, "prefix", current)
        current = prefix if isinstance(prefix, int) else None
    frames.reverse()
    return frames


def summarize(profile: dict[str, Any], symbols: SymbolIndex, thread_filter: str | None) -> dict[str, Any]:
    thread_re = re.compile(thread_filter) if thread_filter else None
    profile_libs = profile.get("libs", [])
    if not isinstance(profile_libs, list):
        profile_libs = []
    leaf: collections.Counter[str] = collections.Counter()
    inclusive: collections.Counter[str] = collections.Counter()
    thread_counts: collections.Counter[str] = collections.Counter()
    resolved_samples = 0
    unresolved_samples = 0
    total_samples = 0

    for thread in profile.get("threads", []):
        if not isinstance(thread, dict):
            continue
        thread_name = str(thread.get("name") or thread.get("processName") or "<unnamed>")
        if thread_re and not thread_re.search(thread_name):
            continue

        for stack_index in sample_stacks(thread.get("samples", {})):
            frames = stack_frames(thread, stack_index)
            if not frames:
                continue
            names: list[str] = []
            any_resolved = False
            for frame_index in frames:
                name, resolved = frame_name(profile_libs, thread, frame_index, symbols)
                names.append(name)
                any_resolved = any_resolved or resolved
            leaf[names[-1]] += 1
            inclusive.update(set(names))
            thread_counts[thread_name] += 1
            total_samples += 1
            if any_resolved:
                resolved_samples += 1
            else:
                unresolved_samples += 1

    return {
        "total_samples": total_samples,
        "resolved_samples": resolved_samples,
        "unresolved_samples": unresolved_samples,
        "threads": dict(thread_counts.most_common()),
        "leaf": leaf,
        "inclusive": inclusive,
    }


def counter_rows(counter: collections.Counter[str], total: int, limit: int) -> list[dict[str, Any]]:
    rows = []
    for name, count in counter.most_common(limit):
        rows.append({"function": name, "samples": count, "percent": round((count * 100.0 / total), 2) if total else 0.0})
    return rows


def truncate_name(name: str, max_len: int) -> str:
    if max_len <= 0 or len(name) <= max_len:
        return name
    if max_len <= 3:
        return name[:max_len]
    return f"{name[: max_len - 3]}..."


def print_markdown(summary: dict[str, Any], limit: int, max_name_len: int) -> None:
    total = int(summary["total_samples"])
    resolved = int(summary["resolved_samples"])
    unresolved = int(summary["unresolved_samples"])
    print(f"- samples: {total}, resolved stacks: {resolved}, unresolved stacks: {unresolved}")
    if summary["threads"]:
        print("- threads:")
        for thread, count in summary["threads"].items():
            pct = (count * 100.0 / total) if total else 0.0
            print(f"  - `{thread}`: {count} ({pct:.2f}%)")

    for title, key in (("Top leaf functions", "leaf"), ("Top inclusive functions", "inclusive")):
        print()
        print(f"### {title}")
        print("| rank | samples | pct | function |")
        print("|---:|---:|---:|---|")
        for rank, row in enumerate(counter_rows(summary[key], total, limit), start=1):
            function = truncate_name(str(row["function"]), max_name_len).replace("|", "\\|")
            print(f"| {rank} | {row['samples']} | {row['percent']:.2f}% | `{function}` |")


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        print("error: --limit must be positive", file=sys.stderr)
        return 2

    profile = load_json(args.profile)
    symbols = load_symbols(args.symbols)
    summary = summarize(profile, symbols, args.thread)
    if args.format == "json":
        print(
            json.dumps(
                {
                    "total_samples": summary["total_samples"],
                    "resolved_samples": summary["resolved_samples"],
                    "unresolved_samples": summary["unresolved_samples"],
                    "threads": summary["threads"],
                    "leaf": counter_rows(summary["leaf"], summary["total_samples"], args.limit),
                    "inclusive": counter_rows(summary["inclusive"], summary["total_samples"], args.limit),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print_markdown(summary, args.limit, args.max_name_len)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
