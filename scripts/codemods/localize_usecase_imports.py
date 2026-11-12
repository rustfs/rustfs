#!/usr/bin/env python3
"""Give every glob-importing module of the three use-case trees its own s3s imports.

The files under `app/bucket_usecase/`, `app/multipart_usecase/` and `app/object/` reach
the s3 wire types through `use super::*;` from the tree's root file. A child that stops
using a name then leaves the root's import unused, and the fix re-flows a braced list that
many slices edit. After this codemod each module that holds `use super::*;` (a "site")
imports by name every s3s name it takes from the root, and the root keeps only the names
its own code uses. A slice that moves a child off s3s then deletes `use` lines in that
child and nowhere else.

What it writes, and nothing else:

  * in each site, right after its `use super::*;`, one `use` item per parent path for the
    root s3s names the site's own code uses, spelled through the alias hub
    `crate::app::storage_api::s3::…`, so no new file names `s3s::` and the footprint
    counters of scripts/check_s3s_footprint.sh do not move. A name used only under
    `#[cfg(…)]` gets that `#[cfg(…)]`. A nested site (an inline `mod tests`) skips names
    its enclosing site in the same file already provides;
  * in the root, its top-level `use s3s…` items drop every name the root's own code does
    not use, and a name used there only under `#[cfg(…)]` moves to an import with that cfg.

The compiler is the oracle: identifiers are matched as text (comments and literals are
blanked; `a::NAME` and `.name` are not uses), so the result must build warning-free.
A trait imported only for its methods leaves no name in the text; list such an import in
TRAIT_IMPORTS. A root s3s import that no site names and the table does not list stops the
run, as does a name the hub does not re-export.

Running it on its own output changes nothing: a site skips names it already binds, and the
root already holds exactly what it uses.

Usage:
  scripts/codemods/localize_usecase_imports.py [--root DIR] [--no-fmt]
  scripts/codemods/localize_usecase_imports.py --self-test

Exit status: 0 on success (also when nothing changed), 1 when a tree cannot be localized.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from dataclasses import dataclass, field
from pathlib import Path

TREES = ("rustfs/src/app/bucket_usecase.rs", "rustfs/src/app/multipart_usecase.rs", "rustfs/src/app/object/mod.rs")
HUB_FILE = "rustfs/src/app/storage_api.rs"
HUB = ("crate", "app", "storage_api", "s3")
ROOT = "root"

# Root s3s imports used only for trait methods: "<root file>|<import path>" -> the sites that
# call them: ROOT, "<file>" or "<file>::<inline module>".
TRAIT_IMPORTS: dict[str, tuple[str, ...]] = {
    "rustfs/src/app/multipart_usecase.rs|s3s::stream::ByteStream": ("rustfs/src/app/multipart_usecase/upload_part.rs",),
}


class LocalizeError(Exception):
    pass


# ---------------------------------------------------------------------------
# Lexing: comments and literals are blanked so brackets and identifiers are code.
# ---------------------------------------------------------------------------

IDENT = re.compile(r"(?<![A-Za-z0-9_])[A-Za-z_][A-Za-z0-9_]*")
CHAR_LITERAL = re.compile(r"'(?:[^'\\\n]|\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.))'")
RAW_STRING = re.compile(r'b?r(#*)"')


def code_only(text: str) -> str:
    out = list(text)
    i, n = 0, len(text)

    def blank(start: int, end: int) -> None:
        for k in range(start, min(end, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        raw = RAW_STRING.match(text, i) if text[i] in "br" else None
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
        elif text.startswith("/*", i):
            depth, end = 1, i + 2
            while end < n and depth:
                step = text[end : end + 2]
                depth += {"/*": 1, "*/": -1}.get(step, 0)
                end += 2 if step in ("/*", "*/") else 1
        elif raw and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            close = text.find('"' + raw.group(1), raw.end())
            end = n if close < 0 else close + 1 + len(raw.group(1))
        elif text[i] == '"':
            end = i + 1
            while end < n and text[end] != '"':
                end += 2 if text[end] == "\\" else 1
            end += 1
        elif text[i] == "'" and (literal := CHAR_LITERAL.match(text, i)):
            end = literal.end()
        else:
            i += 1
            continue
        blank(i, end)
        i = end
    return "".join(out)


def depths(code: str) -> list[int]:
    """Bracket depth outside each character (a bracket has the depth of its surroundings)."""
    result, depth = [], 0
    for ch in code:
        if ch in ")]}":
            depth -= 1
        result.append(depth)
        if ch in "([{":
            depth += 1
    if depth != 0:
        raise LocalizeError("unbalanced brackets")
    return result


def closing(code: str, opening: int) -> int:
    depth = 0
    for k in range(opening, len(code)):
        if code[k] in "([{":
            depth += 1
        elif code[k] in ")]}":
            depth -= 1
            if depth == 0:
                return k
    raise LocalizeError("unclosed bracket")


def item_end(code: str, depth: list[int], start: int) -> int:
    """End of the item, statement or field that starts at `start` (after an attribute)."""
    base = depth[start]
    for k in range(start, len(code)):
        if depth[k] < base or (depth[k] == base and code[k] in ";,"):
            return k + (code[k] in ";,")
        if depth[k] == base and code[k] == "}":
            return k + 1
    return len(code)


# ---------------------------------------------------------------------------
# Use trees
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Leaf:
    path: tuple[str, ...]  # a glob ends in "*"
    alias: str | None = None

    @property
    def bound(self) -> str | None:
        if self.path[-1] == "*" or self.alias == "_":
            return None
        return self.alias or self.path[-1]

    @property
    def key(self) -> str:
        return "::".join(self.path) + (f" as {self.alias}" if self.alias else "")


def flatten_use(tree: str) -> list[Leaf]:
    """Leaves of a use tree given without `use` and `;`."""
    tokens = re.findall(r"::|[{},*]|[A-Za-z_][A-Za-z0-9_]*", tree)
    pos = 0

    def parse(prefix: tuple[str, ...]) -> list[Leaf]:
        nonlocal pos
        path = list(prefix)
        while pos < len(tokens) and tokens[pos] not in (",", "}"):
            token = tokens[pos]
            pos += 1
            if token == "{":
                leaves: list[Leaf] = []
                while tokens[pos] != "}":
                    leaves += parse(tuple(path))
                    pos += tokens[pos] == ","
                pos += 1
                return leaves
            if token == "*":
                return [Leaf((*path, "*"))]
            if token == "as":
                pos += 1
                return [Leaf(tuple(path), tokens[pos - 1])]
            if token != "::" and token != "self":
                path.append(token)
        return [Leaf(tuple(path))]

    leaves = parse(())
    if pos != len(tokens) or not all(leaf.path for leaf in leaves):
        raise LocalizeError(f"cannot parse use tree `{tree.strip()}`")
    return leaves


def render_tree(paths: list[tuple[str, ...]]) -> str:
    common = paths[0][:-1]
    for path in paths[1:]:
        k = 0
        while k < min(len(common), len(path) - 1) and common[k] == path[k]:
            k += 1
        common = common[:k]
    groups: dict[str, list[tuple[str, ...]]] = {}
    for rest in sorted({path[len(common) :] for path in paths}):
        groups.setdefault(rest[0], []).append(rest)
    if len(groups) == 1 and len(next(iter(groups.values()))) == 1:
        return "::".join((*common, *next(iter(groups.values()))[0]))
    parts = ["::".join(group[0]) if len(group) == 1 else render_tree(group) for _, group in sorted(groups.items())]
    inner = "{" + ", ".join(parts) + "}"
    return "::".join((*common, inner)) if common else inner


def render_uses(leaves: list[Leaf], cfg: str | None, indent: str) -> list[str]:
    """One `use` item per parent path (aliases on their own), each with `cfg`."""
    by_parent: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
    lines = []
    for leaf in leaves:
        if leaf.alias:
            lines.append(f"use {'::'.join(leaf.path)} as {leaf.alias};")
        else:
            by_parent.setdefault(leaf.path[:-1], []).append(leaf.path)
    lines += [f"use {render_tree(paths)};" for _, paths in sorted(by_parent.items())]
    attr = f"{indent}#[cfg({cfg})]\n" if cfg else ""
    return [f"{attr}{indent}{line}" for line in sorted(lines)]


# ---------------------------------------------------------------------------
# Source model
# ---------------------------------------------------------------------------


@dataclass
class UseItem:
    start: int  # first attribute or comment line attached to it
    use_at: int  # `use` keyword, or its `pub` qualifier
    first_ident: int  # the tree's first segment
    end: int  # one past the `;`
    leaves: list[Leaf]
    cfg: str | None


@dataclass
class Source:
    path: str
    text: str
    code: str
    depth: list[int]
    cfgs: list[tuple[int, int, int, str]]  # (attribute position, extent start, extent end, predicate)

    def use_items(self, start: int, end: int, depth: int | None = None) -> list[UseItem]:
        items = []
        for match in re.finditer(r"\b(?:pub(?:\([^)]*\))?\s+)?use\b", self.code[start:end]):
            at = start + match.start()
            if depth is not None and self.depth[at] != depth:
                continue
            semicolon = self.code.index(";", at)
            ident = IDENT.search(self.code, start + match.end(), semicolon)
            first, cfg = self.text.rfind("\n", 0, at) + 1, None
            while first > 0:
                previous = self.text.rfind("\n", 0, first - 1) + 1
                line = self.text[previous : first - 1].strip()
                if not (line.startswith("#[") or (line.startswith("//") and not line.startswith(("//!", "///")))):
                    break
                if line.startswith("#[cfg("):
                    cfg = " ".join(line[len("#[cfg(") : -2].split())
                first = previous
            tree = self.code[start + match.end() : semicolon]
            items.append(UseItem(first, at, ident.start() if ident else at, semicolon + 1, flatten_use(tree), cfg))
        return items


def load(path: str, text: str) -> Source:
    code = code_only(text)
    depth = depths(code)
    cfgs = []
    for match in re.finditer(r"#\[cfg\(", code):
        predicate = " ".join(text[match.end() : closing(code, match.end() - 1)].split())
        after = closing(code, match.start() + 1) + 1
        cfgs.append((match.start(), after, item_end(code, depth, after), predicate))
    return Source(path, text, code, depth, cfgs)


@dataclass
class Site:
    name: str  # ROOT, "<file>" or "<file>::<module>"
    src: Source
    start: int
    end: int
    depth: int
    glob_end: int | None  # one past `use super::*;`; None for the root
    indent: str
    test: bool  # compiled only under cfg(test)
    outer: Site | None = None  # enclosing site in the same file
    context: frozenset[str] = frozenset()  # cfgs of the outer site's code that hold this site
    holes: list[tuple[int, int]] = field(default_factory=list)

    def bound(self) -> dict[str, str | None]:
        """Names the site's own `use` items bind -> their cfg."""
        names = {}
        for item in self.src.use_items(self.start, self.end, self.depth):
            if not self.in_hole(item.use_at):
                names.update({leaf.bound: item.cfg for leaf in item.leaves if leaf.bound})
        return names

    def in_hole(self, pos: int) -> bool:
        return any(start <= pos < end for start, end in self.holes)

    def usage(self, names: set[str]) -> dict[str, set[tuple[str, ...]]]:
        """name -> the sets of cfg predicates under which this site's own code names it."""
        src, found = self.src, {}
        items = src.use_items(self.start, self.end)
        scoped = []  # a `use` inside a block binds its names for that block only
        for item in items:
            if src.depth[item.use_at] > self.depth:
                opening = max(k for k in range(self.start, item.use_at) if src.code[k] == "{" and src.depth[k] == src.depth[item.use_at] - 1)
                scoped.append((opening, closing(src.code, opening), {leaf.bound for leaf in item.leaves}))
        for match in IDENT.finditer(src.code, self.start, self.end):
            pos, name = match.start(), match.group()
            if name not in names or self.in_hole(pos):
                continue
            k = pos - 1
            while k >= 0 and src.code[k].isspace():
                k -= 1
            if src.code[k] == "." or src.code[k - 1 : k + 1] == "::":
                continue
            if any(i.use_at <= pos < i.end and pos != i.first_ident for i in items):
                continue
            if any(start < pos < end and name in bound for start, end, bound in scoped):
                continue
            preds = {p for (a, s, e, p) in src.cfgs if s <= pos < e and self.start <= a < self.end and not self.in_hole(a)}
            found.setdefault(name, set()).add(tuple(sorted(preds)))
        return found

    def segments_after(self, name: str) -> set[str]:
        """Path segments that follow `name::` in this site's code (for a crate alias)."""
        found = set()
        for match in re.finditer(rf"(?<![A-Za-z0-9_:]){name}\s*::\s*([A-Za-z_][A-Za-z0-9_]*)", self.src.code[self.start : self.end]):
            if not self.in_hole(self.start + match.start()):
                found.add(match.group(1))
        return found


def glob_end(src: Source, start: int, end: int, depth: int) -> tuple[int, str] | None:
    for match in re.finditer(r"\buse\s+super\s*::\s*\*\s*;", src.code[start:end]):
        at = start + match.start()
        if src.depth[at] == depth:
            return start + match.end(), src.text[src.text.rfind("\n", 0, at) + 1 : at]
    return None


def attributes_above(text: str, at: int) -> list[str]:
    attrs, first = [], text.rfind("\n", 0, at) + 1
    while first > 0:
        previous = text.rfind("\n", 0, first - 1) + 1
        line = text[previous : first - 1].strip()
        if not line.startswith(("#[", "///")):
            break
        attrs.append(line)
        first = previous
    return attrs


def collect_sites(root: Path, path: str, site: Site, out: list[Site]) -> None:
    """Append `site`, the inline glob sites inside it, and the glob-importing files it declares."""
    out.append(site)
    src = site.src

    def walk(outer: Site, start: int, end: int, depth: int, inline: tuple[str, ...], test: bool) -> None:
        for match in re.finditer(r"\bmod\s+([A-Za-z_][A-Za-z0-9_]*)\s*([;{])", src.code[start:end]):
            at, name = start + match.start(), match.group(1)
            if src.depth[at] != depth:
                continue
            attrs = attributes_above(src.text, at)
            if any(a.startswith("#[path") for a in attrs):
                raise LocalizeError(f"{path}: `#[path]` on `mod {name}` is not supported")
            mod_test = test or "#[cfg(test)]" in attrs
            if match.group(2) == ";":
                base = Path(path)
                directory = base.parent if base.name == "mod.rs" else base.parent / base.stem
                child = str(directory.joinpath(*inline, f"{name}.rs"))
                text = (root / child).read_text()
                child_src = load(child, text)
                glob = glob_end(child_src, 0, len(text), 0)
                if glob:
                    collect_sites(root, child, Site(child, child_src, 0, len(text), 0, glob[0], glob[1], mod_test), out)
                continue
            body = start + match.end()
            body_end = closing(src.code, body - 1)
            glob = glob_end(src, body, body_end, depth + 1)
            if glob is None:
                walk(outer, body, body_end, depth + 1, (*inline, name), mod_test)
                continue
            context = frozenset(p for (a, s, e, p) in src.cfgs if s <= at < e and outer.start <= a < outer.end)
            inner = Site(f"{outer.name.split('::')[0]}::{name}", src, body, body_end, depth + 1, glob[0], glob[1], mod_test, outer, context)
            outer.holes.append((body, body_end))
            out.append(inner)
            walk(inner, body, body_end, depth + 1, (*inline, name), mod_test)

    walk(site, site.start, site.end, site.depth, (), site.test)


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def predicate(preds: set[tuple[str, ...]], implied: frozenset[str]) -> str | None:
    """The cfg an import needs when its name is used under `preds`; None when unconditional."""
    reduced = {tuple(p for p in pred if p not in implied) for pred in preds}
    if () in reduced:
        return None
    rendered = sorted(pred[0] if len(pred) == 1 else f"all({', '.join(pred)})" for pred in reduced)
    return rendered[0] if len(rendered) == 1 else f"any({', '.join(rendered)})"


def hub_names(root: Path) -> dict[str, str | None]:
    text = (root / HUB_FILE).read_text()
    src = load(HUB_FILE, text)
    match = re.search(r"\bmod\s+s3\s*\{", src.code)
    if match is None:
        raise LocalizeError(f"{HUB_FILE} has no `mod s3`")
    body_end = closing(src.code, match.end() - 1)
    names: dict[str, str | None] = {}
    for item in src.use_items(match.end(), body_end, src.depth[match.start()] + 1):
        names.update({leaf.bound: item.cfg for leaf in item.leaves if leaf.bound})
    return names


def localize_tree(root: Path, root_file: str, hub: dict[str, str | None]) -> dict[str, str]:
    """New text for every file of the tree that changes."""
    text = (root / root_file).read_text()
    root_site = Site(ROOT, load(root_file, text), 0, len(text), 0, None, "", False)
    sites: list[Site] = []
    collect_sites(root, root_file, root_site, sites)

    s3s_items = [i for i in root_site.src.use_items(0, len(text), 0) if i.leaves[0].path[0] == "s3s"]
    universe: dict[str, Leaf] = {}
    traits: dict[str, Leaf] = {}
    for item in s3s_items:
        for leaf in item.leaves:
            if leaf.bound:
                universe[leaf.bound] = leaf
            else:
                traits[leaf.key] = leaf
    names = set(universe)
    trait_sites: dict[str, list[Leaf]] = {}
    for key, owners in TRAIT_IMPORTS.items():
        tree, _, path = key.partition("|")
        if tree != root_file:
            continue
        leaf = traits.get(path) or next((leaf for leaf in universe.values() if leaf.key == path), None)
        by_name = {site.name: site for site in sites}
        moved = leaf is None and all(o in by_name and path.split("::")[-1] in by_name[o].bound() for o in owners)
        if moved:  # an earlier run already put the import in every listed site
            continue
        if leaf is None or not set(owners) <= set(by_name):
            raise LocalizeError(f"TRAIT_IMPORTS entry `{key}` -> {owners} matches no root import or site")
        for owner in owners:
            trait_sites.setdefault(owner, []).append(leaf)
    for key in traits:
        if not any(leaf.key == key for leaves in trait_sites.values() for leaf in leaves):
            raise LocalizeError(f"{root_file} imports `{key}` without a name; list it in TRAIT_IMPORTS")

    used_anywhere: set[str] = set()
    plans: dict[str, dict[str, str | None]] = {}  # site -> name (or unnamed import path) -> cfg it needs
    for site in sites:
        used = site.usage(names)
        used_anywhere |= set(used)
        implied = frozenset({"test"}) if site.test else frozenset()
        plans[site.name] = {name: predicate(preds, implied) for name, preds in used.items()}
        for leaf in trait_sites.get(site.name, []):
            plans[site.name][leaf.bound or leaf.key] = None
            used_anywhere.add(leaf.bound or leaf.key)
    unused = sorted(name for name in universe if name not in used_anywhere)
    if unused:
        raise LocalizeError(
            f"{root_file} imports {', '.join(unused)} but no site names them; if one is a trait used only "
            "for its methods, list it in TRAIT_IMPORTS"
        )

    edits: dict[str, list[tuple[int, int, str]]] = {}
    for item in s3s_items:
        keep, moved = [], {}
        for leaf in item.leaves:
            cfg = plans[ROOT].get(leaf.bound or leaf.key, "unused")
            if cfg == item.cfg:
                keep.append(leaf)
            elif cfg != "unused":
                moved.setdefault(cfg, []).append(leaf)
        if keep == item.leaves:
            continue
        comments = "".join(line + "\n" for line in text[item.start : item.use_at].splitlines() if line.lstrip().startswith("//"))
        lines = render_uses(keep, item.cfg, "") if keep else []
        lines += [line for cfg, leaves in sorted(moved.items(), key=str) for line in render_uses(leaves, cfg, "")]
        end = item.end + (text[item.end : item.end + 1] == "\n")
        edits.setdefault(root_file, []).append((item.start, end, (comments if keep else "") + "".join(l + "\n" for l in lines)))

    imports: dict[str, dict[str, str | None]] = {ROOT: plans[ROOT]}  # what each site binds afterwards
    for site in sites[1:]:
        provided = site.bound()
        if site.outer is not None:
            outer_bound = site.outer.bound()
            if site.outer.name == ROOT:  # the root's s3s imports are being trimmed to what the root uses
                outer_bound = {name: cfg for name, cfg in outer_bound.items() if name not in universe}
            for name, cfg in {**outer_bound, **imports[site.outer.name]}.items():
                if cfg is None or cfg in site.context:
                    provided.setdefault(name, cfg)
        added: dict[str | None, list[Leaf]] = {}
        for name, cfg in sorted(plans[site.name].items()):
            leaf = universe.get(name) or traits[name]
            if name not in provided or len(leaf.path) == 1:  # a crate alias reaches new paths through its provider
                check_hub(hub, site, leaf, cfg)
            if name in provided:
                continue
            added.setdefault(cfg, []).append(Leaf((*HUB, *leaf.path[1:]), leaf.alias))
        imports[site.name] = {leaf.bound or leaf.key: cfg for cfg, leaves in added.items() for leaf in leaves}
        if added:
            lines = [line for cfg, leaves in sorted(added.items(), key=str) for line in render_uses(leaves, cfg, site.indent)]
            edits.setdefault(site.src.path, []).append((site.glob_end, site.glob_end, "".join("\n" + l for l in lines)))

    sources = {site.src.path: site.src.text for site in sites}
    result = {}
    for path, changes in edits.items():
        new = sources[path]
        for start, end, replacement in sorted(changes, reverse=True):
            new = new[:start] + replacement + new[end:]
        result[path] = new
    return result


def check_hub(hub: dict[str, str | None], site: Site, leaf: Leaf, cfg: str | None) -> None:
    if len(leaf.path) == 1:  # the crate itself under an alias: check what the site reaches through it
        wanted = site.segments_after(leaf.alias or "")
    else:
        wanted = {leaf.path[1]}
    for name in sorted(wanted):
        if name not in hub:
            raise LocalizeError(
                f"{site.name} needs `{name}` from s3s, which `{'::'.join(HUB)}` does not re-export: "
                f"add `pub(crate) use s3s::{name};` to {HUB_FILE}"
            )
        if hub[name] is not None and not site.test and hub[name] != cfg:
            raise LocalizeError(
                f"{site.name} needs `{name}` without `#[cfg({hub[name]})]`, but {HUB_FILE} re-exports it only "
                f"under that cfg: add an unconditional `pub(crate) use s3s::{name};` there"
            )


def run(root: Path, fmt: bool) -> list[str]:
    hub = hub_names(root)
    changed: dict[str, str] = {}
    for tree in TREES:
        changed.update(localize_tree(root, tree, hub))
    for path, text in changed.items():
        (root / path).write_text(text)
    if fmt and changed:
        result = subprocess.run(["rustfmt", "--edition", "2024", *sorted(changed)], cwd=root, check=False)
        if result.returncode != 0:
            raise LocalizeError("rustfmt failed on the written files")
    return sorted(changed)


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", default=".", help="repository root (default: current directory)")
    parser.add_argument("--no-fmt", action="store_true", help="do not run rustfmt on the written files")
    args = parser.parse_args(argv)
    try:
        changed = run(Path(args.root).resolve(), not args.no_fmt)
    except LocalizeError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    print("\n".join(changed) if changed else "nothing to change")
    return 0


# ---------------------------------------------------------------------------
# Self-test: a miniature tree laid out like the real ones.
# ---------------------------------------------------------------------------

HUB_SRC = """pub(crate) mod s3 {
    pub(crate) use s3s::dto;
    #[cfg(test)]
    pub(crate) use s3s::xml;
    pub(crate) use s3s::{S3Error, S3Result};
}
"""


class SelfTest(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.dict(TRAIT_IMPORTS, {}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tree(self, root_src: str, children: dict[str, str], hub: str = HUB_SRC) -> Path:
        base = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(base))
        (base / HUB_FILE).parent.mkdir(parents=True)
        (base / HUB_FILE).write_text(hub)
        for tree in TREES[1:]:
            (base / tree).parent.mkdir(parents=True, exist_ok=True)
            (base / tree).write_text("")
        (base / TREES[0]).write_text(root_src)
        for name, body in children.items():
            path = base / TREES[0].removesuffix(".rs") / f"{name}.rs"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
        return base

    def child(self, base: Path, name: str) -> str:
        return (base / TREES[0].removesuffix(".rs") / f"{name}.rs").read_text()

    ROOT_SRC = "use s3s::dto::{A, B};\nuse s3s::{S3Error, S3Result};\n\nmod a;\n\nfn f() -> S3Result<A> {\n    todo!()\n}\n"

    def test_child_gets_hub_imports_and_root_keeps_what_it_uses(self) -> None:
        base = self.tree(self.ROOT_SRC, {"a": "use super::*;\n\nfn g(_: B) -> Result<(), S3Error> {\n    Ok(())\n}\n"})
        run(base, fmt=False)
        root = (base / TREES[0]).read_text()
        self.assertIn("use s3s::dto::A;\n", root)
        self.assertIn("use s3s::S3Result;\n", root)
        self.assertNotIn("S3Error", root.split("mod a;")[0])
        child = self.child(base, "a")
        self.assertIn("use super::*;\nuse crate::app::storage_api::s3::S3Error;\nuse crate::app::storage_api::s3::dto::B;\n", child)

    def test_second_run_changes_nothing(self) -> None:
        base = self.tree(self.ROOT_SRC, {"a": "use super::*;\n\nfn g(_: B) -> Result<A, S3Error> {\n    todo!()\n}\n"})
        run(base, fmt=False)
        before = {p: p.read_text() for p in base.rglob("*.rs")}
        self.assertEqual(run(base, fmt=False), [])
        self.assertEqual(before, {p: p.read_text() for p in base.rglob("*.rs")})

    def test_test_only_use_gets_cfg_and_nested_site_skips_what_its_parent_provides(self) -> None:
        child = (
            "use super::*;\n\nfn g(_: B) {}\n\n#[cfg(test)]\nfn h(_: S3Error) {}\n\n"
            "#[cfg(test)]\nmod tests {\n    use super::*;\n\n    fn t(_: B, _: S3Error, _: A) {}\n}\n"
        )
        base = self.tree(self.ROOT_SRC, {"a": child})
        run(base, fmt=False)
        text = self.child(base, "a")
        self.assertIn("use super::*;\n#[cfg(test)]\nuse crate::app::storage_api::s3::S3Error;\n", text)
        # B and S3Error come from the file's own imports; A only reaches the module from another file.
        self.assertIn("mod tests {\n    use super::*;\n    use crate::app::storage_api::s3::dto::A;\n\n", text)

    def test_root_test_module_imports_what_the_root_stops_importing(self) -> None:
        root = "use s3s::dto::{A, B};\n\nfn f(_: A) {}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    fn t(_: A, _: B) {}\n}\n"
        base = self.tree(root, {})
        run(base, fmt=False)
        text = (base / TREES[0]).read_text()
        self.assertTrue(text.startswith("use s3s::dto::A;\n"))
        self.assertIn("mod tests {\n    use super::*;\n    use crate::app::storage_api::s3::dto::B;\n\n", text)

    def test_root_name_used_only_in_root_tests_moves_under_cfg_test(self) -> None:
        root = "use s3s::dto::{A, B};\n\nfn f(_: A) {}\n\n#[cfg(test)]\nfn t(_: B) {}\n"
        base = self.tree(root, {})
        run(base, fmt=False)
        self.assertEqual((base / TREES[0]).read_text(), "use s3s::dto::A;\n#[cfg(test)]\nuse s3s::dto::B;\n\nfn f(_: A) {}\n\n#[cfg(test)]\nfn t(_: B) {}\n")

    def test_crate_alias_moves_to_its_users_and_its_comment_goes_with_it(self) -> None:
        root = "use s3s::dto::A;\n\n// Children reach s3s through this alias.\nuse s3s as s3s_crate;\n\nmod a;\n\nfn f(_: A) {}\n"
        base = self.tree(root, {"a": "use super::*;\n\nfn g(_: s3s_crate::dto::B) {}\n"})
        run(base, fmt=False)
        self.assertEqual((base / TREES[0]).read_text(), "use s3s::dto::A;\n\n\nmod a;\n\nfn f(_: A) {}\n")
        self.assertIn("use super::*;\nuse crate::app::storage_api::s3 as s3s_crate;\n", self.child(base, "a"))

    def test_crate_alias_path_missing_from_hub_is_refused_in_a_nested_site(self) -> None:
        child = "use super::*;\n\nfn g(_: s3s_crate::dto::B) {}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    fn t(_: s3s_crate::region::Region) {}\n}\n"
        base = self.tree("use s3s::dto::A;\nuse s3s as s3s_crate;\n\nmod a;\n\nfn f(_: A) {}\n", {"a": child})
        with self.assertRaisesRegex(LocalizeError, r"::tests needs `region`"):
            run(base, fmt=False)

    def test_listed_trait_import_moves_to_its_caller_and_a_second_run_accepts_the_entry(self) -> None:
        root = "use s3s::dto::A;\nuse s3s::stream::ByteStream;\n\nmod a;\n\nfn f(_: A) {}\n"
        child_path = f"{TREES[0].removesuffix('.rs')}/a.rs"
        base = self.tree(root, {"a": "use super::*;\n\nfn g(b: A) { b.remaining_length(); }\n"}, HUB_SRC[:-2] + "    pub(crate) use s3s::stream;\n}\n")
        with mock.patch.dict(TRAIT_IMPORTS, {f"{TREES[0]}|s3s::stream::ByteStream": (child_path,)}):
            run(base, fmt=False)
            self.assertNotIn("ByteStream", (base / TREES[0]).read_text())
            self.assertIn("use crate::app::storage_api::s3::stream::ByteStream;\n", self.child(base, "a"))
            self.assertEqual(run(base, fmt=False), [])

    def test_comments_strings_paths_and_fields_are_not_uses(self) -> None:
        child = 'use super::*;\n\nfn g(x: X) {\n    // B\n    let _ = "B";\n    let _ = other::B;\n    let _ = x.B;\n}\n'
        base = self.tree("use s3s::dto::{A, B};\n\nmod a;\n\nfn f(_: A) {}\n", {"a": child})
        with self.assertRaisesRegex(LocalizeError, "imports B but no site names them"):
            run(base, fmt=False)

    def test_name_a_block_imports_itself_is_not_a_use_of_the_root_import(self) -> None:
        child = "use super::*;\n\nfn g() {\n    use crate::other::B;\n    let _: B = todo!();\n}\n"
        base = self.tree("use s3s::dto::{A, B};\n\nmod a;\n\nfn f(_: A) {}\n", {"a": child})
        with self.assertRaisesRegex(LocalizeError, "imports B but no site names them"):
            run(base, fmt=False)

    def test_unnamed_trait_import_must_be_listed(self) -> None:
        base = self.tree("use s3s::dto::A;\nuse s3s::stream::ByteStream as _;\n\nfn f(_: A) {}\n", {})
        with self.assertRaisesRegex(LocalizeError, "TRAIT_IMPORTS"):
            run(base, fmt=False)

    def test_stale_trait_entry_is_refused(self) -> None:
        base = self.tree("use s3s::dto::A;\n\nfn f(_: A) {}\n", {})
        with mock.patch.dict(TRAIT_IMPORTS, {f"{TREES[0]}|s3s::stream::ByteStream": (ROOT,)}):
            with self.assertRaisesRegex(LocalizeError, "matches no root import"):
                run(base, fmt=False)

    def test_name_missing_from_hub_is_refused(self) -> None:
        base = self.tree("use s3s::dto::A;\nuse s3s::region::Region;\n\nmod a;\n\nfn f(_: A) {}\n", {"a": "use super::*;\n\nfn g(_: Region) {}\n"})
        with self.assertRaisesRegex(LocalizeError, r"add `pub\(crate\) use s3s::region;`"):
            run(base, fmt=False)

    def test_test_only_hub_name_is_refused_for_production_code(self) -> None:
        base = self.tree("use s3s::dto::A;\nuse s3s::xml;\n\nmod a;\n\nfn f(_: A) {}\n", {"a": "use super::*;\n\nfn g() { xml::f(); }\n"})
        with self.assertRaisesRegex(LocalizeError, "only under that cfg"):
            run(base, fmt=False)

    def test_path_attribute_is_refused(self) -> None:
        base = self.tree('use s3s::dto::A;\n\n#[path = "x.rs"]\nmod a;\n\nfn f(_: A) {}\n', {"a": "use super::*;\n"})
        with self.assertRaisesRegex(LocalizeError, "#\\[path\\]"):
            run(base, fmt=False)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
