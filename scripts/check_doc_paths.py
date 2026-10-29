#!/usr/bin/env python3
"""Check source-tree paths in docs and local links in contributor/agent guidance.

This intentionally handles the repository's ordinary Markdown links and headings,
not every Markdown extension. Links/headings exclude code examples; source paths
are still checked in executable code fences. Markdown examples, comments,
external URLs, and placeholder/glob paths are excluded. No network requests are made.
"""

from __future__ import annotations

import html
from pathlib import Path
import re
import subprocess
import sys
import unicodedata
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
PATH_TOKEN = re.compile(
    r"(?<![\w./-])(?:crates|rustfs|scripts|docs|helm|deploy|\.github|\.config|\.agents|\.gemini)/"
    r"[^\s`\"'()\[\],;:|]+"
)
PATH_SUFFIXES = {".rs", ".md", ".sh", ".toml", ".yml", ".yaml", ".mak", ".proto", ".json"}
URL = re.compile(r"(?:[a-zA-Z][\w+.-]*://|mailto:)[^\s<>`]+")
INLINE_CODE = re.compile(r"(?<!`)(`+)(?!`)(.*?)\1(?!`)")
LINK = re.compile(
    r"\[[^\]\n]*\]\(\s*(?:<([^>\n]+)>|((?:\\.|[^\s()\\]|\([^()\n]*\))+))"
    r"(?:\s+[\"'][^\n]*?[\"'])?\s*\)"
)
DEFINITION = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(?:<([^>\n]+)>|(\S+))")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")


def prose_lines(text: str, include_code_paths: bool = False):
    """Keep source lines; optionally retain code other than Markdown examples."""
    text = re.sub(r"<!--.*?-->", lambda match: "\n" * match[0].count("\n"), text, flags=re.S)
    lines = text.splitlines()
    frontmatter_end = 0
    if lines and lines[0] == "---":
        frontmatter_end = next((index for index, line in enumerate(lines[1:], 1) if line in {"---", "..."}), 0)
    fence = ""
    markdown_example = False
    for number, line in enumerate(lines, 1):
        if frontmatter_end and number <= frontmatter_end + 1:
            continue
        marker = FENCE.match(line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = ""
            elif include_code_paths and not markdown_example:
                yield number, line
            continue
        if marker:
            fence = marker[1]
            language = marker[2].strip().split()
            markdown_example = bool(language and language[0].lower() in {"md", "markdown"})
            continue
        if not include_code_paths and line.startswith(("    ", "\t")):
            continue
        yield number, line


def guidance(path: Path) -> bool:
    return path.name == "AGENTS.md" or path.as_posix() in {"CONTRIBUTING.md", "ARCHITECTURE.md"} or path.parts[0] == ".agents"


def doc_files(root: Path):
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root, check=True, capture_output=True,
    )
    for name in sorted(set(result.stdout.decode().split("\0")) - {""}):
        path = Path(name)
        if path.suffix == ".md" and (guidance(path) or path.parts[0] == "docs") and (root / path).is_file():
            yield path


def placeholder(value: str) -> bool:
    return any(char in value for char in "*<>{}…$")


def heading_ids(path: Path) -> set[str]:
    anchors = set()
    generated = set()
    previous = ""
    previous_number = 0
    for number, line in prose_lines(path.read_text(encoding="utf-8")):
        if number != previous_number + 1:
            previous = ""
        previous_number = number
        for explicit in re.finditer(r'<(?:a|h[1-6])\b[^>]*\b(?:id|name)=["\']([^"\']+)["\']', line, re.I):
            anchors.add(html.unescape(explicit[1]))
        heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        title = heading[1] if heading else previous if previous.strip() and re.fullmatch(r" {0,3}(?:=+|-+)\s*", line) else None
        previous = line
        if title is None:
            continue
        title = LINK.sub(lambda match: match[0][1:match[0].index("]")], title)
        title = html.unescape(re.sub(r"<[^>]*>", "", title)).lower()
        title = INLINE_CODE.sub(lambda match: match[2], title)
        title = re.sub(r"(?<!\w)(\*\*|__|\*|_|~~)(.+?)\1(?!\w)", r"\2", title)
        slug = "".join(char for char in title if char in " -_" or unicodedata.category(char)[0] in "LNM").replace(" ", "-")
        anchor, suffix = slug, 0
        while anchor in generated:
            suffix += 1
            anchor = f"{slug}-{suffix}"
        generated.add(anchor)
        anchors.add(anchor)
    return anchors


def check(root: Path) -> list[str]:
    failures = []
    anchors = {}
    for doc in doc_files(root):
        strict_links = guidance(doc)
        text = (root / doc).read_text(encoding="utf-8")
        for number, line in prose_lines(text):
            link_text = INLINE_CODE.sub("", line)
            links = list(LINK.finditer(link_text))
            definition = DEFINITION.match(link_text)
            if definition:
                links.append(definition)
            if strict_links:
                for match in links:
                    value = re.sub(r"\\([^\w\s])", r"\1", html.unescape(match[1] or match[2]))
                    if placeholder(value):
                        continue
                    url = urlsplit(value)
                    if url.scheme or url.netloc:
                        continue
                    local_path = unquote(url.path)
                    if local_path.startswith("/"):
                        target = root / local_path.lstrip("/")
                    else:
                        target = root / doc.parent / local_path if local_path else root / doc
                    target = target.resolve()
                    if not target.is_relative_to(root.resolve()) or not target.exists():
                        failures.append(f"stale doc link: {doc}:{number} -> {value}")
                    elif url.fragment and target.suffix == ".md":
                        if target not in anchors:
                            anchors[target] = heading_ids(target)
                        if unquote(url.fragment) not in anchors[target]:
                            failures.append(f"stale heading anchor: {doc}:{number} -> {value}")
        for number, line in prose_lines(text, include_code_paths=True):
            # Preserve inline path references while ignoring literal Markdown-link examples.
            path_text = INLINE_CODE.sub(lambda match: "" if LINK.search(match[2]) else match[2], line)
            if strict_links:
                path_text = LINK.sub("", path_text)
                path_text = DEFINITION.sub("", path_text)
            path_text = URL.sub("", path_text)
            for ref in set(PATH_TOKEN.findall(path_text)):
                ref = ref.rstrip(".!?").split("#", 1)[0]
                if not placeholder(ref) and Path(ref).suffix in PATH_SUFFIXES and not (root / ref).exists():
                    failures.append(f"stale path reference: {doc}:{number} -> {ref}")
    return failures


def main() -> int:
    try:
        failures = check(ROOT)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"doc path check failed: {error}", file=sys.stderr)
        return 1
    if failures:
        print("\n".join(failures), file=sys.stderr)
        print(f"\n{len(failures)} stale doc reference(s) found. Fix the reference to match the current tree.", file=sys.stderr)
        return 1
    print("doc path check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
