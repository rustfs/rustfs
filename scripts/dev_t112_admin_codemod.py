#!/usr/bin/env python3
"""Codemod for the T1.12 admin boundary migration (rustfs/backlog#2788).

Rewrites admin handler files under ``rustfs/src/admin/handlers/**`` from the
``s3s`` request/response types (``S3Request<Body>`` / ``S3Response<(StatusCode,
Body)>`` / ``crate::admin::router::Operation``) to the owned boundary defined in
``rustfs/src/admin/operation.rs`` (``AdminRequest`` / ``AdminResponse`` /
``crate::admin::operation::Operation``).

Usage::

    python3 scripts/dev_t112_admin_codemod.py [--check] [--credential-methods] [--rustfmt] <file.rs>...

``--check`` prints what would change (per-file line deltas and the TODO list)
without writing anything.  ``--credential-methods`` additionally rewrites the
obvious ``cred.access_key`` / ``cred.secret_key`` field accesses on bindings
taken from ``req.credentials()`` into the ``RequestCredentials`` accessor
methods; by default those sites only get a TODO.  ``--rustfmt`` runs
``rustfmt --edition 2024`` on every file that was written (syntax only, no
compilation).

The script is idempotent: running it again on an already migrated file makes no
further change, and TODO comments are never duplicated.

Everything the script cannot rewrite mechanically is left in place with a
``// TODO(T1.12-codemod): <reason>`` line inserted directly above it.  Every
TODO (new or pre-existing) is listed on stdout as ``file:line: reason``.

Rewrite rules (see the per-phase functions below for the exact shapes):

1. imports: ``Operation`` leaves ``crate::admin::router`` imports and the owned
   names are imported from ``crate::admin::operation``; ``Body``/``S3Request``/
   ``S3Response`` leave ``s3s``/``storage_api::s3`` imports; ``Body`` is imported
   from ``rustfs_s3_types`` when still used; ``s3s::header::*`` becomes
   ``http::header::*``; ``LegacyAdminAdapter`` statics are removed and routes
   register the handler directly.
2. signatures: ``S3Request<Body>`` -> ``AdminRequest``,
   ``S3Response<(StatusCode, Body)>`` -> ``AdminResponse``; ``mut`` on by-value
   ``AdminRequest`` parameters is kept only when the body needs it.
3. request fields: ``req.uri``/``req.headers``/``req.method``/``req.credentials``
   become accessor calls; ``req.extensions`` lookups become ``remote_addr()``,
   ``req_info()``, ``req_info_mut()``, ``install_req_info()``, ``client_info()``,
   ``request_context()``; ``req.region``/``req.service`` and the remaining
   ``req.extensions`` uses get a TODO.
4. helpers: ``authorize_admin_request(&req, ..)`` ->
   ``authorize_admin_envelope(req.envelope(), ..)``; ``*_from_req(&req)`` ->
   ``*_from_envelope(req.envelope())``; ``*_from_extensions(&req.extensions`` ->
   ``*_from_envelope(req.envelope()``; ``KmsAdminAudit::from_request(..)`` ->
   ``from_envelope(req.envelope(), ..)``; ``CallerIdentity::resolve(&req)`` ->
   ``resolve_envelope(req.envelope())``; ``AccountAudit*::from_request(&req)`` ->
   ``from_envelope(req.envelope())``.  Import lines follow the renames.
5. request body: ``req.input`` / ``std::mem::take(&mut req.input)`` ->
   ``req.take_body()``; assignments to ``req.input`` get a TODO.
6. responses: ``S3Response::new((A, B))`` -> ``AdminResponse::new(A, B)``;
   ``S3Response::with_headers((A, B), H)`` -> ``AdminResponse::with_headers(A, B, H)``;
   other shapes and response field access get a TODO.
7. tests: ``S3Request { .. }`` literals become
   ``crate::admin::operation::test_support::TestRequest::new(method, uri)
   .headers(..).extensions(..).body(..)[.credentials(a, b, None)].build()``.
   Literals that feed the s3s-facing ``S3Router`` directly (``router.call`` /
   ``check_access``) and literals in production code are left alone, together
   with the function that owns them, under a TODO.
8. string literals in source-inspecting tests follow the helper renames; any
   remaining mention of a pre-migration symbol gets a TODO.
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

TODO_PREFIX = "// TODO(T1.12-codemod): "
SENT_OPEN = "\x01"
SENT_CLOSE = "\x02"
MAX_WIDTH = 130

# --------------------------------------------------------------------------- reasons

R_REGION = "request region/service is now Option<&str> via region()/service(); adjust the caller"
R_EXT = "request extensions are private on AdminRequest; use an accessor (remote_addr/req_info/client_info/request_context) or envelope()"
R_URI_ASSIGN = "assigning the request URI is not supported on AdminRequest"
R_HDR_MUT = "mutating request headers needs envelope_mut() on AdminRequest"
R_METHOD_ASSIGN = "assigning the request method is not supported on AdminRequest"
R_CRED_ASSIGN = "assigning request credentials is not supported on AdminRequest; build the request with credentials instead"
R_CRED_FIELD = "RequestCredentials exposes access_key()/secret_key()/session_token() methods (secret_key() returns &str) instead of fields"
R_BORROW = "AdminRequest accessors borrow (uri()/headers()/credentials()); clone or change the callee if it needs ownership"
R_INPUT_ASSIGN = "assigning the request body is not supported on AdminRequest; build the request with the body instead"
R_RESP_CTOR = "S3Response::new/with_headers with a non-(StatusCode, Body) output has no AdminResponse equivalent"
R_RESP_TYPE = "S3Response with a non-(StatusCode, Body) output has no AdminResponse equivalent"
R_REQ_TYPE = "S3Request with a non-Body input has no AdminRequest equivalent"
R_RESP_FIELD = "AdminResponse fields are private; use with_headers()/into_parts() instead of field access"
R_OUTPUT = "S3Response output tuple is gone; destructure AdminResponse::into_parts() instead"
R_LITERAL = "S3Request literal has an unsupported field value; build it with test_support::TestRequest manually"
R_LITERAL_REGION = "TestRequest region/service value is not None; verify the builder accepts it"
R_ROUTER = "router-level test drives the s3s-facing S3Router; keep s3s request types until the router boundary moves"
R_PROD_LITERAL = "production code builds an s3s S3Request by hand; construct AdminRequest::new(RequestEnvelope, Body) or keep the s3s type until the generic auth path moves"
R_STRING = "string literal references a pre-migration symbol; update the source-inspecting test"

# --------------------------------------------------------------------------- scanner

_TOKEN_RE = re.compile(r"//[^\n]*|/\*|(?<![\w])b?r#*\"|(?<![\w])b\"|\"|'(?:\\[^']*|[^'\\])'|[()\[\]{}]")


class Scanner:
    """Marks comment/string regions and pairs brackets for a Rust source text."""

    def __init__(self, text: str) -> None:
        self.text = text
        n = len(text)
        mask = bytearray(b"\x01") * n  # 1 = code
        strings: list[tuple[int, int]] = []
        pairs: dict[int, int] = {}
        stack: list[int] = []
        pos = 0
        while True:
            m = _TOKEN_RE.search(text, pos)
            if m is None:
                break
            tok = m.group(0)
            i = m.start()
            if tok.startswith("//"):
                j = m.end()
                mask[i:j] = bytes(j - i)
                pos = j
            elif tok == "/*":
                j = _skip_block_comment(text, i)
                mask[i:j] = bytes(j - i)
                pos = j
            elif tok.endswith('"'):
                if "r" in tok:
                    hashes = tok.count("#")
                    close = '"' + "#" * hashes
                    k = text.find(close, m.end())
                    j = n if k < 0 else k + len(close)
                else:
                    j = _skip_plain_string(text, m.end() - 1)
                mask[i:j] = bytes(j - i)
                strings.append((i, j))
                pos = j
            elif tok.startswith("'"):
                j = m.end()
                mask[i:j] = bytes(j - i)
                pos = j
            else:
                if tok in "([{":
                    stack.append(i)
                elif stack:
                    o = stack.pop()
                    pairs[o] = i
                    pairs[i] = o
                pos = m.end()
        self.mask = mask
        self.pairs = pairs
        self.strings = strings

    def is_code(self, i: int) -> bool:
        return 0 <= i < len(self.mask) and self.mask[i] == 1

    def match(self, i: int) -> int | None:
        return self.pairs.get(i)

    def next_code(self, pattern: re.Pattern[str], start: int) -> re.Match[str] | None:
        pos = start
        while True:
            m = pattern.search(self.text, pos)
            if m is None:
                return None
            if self.is_code(m.start()):
                return m
            pos = m.end() if m.end() > m.start() else m.start() + 1


def _skip_block_comment(text: str, i: int) -> int:
    depth = 1
    j = i + 2
    n = len(text)
    while j < n and depth:
        if text.startswith("/*", j):
            depth += 1
            j += 2
        elif text.startswith("*/", j):
            depth -= 1
            j += 2
        else:
            j += 1
    return j


def _skip_plain_string(text: str, i: int) -> int:
    """``i`` points at the opening quote; returns the index after the closing quote."""
    j = i + 1
    n = len(text)
    while j < n:
        c = text[j]
        if c == "\\":
            j += 2
            continue
        if c == '"':
            return j + 1
        j += 1
    return n


# --------------------------------------------------------------------------- text utils


def line_start(text: str, pos: int) -> int:
    return text.rfind("\n", 0, pos) + 1


def line_end(text: str, pos: int) -> int:
    j = text.find("\n", pos)
    return len(text) if j < 0 else j


def indent_of(text: str, pos: int) -> str:
    ls = line_start(text, pos)
    m = re.match(r"[ \t]*", text[ls:])
    return m.group(0) if m else ""


def add_sentinel(text: str, pos: int, reason: str) -> str:
    """Attach a TODO sentinel to the end of the line containing ``pos``."""
    le = line_end(text, pos)
    token = f"{SENT_OPEN}{reason}{SENT_CLOSE}"
    if token in text[line_start(text, pos):le]:
        return text
    return text[:le] + token + text[le:]


def materialize_todos(text: str) -> str:
    """Turn end-of-line sentinels into TODO comment lines above their line."""
    if SENT_OPEN not in text:
        return text
    out_lines: list[str] = []
    for line in text.split("\n"):
        reasons: list[str] = []
        for m in re.finditer(f"{SENT_OPEN}(.*?){SENT_CLOSE}", line):
            if m.group(1) not in reasons:
                reasons.append(m.group(1))
        if not reasons:
            out_lines.append(line)
            continue
        clean = re.sub(f"{SENT_OPEN}.*?{SENT_CLOSE}", "", line)
        indent = re.match(r"[ \t]*", clean).group(0)
        existing: list[str] = []
        k = len(out_lines) - 1
        while k >= 0 and out_lines[k].strip().startswith(TODO_PREFIX.strip()):
            existing.append(out_lines[k].strip())
            k -= 1
        for reason in reasons:
            todo = f"{TODO_PREFIX}{reason}"
            if todo not in existing:
                out_lines.append(f"{indent}{todo}")
        out_lines.append(clean)
    return "\n".join(out_lines)


def split_top_level(sc: Scanner, start: int, end: int) -> list[tuple[int, int]]:
    """Split ``text[start:end]`` at top-level commas; returns (s, e) spans."""
    spans: list[tuple[int, int]] = []
    depth = 0
    i = start
    item_start = start
    text = sc.text
    while i < end:
        if not sc.is_code(i):
            i += 1
            continue
        c = text[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            spans.append((item_start, i))
            item_start = i + 1
        i += 1
    if text[item_start:end].strip():
        spans.append((item_start, end))
    return spans


def strip_span(text: str, span: tuple[int, int]) -> str:
    return text[span[0]:span[1]].strip()


# --------------------------------------------------------------------------- function spans


@dataclass
class FnSpan:
    name: str
    ordinal: int  # index among same-named functions, in file order
    sig_start: int
    params: tuple[int, int]  # inside the parens
    body: tuple[int, int]  # inside the braces

    @property
    def key(self) -> tuple[str, int]:
        return (self.name, self.ordinal)


_FN_RE = re.compile(r"\bfn\s+(\w+)\s*(<|\()")


def iter_fns(sc: Scanner) -> list[FnSpan]:
    text = sc.text
    out: list[FnSpan] = []
    counts: Counter[str] = Counter()
    pos = 0
    while True:
        m = sc.next_code(_FN_RE, pos)
        if m is None:
            break
        pos = m.end()
        i = m.end() - 1
        if text[i] == "<":
            depth = 0
            while i < len(text):
                if sc.is_code(i):
                    if text[i] == "<":
                        depth += 1
                    elif text[i] == ">":
                        depth -= 1
                        if depth == 0:
                            i += 1
                            break
                i += 1
            while i < len(text) and text[i] != "(":
                i += 1
            if i >= len(text):
                break
        p_open = i
        p_close = sc.match(p_open)
        if p_close is None:
            continue
        j = p_close + 1
        body_open = None
        while j < len(text):
            if sc.is_code(j):
                if text[j] == ";":
                    break
                if text[j] == "{":
                    body_open = j
                    break
            j += 1
        if body_open is None:
            continue
        body_close = sc.match(body_open)
        if body_close is None:
            continue
        name = m.group(1)
        out.append(FnSpan(name, counts[name], m.start(), (p_open + 1, p_close), (body_open + 1, body_close)))
        counts[name] += 1
    return out


def enclosing_fn(fns: list[FnSpan], pos: int) -> FnSpan | None:
    inside = [f for f in fns if f.body[0] <= pos < f.body[1]]
    if not inside:
        return None
    return min(inside, key=lambda f: f.body[1] - f.body[0])


# --------------------------------------------------------------------------- context


@dataclass
class Ctx:
    path: Path
    credential_methods: bool
    frozen: dict[tuple[str, int], str] = field(default_factory=dict)  # fn key -> reason
    adapter_map: dict[str, str] = field(default_factory=dict)

    def frozen_spans(self, sc: Scanner, fns: list[FnSpan] | None = None) -> list[tuple[int, int]]:
        if not self.frozen:
            return []
        fns = fns if fns is not None else iter_fns(sc)
        return [(f.sig_start, f.body[1]) for f in fns if f.key in self.frozen]


def in_spans(pos: int, spans: list[tuple[int, int]]) -> bool:
    return any(s <= pos < e for s, e in spans)


_ROUTER_CALL_RE = re.compile(r"\.check_access\(|\brouter\s*\.\s*call\(")
_LITERAL_RE = re.compile(r"(?<![\w:])(?:s3s::)?S3Request\s*\{")
_TEST_ATTR_RE = re.compile(r"#\[(?:tokio::)?test\]|#\[cfg\(test\)\]|#\[serial\]")


def test_region_start(text: str, path: Path) -> int:
    """Offset where test-only code starts (whole file for ``tests.rs`` modules)."""
    if path.name == "tests.rs" or "/tests/" in path.as_posix():
        return 0
    m = re.search(r"^#\[cfg\(test\)\]\s*\n(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+", text, re.M)
    return m.start() if m else len(text)


def is_test_code(text: str, pos: int, fns: list[FnSpan], test_start: int) -> bool:
    if pos >= test_start:
        return True
    f = enclosing_fn(fns, pos)
    if f is None:
        return False
    above = text[max(0, line_start(text, f.sig_start) - 200):f.sig_start]
    return _TEST_ATTR_RE.search(above) is not None


def detect_frozen_fns(text: str, path: Path) -> dict[tuple[str, int], str]:
    """Functions that must keep the s3s request types, with the reason."""
    sc = Scanner(text)
    fns = iter_fns(sc)
    frozen: dict[tuple[str, int], str] = {}
    for f in fns:
        if _ROUTER_CALL_RE.search(text[f.body[0]:f.body[1]]):
            frozen[f.key] = R_ROUTER
    builders = {m.group(1) for m in re.finditer(r"\brouter\s*\.\s*call\(\s*(\w+)\(", text)}
    for f in fns:
        if f.name in builders:
            frozen[f.key] = R_ROUTER
    test_start = test_region_start(text, path)
    pos = 0
    while True:
        m = sc.next_code(_LITERAL_RE, pos)
        if m is None:
            break
        pos = m.end()
        if is_test_code(text, m.start(), fns, test_start):
            continue
        f = enclosing_fn(fns, m.start())
        if f is not None and f.key not in frozen:
            frozen[f.key] = R_PROD_LITERAL
    return frozen


# --------------------------------------------------------------------------- request identifiers


_REQ_PARAM_RE = re.compile(r"(?<![\w&])(?:mut\s+)?(\w+)\s*:\s*&?\s*(?:mut\s+)?(?:s3s::)?(?:S3Request<[^>]*>|AdminRequest)\b")


def request_scopes(sc: Scanner, fns: list[FnSpan]) -> list[tuple[str, int, int]]:
    """Identifiers typed as the admin request, with the span they are valid in."""
    scopes: list[tuple[str, int, int]] = []
    for f in fns:
        params = sc.text[f.params[0]:f.params[1]]
        for m in _REQ_PARAM_RE.finditer(params):
            if m.group(1) != "self":
                scopes.append((m.group(1), f.sig_start, f.body[1]))
    return scopes


def is_request_ident(name: str, pos: int, scopes: list[tuple[str, int, int]]) -> bool:
    if name == "req":
        return True
    return any(n == name and s <= pos < e for n, s, e in scopes)


# --------------------------------------------------------------------------- rewrite helpers


def rewrite(text: str, pattern: re.Pattern[str], fn, *, only_code: bool = True, ctx: Ctx | None = None) -> str:
    """Apply ``fn(match, sc, scopes) -> (replacement|None, reason|None)`` back to front."""
    sc = Scanner(text)
    fns = iter_fns(sc)
    frozen = ctx.frozen_spans(sc, fns) if ctx else []
    scopes = request_scopes(sc, fns)
    out = text
    for m in reversed(list(pattern.finditer(text))):
        if only_code and not sc.is_code(m.start()):
            continue
        if frozen and in_spans(m.start(), frozen):
            continue
        res = fn(m, sc, scopes)
        if res is None:
            continue
        repl, reason = res
        if repl is not None:
            out = out[:m.start()] + repl + out[m.end():]
        if reason:
            out = add_sentinel(out, m.start(), reason)
    return materialize_todos(out)


def simple_sub(text: str, pattern: str, repl: str, *, ctx: Ctx | None = None, only_code: bool = True) -> str:
    return rewrite(text, re.compile(pattern), lambda m, sc, scopes: (m.expand(repl), None), ctx=ctx, only_code=only_code)


def req_sub(text: str, pattern: str, repl: str | None, reason: str | None, ctx: Ctx, *, group: int = 1) -> str:
    """Substitute ``pattern`` whose ``group`` captures the request identifier."""

    def fn(m, sc, scopes):
        if not is_request_ident(m.group(group), m.start(group), scopes):
            return None
        return (m.expand(repl) if repl is not None else None, reason)

    return rewrite(text, re.compile(pattern), fn, ctx=ctx)


# --------------------------------------------------------------------------- phase: request literals (tests)


_STR = r"\"(?:[^\"\\]|\\.)*\""


def _unwrap_uri(expr: str) -> str:
    m = re.fullmatch(rf"(?:http::)?Uri::from_static\(\s*({_STR})\s*\)", expr, re.S)
    if m:
        return m.group(1)
    m = re.fullmatch(rf"({_STR})\s*\.parse\(\)\s*\.(?:unwrap|expect)\([^()]*\)", expr, re.S)
    if m:
        return m.group(1)
    return expr


def _unwrap_into(expr: str) -> str:
    m = re.fullmatch(rf"({_STR})\s*\.into\(\)", expr, re.S)
    return m.group(1) if m else expr


def _unwrap_secret(expr: str) -> str:
    m = re.fullmatch(r"(?:s3s::auth::|s3_auth::)?SecretKey::from\((.*)\)", expr, re.S)
    return _unwrap_into(m.group(1).strip()) if m else _unwrap_into(expr)


_CRED_LIT_RE = re.compile(r"\s*Some\(\s*(?:s3s::auth::|s3_auth::)?Credentials\s*\{")


def _parse_credentials(sc: Scanner, span: tuple[int, int]) -> tuple[str, str] | None:
    text = sc.text
    m = _CRED_LIT_RE.match(text, span[0])
    if not m or m.end() > span[1]:
        return None
    open_brace = m.end() - 1
    close = sc.match(open_brace)
    if close is None:
        return None
    fields: dict[str, str] = {}
    for s, e in split_top_level(sc, open_brace + 1, close):
        item = text[s:e].strip()
        fm = re.match(r"(\w+)\s*:\s*(.*)$", item, re.S)
        if fm:
            fields[fm.group(1)] = fm.group(2).strip()
        elif re.fullmatch(r"\w+", item):
            fields[item] = item
    if set(fields) != {"access_key", "secret_key"}:
        return None
    return (_unwrap_into(fields["access_key"]), _unwrap_secret(fields["secret_key"]))


def _reindent(expr: str, old_indent: str, new_indent: str) -> str:
    lines = expr.split("\n")
    if len(lines) == 1:
        return expr
    out = [lines[0]]
    for ln in lines[1:]:
        if ln.startswith(old_indent):
            ln = new_indent + ln[len(old_indent):]
        out.append(ln)
    return "\n".join(out)


def convert_request_literals(text: str, ctx: Ctx) -> str:
    pos = 0
    while True:
        sc = Scanner(text)
        m = sc.next_code(_LITERAL_RE, pos)
        if m is None:
            break
        open_brace = m.end() - 1
        close_brace = sc.match(open_brace)
        if close_brace is None:
            pos = m.end()
            continue
        if in_spans(m.start(), ctx.frozen_spans(sc)):
            pos = m.end() + 1
            continue

        def give_up() -> tuple[str, int]:
            return materialize_todos(add_sentinel(text, m.start(), R_LITERAL)), close_brace + 1

        fields: dict[str, tuple[int, int]] = {}
        ok = True
        for s, e in split_top_level(sc, open_brace + 1, close_brace):
            item = text[s:e]
            fm = re.match(r"\s*(\w+)\s*:\s*", item)
            if fm:
                fields[fm.group(1)] = (s + fm.end(), e)
            elif re.fullmatch(r"\s*\w+\s*", item):
                name = item.strip()
                fields[name] = (s + item.index(name), s + item.index(name) + len(name))
            else:
                ok = False
        needed = {"method", "uri", "headers", "extensions", "input"}
        optional = {"credentials", "region", "service", "trailing_headers"}
        if not ok or not needed <= set(fields) or set(fields) - needed - optional:
            text, pos = give_up()
            continue
        creds = None
        if "credentials" in fields and strip_span(text, fields["credentials"]) != "None":
            creds = _parse_credentials(sc, fields["credentials"])
            if creds is None:
                text, pos = give_up()
                continue
        if "trailing_headers" in fields and strip_span(text, fields["trailing_headers"]) != "None":
            text, pos = give_up()
            continue

        base_indent = indent_of(text, m.start())
        chain_indent = base_indent + "    "

        def value(name: str) -> str:
            s, e = fields[name]
            return _reindent(text[s:e].strip(), indent_of(text, s), chain_indent)

        parts = [f"crate::admin::operation::test_support::TestRequest::new({value('method')}, {_unwrap_uri(value('uri'))})"]
        parts.append(f".headers({value('headers')})")
        parts.append(f".extensions({value('extensions')})")
        parts.append(f".body({value('input')})")
        if creds is not None:
            parts.append(f".credentials({creds[0]}, {creds[1]}, None)")
        reasons: list[str] = []
        for opt in ("region", "service"):
            if opt in fields and strip_span(text, fields[opt]) != "None":
                parts.append(f".{opt}({value(opt)})")
                reasons.append(R_LITERAL_REGION)
        parts.append(".build()")
        replacement = parts[0] + "".join(f"\n{chain_indent}{p}" for p in parts[1:])
        text = text[:m.start()] + replacement + text[close_brace + 1:]
        for reason in reasons:
            text = add_sentinel(text, m.start(), reason)
        text = materialize_todos(text)
        pos = m.start() + len(replacement)
    return text


# --------------------------------------------------------------------------- phase: types


def rename_types(text: str, ctx: Ctx) -> str:
    text = simple_sub(text, r"(?<![\w:])(?:s3s::)?S3Request<\s*(?:s3s::)?Body\s*>", "AdminRequest", ctx=ctx)
    text = simple_sub(
        text,
        r"(?<![\w:])(?:s3s::)?S3Response<\s*\(\s*(?:http::|hyper::)?StatusCode\s*,\s*(?:s3s::)?Body\s*\)\s*>",
        "AdminResponse",
        ctx=ctx,
    )
    text = rewrite(text, re.compile(r"(?<![\w:])(?:s3s::)?S3Response<"), lambda m, sc, scopes: (None, R_RESP_TYPE), ctx=ctx)
    text = rewrite(text, re.compile(r"(?<![\w:])(?:s3s::)?S3Request<"), lambda m, sc, scopes: (None, R_REQ_TYPE), ctx=ctx)
    sc = Scanner(text)
    for f in reversed(iter_fns(sc)):
        reason = ctx.frozen.get(f.key)
        if reason:
            text = add_sentinel(text, f.sig_start, reason)
    return materialize_todos(text)


def qualify_frozen(text: str, ctx: Ctx) -> str:
    """Frozen functions keep s3s types; spell them out so the s3s imports can go."""
    if not ctx.frozen:
        return text
    sc = Scanner(text)
    spans = ctx.frozen_spans(sc)
    out = text
    for m in reversed(list(re.finditer(r"(?<![\w:])(S3Request|S3Response|Body)\b", text))):
        if sc.is_code(m.start()) and in_spans(m.start(), spans):
            out = out[:m.start()] + "s3s::" + out[m.start():]
    return out


# --------------------------------------------------------------------------- phase: helpers and fields

_HELPER_RENAMES = {
    "authorize_admin_request": "authorize_admin_envelope",
    "object_store_from_req": "object_store_from_envelope",
    "app_context_from_req": "app_context_from_envelope",
}


def rewrite_helpers(text: str, ctx: Ctx) -> str:
    text = simple_sub(text, r"\b(\w+)_from_extensions\(\s*&(\w+)\.extensions\b", r"\1_from_envelope(\2.envelope()", ctx=ctx)
    text = req_sub(
        text,
        r"\bKmsAdminAudit::from_request\(\s*&(\w+)\.extensions\s*,\s*&\1\.headers\s*,\s*",
        r"KmsAdminAudit::from_envelope(\1.envelope(), ",
        None,
        ctx,
    )
    text = req_sub(text, r"\bCallerIdentity::resolve\(\s*&?(\w+)\s*\)", r"CallerIdentity::resolve_envelope(\1.envelope())", None, ctx)
    text = req_sub(text, r"\b(AccountAudit\w*)::from_request\(\s*&?(\w+)\s*\)", r"\1::from_envelope(\2.envelope())", None, ctx, group=2)

    rx = re.compile(r"\bauthorize_admin_request\((\s*)&?(\w+)(\s*),")

    def auth_fn(m, sc, scopes):
        if sc.is_code(m.start()) and not is_request_ident(m.group(2), m.start(2), scopes):
            return None
        return (f"authorize_admin_envelope({m.group(1)}{m.group(2)}.envelope(){m.group(3)},", None)

    # Also inside string literals: source-inspecting tests grep their own file.
    text = rewrite(text, rx, auth_fn, ctx=ctx, only_code=False)
    text = req_sub(text, r"\b(object_store|app_context)_from_req\(\s*&?(\w+)\s*\)", r"\1_from_envelope(\2.envelope())", None, ctx, group=2)
    return text


SEP = r"(\s*\.\s*)"  # `req.field` or `req\n    .field`


def rewrite_fields(text: str, ctx: Ctx) -> str:
    rx = re.compile(
        r"\b(\w+)\s*\.\s*extensions\s*\.\s*get::<Option<RemoteAddr>>\(\)\s*"
        r"(?:\.\s*and_then\(\|(\w+)\|\s*\2\.map\(\|(\w+)\|\s*\3\.0((?:\.\w+\(\))*)\)\)|\.\s*copied\(\)\s*\.\s*flatten\(\))"
    )

    def remote_fn(m, sc, scopes):
        if not is_request_ident(m.group(1), m.start(1), scopes):
            return None
        tail = m.group(4) or ""
        if tail:
            return (f"{m.group(1)}.remote_addr().map(|{m.group(3)}| {m.group(3)}{tail})", None)
        return (f"{m.group(1)}.remote_addr()", None)

    text = rewrite(text, rx, remote_fn, ctx=ctx)
    text = req_sub(text, rf"\b(\w+){SEP}extensions\s*\.\s*get::<ReqInfo>\(\)", r"\1\2req_info()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}extensions\s*\.\s*get_mut::<ReqInfo>\(\)", r"\1\2req_info_mut()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}extensions\s*\.\s*insert\((?=ReqInfo\s*\{{|req_info\b)", r"\1\2install_req_info(", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}extensions\s*\.\s*get::<ClientInfo>\(\)", r"\1\2client_info()", None, ctx)
    text = req_sub(
        text,
        rf"request_context:\s*(\w+){SEP}extensions\s*\.\s*get\(\)\s*\.\s*cloned\(\)",
        r"request_context: \1\2request_context().cloned()",
        None,
        ctx,
    )
    text = req_sub(text, rf"\b(\w+){SEP}extensions\b", None, R_EXT, ctx)

    # uri
    text = req_sub(text, rf"\b(\w+){SEP}uri\s*=(?!=)", None, R_URI_ASSIGN, ctx)
    text = req_sub(text, rf"&(\w+){SEP}uri\b(?!\s*\()", r"\1\2uri()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}uri\b(?![\s(.]|\s*=[^=])", r"\1\2uri()", R_BORROW, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}uri\b(?!\s*\()(?!\s*=[^=])", r"\1\2uri()", None, ctx)
    # headers
    text = req_sub(
        text, rf"\b(\w+){SEP}headers\s*\.\s*(?:insert|append|remove|clear|entry|get_mut|extend|drain)\b", None, R_HDR_MUT, ctx
    )
    text = req_sub(text, rf"&(\w+){SEP}headers\b(?!\s*\()", r"\1\2headers()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}headers\b(?![\s(.]|\s*=[^=])", r"\1\2headers()", R_BORROW, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}headers\b(?!\s*\()(?!\s*=[^=])", r"\1\2headers()", None, ctx)
    # method
    text = req_sub(text, rf"\b(\w+){SEP}method\s*=(?!=)", None, R_METHOD_ASSIGN, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}method\b(?!\s*\()(?!\s*=[^=])", r"\1\2method()", None, ctx)
    # region / service
    text = req_sub(text, rf"\b(\w+){SEP}(?:region|service)\b(?!\s*\()", None, R_REGION, ctx)
    # credentials
    text = req_sub(text, rf"\b(\w+){SEP}credentials\s*=(?!=)", None, R_CRED_ASSIGN, ctx)
    text = req_sub(
        text,
        rf"let\s+Some\(\s*ref\s+(\w+)\s*\)\s*=\s*(\w+){SEP}credentials\b(?!\s*\()",
        r"let Some(\1) = \2\3credentials()",
        None,
        ctx,
        group=2,
    )
    text = req_sub(text, rf"&(\w+){SEP}credentials\b(?!\s*\()", r"\1\2credentials()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}credentials\s*\.\s*as_ref\(\)", r"\1\2credentials()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}credentials\b(?![\s(.]|\s*=[^=])", r"\1\2credentials()", R_BORROW, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}credentials\b(?!\s*\()(?!\s*=[^=])", r"\1\2credentials()", None, ctx)
    text = credential_fields(text, ctx)
    return text


_CRED_BIND_RE = re.compile(r"Some\(\s*(\w+)\s*\)\s*=\s*(\w+)\s*\.\s*credentials\(\)")


def credential_fields(text: str, ctx: Ctx) -> str:
    """Flag (or with ``--credential-methods`` rewrite) field access on ``req.credentials()`` bindings."""
    sc = Scanner(text)
    fns = iter_fns(sc)
    frozen = ctx.frozen_spans(sc, fns)
    scopes = request_scopes(sc, fns)
    edits: list[tuple[int, int, str | None, str | None]] = []
    seen: set[int] = set()
    for m in _CRED_BIND_RE.finditer(text):
        if not sc.is_code(m.start()) or in_spans(m.start(), frozen):
            continue
        if not is_request_ident(m.group(2), m.start(2), scopes):
            continue
        name = m.group(1)
        f = enclosing_fn(fns, m.start())
        if f is None:
            continue
        region_start = line_end(text, m.end())
        region_end = f.body[1]
        esc = re.escape(name)
        rebind = re.compile(rf"\blet\s+(?:mut\s+)?(?:\(\s*)?(?:\w+\s*,\s*)*{esc}\b|\blet\s+Some\(\s*(?:ref\s+)?{esc}\s*\)")
        rb = sc.next_code(rebind, region_start)
        if rb is not None and rb.start() < region_end:
            # The shadowing statement's initializer may still read the old binding.
            semi = sc.next_code(re.compile(";"), rb.end())
            region_end = min(region_end, semi.end() if semi else rb.end())
        field_rx = re.compile(rf"(&)?(?<![\w.]){esc}\.(access_key|secret_key|session_token)\b(\.(\w+)\(\))?(?!\s*\()")
        pos = region_start
        while True:
            fm = sc.next_code(field_rx, pos)
            if fm is None or fm.start() >= region_end:
                break
            pos = fm.end()
            if fm.start() in seen:
                continue
            seen.add(fm.start())
            amp, fld, call, callee = fm.group(1), fm.group(2), fm.group(3), fm.group(4)
            repl = None
            if ctx.credential_methods:
                if call is None and (amp or fld == "session_token"):
                    repl = f"{name}.{fld}()"
                elif callee in {"expose", "as_str", "as_deref"} and not amp:
                    repl = f"{name}.{fld}()"
                elif callee == "clone" and not amp and fld != "session_token":
                    repl = f"{name}.{fld}().to_string()"
            edits.append((fm.start(), fm.end(), repl, None if repl else R_CRED_FIELD))
    out = text
    for s, e, repl, reason in sorted(edits, reverse=True):
        if repl is not None:
            out = out[:s] + repl + out[e:]
        if reason:
            out = add_sentinel(out, s, reason)
    return materialize_todos(out)


# --------------------------------------------------------------------------- phase: body


def rewrite_body(text: str, ctx: Ctx) -> str:
    text = req_sub(text, r"std::mem::take\(\s*&mut\s+(\w+)\s*\.\s*input\s*\)", r"\1.take_body()", None, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}input\s*=(?!=)", None, R_INPUT_ASSIGN, ctx)
    text = req_sub(text, rf"\b(\w+){SEP}input\b(?!\s*\()(?!\s*=[^=])", r"\1\2take_body()", None, ctx)
    return text


# --------------------------------------------------------------------------- phase: responses

_RESP_CTOR_RE = re.compile(r"(?<![\w:])(?:s3s::)?S3Response::(new|with_headers)\(")


def rewrite_responses(text: str, ctx: Ctx) -> str:
    pos = 0
    while True:
        sc = Scanner(text)
        m = sc.next_code(_RESP_CTOR_RE, pos)
        if m is None:
            break
        if in_spans(m.start(), ctx.frozen_spans(sc)):
            pos = m.end()
            continue
        kind = m.group(1)
        p_open = m.end() - 1
        p_close = sc.match(p_open)
        if p_close is None:
            pos = m.end()
            continue
        args = split_top_level(sc, p_open + 1, p_close)
        expected = 1 if kind == "new" else 2
        elems: list[str] = []
        if len(args) == expected:
            s, e = args[0]
            first = text[s:e]
            t_open = s + (len(first) - len(first.lstrip()))
            t_close = sc.match(t_open) if text[t_open] == "(" else None
            if t_close is not None and not text[t_close + 1:e].strip():
                inner = split_top_level(sc, t_open + 1, t_close)
                if len(inner) == 2:
                    elems = [strip_span(text, inner[0]), strip_span(text, inner[1])]
        if not elems:
            text = materialize_todos(add_sentinel(text, m.start(), R_RESP_CTOR))
            pos = p_close + 1
            continue
        if kind == "with_headers":
            elems.append(strip_span(text, args[1]))
        close_ls = line_start(text, p_close)
        if "\n" in text[m.start():p_close + 1] and not text[close_ls:p_close].strip():
            close_indent = text[close_ls:p_close]
            elem_indent = close_indent + "    "
            body = "".join(f"\n{elem_indent}{el}," for el in elems)
            replacement = f"AdminResponse::{kind}({body}\n{close_indent})"
        else:
            replacement = f"AdminResponse::{kind}({', '.join(elems)})"
        text = text[:m.start()] + replacement + text[p_close + 1:]
        pos = m.start() + len(replacement)
    text = simple_sub(text, r"(?<![\w:])s3s::Body::", "Body::", ctx=ctx)

    # Field access on owned responses.
    sc = Scanner(text)
    fns = iter_fns(sc)
    bound: list[tuple[str, int, int]] = []
    for m in re.finditer(r"\blet\s+(?:mut\s+)?(\w+)\s*=\s*AdminResponse::", text):
        if not sc.is_code(m.start()):
            continue
        f = enclosing_fn(fns, m.start())
        bound.append((m.group(1), m.end(), f.body[1] if f else len(text)))

    def resp_field(m, sc, scopes):
        if any(n == m.group(1) and s <= m.start() < e for n, s, e in bound):
            return (None, R_RESP_FIELD)
        return None

    text = rewrite(text, re.compile(r"\b(\w+)\.(?:headers|status|body|extensions|output)\b"), resp_field, ctx=ctx)
    frozen_names = {name for name, _ in ctx.frozen}

    def output_fn(m, sc, scopes):
        name = m.group(1)
        if name == "self":
            return None
        f = enclosing_fn(fns, m.start())
        if f is not None and frozen_names:
            body_before = text[f.body[0]:m.start()]
            binding = None
            for bm in re.finditer(rf"\blet\s+(?:mut\s+)?{re.escape(name)}\s*=([^;]*);", body_before):
                binding = bm.group(1)
            if binding and any(re.search(rf"\b{re.escape(fz)}\s*\(", binding) for fz in frozen_names):
                return None
        return (None, R_OUTPUT)

    text = rewrite(text, re.compile(r"(?<![\w.])(\w+)\.output\b"), output_fn, ctx=ctx)
    return text


# --------------------------------------------------------------------------- phase: mut params

_MUT_NEED_TMPL = (
    r"\b{name}\s*\.\s*(?:take_body|req_info_mut|install_req_info|envelope_mut)\(|&mut\s+{name}\b|\b{name}\s*\.\s*\w+\s*=[^=]"
    r"|\b{name}\s*\.\s*headers\(\)\s*\.\s*(?:insert|append|remove|clear|entry|get_mut|extend|drain)\("
)


def normalize_mut(text: str, ctx: Ctx) -> str:
    sc = Scanner(text)
    fns = iter_fns(sc)
    frozen = ctx.frozen_spans(sc, fns)
    edits: list[tuple[int, int, str]] = []
    for f in fns:
        if in_spans(f.sig_start, frozen):
            continue
        params = text[f.params[0]:f.params[1]]
        body = text[f.body[0]:f.body[1]]
        for m in re.finditer(r"(?<![\w&])(mut\s+)?(\w+)\s*:\s*AdminRequest\b", params):
            if params[:m.start()].rstrip().endswith("&"):
                continue
            name = m.group(2)
            needs = re.search(_MUT_NEED_TMPL.format(name=re.escape(name)), body) is not None
            has = m.group(1) is not None
            abs_start = f.params[0] + m.start()
            if needs and not has:
                edits.append((abs_start, abs_start, "mut "))
            elif has and not needs:
                edits.append((abs_start, abs_start + len(m.group(1)), ""))
    out = text
    for s, e, repl in sorted(edits, reverse=True):
        out = out[:s] + repl + out[e:]
    return out


# --------------------------------------------------------------------------- use-tree model


@dataclass
class UseTree:
    path: list[str]
    children: list["UseTree"] | None = None
    alias: str | None = None

    @property
    def leaf(self) -> str:
        return self.path[-1] if self.path else ""


def parse_use_tree(s: str) -> UseTree:
    s = s.strip()
    pos = 0

    def ws(pattern: str) -> re.Match[str] | None:
        nonlocal pos
        m = re.match(pattern, s[pos:])
        if m:
            pos += m.end()
        return m

    def parse_group() -> list[UseTree]:
        children: list[UseTree] = []
        while not ws(r"\s*\}"):
            children.append(parse_node())
            ws(r"\s*,")
        return children

    def parse_node() -> UseTree:
        path: list[str] = []
        while True:
            m = ws(r"\s*(\w+|\*)")
            if m:
                path.append(m.group(1))
                if ws(r"\s*::"):
                    if ws(r"\s*\{"):
                        return UseTree(path, parse_group())
                    continue
                am = ws(r"\s*as\s+(\w+)")
                return UseTree(path, None, am.group(1) if am else None)
            if ws(r"\s*\{"):
                return UseTree(path, parse_group())
            raise ValueError(f"cannot parse use tree: {s!r} at {pos}")

    return parse_node()


def _version_key(seg: str) -> tuple:
    if seg == "self":
        return (0,)
    if seg == "super":
        return (1,)
    if seg == "crate":
        return (2,)
    parts = re.findall(r"\d+|\D+", seg)
    return (3,) + tuple((1, int(p)) if p.isdigit() else (0, p) for p in parts)


def tree_sort_key(t: UseTree) -> tuple:
    key = tuple(_version_key(p) for p in t.path)
    if t.children is not None:
        key = key + ((9,),)
    return key


def serialize_tree(t: UseTree) -> str:
    base = "::".join(t.path)
    if t.children is None:
        return base + (f" as {t.alias}" if t.alias else "")
    kids = ", ".join(serialize_tree(c) for c in sorted(t.children, key=tree_sort_key))
    return f"{base}::{{{kids}}}" if base else f"{{{kids}}}"


def _format_tree(t: UseTree, indent: str, prefix_len: int, suffix_len: int) -> str:
    """rustfmt-like layout: one line if it fits, vertical if nested groups, else fill."""
    one = serialize_tree(t)
    if t.children is None:
        return one
    kids = sorted(t.children, key=tree_sort_key)
    nested = any(c.children is not None for c in kids)
    if not nested and len(indent) + prefix_len + len(one) + suffix_len <= MAX_WIDTH:
        return one
    base = "::".join(t.path)
    inner = indent + "    "
    head = f"{base}::{{" if base else "{"
    if nested:
        lines = [head]
        for c in kids:
            lines.append(inner + _format_tree(c, inner, 0, 1) + ",")
        lines.append(indent + "}")
        return "\n".join(lines)
    lines = [head]
    cur = ""
    for c in kids:
        piece = serialize_tree(c) + ","
        if cur and len(inner) + len(cur) + 1 + len(piece) > MAX_WIDTH:
            lines.append(inner + cur)
            cur = piece
        else:
            cur = f"{cur} {piece}" if cur else piece
    lines.append(inner + cur)
    lines.append(indent + "}")
    return "\n".join(lines)


def serialize_use(t: UseTree, indent: str, attrs: str = "") -> str:
    return f"{attrs}{indent}use {_format_tree(t, indent, 4, 1)};"


def collapse(t: UseTree) -> UseTree:
    """Fold ``a::{b}`` into ``a::b`` and ``a::{self}`` into ``a``."""
    if t.children is None:
        return t
    t.children = [collapse(c) for c in t.children]
    if len(t.children) == 1:
        c = t.children[0]
        if c.path == ["self"] and c.children is None:
            return UseTree(t.path, None, c.alias)
        if c.path and c.path[0] != "self":
            return UseTree(t.path + c.path, c.children, c.alias)
    return t


def leaves(t: UseTree, prefix: list[str] | None = None) -> list[tuple[list[str], UseTree]]:
    """All leaves as (full path, node)."""
    full = (prefix or []) + t.path
    if t.children is None:
        return [(full, t)]
    out: list[tuple[list[str], UseTree]] = []
    for c in t.children:
        out.extend(leaves(c, full))
    return out


@dataclass
class UseStmt:
    start: int  # includes attribute lines directly above
    end: int  # just after ';'
    indent: str
    attrs: str
    tree: UseTree
    top_level: bool


_USE_RE = re.compile(r"^([ \t]*)(pub(?:\([^)]*\))?\s+)?use\s", re.M)


def find_use_stmts(text: str, sc: Scanner | None = None) -> list[UseStmt]:
    sc = sc or Scanner(text)
    out: list[UseStmt] = []
    for m in _USE_RE.finditer(text):
        if not sc.is_code(m.start() + len(m.group(1))):
            continue
        if m.group(2):
            continue  # `pub use` re-exports are left alone
        i = m.end()
        while i < len(text) and not (sc.is_code(i) and text[i] == ";"):
            i += 1
        if i >= len(text):
            continue
        try:
            tree = parse_use_tree(text[m.end():i])
        except ValueError:
            continue
        start = m.start()
        attrs = ""
        while start > 0:
            pls = line_start(text, start - 1)
            prev = text[pls:start - 1]
            if re.match(r"\s*#\[", prev):
                attrs = prev + "\n" + attrs
                start = pls
                continue
            break
        out.append(UseStmt(start, i + 1, m.group(1), attrs, tree, m.group(1) == ""))
    return out


def replace_stmt(text: str, stmt: UseStmt, new_tree: UseTree | None) -> str:
    end = stmt.end
    if new_tree is None:
        if end < len(text) and text[end] == "\n":
            end += 1
        return text[:stmt.start] + text[end:]
    return text[:stmt.start] + serialize_use(new_tree, stmt.indent, stmt.attrs) + text[end:]


def remove_leaves(tree: UseTree, pred) -> UseTree | None:
    """Drop leaves where ``pred(full_path, node)`` holds; prune/collapse; None when nothing is left."""

    def walk(node: UseTree, prefix: list[str]) -> UseTree | None:
        full = prefix + node.path
        if node.children is None:
            return None if pred(full, node) else node
        kids = [k for k in (walk(c, full) for c in node.children) if k is not None]
        node.children = kids
        return node if kids else None

    out = walk(tree, [])
    return collapse(out) if out is not None else None


def _header_tree(names: list[str]) -> UseTree:
    return collapse(UseTree(["http", "header"], [UseTree([n]) for n in sorted(dict.fromkeys(names), key=_version_key)]))


def _enclosing_block(sc: Scanner, pos: int) -> tuple[int, int]:
    """Span of the innermost ``{ .. }`` containing ``pos`` (whole file when none)."""
    best = (0, len(sc.text))
    for o, c in sc.pairs.items():
        if o < c and sc.text[o] == "{" and o < pos < c and (c - o) < (best[1] - best[0]):
            best = (o, c)
    return best


def edit_use_trees(text: str, editor) -> str:
    """Edit leaves in place.

    ``editor(stmt, full_path, node) -> None`` keeps the leaf, ``[]`` drops it and a
    list of nodes replaces it (paths relative to the leaf's parent group).  Empty
    groups are pruned and single-item groups collapsed afterwards.
    """
    for stmt in reversed(find_use_stmts(text)):
        changed = False

        def walk(node: UseTree, prefix: list[str], is_root: bool) -> list[UseTree]:
            nonlocal changed
            full = prefix + node.path
            if node.children is None:
                res = editor(stmt, full, node)
                if res is None:
                    return [node]
                changed = True
                if is_root and len(res) != 1:
                    return [UseTree(node.path[:-1], res)] if res else []
                return res
            kids: list[UseTree] = []
            for c in node.children:
                kids.extend(walk(c, full, False))
            node.children = kids
            return [node] if kids else []

        new = walk(stmt.tree, [], True)
        if not changed:
            continue
        text = replace_stmt(text, stmt, collapse(new[0]) if new else None)
    return text


def code_uses(text: str, name: str, sc: Scanner | None = None, stmts: list[UseStmt] | None = None) -> bool:
    """True if ``name`` occurs unqualified in code outside ``use`` statements."""
    sc = sc or Scanner(text)
    stmts = stmts if stmts is not None else find_use_stmts(text, sc)
    for m in re.finditer(rf"(?<![\w:]){re.escape(name)}\b", text):
        if sc.is_code(m.start()) and not any(s.start <= m.start() < s.end for s in stmts):
            return True
    return False


def imported_names(stmts: list[UseStmt], *, top_level_only: bool = False) -> set[str]:
    names: set[str] = set()
    for s in stmts:
        if top_level_only and not s.top_level:
            continue
        for path, node in leaves(s.tree):
            names.add(node.alias or path[-1])
    return names


def insert_top_level_use(text: str, new_tree: UseTree) -> str:
    """Insert a top-level ``use`` statement at its rustfmt-sorted position."""
    stmts = [s for s in find_use_stmts(text) if s.top_level]
    line = serialize_use(new_tree, "")
    if not stmts:
        m = re.search(r"^(?!//|#!|\s*$)", text, re.M)
        pos = m.start() if m else 0
        return text[:pos] + line + "\n\n" + text[pos:]
    block = [stmts[0]]
    for s in stmts[1:]:
        gap = text[block[-1].end:s.start]
        if gap.strip() == "" and gap.count("\n") <= 1:
            block.append(s)
        else:
            break
    new_key = tree_sort_key(new_tree)
    for s in block:
        if tree_sort_key(s.tree) > new_key:
            return text[:s.start] + line + "\n" + text[s.start:]
    last = block[-1]
    return text[:last.end] + "\n" + line + text[last.end:]


def _insert_nested_use(text: str, stmt: UseStmt, tree: UseTree) -> str:
    """Insert ``tree`` as a use statement where the (nested) ``stmt`` used to be."""
    return text[:stmt.start] + serialize_use(tree, stmt.indent) + "\n" + text[stmt.start:]


def merge_into_top_level(text: str, root: list[str], names: list[str]) -> str:
    """Add ``root::name`` leaves to an existing top-level statement rooted at ``root`` or insert one."""
    stmts = [s for s in find_use_stmts(text) if s.top_level and s.tree.path[:len(root)] == root]
    new_nodes = [UseTree([n]) for n in names]
    if stmts:
        stmt = stmts[0]
        t = stmt.tree
        if t.path == root and t.children is not None:
            t.children.extend(new_nodes)
            return replace_stmt(text, stmt, t)
        rest = UseTree(t.path[len(root):], t.children, t.alias)
        return replace_stmt(text, stmt, UseTree(root, [rest] + new_nodes))
    return insert_top_level_use(text, collapse(UseTree(root, new_nodes)))


# --------------------------------------------------------------------------- phase: imports

_DROP_FROM_S3S = {"Body", "S3Request", "S3Response"}


def rewrite_imports(text: str, ctx: Ctx) -> str:
    # LegacyAdminAdapter statics: `static X: LegacyAdminAdapter = LegacyAdminAdapter(&HANDLER);`
    rx = re.compile(r"^[ \t]*static\s+(\w+)\s*:\s*LegacyAdminAdapter\s*=\s*LegacyAdminAdapter\(\s*&(\w+)\s*\)\s*;[ \t]*\n", re.M)
    for m in reversed(list(rx.finditer(text))):
        ctx.adapter_map[m.group(1)] = m.group(2)
        text = text[:m.start()] + text[m.end():]
    for adapter, handler in ctx.adapter_map.items():
        text = simple_sub(text, rf"&{re.escape(adapter)}\b", f"&{handler}")

    # router::Operation / router::LegacyAdminAdapter
    def router_editor(stmt, full, node):
        if full[-2:] == ["router", "LegacyAdminAdapter"]:
            return []
        if full[-2:] == ["router", "Operation"] and node.alias is None:
            if len(node.path) >= 2:
                # `use crate::admin::router::Operation;` keeps its slot (test modules rely on it).
                return [UseTree(node.path[:-2] + ["operation", "Operation"])]
            return []  # group member: re-imported from crate::admin::operation below
        return None

    text = edit_use_trees(text, router_editor)

    # s3s imports: drop Body/S3Request/S3Response, move header::* to http::header
    sc = Scanner(text)
    stmts = find_use_stmts(text, sc)
    pending_top: list[list[str]] = []
    for stmt in reversed(stmts):
        if stmt.tree.path[:1] != ["s3s"]:
            continue
        headers: list[str] = []
        dropped = False
        for full, node in leaves(stmt.tree):
            if node.alias is not None:
                continue
            if full[:2] == ["s3s", "header"] and len(full) == 3:
                headers.append(full[2])
            elif len(full) == 2 and full[1] in _DROP_FROM_S3S:
                dropped = True
        if not headers and not dropped:
            continue

        def gone(full, node):
            if node.alias is not None:
                return False
            return (full[:2] == ["s3s", "header"] and len(full) == 3) or (len(full) == 2 and full[1] in _DROP_FROM_S3S)

        new_tree = remove_leaves(stmt.tree, gone)
        if stmt.top_level:
            text = replace_stmt(text, stmt, new_tree)
            if headers:
                pending_top.append(headers)
            continue
        # Nested module: the http import takes the s3s import's slot, deduplicated within the block.
        block = _enclosing_block(sc, stmt.start)
        scope = {
            path[-1]
            for s2 in stmts
            if block[0] <= s2.start < block[1]
            for path, _ in leaves(s2.tree)
            if path[:2] in (["http", "header"], ["hyper", "header"])
        }
        names = [h for h in dict.fromkeys(headers) if h not in scope]
        pieces = []
        if names:
            pieces.append(serialize_use(_header_tree(names), stmt.indent))
        if new_tree is not None:
            pieces.append(serialize_use(new_tree, stmt.indent, stmt.attrs))
        if pieces:
            text = text[:stmt.start] + "\n".join(pieces) + text[stmt.end:]
        else:
            text = replace_stmt(text, stmt, None)
    for headers in pending_top:
        top_stmts = [s2 for s2 in find_use_stmts(text) if s2.top_level]
        already = {path[-1] for s2 in top_stmts for path, _ in leaves(s2.tree) if path[:2] in (["http", "header"], ["hyper", "header"])}
        names = [h for h in dict.fromkeys(headers) if h not in already]
        if names:
            text = insert_top_level_use(text, _header_tree(names))

    # crate::admin::storage_api::s3 facade imports
    def storage_editor(stmt, full, node):
        if full[:4] == ["crate", "admin", "storage_api", "s3"] and len(full) == 5 and full[4] in _DROP_FROM_S3S and node.alias is None:
            return []
        return None

    text = edit_use_trees(text, storage_editor)

    # helper renames in imports
    renames = dict(_HELPER_RENAMES)
    for m in re.finditer(r"\b(\w+)_from_envelope\b", text):
        renames[f"{m.group(1)}_from_extensions"] = f"{m.group(1)}_from_envelope"
    sc = Scanner(text)
    stmts = find_use_stmts(text, sc)
    imported = imported_names(stmts)
    for old, new in renames.items():
        if old not in imported or not code_uses(text, new, sc, stmts):
            continue
        old_used = code_uses(text, old, sc, stmts)
        new_imported = new in imported

        def ren(stmt, full, node, old=old, new=new, old_used=old_used, new_imported=new_imported):
            if full[-1] != old or node.alias is not None:
                return None
            renamed = UseTree(node.path[:-1] + [new])
            if old_used:
                return [node] if new_imported else [node, renamed]
            return [renamed] if not new_imported else []

        text = edit_use_trees(text, ren)
        sc = Scanner(text)
        stmts = find_use_stmts(text, sc)
        imported = imported_names(stmts)

    # crate::admin::operation imports
    needed = [n for n in ("AdminRequest", "AdminResponse") if code_uses(text, n, sc, stmts)]
    if re.search(r"^impl Operation for\b", text, re.M):
        needed.append("Operation")
    if needed:
        op_imported = {path[-1] for s in stmts if s.top_level for path, _ in leaves(s.tree) if path[:3] == ["crate", "admin", "operation"]}
        missing = [n for n in needed if n not in op_imported]
        if missing:
            text = merge_into_top_level(text, ["crate", "admin", "operation"], missing)
            sc = Scanner(text)
            stmts = find_use_stmts(text, sc)

    # Body from rustfs_s3_types
    if code_uses(text, "Body", sc, stmts) and "Body" not in imported_names(stmts, top_level_only=True):
        text = merge_into_top_level(text, ["rustfs_s3_types"], ["Body"])
    return text


# --------------------------------------------------------------------------- phase: prune unused imports

PRUNE_NAMES = ["RemoteAddr", "ReqInfo", "ClientInfo", "Uri", "LegacyAdminAdapter", "S3Request", "S3Response", "Body"]


def prune_unused_imports(text: str) -> str:
    sc = Scanner(text)
    stmts = find_use_stmts(text, sc)
    imported = imported_names(stmts)
    for name in PRUNE_NAMES:
        if name not in imported or code_uses(text, name, sc, stmts):
            continue
        text = edit_use_trees(text, lambda stmt, full, node, name=name: [] if full[-1] == name and node.alias is None else None)
        sc = Scanner(text)
        stmts = find_use_stmts(text, sc)
    return text


# --------------------------------------------------------------------------- phase: nested module imports

_MOD_RE = re.compile(r"^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{", re.M)
_NESTED_NAMES = ("AdminRequest", "AdminResponse", "Operation", "Body")


def fix_nested_imports(text: str) -> str:
    """Inline modules that import explicitly (no ``use super::*``) need the owned names themselves."""
    sc = Scanner(text)
    stmts = find_use_stmts(text, sc)
    edits: list[tuple[int, int, str]] = []
    for m in _MOD_RE.finditer(text):
        if not sc.is_code(m.start()):
            continue
        open_b = m.end() - 1
        close_b = sc.match(open_b)
        if close_b is None:
            continue
        block_stmts = [st for st in stmts if open_b < st.start < close_b]
        if any(path == ["super", "*"] for st in block_stmts for path, _ in leaves(st.tree)):
            continue
        imported = {node.alias or path[-1] for st in block_stmts for path, node in leaves(st.tree)}
        needed = []
        for name in _NESTED_NAMES:
            if name in imported:
                continue
            for um in re.finditer(rf"(?<![\w:]){re.escape(name)}\b", text[open_b:close_b]):
                pos = open_b + um.start()
                if sc.is_code(pos) and not any(st.start <= pos < st.end for st in block_stmts):
                    needed.append(name)
                    break
        if not needed:
            continue
        indent = m.group(1) + "    "
        ops = [n for n in needed if n != "Body"]
        lines = []
        if ops:
            lines.append(serialize_use(collapse(UseTree(["crate", "admin", "operation"], [UseTree([n]) for n in ops])), indent))
        if "Body" in needed:
            lines.append(serialize_use(UseTree(["rustfs_s3_types", "Body"]), indent))
        anchor = block_stmts[0].start if block_stmts else line_end(text, open_b) + 1
        edits.append((anchor, anchor, "\n".join(lines) + "\n"))
    for s_, e_, repl in sorted(edits, reverse=True):
        text = text[:s_] + repl + text[e_:]
    return text


# --------------------------------------------------------------------------- phase: strings

_STRING_SYMBOL_RE = re.compile(
    r"S3Request\b|S3Response\b|authorize_admin_request|_from_req\(|_from_extensions\("
    r"|req\.(?:extensions|credentials|input|headers|uri)\b(?!\s*\()"
)

# Source-inspecting tests assert on code fragments; keep those fragments in step with the code rewrites.
_STRING_MIRROR = [
    (re.compile(r"authorize_admin_request\((\s*)&?req(\s*),"), r"authorize_admin_envelope(\1req.envelope()\2,"),
    (re.compile(r"authorize_admin_request\("), "authorize_admin_envelope("),
    (re.compile(r"\b(\w+)_from_extensions\(&req\.extensions"), r"\1_from_envelope(req.envelope()"),
    (re.compile(r"\b(object_store|app_context)_from_req\(&?req\)"), r"\1_from_envelope(req.envelope())"),
    (re.compile(r"KmsAdminAudit::from_request\(&req\.extensions, &req\.headers, "), "KmsAdminAudit::from_envelope(req.envelope(), "),
    (re.compile(r"CallerIdentity::resolve\(&req\)"), "CallerIdentity::resolve_envelope(req.envelope())"),
    (re.compile(r"std::mem::take\(&mut req\.input\)"), "req.take_body()"),
    (re.compile(r"&req\.(uri|headers)\b"), r"req.\1()"),
    (re.compile(r"\breq\.(uri|headers|method)\."), r"req.\1()."),
    (re.compile(r"\breq\.credentials\.(is_none|as_ref)\(\)"), r"req.credentials().\1()"),
    (re.compile(r"&req\.credentials\b"), "req.credentials()"),
    (re.compile(r"\breq\.input\b(?!\s*=)"), "req.take_body()"),
    (re.compile(r"(?<![\w:])S3Request<Body>"), "AdminRequest"),
    (re.compile(r"(?<![\w:])S3Response<\(StatusCode, Body\)>"), "AdminResponse"),
]


def rewrite_strings(text: str, ctx: Ctx) -> str:
    sc = Scanner(text)
    for s, e in reversed(sc.strings):
        lit = text[s:e]
        new = lit
        for rx, repl in _STRING_MIRROR:
            new = rx.sub(repl, new)
        if new != lit:
            text = text[:s] + new + text[e:]
    sc = Scanner(text)
    for s, e in reversed(sc.strings):
        if _STRING_SYMBOL_RE.search(text[s:e]):
            text = add_sentinel(text, s, R_STRING)
    return materialize_todos(text)


# --------------------------------------------------------------------------- driver

_NEEDS_RE = re.compile(r"\bS3Request\b|\bS3Response\b|\bimpl Operation for\b|router::Operation\b|\bLegacyAdminAdapter\b")


def transform(text: str, ctx: Ctx) -> str:
    if not _NEEDS_RE.search(text):
        return text
    ctx.frozen = detect_frozen_fns(text, ctx.path)
    text = convert_request_literals(text, ctx)
    text = rename_types(text, ctx)
    text = qualify_frozen(text, ctx)
    text = rewrite_helpers(text, ctx)
    text = rewrite_fields(text, ctx)
    text = rewrite_body(text, ctx)
    text = rewrite_responses(text, ctx)
    text = normalize_mut(text, ctx)
    text = rewrite_imports(text, ctx)
    text = rewrite_strings(text, ctx)
    text = prune_unused_imports(text)
    text = fix_nested_imports(text)
    return text


def todo_list(text: str) -> list[tuple[int, str]]:
    out = []
    for i, line in enumerate(text.split("\n"), 1):
        s = line.strip()
        if s.startswith(TODO_PREFIX.strip()):
            out.append((i, s[len(TODO_PREFIX.strip()):].strip()))
    return out


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--check", action="store_true", help="report changes and TODOs without writing")
    ap.add_argument(
        "--credential-methods",
        action="store_true",
        help="rewrite obvious cred.access_key/secret_key field access into accessor calls instead of only flagging it",
    )
    ap.add_argument("--rustfmt", action="store_true", help="run `rustfmt --edition 2024` on every written file")
    ap.add_argument("files", nargs="+", type=Path)
    args = ap.parse_args(argv)

    total_files = 0
    changed_files = 0
    reasons: Counter[str] = Counter()
    todo_total = 0
    written: list[Path] = []
    for path in args.files:
        total_files += 1
        original = path.read_text(encoding="utf-8")
        ctx = Ctx(path=path, credential_methods=args.credential_methods)
        result = transform(original, ctx)
        todos = todo_list(result)
        todo_total += len(todos)
        for _, reason in todos:
            reasons[reason] += 1
        if result != original:
            changed_files += 1
            diff = list(difflib.unified_diff(original.split("\n"), result.split("\n"), lineterm="", n=0))
            added = sum(1 for l in diff if l.startswith("+") and not l.startswith("+++"))
            removed = sum(1 for l in diff if l.startswith("-") and not l.startswith("---"))
            verb = "would change" if args.check else "changed"
            print(f"{path}: {verb} (+{added} -{removed} lines), todos={len(todos)}")
            if not args.check:
                path.write_text(result, encoding="utf-8")
                written.append(path)
        else:
            print(f"{path}: unchanged, todos={len(todos)}")
        for line, reason in todos:
            print(f"  {path}:{line}: {reason}")
    if args.rustfmt and written:
        subprocess.run(["rustfmt", "--edition", "2024", *map(str, written)], check=False)
    print(f"\nSummary: files={total_files} changed={changed_files} unchanged={total_files - changed_files} todos={todo_total}")
    if reasons:
        print("TODOs by reason:")
        for reason, count in reasons.most_common():
            print(f"  {count:4d}  {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
