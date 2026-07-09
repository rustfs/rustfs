#!/usr/bin/env bash
#
# Fail when an agent-instruction or architecture document references a
# repository file path that no longer exists. Keeps CLAUDE.md / AGENTS.md /
# ARCHITECTURE.md / docs/architecture honest after refactors move code.
#
# Checked files: all tracked AGENTS.md, CLAUDE.md, ARCHITECTURE.md, and
# docs/architecture/*.md (archived plans under docs/superpowers/plans are
# intentionally NOT checked — they are historical).
#
# A reference is any token starting with crates/, rustfs/, scripts/, docs/,
# .github/ or .config/ that ends in a known file extension. Tokens containing
# globs or placeholders, extensionless tokens (HTTP routes, directory
# references, org/repo shorthands), and lines containing URLs are skipped.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILURES=0

doc_files() {
  git ls-files 'AGENTS.md' '*/AGENTS.md' 'CLAUDE.md' 'ARCHITECTURE.md' 'docs/architecture/*.md'
}

check_file() {
  local doc="$1"
  # Path-like tokens; strip surrounding backticks/brackets/punctuation later.
  # Lines with URLs are skipped wholesale: path fragments inside links are
  # external, not repo paths.
  # grep exits 1 on no matches; that must not kill the script under pipefail.
  { grep -vE 'https?://' "$doc" || true; } \
    | { grep -oE '(crates|rustfs|scripts|docs|\.github|\.config)/[A-Za-z0-9_./-]+' || true; } \
    | sed -e 's/[.,;:)]*$//' -e 's:/$::' \
    | sort -u \
    | while IFS= read -r ref; do
        case "$ref" in
          (*'*'*|*'<'*|*'>'*|*'{'*|*'}'*|*'…'*) continue ;;
        esac
        # Only verify references with a known file extension. Extensionless
        # tokens are too often HTTP routes (rustfs/admin/v3/...), org/repo
        # shorthands (rustfs/backlog), or glob prefixes — not repo paths.
        case "$ref" in
          (*.rs|*.md|*.sh|*.toml|*.yml|*.yaml|*.mak|*.proto) ;;
          (*) continue ;;
        esac
        if [[ ! -e "$ref" ]]; then
          printf 'stale path reference: %s -> %s\n' "$doc" "$ref" >&2
          echo "FAIL" >> "$FAIL_MARKER"
        fi
      done
}

FAIL_MARKER="$(mktemp)"
trap 'rm -f "$FAIL_MARKER"' EXIT

while IFS= read -r doc; do
  [[ -f "$doc" ]] && check_file "$doc"
done < <(doc_files)

if [[ -s "$FAIL_MARKER" ]]; then
  count="$(wc -l < "$FAIL_MARKER" | tr -d ' ')"
  printf '\n%s stale doc path reference(s) found.\n' "$count" >&2
  printf 'Fix the doc to match the current tree, or update the moved code path.\n' >&2
  exit 1
fi

printf 'doc path check passed\n'
