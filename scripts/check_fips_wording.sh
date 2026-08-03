#!/usr/bin/env bash
set -euo pipefail

# Guard: outward README and CHANGELOG material must not make an unsupported
# FIPS validation or certification claim. The detailed policy and permitted
# qualifiers live in docs/operations/kms-cryptographic-compliance.md; this
# check intentionally scans only the two public project-facing documents.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_FIPS_WORDING_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

cd "$ROOT_DIR"

TARGETS=(README.md CHANGELOG.md)
FORBIDDEN_PATTERNS=(
  'FIPS[[:space:]]+(140-[23](/[[:space:]]*140-[23])?[[:space:]]+)?(validated|certified|compliant)'
  'FIPS[[:space:]]+mode'
  'FIPS-enabled'
  'NIST[[:space:]]+(certified|approved)'
  'CMVP[[:space:]]+certificate'
  '(meets|satisfies)[[:space:]]+FIPS'
)

status=0

for target in "${TARGETS[@]}"; do
  if [[ ! -f "$target" ]]; then
    printf 'FIPS wording guard failed: %s is missing\n' "$target" >&2
    status=1
    continue
  fi

  for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    matches="$(grep -E -i -n -- "$pattern" "$target" || true)"
    if [[ -n "$matches" ]]; then
      printf 'FIPS wording guard failed: forbidden pattern /%s/ in %s:\n%s\n' \
        "$pattern" "$target" "$matches" >&2
      status=1
    fi
  done
done

if [[ "$status" -ne 0 ]]; then
  printf 'Remove unsupported FIPS validation wording from README.md or CHANGELOG.md.\n' >&2
  exit "$status"
fi

printf 'FIPS wording guard passed (README.md and CHANGELOG.md contain no forbidden claims).\n'
