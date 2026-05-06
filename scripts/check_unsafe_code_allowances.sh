#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

status=0

while IFS=: read -r file line _; do
  start=$((line > 3 ? line - 3 : 1))
  end=$((line + 6))
  if ! sed -n "${start},${end}p" "$file" | rg -q "SAFETY:"; then
    printf '%s:%s: unsafe_code allowance must have a nearby SAFETY comment\n' "$file" "$line" >&2
    status=1
  fi
done < <(rg -n '^[[:space:]]*#!?\[allow\([^]]*\bunsafe_code\b[^]]*\)\]' --glob '*.rs' .)

exit "$status"
