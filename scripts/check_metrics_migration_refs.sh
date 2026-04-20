#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Allowed references during migration bootstrap (T00 baseline).
# Keep entries minimal and remove them as callsites are migrated.
ALLOWLIST=(
  "crates/obs/src/lib.rs"
  "crates/obs/src/metrics/mod.rs"
  "crates/obs/src/metrics/scheduler.rs"
)

is_allowed_path() {
  local path="$1"
  local allow
  for allow in "${ALLOWLIST[@]}"; do
    if [[ "$path" == "$allow" ]]; then
      return 0
    fi
  done

  return 1
}

MATCHES=()
while IFS= read -r line; do
  MATCHES+=("$line")
done < <(
  cd "$ROOT_DIR"
  rg -n --no-heading \
    -e 'rustfs_metrics::' \
    -e '\binit_metrics_system\b' \
    -e '\binit_metrics_collectors\b' \
    rustfs/src crates \
    --glob '**/*.rs' \
    --glob '!**/tests/**' \
    --glob '!docs/**' || true
)

while IFS= read -r line; do
  MATCHES+=("$line")
done < <(
  cd "$ROOT_DIR"
  rg -n --no-heading \
    -e 'rustfs-metrics' \
    -e 'crates/metrics' \
    Cargo.toml rustfs crates \
    --glob '**/*.toml' \
    --glob '**/*.rs' \
    --glob '!**/tests/**' \
    --glob '!docs/**' || true
)

VIOLATIONS=()

for hit in "${MATCHES[@]}"; do
  file="${hit%%:*}"
  if is_allowed_path "$file"; then
    continue
  fi

  VIOLATIONS+=("$hit")
done

if (( ${#VIOLATIONS[@]} > 0 )); then
  echo "Metrics migration reference guard failed: found non-allowlisted references"
  printf '%s\n' "${VIOLATIONS[@]}"
  exit 1
fi

echo "Metrics migration reference guard passed."
