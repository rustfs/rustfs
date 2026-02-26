#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_FILE="${ROOT_DIR}/scripts/layer-dependency-baseline.txt"
MODE="check"

if [[ "${1:-}" == "--update-baseline" ]]; then
  MODE="update"
fi

classify_source_layer() {
  local file="$1"

  if [[ "$file" == rustfs/src/app/* ]]; then
    printf 'app'
  elif [[ "$file" == rustfs/src/admin/* ]] || [[ "$file" == rustfs/src/storage/ecfs.rs ]] || [[ "$file" == rustfs/src/storage/s3_api/* ]]; then
    printf 'interface'
  elif [[ "$file" == rustfs/src/* ]]; then
    printf 'infra'
  else
    printf 'unknown'
  fi
}

classify_target_layer() {
  local path="$1"
  local root="${path%%::*}"

  case "$root" in
    app)
      printf 'app'
      ;;
    admin)
      printf 'interface'
      ;;
    storage)
      if [[ "$path" == ecfs* ]] || [[ "$path" == s3_api* ]]; then
        printf 'interface'
      else
        printf 'infra'
      fi
      ;;
    *)
      printf 'infra'
      ;;
  esac
}

layer_rank() {
  case "$1" in
    interface)
      printf '3'
      ;;
    app)
      printf '2'
      ;;
    infra)
      printf '1'
      ;;
    *)
      printf '0'
      ;;
  esac
}

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

VIOLATIONS_RAW="${TMP_DIR}/violations_raw.txt"
EDGES_RAW="${TMP_DIR}/edges_raw.txt"
CURRENT_BASELINE="${TMP_DIR}/current_baseline.txt"

: >"$VIOLATIONS_RAW"
: >"$EDGES_RAW"

while IFS= read -r line; do
  file="${line%%:*}"
  rest="${line#*:}"
  lineno="${rest%%:*}"
  text="${rest#*:}"

  source_layer="$(classify_source_layer "$file")"
  if [[ "$source_layer" == "unknown" ]]; then
    continue
  fi

  import_path="$(sed -E 's/.*use[[:space:]]+crate::([^;{ ]+).*/\1/' <<<"$text")"
  if [[ -z "$import_path" ]] || [[ "$import_path" == "$text" ]]; then
    continue
  fi

  target_layer="$(classify_target_layer "$import_path")"
  if [[ "$target_layer" == "unknown" ]]; then
    continue
  fi

  if [[ "$source_layer" != "$target_layer" ]]; then
    printf '%s->%s\n' "$source_layer" "$target_layer" >>"$EDGES_RAW"
  fi

  source_rank="$(layer_rank "$source_layer")"
  target_rank="$(layer_rank "$target_layer")"

  if (( source_rank < target_rank )); then
    printf 'dep|%s|%s->%s|crate::%s\n' "$file" "$source_layer" "$target_layer" "$import_path" >>"$VIOLATIONS_RAW"
    printf '%s:%s reverse dependency %s->%s via crate::%s\n' "$file" "$lineno" "$source_layer" "$target_layer" "$import_path"
  fi
done < <(cd "$ROOT_DIR" && rg -n --no-heading -g '*.rs' 'use[[:space:]]+crate::' rustfs/src)

sort -u "$VIOLATIONS_RAW" >"${TMP_DIR}/violations_sorted.txt"

sort -u "$EDGES_RAW" >"${TMP_DIR}/edges_sorted.txt"
while IFS= read -r edge; do
  [[ -z "$edge" ]] && continue
  left="${edge%%->*}"
  right="${edge#*->}"
  reverse="${right}->${left}"
  if grep -Fxq "$reverse" "${TMP_DIR}/edges_sorted.txt"; then
    if [[ "$left" < "$right" ]]; then
      printf 'cycle|%s<->%s\n' "$left" "$right"
    fi
  fi
done <"${TMP_DIR}/edges_sorted.txt" | sort -u >"${TMP_DIR}/cycles_sorted.txt"

cat "${TMP_DIR}/violations_sorted.txt" "${TMP_DIR}/cycles_sorted.txt" | sort -u >"$CURRENT_BASELINE"

if [[ "$MODE" == "update" ]]; then
  cp "$CURRENT_BASELINE" "$BASELINE_FILE"
  echo "Updated baseline: $BASELINE_FILE"
  exit 0
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "Baseline file missing: $BASELINE_FILE"
  echo "Run: scripts/check_layer_dependencies.sh --update-baseline"
  exit 1
fi

sort -u "$BASELINE_FILE" >"${TMP_DIR}/baseline_sorted.txt"

NEW_ITEMS="${TMP_DIR}/new_items.txt"
comm -13 "${TMP_DIR}/baseline_sorted.txt" "$CURRENT_BASELINE" >"$NEW_ITEMS"

if [[ -s "$NEW_ITEMS" ]]; then
  echo "Layer dependency guard failed: new reverse dependencies or cycles detected"
  cat "$NEW_ITEMS"
  exit 1
fi

echo "Layer dependency guard passed (no new reverse dependencies/cycles)."
