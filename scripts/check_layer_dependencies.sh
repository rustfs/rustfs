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
  local storage_path

  case "$root" in
    app)
      printf 'app'
      ;;
    admin)
      printf 'interface'
      ;;
    storage)
      storage_path="${path#storage::}"
      if [[ "$storage_path" == "ecfs" ]] || [[ "$storage_path" == ecfs::* ]] ||
        [[ "$storage_path" == "s3_api" ]] || [[ "$storage_path" == s3_api::* ]]; then
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

normalize_import_group_item() {
  local prefix="$1"
  local item="$2"
  local path nested_prefix nested_items

  item="$(sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//; s/[[:space:]]+as[[:space:]].*$//' <<<"$item")"
  item="$(sed -E 's/[[:space:]]//g' <<<"$item")"
  prefix="$(sed -E 's/[[:space:]]//g' <<<"$prefix")"

  [[ -z "$item" ]] && return 0
  if [[ "$item" == "self" ]]; then
    prefix="${prefix%::}"
    [[ -n "$prefix" ]] && printf '%s\n' "$prefix"
    return 0
  fi

  if [[ "$item" == *"{"* ]]; then
    nested_prefix="${item%%\{*}"
    nested_items="${item#*\{}"
    nested_items="${nested_items%\}*}"
    nested_prefix="${nested_prefix%::}"
    if [[ -n "$prefix" ]]; then
      nested_prefix="${prefix}::${nested_prefix}"
    fi
    normalize_import_group "$nested_prefix" "$nested_items"
    return 0
  fi

  if [[ -n "$prefix" ]]; then
    path="${prefix}::${item}"
  else
    path="$item"
  fi

  if [[ "$path" == *"::*" ]]; then
    path="${path%::*}"
  fi
  while [[ "$path" == *"::" ]]; do
    path="${path%::}"
  done

  [[ -n "$path" ]] && printf '%s\n' "$path"
}

normalize_import_group() {
  local prefix="$1"
  local group="$2"
  local item="" char depth=0 i

  for ((i = 0; i < ${#group}; i++)); do
    char="${group:i:1}"
    case "$char" in
      "{")
        depth=$((depth + 1))
        ;;
      "}")
        depth=$((depth - 1))
        ;;
      ",")
        if (( depth == 0 )); then
          normalize_import_group_item "$prefix" "$item"
          item=""
          continue
        fi
        ;;
    esac
    item+="$char"
  done

  normalize_import_group_item "$prefix" "$item"
}

normalize_import_path() {
  local text="$1"
  local path

  path="$(sed -E 's/.*use[[:space:]]+crate::([^;]+);?.*/\1/' <<<"$text")"
  if [[ -z "$path" ]] || [[ "$path" == "$text" ]]; then
    return 0
  fi

  path="$(sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' <<<"$path")"
  if [[ "$path" == *"{"* ]]; then
    local prefix group

    prefix=""
    group="$path"
    if [[ "$path" != \{* ]]; then
      prefix="${path%%\{*}"
      group="${path#*\{}"
    else
      group="${path#\{}"
    fi
    group="${group%\}*}"
    prefix="${prefix%::}"
    normalize_import_group "$prefix" "$group"
    return 0
  fi

  path="$(sed -E 's/[[:space:]]+as[[:space:]].*$//' <<<"$path")"
  path="$(sed -E 's/[[:space:]]//g' <<<"$path")"

  if [[ "$path" == *"::*" ]]; then
    path="${path%::*}"
  fi
  while [[ "$path" == *"::" ]]; do
    path="${path%::}"
  done

  printf '%s\n' "$path"
}

emit_crate_use_statements() {
  (cd "$ROOT_DIR" && rg --files -g '*.rs' rustfs/src | while IFS= read -r file; do
    perl -0777 -ne '
      while (/\buse\s+crate::.*?;/sg) {
        my $statement = $&;
        my $line = substr($_, 0, $-[0]) =~ tr/\n//;
        $line += 1;
        $statement =~ s/\s+/ /g;
        print "$ARGV:$line:$statement\n";
      }
    ' "$file"
  done)
}

normalize_baseline_file() {
  local input="$1"
  local output="$2"
  local line status first second third

  : >"$output"

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "$line" == \#* ]] && continue

    IFS='|' read -r status first second third _ <<<"$line"
    case "$status" in
      dep)
        if [[ -n "$first" ]] && [[ -n "$second" ]] && [[ -n "$third" ]]; then
          printf 'dep|%s|%s|%s\n' "$first" "$second" "$third" >>"$output"
        fi
        ;;
      cycle)
        if [[ -n "$first" ]]; then
          printf 'cycle|%s\n' "$first" >>"$output"
        fi
        ;;
      accepted | todo)
        if [[ "$first" == "cycle" ]] && [[ -n "$second" ]]; then
          printf 'cycle|%s\n' "$second" >>"$output"
        elif [[ -n "$first" ]] && [[ -n "$second" ]] && [[ -n "$third" ]]; then
          printf 'dep|%s|%s|%s\n' "$first" "$second" "$third" >>"$output"
        fi
        ;;
    esac
  done <"$input"

  sort -u -o "$output" "$output"
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

  while IFS= read -r import_path; do
    if [[ -z "$import_path" ]]; then
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
    fi
  done < <(normalize_import_path "$text")
done < <(emit_crate_use_statements)

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

normalize_baseline_file "$BASELINE_FILE" "${TMP_DIR}/baseline_sorted.txt"

NEW_ITEMS="${TMP_DIR}/new_items.txt"
comm -13 "${TMP_DIR}/baseline_sorted.txt" "$CURRENT_BASELINE" >"$NEW_ITEMS"

STALE_ITEMS="${TMP_DIR}/stale_items.txt"
comm -23 "${TMP_DIR}/baseline_sorted.txt" "$CURRENT_BASELINE" >"$STALE_ITEMS"

if [[ -s "$NEW_ITEMS" ]]; then
  echo "Layer dependency guard failed: new reverse dependencies or cycles detected"
  cat "$NEW_ITEMS"
  exit 1
fi

if [[ -s "$STALE_ITEMS" ]]; then
  echo "Layer dependency guard failed: stale baseline entries detected"
  cat "$STALE_ITEMS"
  exit 1
fi

echo "Layer dependency guard passed (no new reverse dependencies/cycles)."
