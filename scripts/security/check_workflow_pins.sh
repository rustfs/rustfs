#!/usr/bin/env bash
set -euo pipefail

mode="report"
if [[ "${1:-}" == "--enforce" ]]; then
  mode="enforce"
fi

if [[ "${1:-}" != "" && "${1:-}" != "--enforce" ]]; then
  echo "usage: $0 [--enforce]" >&2
  exit 2
fi

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

report_unpinned() {
  local file="$1"
  local line_no="$2"
  local action_ref="$3"
  local reason="$4"

  printf '%s:%s: %s (%s)\n' "$file" "$line_no" "$action_ref" "$reason"

  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    printf '::warning file=%s,line=%s::Unpinned third-party action %s (%s)\n' \
      "$file" "$line_no" "$action_ref" "$reason"
  fi
}

files=()
while IFS= read -r -d '' file; do
  files+=("$file")
done < <(find .github/workflows .github/actions -type f \( -name '*.yml' -o -name '*.yaml' \) -print0 2>/dev/null | sort -z)

unpinned=0
for file in "${files[@]}"; do
  line_no=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_no=$((line_no + 1))
    stripped="$(trim "$line")"

    [[ -z "$stripped" || "$stripped" == \#* ]] && continue

    if [[ "$stripped" =~ ^-?[[:space:]]*uses:[[:space:]]*(.+)$ ]]; then
      action_ref="$(trim "${BASH_REMATCH[1]}")"
      action_ref="$(trim "${action_ref%% #*}")"
      action_ref="${action_ref%\"}"
      action_ref="${action_ref#\"}"
      action_ref="${action_ref%\'}"
      action_ref="${action_ref#\'}"

      [[ -z "$action_ref" ]] && continue
      [[ "$action_ref" == ./* ]] && continue
      [[ "$action_ref" == docker://* ]] && continue

      if [[ "$action_ref" != *@* ]]; then
        report_unpinned "$file" "$line_no" "$action_ref" "missing @ref"
        unpinned=$((unpinned + 1))
        continue
      fi

      ref="${action_ref##*@}"
      if [[ ! "$ref" =~ ^[0-9a-fA-F]{40}$ ]]; then
        report_unpinned "$file" "$line_no" "$action_ref" "ref is not a full-length commit SHA"
        unpinned=$((unpinned + 1))
      fi
    fi
  done < "$file"
done

if [[ "$unpinned" -eq 0 ]]; then
  echo "All third-party GitHub Actions are pinned to full-length commit SHAs."
  exit 0
fi

echo "Found $unpinned unpinned third-party GitHub Action reference(s)."

if [[ "$mode" == "enforce" ]]; then
  exit 1
fi

echo "Report-only mode; rerun with --enforce after existing refs are pinned."
