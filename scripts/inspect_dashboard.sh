#!/usr/bin/env bash
#
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

DASHBOARD_PATH=".docker/observability/grafana/dashboards/rustfs.json"

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required by scripts/inspect_dashboard.sh" >&2
  exit 1
fi

if [[ ! -f "$DASHBOARD_PATH" ]]; then
  echo "ERROR: dashboard file not found: $DASHBOARD_PATH" >&2
  exit 1
fi

jq -r '
  def pystr:
    if . == null then "None" else tostring end;
  def pyrepr:
    if . == null then "None"
    elif type == "string" then
      "\u0027"
      + (gsub("\\\\"; "\\\\\\\\")
      | gsub("\u0027"; "\\\\\u0027")
      | gsub("\n"; "\\\\n")
      | gsub("\r"; "\\\\r")
      | gsub("\t"; "\\\\t"))
      + "\u0027"
    else tostring
    end;

  "Total panels: \(.panels | length)",
  "Dashboard version: \(.version)",
  "",
  (
    .panels[]
    | [
        (.id // 0),
        (.type | pystr),
        (.gridPos.y | pystr),
        (.gridPos.h | pystr),
        (.gridPos.w | pystr),
        (.title | pyrepr)
      ]
    | @tsv
  )
' "$DASHBOARD_PATH" | {
  IFS= read -r total_line
  IFS= read -r version_line
  IFS= read -r blank_line

  printf '%s\n' "$total_line"
  printf '%s\n' "$version_line"
  printf '%s\n' "$blank_line"

  while IFS=$'\t' read -r pid ptype y h w title; do
    printf "  id=%4d  type=%-12s  y=%-4s  h=%-4s  w=%-4s  title=%s\n" \
      "$pid" "$ptype" "$y" "$h" "$w" "$title"
  done
}

