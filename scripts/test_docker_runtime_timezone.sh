#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

for dockerfile in Dockerfile Dockerfile.glibc; do
  runtime_stage=$(
    awk '
      /^FROM[[:space:]]/ { stage = "" }
      { stage = stage $0 ORS }
      END { printf "%s", stage }
    ' "$ROOT_DIR/$dockerfile"
  )

  if ! grep -Eq '^[[:space:]]*tzdata([[:space:]]*\\)?[[:space:]]*$' <<<"$runtime_stage"; then
    echo "$dockerfile runtime stage must install tzdata" >&2
    exit 1
  fi

  if ! grep -Fq 'test "$(TZ=Asia/Kolkata date +%z)" = "+0530"' <<<"$runtime_stage"; then
    echo "$dockerfile runtime stage must verify IANA timezone resolution" >&2
    exit 1
  fi
done
