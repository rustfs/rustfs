#!/bin/sh
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

set -eu

ACCESS_KEY="${RUSTFS_SITE_REPL_ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${RUSTFS_SITE_REPL_SECRET_KEY:-rustfsadmin}"
BUCKET="${RUSTFS_SITE_REPL_FLOW_BUCKET:-site-repl-flow-check}"
PREFIX="${RUSTFS_SITE_REPL_FLOW_PREFIX:-flow-$(date +%Y%m%d-%H%M%S)}"
WAIT_ATTEMPTS="${RUSTFS_SITE_REPL_WAIT_ATTEMPTS:-90}"
WAIT_SLEEP_SECONDS="${RUSTFS_SITE_REPL_WAIT_SLEEP_SECONDS:-2}"

SITE1_ENDPOINT="${RUSTFS_SITE1_ENDPOINT:-http://127.0.0.1:9000}"
SITE2_ENDPOINT="${RUSTFS_SITE2_ENDPOINT:-http://127.0.0.1:9010}"
SITE3_ENDPOINT="${RUSTFS_SITE3_ENDPOINT:-http://127.0.0.1:9020}"

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

require_command() {
  if ! command_exists "$1"; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

checksum_file() {
  if command_exists sha256sum; then
    sha256sum "$1" | awk '{print $1}'
  elif command_exists shasum; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "missing required command: sha256sum or shasum" >&2
    exit 1
  fi
}

site_endpoint() {
  case "$1" in
    site1) printf '%s\n' "$SITE1_ENDPOINT" ;;
    site2) printf '%s\n' "$SITE2_ENDPOINT" ;;
    site3) printf '%s\n' "$SITE3_ENDPOINT" ;;
    *) echo "unknown site alias: $1" >&2; exit 1 ;;
  esac
}

other_sites() {
  case "$1" in
    site1) printf '%s\n' "site2 site3" ;;
    site2) printf '%s\n' "site1 site3" ;;
    site3) printf '%s\n' "site1 site2" ;;
    *) echo "unknown source site: $1" >&2; exit 1 ;;
  esac
}

wait_for_object() {
  site="$1"
  object="$2"
  attempt=1

  while [ "$attempt" -le "$WAIT_ATTEMPTS" ]; do
    if mc stat "$site/$BUCKET/$object" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$WAIT_SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "object was not replicated in time: $site/$BUCKET/$object" >&2
  return 1
}

wait_for_bucket() {
  site="$1"
  attempt=1

  while [ "$attempt" -le "$WAIT_ATTEMPTS" ]; do
    if mc stat "$site/$BUCKET" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$WAIT_SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "bucket was not replicated in time: $site/$BUCKET" >&2
  return 1
}

require_command mc
require_command dd
require_command awk
require_command date
require_command mktemp
require_command tr
require_command wc

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rustfs-site-repl-flow.XXXXXX")"
MC_CONFIG_DIR="$WORK_DIR/mc"
export MC_CONFIG_DIR

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

mkdir -p "$MC_CONFIG_DIR" "$WORK_DIR/src" "$WORK_DIR/downloads"

for site in site1 site2 site3; do
  mc alias set "$site" "$(site_endpoint "$site")" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
done

for site in site1 site2 site3; do
  echo "checking S3 API for $site"
  mc ls "$site" >/dev/null
done

echo "ensuring bucket exists on site1: $BUCKET"
mc mb --ignore-existing "site1/$BUCKET" >/dev/null

for site in site1 site2 site3; do
  wait_for_bucket "$site"
done

cat <<'EOF' | while read -r size_mb source_site object_name; do
10 site1 object-010m.bin
25 site2 object-025m.bin
50 site3 object-050m.bin
75 site1 object-075m.bin
100 site2 object-100m.bin
EOF
  src_file="$WORK_DIR/src/$object_name"
  object="$PREFIX/$object_name"

  echo "creating ${size_mb}MiB file: $object_name"
  dd if=/dev/urandom of="$src_file" bs=1048576 count="$size_mb" >/dev/null 2>&1
  src_checksum="$(checksum_file "$src_file")"
  src_bytes="$(wc -c < "$src_file" | tr -d ' ')"

  echo "uploading $object_name to $source_site ($src_bytes bytes)"
  mc cp "$src_file" "$source_site/$BUCKET/$object" >/dev/null

  for site in $(other_sites "$source_site"); do
    wait_for_object "$site" "$object"

    dst_file="$WORK_DIR/downloads/$site-$object_name"
    verified=false
    attempt=1
    while [ "$attempt" -le "$WAIT_ATTEMPTS" ]; do
      rm -f "$dst_file"
      echo "downloading $object_name from $site"
      if mc cp "$site/$BUCKET/$object" "$dst_file" >/dev/null 2>&1; then
        dst_checksum="$(checksum_file "$dst_file")"
        dst_bytes="$(wc -c < "$dst_file" | tr -d ' ')"

        if [ "$dst_checksum" = "$src_checksum" ] && [ "$dst_bytes" = "$src_bytes" ]; then
          verified=true
          break
        fi
      fi

      sleep "$WAIT_SLEEP_SECONDS"
      attempt=$((attempt + 1))
    done

    if [ "$verified" != "true" ]; then
      echo "download verification failed for $site/$BUCKET/$object" >&2
      echo "expected checksum: $src_checksum" >&2
      echo "expected bytes:    $src_bytes" >&2
      exit 1
    fi
  done

  echo "verified replicated downloads for $object_name"
done

echo "site replication object flow check passed"
echo "bucket: $BUCKET"
echo "prefix: $PREFIX"
