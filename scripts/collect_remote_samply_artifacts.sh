#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/collect_remote_samply_artifacts.sh --mapping <file> --out-dir <dir> [options]

Copy samply profiles and symbol artifacts from RustFS nodes without allowing
scp to consume the mapping loop's stdin.

Mapping file format:
  <node> <remote_artifact_dir>

Example:
  vm004 /data/rustfs/hotpath/20260808-put-1m
  vm005 /data/rustfs/hotpath/20260808-put-1m

Options:
  --mapping <file>     Node and remote artifact directory pairs.
  --out-dir <dir>      Local directory where node subdirectories are created.
  --remote-root <dir>  Required parent path for remote artifact directories.
                       Default: /data/rustfs.
  --ssh-bin <path>     SSH binary or test double. Default: ssh.
  --scp-bin <path>     SCP binary or test double. Default: scp.
  -h, --help           Show this help.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 2
}

shell_quote() {
  local value=${1//\'/\'\\\'\'}
  printf "'%s'" "$value"
}

validate_node() {
  [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]] || die "node contains unsafe characters: $1"
}

validate_path() {
  [[ "$1" =~ ^/[A-Za-z0-9._/@+=-]+$ ]] || die "path contains unsafe characters: $1"
}

MAPPING=""
OUT_DIR=""
REMOTE_ROOT="/data/rustfs"
SSH_BIN="${SSH_BIN:-ssh}"
SCP_BIN="${SCP_BIN:-scp}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mapping) MAPPING="${2:-}"; shift 2 ;;
    --out-dir) OUT_DIR="${2:-}"; shift 2 ;;
    --remote-root) REMOTE_ROOT="${2:-}"; shift 2 ;;
    --ssh-bin) SSH_BIN="${2:-}"; shift 2 ;;
    --scp-bin) SCP_BIN="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$MAPPING" ]] || die "--mapping is required"
[[ -f "$MAPPING" ]] || die "mapping file not found: $MAPPING"
[[ -n "$OUT_DIR" ]] || die "--out-dir is required"
[[ -n "$REMOTE_ROOT" ]] || die "--remote-root must not be empty"
[[ "$REMOTE_ROOT" == /* ]] || die "--remote-root must be an absolute path"
validate_path "$REMOTE_ROOT"

REMOTE_ROOT="${REMOTE_ROOT%/}"
mkdir -p "$OUT_DIR"

processed=0
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "${line//[[:space:]]/}" ]] && continue
  [[ "$line" =~ ^[[:space:]]*# ]] && continue

  read -r node remote_dir extra <<<"$line"
  [[ -n "${node:-}" && -n "${remote_dir:-}" && -z "${extra:-}" ]] || die "mapping lines must contain exactly two fields: $line"
  validate_node "$node"
  [[ "$remote_dir" == /* ]] || die "remote artifact dir must be absolute for $node: $remote_dir"
  validate_path "$remote_dir"
  [[ "$remote_dir" == "$REMOTE_ROOT"/* ]] || die "remote artifact dir must be under $REMOTE_ROOT for $node: $remote_dir"

  node_out_dir="$OUT_DIR/$node"
  mkdir -p "$node_out_dir"

  quoted_remote_dir=$(shell_quote "$remote_dir")
  remote_cmd="chmod -R a+rX $quoted_remote_dir; find $quoted_remote_dir -maxdepth 1 -type f -printf '%f %s bytes\n'"
  "$SSH_BIN" "$node" "sudo su - root -c $(shell_quote "$remote_cmd")" >"$OUT_DIR/${node}-files.txt" 2>&1 </dev/null
  "$SCP_BIN" -q -r "$node:${remote_dir%/}/"* "$node_out_dir/" </dev/null 2>"$OUT_DIR/${node}-scp.err"
  processed=$((processed + 1))
done <"$MAPPING"

[[ "$processed" -gt 0 ]] || die "mapping file did not contain any nodes"
echo "collected_nodes=$processed"
