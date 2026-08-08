#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/capture_remote_journal_errors.sh --nodes <csv> --since <iso-time> --label <label> --out-dir <dir> [options]

Capture RustFS journal lines matching auth/error/failure patterns for a UTC
validation window. The --since value is normalized to the journalctl-friendly
"YYYY-MM-DD HH:MM:SS UTC" form before it is sent to remote nodes.

Options:
  --nodes <csv>          Comma-separated node names, for example vm004,vm005.
  --since <iso-time>     UTC ISO timestamp, for example 2026-08-08T09:05:45Z.
  --label <label>        Prefix used for output files.
  --out-dir <dir>        Local output directory.
  --unit <name>          systemd unit name. Default: rustfs.
  --filter-regex <expr>  grep -Ei pattern. Default captures auth/signature/error/warn/fail/panic.
  --ssh-bin <path>       SSH binary or test double. Default: ssh.
  -h, --help             Show this help.
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

validate_name() {
  local field="$1"
  local value="$2"
  [[ "$value" =~ ^[A-Za-z0-9._@-]+$ ]] || die "$field contains unsafe characters: $value"
}

format_since_utc() {
  python3 - "$1" <<'PY'
from datetime import datetime, timezone
import sys

value = sys.argv[1].strip()
if value.endswith("Z"):
    value = value[:-1] + "+00:00"
try:
    parsed = datetime.fromisoformat(value)
except ValueError as err:
    raise SystemExit(f"invalid ISO timestamp: {err}")
if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=timezone.utc)
print(parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
PY
}

NODES_CSV=""
SINCE_ISO=""
LABEL=""
OUT_DIR=""
UNIT="rustfs"
FILTER_REGEX="No valid auth token|auth|signature|error|panic|fail|warn"
SSH_BIN="${SSH_BIN:-ssh}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES_CSV="${2:-}"; shift 2 ;;
    --since) SINCE_ISO="${2:-}"; shift 2 ;;
    --label) LABEL="${2:-}"; shift 2 ;;
    --out-dir) OUT_DIR="${2:-}"; shift 2 ;;
    --unit) UNIT="${2:-}"; shift 2 ;;
    --filter-regex) FILTER_REGEX="${2:-}"; shift 2 ;;
    --ssh-bin) SSH_BIN="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$NODES_CSV" ]] || die "--nodes is required"
[[ -n "$SINCE_ISO" ]] || die "--since is required"
[[ -n "$LABEL" ]] || die "--label is required"
[[ -n "$OUT_DIR" ]] || die "--out-dir is required"
[[ -n "$UNIT" ]] || die "--unit must not be empty"
validate_name "--label" "$LABEL"
validate_name "--unit" "$UNIT"

since_journal=$(format_since_utc "$SINCE_ISO")
mkdir -p "$OUT_DIR"

IFS=',' read -r -a nodes <<<"$NODES_CSV"
captured=0
for node in "${nodes[@]}"; do
  node="${node//[[:space:]]/}"
  [[ -n "$node" ]] || continue
  validate_name "node" "$node"

  output_file="$OUT_DIR/${LABEL}-${node}-journal-errors.txt"
  journal_cmd="journalctl -u $(shell_quote "$UNIT") --since $(shell_quote "$since_journal") --no-pager"
  remote_cmd="sudo su - root -c $(shell_quote "$journal_cmd")"
  "$SSH_BIN" "$node" "$remote_cmd" 2>&1 | grep -Ei "$FILTER_REGEX" >"$output_file" || true
  captured=$((captured + 1))
done

[[ "$captured" -gt 0 ]] || die "--nodes did not contain any usable node names"
echo "journal_since=$since_journal"
echo "captured_nodes=$captured"
