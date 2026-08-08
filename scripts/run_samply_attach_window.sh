#!/usr/bin/env bash
set -euo pipefail

PID=""
DURATION_SECS=""
OUTPUT=""
RATE="999"
PRESYMBOLICATE="true"

usage() {
  cat <<'USAGE'
Usage: scripts/run_samply_attach_window.sh --pid <pid> --duration-secs <n> --output <profile.json.gz> [options]

Attach samply to an already-running process for a bounded window and force a
Ctrl+C-style shutdown so samply writes the profile artifact. This script uses
direct `samply record -p`; `cargo samply` launches a cargo target and is not
suitable for attaching to an existing RustFS service PID.

Options:
  --pid <pid>              Existing process id to profile.
  --duration-secs <n>      Sampling window in seconds.
  --output <path>          Profile output path.
  --rate <hz>              Sampling rate. Default: 999.
  --no-presymbolicate      Do not request samply's .syms.json sidecar.
  -h, --help               Show this help.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid) PID="${2:-}"; shift 2 ;;
    --duration-secs) DURATION_SECS="${2:-}"; shift 2 ;;
    --output) OUTPUT="${2:-}"; shift 2 ;;
    --rate) RATE="${2:-}"; shift 2 ;;
    --no-presymbolicate) PRESYMBOLICATE="false"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ "$PID" =~ ^[0-9]+$ ]] || die "--pid must be a positive integer"
[[ "$DURATION_SECS" =~ ^[0-9]+$ && "$DURATION_SECS" -gt 0 ]] || die "--duration-secs must be a positive integer"
[[ "$RATE" =~ ^[0-9]+$ && "$RATE" -gt 0 ]] || die "--rate must be a positive integer"
[[ -n "$OUTPUT" ]] || die "--output is required"
kill -0 "$PID" 2>/dev/null || die "process $PID is not running"
command -v timeout >/dev/null 2>&1 || die "timeout is required"
command -v samply >/dev/null 2>&1 || die "samply is required"

mkdir -p "$(dirname "$OUTPUT")"

cmd=(samply record -p "$PID" -r "$RATE" --save-only)
if [[ "$PRESYMBOLICATE" == "true" ]]; then
  cmd+=(--unstable-presymbolicate)
fi
cmd+=(-o "$OUTPUT")

set +e
timeout -s INT --kill-after=10s "${DURATION_SECS}s" "${cmd[@]}"
status=$?
set -e

case "$status" in
  0|124|130) ;;
  *) exit "$status" ;;
esac

[[ -s "$OUTPUT" ]] || die "samply did not write profile output: $OUTPUT"
if [[ "$PRESYMBOLICATE" == "true" ]]; then
  syms_output="${OUTPUT%.gz}.syms.json"
  [[ -s "$syms_output" ]] || die "samply did not write symbol sidecar: $syms_output"
fi

echo "profile=$OUTPUT"
if [[ "$PRESYMBOLICATE" == "true" ]]; then
  echo "symbols=${OUTPUT%.gz}.syms.json"
fi
