#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/sample_remote_rustfs_rss.sh --nodes <csv> --duration-secs <n> --out <file> [options]

Sample RustFS process CPU and RSS from remote nodes for a bounded window.
The output is TSV and is intended to run beside warp/samply validation.

Options:
  --nodes <csv>           Comma-separated node list, for example vm004,vm005.
  --duration-secs <n>     Total sampling window in seconds.
  --out <file>            TSV output path.
  --interval-secs <n>     Sampling interval in seconds. Default: 5.
  --ssh-bin <path>        SSH binary or test double. Default: ssh.
  -h, --help              Show this help.
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

NODES_CSV=""
DURATION_SECS=""
OUT=""
INTERVAL_SECS="5"
SSH_BIN="${SSH_BIN:-ssh}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES_CSV="${2:-}"; shift 2 ;;
    --duration-secs) DURATION_SECS="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --interval-secs) INTERVAL_SECS="${2:-}"; shift 2 ;;
    --ssh-bin) SSH_BIN="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$NODES_CSV" ]] || die "--nodes is required"
[[ "$DURATION_SECS" =~ ^[0-9]+$ && "$DURATION_SECS" -gt 0 ]] || die "--duration-secs must be a positive integer"
[[ "$INTERVAL_SECS" =~ ^[0-9]+$ && "$INTERVAL_SECS" -gt 0 ]] || die "--interval-secs must be a positive integer"
[[ -n "$OUT" ]] || die "--out is required"

IFS=',' read -r -a nodes <<<"$NODES_CSV"
[[ "${#nodes[@]}" -gt 0 ]] || die "--nodes did not contain any nodes"
for node in "${nodes[@]}"; do
  [[ -n "$node" ]] || die "--nodes contains an empty entry"
  validate_node "$node"
done

mkdir -p "$(dirname "$OUT")"
printf 'ts_utc\tnode\tpid\tpcpu\trss_kib\tetime\n' >"$OUT"

# shellcheck disable=SC2016
remote_inner='pid=$(pidof rustfs 2>/dev/null | awk "{print \$1}" || true); if [ -n "$pid" ]; then ps -o pid=,pcpu=,rss=,etime= -p "$pid"; fi'
remote_cmd="sudo su - root -c $(shell_quote "$remote_inner")"

deadline=$((SECONDS + DURATION_SECS))
while (( SECONDS < deadline )); do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  for node in "${nodes[@]}"; do
    sample=$("$SSH_BIN" "$node" "$remote_cmd" </dev/null || true)
    if [[ -n "${sample//[[:space:]]/}" ]]; then
      while read -r pid pcpu rss_kib etime extra; do
        [[ -n "${pid:-}" && -n "${pcpu:-}" && -n "${rss_kib:-}" && -n "${etime:-}" && -z "${extra:-}" ]] || continue
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$ts" "$node" "$pid" "$pcpu" "$rss_kib" "$etime" >>"$OUT"
      done <<<"$sample"
    fi
  done
  (( SECONDS >= deadline )) && break
  sleep "$INTERVAL_SECS"
done

echo "rss_samples=$(( $(wc -l <"$OUT") - 1 ))"
