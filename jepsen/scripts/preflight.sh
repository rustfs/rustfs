#!/bin/bash
# Check SSH nodes before a Jepsen run.
# Usage: jepsen/scripts/preflight.sh --drives 4 n1.orb.local n2.orb.local n3.orb.local n4.orb.local
set -euo pipefail

drives=4
if [ "${1:-}" = "--drives" ]; then
  drives=$2
  shift 2
fi

if [ "$#" -lt 1 ]; then
  echo "usage: preflight.sh --drives N host..." >&2
  exit 2
fi

user=${JEPSEN_SSH_USER:-jepsen}
key=${JEPSEN_SSH_KEY:-$HOME/.ssh/id_ed25519}
ssh_opts=(-i "$key" -o StrictHostKeyChecking=accept-new -o BatchMode=yes)

fail=0
for host in "$@"; do
  short=${host%%.orb.local}
  echo "== ${host} (drives ${drives})"
  if ! ssh "${ssh_opts[@]}" "${user}@${host}" "sudo -n true"; then
    echo "FAIL ${host}: sudo -n true" >&2
    fail=1
    continue
  fi
  if ! ssh "${ssh_opts[@]}" "${user}@${host}" "bash -s" -- "$short" "$drives" <<'REMOTE'
set -eu
short=$1
drives=$2
i=1
while [ "$i" -le "$drives" ]; do
  test -d "/var/lib/rustfs/data${i}"
  i=$((i + 1))
done
sudo -n iptables -L -n >/dev/null
if sudo -n tc qdisc add dev lo root netem delay 1ms; then
  sudo -n tc qdisc del dev lo root
  echo "tc netem: available"
else
  echo "tc netem: NOT available (drop packet from --faults)"
fi
ip=$(getent ahostsv4 "${short}.orb.local" | awk 'NR==1 { print $1 }')
echo "resolved ${short}.orb.local -> ${ip}"
if ! ip -4 addr show | grep -q "inet ${ip}/"; then
  echo "FAIL: ${short}.orb.local does not resolve to a local address" >&2
  exit 1
fi
uname -m
awk '/MemAvailable/ { print }' /proc/meminfo
REMOTE
  then
    echo "FAIL ${host}" >&2
    fail=1
  fi
done

exit "$fail"
