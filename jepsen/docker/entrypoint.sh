#!/bin/sh
# Prepare a Debian node for Jepsen. RustFS itself is started later over SSH.
set -eu

if [ -z "${JEPSEN_SSH_PUBKEY:-}" ]; then
  echo "JEPSEN_SSH_PUBKEY is required (the Mac user's public key)." >&2
  exit 1
fi

if [ -z "${CLUSTER_NODES:-}" ] || [ -z "${DRIVES:-}" ]; then
  echo "CLUSTER_NODES and DRIVES are required." >&2
  exit 1
fi

mkdir -p /run/sshd /home/jepsen/.ssh
printf '%s\n' "$JEPSEN_SSH_PUBKEY" > /home/jepsen/.ssh/authorized_keys
chown -R jepsen:jepsen /home/jepsen/.ssh
chmod 700 /home/jepsen/.ssh
chmod 600 /home/jepsen/.ssh/authorized_keys
ssh-keygen -A

i=1
while [ "$i" -le "$DRIVES" ]; do
  mkdir -p "/var/lib/rustfs/data${i}"
  i=$((i + 1))
done

# Resolve service names through Docker DNS before any /etc/hosts alias is added.
# A later alias of n1.orb.local to that address lets RustFS treat the URL host
# as local without making every node resolve n1 to 127.0.0.1.
tmp=$(mktemp)
if [ -f /etc/hosts ]; then
  grep -v 'orb.local' /etc/hosts > "$tmp" || true
else
  : > "$tmp"
fi

old_ifs=$IFS
IFS=,
for name in $CLUSTER_NODES; do
  ip=$(getent ahostsv4 "$name" | awk 'NR==1 { print $1 }')
  if [ -z "$ip" ]; then
    echo "Docker DNS did not resolve ${name}." >&2
    exit 1
  fi
  printf '%s %s.orb.local\n' "$ip" "$name" >> "$tmp"
done
IFS=$old_ifs
cat "$tmp" > /etc/hosts
rm -f "$tmp"

exec /usr/sbin/sshd -D -e
