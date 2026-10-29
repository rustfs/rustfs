#!/bin/sh
# This fixture checks missing and corrupt receipts, not crash durability.
# Keep the real ECStore and its full-tail writes, but avoid overlay fsync cost
# when this Linux runner has enough tmpfs space for all 1,001 pairs.
set -eu

if [ "$(uname -s)" != Linux ] || [ ! -d /dev/shm ] || [ ! -w /dev/shm ]; then
    echo 'Receipt fixture: using the default temporary directory'
    exit 0
fi

fs_type=$(stat -f -c %T /dev/shm 2>/dev/null || true)
free_kib=$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $4 }' || true)
case "$free_kib" in
    ''|*[!0-9]*) free_kib=0 ;;
esac

# A 64 MiB tmpfs peaked at about 32 MiB in the full fixture. Leave at least
# 16 MiB of headroom and fall back when the mount is too small or unavailable.
if [ "$fs_type" = tmpfs ] && [ "$free_kib" -ge 49152 ]; then
    printf 'TMPDIR=/dev/shm\n' >> "$NEXTEST_ENV"
    echo "Receipt fixture: using tmpfs with ${free_kib} KiB free"
else
    echo 'Receipt fixture: using the default temporary directory'
fi
