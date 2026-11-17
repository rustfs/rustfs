#!/bin/sh
# This fixture checks missing and corrupt receipts, not crash durability.
# Keep the real ECStore and its full-tail writes, but avoid overlay fsync cost
# when this Linux runner has enough tmpfs space for all 1,001 pairs.
set -eu

# The fixture peaks near 32 MiB outside the per-disk delete reserves. Its two
# four-disk pools allocate 8 MiB per disk; keep this value aligned with
# `crates/ecstore/src/disk/local/delete_reserve.rs::RESERVE_BYTES`. Leave a
# further 16 MiB for tmpfs metadata and allocator variance.
fixture_peak_kib=32768
fixture_disk_count=8
delete_reserve_per_disk_kib=8192
headroom_kib=16384
minimum_free_kib=$((fixture_peak_kib + fixture_disk_count * delete_reserve_per_disk_kib + headroom_kib))

receipt_tmpfs_is_eligible() {
    [ "$1" = tmpfs ] && [ "$2" -ge "$minimum_free_kib" ]
}

if [ "${1:-}" = --self-test ]; then
    if receipt_tmpfs_is_eligible tmpfs 65536; then
        echo 'Receipt fixture self-test: 64 MiB tmpfs must be rejected' >&2
        exit 1
    fi
    receipt_tmpfs_is_eligible tmpfs "$minimum_free_kib"
    if receipt_tmpfs_is_eligible ext4 "$minimum_free_kib"; then
        echo 'Receipt fixture self-test: non-tmpfs mount must be rejected' >&2
        exit 1
    fi
    echo "Receipt fixture self-test: minimum ${minimum_free_kib} KiB"
    exit 0
fi

if [ "$(uname -s)" != Linux ] || [ ! -d /dev/shm ] || [ ! -w /dev/shm ]; then
    echo 'Receipt fixture: using the default temporary directory'
    exit 0
fi

fs_type=$(stat -f -c %T /dev/shm 2>/dev/null || true)
free_kib=$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $4 }' || true)
case "$free_kib" in
    ''|*[!0-9]*) free_kib=0 ;;
esac

if receipt_tmpfs_is_eligible "$fs_type" "$free_kib"; then
    printf 'TMPDIR=/dev/shm\n' >> "$NEXTEST_ENV"
    echo "Receipt fixture: using tmpfs with ${free_kib} KiB free"
else
    echo "Receipt fixture: using the default temporary directory (${free_kib} KiB free, ${minimum_free_kib} KiB required)"
fi
