#!/bin/sh
# These tests exercise receipts or namespace locks using real ECStore files.
set -eu

self_test=false
if [ "${1:-}" = --self-test ]; then
    self_test=true
    shift
fi
fixture=${1:-receipt}
case "$fixture" in
    receipt)
        fixture_name="Receipt fixture"
        # Two four-disk pools and 1,001 object/receipt pairs.
        fixture_disk_count=8
        fixture_peak_kib=32768
        ;;
    namespace-locks)
        fixture_name="Namespace lock fixture"
        # Three small buckets share one four-disk store.
        fixture_disk_count=4
        fixture_peak_kib=4096
        ;;
    *)
        echo "Unknown temporary-directory fixture: $1" >&2
        exit 2
        ;;
esac

# Each disk allocates a delete reserve. Leave additional space for metadata.
delete_reserve_per_disk_kib=8192
headroom_kib=16384
minimum_free_kib=$((fixture_peak_kib + fixture_disk_count * delete_reserve_per_disk_kib + headroom_kib))

tmpfs_is_eligible() {
    [ "$1" = tmpfs ] && [ "$2" -ge "$minimum_free_kib" ]
}

if [ "$self_test" = true ]; then
    if tmpfs_is_eligible tmpfs "$((minimum_free_kib - 1))"; then
        echo "$fixture_name self-test: insufficient capacity must be rejected" >&2
        exit 1
    fi
    # Hosted runners provision a 64 MiB shared-memory mount.
    if tmpfs_is_eligible tmpfs 65536; then
        [ "$fixture" = namespace-locks ]
    else
        [ "$fixture" = receipt ]
    fi
    tmpfs_is_eligible tmpfs "$minimum_free_kib"
    if tmpfs_is_eligible ext4 "$minimum_free_kib"; then
        echo "$fixture_name self-test: non-tmpfs mount must be rejected" >&2
        exit 1
    fi
    echo "$fixture_name self-test: minimum ${minimum_free_kib} KiB"
    exit 0
fi

if [ "$(uname -s)" != Linux ] || [ ! -d /dev/shm ] || [ ! -w /dev/shm ]; then
    echo "$fixture_name: using the default temporary directory"
    exit 0
fi

fs_type=$(stat -f -c %T /dev/shm 2>/dev/null || true)
free_kib=$(df -Pk /dev/shm 2>/dev/null | awk 'NR == 2 { print $4 }' || true)
case "$free_kib" in
    ''|*[!0-9]*) free_kib=0 ;;
esac

if tmpfs_is_eligible "$fs_type" "$free_kib"; then
    printf 'TMPDIR=/dev/shm\n' >> "$NEXTEST_ENV"
    echo "$fixture_name: using tmpfs with ${free_kib} KiB free"
else
    echo "$fixture_name: using the default temporary directory (${free_kib} KiB free, ${minimum_free_kib} KiB required)"
fi
