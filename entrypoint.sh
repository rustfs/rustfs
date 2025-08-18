#!/bin/sh

# If command starts with an option, prepend rustfs.
if [ "${1}" != "rustfs" ]; then
    if [ -n "${1}" ]; then
        set -- rustfs "$@"
    fi
fi

# Parse RUSTFS_VOLUMES into array (support space, comma, tab as separator)
VOLUME_RAW="${RUSTFS_VOLUMES:-/data}"
# Replace comma and tab with space, then split
VOLUME_RAW=$(echo "$VOLUME_RAW" | tr ',\t' '  ')

# Only keep local volumes (start with /, not http/https)
LOCAL_VOLUMES=""
for vol in $VOLUME_RAW; do
    if [ "$vol" != "${vol#/}" ] && [ "$vol" = "${vol#http://}" ] && [ "$vol" = "${vol#https://}" ]; then
        LOCAL_VOLUMES="$LOCAL_VOLUMES $vol"
    fi
done

# Always ensure /logs is included
include_logs=1
for vol in $LOCAL_VOLUMES; do
    if [ "$vol" = "/logs" ]; then
        include_logs=0
        break
    fi
done
if [ $include_logs -eq 1 ]; then
    LOCAL_VOLUMES="$LOCAL_VOLUMES /logs"
fi

echo "📦 Initializing mount directories: $LOCAL_VOLUMES"

# Create directories if they don't exist
for vol in $LOCAL_VOLUMES; do
    if [ ! -d "$vol" ]; then
        echo "📁 Creating directory: $vol"
        mkdir -p "$vol"
    fi
done

# Warn if default credentials are used
if [ "$RUSTFS_ACCESS_KEY" = "rustfsadmin" ] || [ "$RUSTFS_SECRET_KEY" = "rustfsadmin" ]; then
    echo "⚠️  WARNING: Using default RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY"
    echo "⚠️  It is strongly recommended to override these values in production!"
fi

echo "🚀 Starting application: $*"

# Switch user and run the application
docker_switch_user() {
    if [ -n "${RUSTFS_USERNAME}" ] && [ -n "${RUSTFS_GROUPNAME}" ]; then
        if [ -n "${RUSTFS_UID}" ] && [ -n "${RUSTFS_GID}" ]; then
            chroot --userspec=${RUSTFS_UID}:${RUSTFS_GID} / "$@"
        else
            echo "${RUSTFS_USERNAME}:x:1000:1000:${RUSTFS_USERNAME}:/:/sbin/nologin" >>/etc/passwd
            echo "${RUSTFS_GROUPNAME}:x:1000" >>/etc/group
            chroot --userspec=${RUSTFS_USERNAME}:${RUSTFS_GROUPNAME} / "$@"
        fi
    else
        exec "$@"
    fi
}

# Switch to user if applicable
docker_switch_user "$@"
