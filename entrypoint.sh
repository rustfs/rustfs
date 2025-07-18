#!/bin/bash
set -e

# Function to adjust rustfs user/group to match mounted directory ownership
fix_permissions() {
    local dir="$1"
    local user="rustfs"
    local group="rustfs"

    # Skip if directory doesn't exist or isn't mounted
    if [ ! -d "$dir" ]; then
        echo "Directory $dir does not exist, creating it"
        mkdir -p "$dir"
        chown "$user:$group" "$dir"
        chmod 700 "$dir"
        return
    fi

    # Get directory ownership
    local dir_uid=$(stat -c %u "$dir")
    local dir_gid=$(stat -c %g "$dir")

    # If directory is owned by root or inaccessible, skip adjustment
    if [ "$dir_uid" = "0" ] || [ "$dir_gid" = "0" ]; then
        echo "Warning: Directory $dir is owned by root, skipping UID/GID adjustment"
        chown "$user:$group" "$dir"
        chmod 700 "$dir"
        return
    fi

    # Adjust rustfs user/group to match directory ownership
    if [ "$dir_uid" != "$(id -u $user)" ]; then
        echo "Adjusting UID of $user to $dir_uid for $dir"
        usermod -u "$dir_uid" "$user"
    fi
    if [ "$dir_gid" != "$(id -g $group)" ]; then
        echo "Adjusting GID of $group to $dir_gid for $dir"
        groupmod -g "$dir_gid" "$group"
    fi

    # Ensure permissions are correct
    chown "$user:$group" "$dir"
    chmod 700 "$dir"
}

# Fix permissions for /data and /logs
fix_permissions "/data"
fix_permissions "/logs"

# Run RustFS as the rustfs user
exec gosu rustfs:rustfs "$@"