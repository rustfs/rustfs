#!/bin/bash
set -e

APP_USER=rustfs
APP_GROUP=rustfs
APP_UID=${PUID:-1000}
APP_GID=${PGID:-1000}

# Parse RUSTFS_VOLUMES into array (support space, comma, tab as separator)
VOLUME_RAW="${RUSTFS_VOLUMES:-/data}"
# Replace comma and tab with space, then split
VOLUME_RAW=$(echo "$VOLUME_RAW" | tr ',\t' '  ')
read -ra ALL_VOLUMES <<< "$VOLUME_RAW"

# Only keep local volumes (start with /, not http/https)
LOCAL_VOLUMES=()
for vol in "${ALL_VOLUMES[@]}"; do
  if [[ "$vol" =~ ^/ ]] && [[ ! "$vol" =~ ^https?:// ]]; then
    LOCAL_VOLUMES+=("$vol")
  fi
done

# Always ensure /logs is included
include_logs=1
for vol in "${LOCAL_VOLUMES[@]}"; do
  if [ "$vol" = "/logs" ]; then
    include_logs=0
    break
  fi
done
if [ $include_logs -eq 1 ]; then
  LOCAL_VOLUMES+=("/logs")
fi

echo "📦 Initializing mount directories: ${LOCAL_VOLUMES[*]}"

# Create directories if they don't exist
for vol in "${LOCAL_VOLUMES[@]}"; do
  if [ ! -d "$vol" ]; then
    echo "📁 Creating directory: $vol"
    mkdir -p "$vol"
  fi
done

# Warn if default credentials are used
if [[ "$RUSTFS_ACCESS_KEY" == "rustfsadmin" || "$RUSTFS_SECRET_KEY" == "rustfsadmin" ]]; then
  echo "⚠️ WARNING: Using default RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY"
  echo "⚠️ It is strongly recommended to override these values in production!"
fi

echo "🚀 Starting application: $*"

# Switch user and run the application
docker_switch_user() {
  if [ -n "${APP_UID}" ] && [ -n "${APP_GID}" ]; then
    # Use specified UID/GID
    if command -v chroot >/dev/null 2>&1; then
      exec chroot --userspec=${APP_UID}:${APP_GID} / "$@"
    else
      # Fallback to gosu if chroot is not available
      # Validate APP_UID and APP_GID are numeric
      if ! [[ "${APP_UID}" =~ ^[0-9]+$ ]] || ! [[ "${APP_GID}" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: APP_UID and APP_GID must be numeric values." >&2
        exit 1
      fi
      exec chroot --userspec="${APP_UID}:${APP_GID}" / "$@"
    else
      # Fallback to gosu if chroot is not available
      exec gosu "${APP_UID}:${APP_GID}" "$@"
    fi
  else
    # Use default user
    exec gosu "$APP_USER" "$@"
  fi
}

docker_switch_user "$@"