#!/bin/bash
set -e

APP_USER=rustfs
APP_GROUP=rustfs
APP_UID=${PUID:-1000}
APP_GID=${PGID:-1000}

# Parse RUSTFS_VOLUMES into array (comma-separated)
IFS=',' read -ra VOLUMES <<< "${RUSTFS_VOLUMES:-/data}"

# Try to update rustfs UID/GID if needed (requires root and shadow tools)
update_user_group_ids() {
  local uid="$1"
  local gid="$2"
  local user="$3"
  local group="$4"
  local updated=0
  if [ "$(id -u "$user")" != "$uid" ]; then
    if command -v usermod >/dev/null 2>&1; then
      echo "🔧 Updating UID of $user to $uid"
      usermod -u "$uid" "$user"
      updated=1
    fi
  fi
  if [ "$(id -g "$group")" != "$gid" ]; then
    if command -v groupmod >/dev/null 2>&1; then
      echo "🔧 Updating GID of $group to $gid"
      groupmod -g "$gid" "$group"
      updated=1
    fi
  fi
  return $updated
}

echo "📦 Initializing mount directories: ${VOLUMES[*]}"

for vol in "${VOLUMES[@]}"; do
  if [ ! -d "$vol" ]; then
    echo "📁 Creating directory: $vol"
    mkdir -p "$vol"
  fi

  # Alpine busybox stat does not support -c, coreutils is required
  dir_uid=$(stat -c '%u' "$vol")
  dir_gid=$(stat -c '%g' "$vol")

  if [ "$dir_uid" != "$APP_UID" ] || [ "$dir_gid" != "$APP_GID" ]; then
    if [[ "$SKIP_CHOWN" != "true" ]]; then
      # Prefer to update rustfs user/group UID/GID
      update_user_group_ids "$dir_uid" "$dir_gid" "$APP_USER" "$APP_GROUP" || \
      {
        echo "🔧 Fixing ownership for: $vol → $APP_USER:$APP_GROUP (recursive chown)"
        chown -R "$APP_USER:$APP_GROUP" "$vol"
      }
    else
      echo "⚠️ SKIP_CHOWN is enabled. Skipping ownership fix for: $vol"
    fi
  fi
  chmod 700 "$vol"
done

# Warn if default credentials are used
if [[ "$RUSTFS_ACCESS_KEY" == "rustfsadmin" || "$RUSTFS_SECRET_KEY" == "rustfsadmin" ]]; then
  echo "⚠️ WARNING: Using default RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY"
  echo "⚠️ It is strongly recommended to override these values in production!"
fi

echo "🚀 Starting application: $*"
exec gosu "$APP_USER" "$@"
