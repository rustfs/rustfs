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
    # Not a URL (http/https), just a local path
    LOCAL_VOLUMES+=("$vol")
  fi
  # If it's a URL (http/https), skip
  # If it's an empty string, skip
  # If it's a local path, keep
  # (We don't support other protocols here)
done

# Always ensure /logs is included for permission fix
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

echo "📦 Initializing mount directories: ${LOCAL_VOLUMES[*]}"

for vol in "${LOCAL_VOLUMES[@]}"; do
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
        echo "🔧 Fixing ownership for: $vol → $APP_USER:$APP_GROUP"
        if [[ -n "$CHOWN_RECURSION_DEPTH" ]]; then
          echo "🔧 Applying ownership fix with recursion depth: $CHOWN_RECURSION_DEPTH"
          find "$vol" -mindepth 0 -maxdepth "$CHOWN_RECURSION_DEPTH" -exec chown "$APP_USER:$APP_GROUP" {} \;
        else
          echo "🔧 Applying ownership fix recursively (full depth)"
          chown -R "$APP_USER:$APP_GROUP" "$vol"
        fi
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
