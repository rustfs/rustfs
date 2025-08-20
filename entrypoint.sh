#!/bin/sh
set -e

# 1) Normalize command:
# - No arguments: default to execute rustfs
# - First argument starts with '-': treat as rustfs arguments, auto-prefix rustfs
# - First argument is 'rustfs': replace with absolute path to avoid PATH interference
if [ $# -eq 0 ] || [ "${1#-}" != "$1" ]; then
  set -- /usr/bin/rustfs "$@"
elif [ "$1" = "rustfs" ]; then
  shift
  set -- /usr/bin/rustfs "$@"
fi

# 2) Parse and create local mount directories (ignore http/https), ensure /logs is included
VOLUME_RAW="${RUSTFS_VOLUMES:-/data}"
# Convert comma/tab to space
VOLUME_LIST=$(echo "$VOLUME_RAW" | tr ',\t' ' ')
LOCAL_VOLUMES=""
for vol in $VOLUME_LIST; do
  case "$vol" in
    /*)
      case "$vol" in
        http://*|https://*) : ;;
        *) LOCAL_VOLUMES="$LOCAL_VOLUMES $vol" ;;
      esac
      ;;
    *)
      : # skip non-local paths
      ;;
  esac
done
# Ensure /logs is included
case " $LOCAL_VOLUMES " in
  *" /logs "*) : ;;
  *) LOCAL_VOLUMES="$LOCAL_VOLUMES /logs" ;;
esac

echo "Initializing mount directories:$LOCAL_VOLUMES"
for vol in $LOCAL_VOLUMES; do
  if [ ! -d "$vol" ]; then
    echo "  mkdir -p $vol"
    mkdir -p "$vol"
    # If target user is specified, try to set directory owner to that user (non-recursive to avoid large disk overhead)
    if [ -n "$RUSTFS_UID" ] && [ -n "$RUSTFS_GID" ]; then
      chown "$RUSTFS_UID:$RUSTFS_GID" "$vol" 2>/dev/null || true
    elif [ -n "$RUSTFS_USERNAME" ] && [ -n "$RUSTFS_GROUPNAME" ]; then
      chown "$RUSTFS_USERNAME:$RUSTFS_GROUPNAME" "$vol" 2>/dev/null || true
    fi
  fi
done

# 3) Default credentials warning
if [ "${RUSTFS_ACCESS_KEY}" = "rustfsadmin" ] || [ "${RUSTFS_SECRET_KEY}" = "rustfsadmin" ]; then
  echo "!!!WARNING: Using default RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY. Override them in production!"
fi

echo "Starting: $*"
exec "$@"
