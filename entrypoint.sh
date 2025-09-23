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

# 2) Process data volumes (separate from log directory)
DATA_VOLUMES=""
process_data_volumes() {
  VOLUME_RAW="${RUSTFS_VOLUMES:-/data}"
  # Convert comma/tab to space
  VOLUME_LIST=$(echo "$VOLUME_RAW" | tr ',\t' ' ')
  
  for vol in $VOLUME_LIST; do
    case "$vol" in
      /*)
        case "$vol" in
          http://*|https://*) : ;;
          *) DATA_VOLUMES="$DATA_VOLUMES $vol" ;;
        esac
        ;;
      *)
        : # skip non-local paths
        ;;
    esac
  done
  
  echo "Initializing data directories:$DATA_VOLUMES"
  for vol in $DATA_VOLUMES; do
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
}

# 3) Process log directory (separate from data volumes)
process_log_directory() {
  LOG_DIR="${RUSTFS_OBS_LOG_DIRECTORY:-/logs}"
  
  echo "Initializing log directory: $LOG_DIR"
  if [ ! -d "$LOG_DIR" ]; then
    echo "  mkdir -p $LOG_DIR"
    mkdir -p "$LOG_DIR"
    # If target user is specified, try to set directory owner to that user (non-recursive to avoid large disk overhead)
    if [ -n "$RUSTFS_UID" ] && [ -n "$RUSTFS_GID" ]; then
      chown "$RUSTFS_UID:$RUSTFS_GID" "$LOG_DIR" 2>/dev/null || true
    elif [ -n "$RUSTFS_USERNAME" ] && [ -n "$RUSTFS_GROUPNAME" ]; then
      chown "$RUSTFS_USERNAME:$RUSTFS_GROUPNAME" "$LOG_DIR" 2>/dev/null || true
    fi
  fi
}

# Execute the separate processes
process_data_volumes
process_log_directory

# 4) Default credentials warning
if [ "${RUSTFS_ACCESS_KEY}" = "rustfsadmin" ] || [ "${RUSTFS_SECRET_KEY}" = "rustfsadmin" ]; then
  echo "!!!WARNING: Using default RUSTFS_ACCESS_KEY or RUSTFS_SECRET_KEY. Override them in production!"
fi

echo "Starting: $*"
set -- "$@" $DATA_VOLUMES
exec "$@"
