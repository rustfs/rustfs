#!/bin/sh
set -e

# 1) Normalize command:
# - No arguments: default to execute rustfs with DATA_VOLUMES
# - First argument starts with '-': treat as rustfs arguments, auto-prefix rustfs
# - First argument is 'rustfs': replace with absolute path to avoid PATH interference
# - Otherwise: treat as full rustfs arguments (e.g., /data paths)
if [ $# -eq 0 ]; then
  set -- /usr/bin/rustfs
elif [ "${1#-}" != "$1" ]; then
  set -- /usr/bin/rustfs "$@"
elif [ "$1" = "rustfs" ]; then
  shift
  set -- /usr/bin/rustfs "$@"
elif [ "$1" = "/usr/bin/rustfs" ]; then
  : # already normalized
elif [ "$1" = "cargo" ]; then
  : # Pass through cargo command as-is
else
  set -- /usr/bin/rustfs "$@"
fi

DEFAULT_ROOT_CREDENTIAL="rustfsadmin"

resolve_credential_source() {
  CREDENTIAL_ENV_NAME="$1"
  CREDENTIAL_FILE_ENV_NAME="$2"
  CREDENTIAL_VALUE_OPTION="$3"
  CREDENTIAL_FILE_OPTION="$4"
  shift 4

  CREDENTIAL_DIRECT_SET=false
  CREDENTIAL_DIRECT_VALUE=""
  CREDENTIAL_FILE_SET=false
  CREDENTIAL_FILE_VALUE=""

  eval "CREDENTIAL_ENV_VALUE=\${$CREDENTIAL_ENV_NAME:-}"
  eval "CREDENTIAL_ENV_FILE_VALUE=\${$CREDENTIAL_FILE_ENV_NAME:-}"

  if [ -n "$CREDENTIAL_ENV_VALUE" ]; then
    CREDENTIAL_DIRECT_SET=true
    CREDENTIAL_DIRECT_VALUE="$CREDENTIAL_ENV_VALUE"
  fi

  if [ -n "$CREDENTIAL_ENV_FILE_VALUE" ]; then
    CREDENTIAL_FILE_SET=true
    CREDENTIAL_FILE_VALUE="$CREDENTIAL_ENV_FILE_VALUE"
  fi

  while [ "$#" -gt 0 ]; do
    arg="$1"
    case "$arg" in
      --)
        break
        ;;
      --"$CREDENTIAL_VALUE_OPTION"=*)
        CREDENTIAL_DIRECT_SET=true
        CREDENTIAL_DIRECT_VALUE="${arg#*=}"
        ;;
      --"$CREDENTIAL_VALUE_OPTION")
        shift
        if [ "$#" -eq 0 ]; then
          echo "error:--$CREDENTIAL_VALUE_OPTION requires a value."
          return
        fi
        CREDENTIAL_DIRECT_SET=true
        CREDENTIAL_DIRECT_VALUE="$1"
        ;;
      --"$CREDENTIAL_FILE_OPTION"=*)
        CREDENTIAL_FILE_SET=true
        CREDENTIAL_FILE_VALUE="${arg#*=}"
        ;;
      --"$CREDENTIAL_FILE_OPTION")
        shift
        if [ "$#" -eq 0 ]; then
          echo "error:--$CREDENTIAL_FILE_OPTION requires a value."
          return
        fi
        CREDENTIAL_FILE_SET=true
        CREDENTIAL_FILE_VALUE="$1"
        ;;
    esac

    shift
  done

  if [ "$CREDENTIAL_DIRECT_SET" = "true" ] && [ "$CREDENTIAL_FILE_SET" = "true" ]; then
    echo "conflict"
    return
  fi

  if [ "$CREDENTIAL_DIRECT_SET" = "true" ]; then
    echo "value:$CREDENTIAL_DIRECT_VALUE"
    return
  fi

  if [ "$CREDENTIAL_FILE_SET" = "true" ]; then
    echo "file:$CREDENTIAL_FILE_VALUE"
    return
  fi

  echo "missing"
}

validate_credential_source() {
  CREDENTIAL_NAME="$1"
  FILE_NAME="$2"
  CREDENTIAL_SOURCE="$3"

  case "$CREDENTIAL_SOURCE" in
    error:*)
      echo "ERROR: ${CREDENTIAL_SOURCE#error:}" >&2
      exit 1
      ;;
    conflict)
      echo "ERROR: Set either $CREDENTIAL_NAME or $FILE_NAME, not both." >&2
      exit 1
      ;;
    missing)
      echo "ERROR: $CREDENTIAL_NAME or $FILE_NAME must be set explicitly for container startup." >&2
      exit 1
      ;;
    value:*)
      CREDENTIAL_VALUE="${CREDENTIAL_SOURCE#value:}"
      if [ -z "$CREDENTIAL_VALUE" ]; then
        echo "ERROR: $CREDENTIAL_NAME must not be empty." >&2
        exit 1
      fi
      if [ "$CREDENTIAL_VALUE" = "$DEFAULT_ROOT_CREDENTIAL" ]; then
        echo "ERROR: $CREDENTIAL_NAME must not use the default $DEFAULT_ROOT_CREDENTIAL credential." >&2
        exit 1
      fi
      ;;
    file:*)
      CREDENTIAL_FILE="${CREDENTIAL_SOURCE#file:}"
      if [ -z "$CREDENTIAL_FILE" ]; then
        echo "ERROR: $FILE_NAME must not be empty." >&2
        exit 1
      fi
      if [ ! -r "$CREDENTIAL_FILE" ]; then
        echo "ERROR: $FILE_NAME points to an unreadable file." >&2
        exit 1
      fi
      if IFS= read -r CREDENTIAL_FILE_CONTENT < "$CREDENTIAL_FILE"; then
        :
      else
        CREDENTIAL_FILE_CONTENT=""
      fi
      if [ -z "$CREDENTIAL_FILE_CONTENT" ]; then
        echo "ERROR: $FILE_NAME must not be empty." >&2
        exit 1
      fi
      if [ "$CREDENTIAL_FILE_CONTENT" = "$DEFAULT_ROOT_CREDENTIAL" ]; then
        echo "ERROR: $FILE_NAME must not contain the default $DEFAULT_ROOT_CREDENTIAL credential." >&2
        exit 1
      fi
      ;;
  esac
}

if [ "$1" = "/usr/bin/rustfs" ]; then
  ACCESS_SOURCE=$(resolve_credential_source "RUSTFS_ACCESS_KEY" "RUSTFS_ACCESS_KEY_FILE" "access-key" "access-key-file" "$@")
  SECRET_SOURCE=$(resolve_credential_source "RUSTFS_SECRET_KEY" "RUSTFS_SECRET_KEY_FILE" "secret-key" "secret-key-file" "$@")
  validate_credential_source "RUSTFS_ACCESS_KEY" "RUSTFS_ACCESS_KEY_FILE" "$ACCESS_SOURCE"
  validate_credential_source "RUSTFS_SECRET_KEY" "RUSTFS_SECRET_KEY_FILE" "$SECRET_SOURCE"
fi

# 2) Process data volumes (separate from log directory)
DATA_VOLUMES=""
process_data_volumes() {
  VOLUME_RAW="${RUSTFS_VOLUMES:-/data}"
  # Convert comma/tab to space
  VOLUME_LIST_RAW=$(echo "$VOLUME_RAW" | tr ',\t' ' ')
  
  VOLUME_LIST=""
  for vol in $VOLUME_LIST_RAW; do
      # Helper to manually expand {N..M} since sh doesn't support it on variables
      if echo "$vol" | grep -E -q "\{[0-9]+\.\.\.?[0-9]+\}"; then
           PREFIX=${vol%%\{*}
           SUFFIX=${vol##*\}}
           RANGE=${vol#*\{}
           RANGE=${RANGE%\}}
           RANGE=$(echo "$RANGE" | sed 's/\.\.\./../')
           START=${RANGE%%..*}
           END=${RANGE##*..}
           
           # Check if START and END are numbers
           if [ "$START" -eq "$START" ] 2>/dev/null && [ "$END" -eq "$END" 2>/dev/null ]; then
               i=$START
               while [ "$i" -le "$END" ]; do
                 VOLUME_LIST="$VOLUME_LIST ${PREFIX}${i}${SUFFIX}"
                 i=$((i+1))
               done
           else
               # Fallback if not numbers
               VOLUME_LIST="$VOLUME_LIST $vol"
           fi
      else
           VOLUME_LIST="$VOLUME_LIST $vol"
      fi
  done

  for vol in $VOLUME_LIST; do
    case "$vol" in
      /*) DATA_VOLUMES="$DATA_VOLUMES $vol" ;;
      *)  : ;; # skip non-absolute paths (including http(s):// URLs)
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
  # Output logs to stdout
  if [ -z "$RUSTFS_OBS_LOG_DIRECTORY" ]; then
    echo "OBS log directory not configured and logs outputs to stdout"
    return
  fi

  # Output logs to remote endpoint
  if [ "${RUSTFS_OBS_LOG_DIRECTORY}" != "${RUSTFS_OBS_LOG_DIRECTORY#*://}" ]; then
    echo "Output logs to remote endpoint"
    return
  fi

  # Outputs logs to local directory
  LOG_DIR="${RUSTFS_OBS_LOG_DIRECTORY}"
  
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

# 5) Append DATA_VOLUMES only if no data paths in arguments
# Check if any argument looks like a data path (starts with / and not an option)
HAS_DATA_PATH=false
for arg in "$@"; do
  case "$arg" in
    /usr/bin/rustfs) continue ;;
    -*) continue ;;
    /*) HAS_DATA_PATH=true; break ;;
  esac
done

if [ "$HAS_DATA_PATH" = "false" ] && [ -n "$DATA_VOLUMES" ]; then
  echo "Starting: $* $DATA_VOLUMES"
  set -- "$@" $DATA_VOLUMES
else
  echo "Starting: $*"
fi

exec "$@"
