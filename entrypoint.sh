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

  # ${VAR+x} distinguishes unset from present-but-empty: an env var explicitly
  # set to "" (e.g. an unexpanded compose interpolation) is a malformed value
  # that must hit the empty-value hard failure, not the missing-credential
  # warning — the binary would otherwise run with an empty root credential.
  eval "CREDENTIAL_ENV_PRESENT=\${$CREDENTIAL_ENV_NAME+x}"
  eval "CREDENTIAL_ENV_VALUE=\${$CREDENTIAL_ENV_NAME:-}"
  eval "CREDENTIAL_ENV_FILE_PRESENT=\${$CREDENTIAL_FILE_ENV_NAME+x}"
  eval "CREDENTIAL_ENV_FILE_VALUE=\${$CREDENTIAL_FILE_ENV_NAME:-}"

  if [ -n "$CREDENTIAL_ENV_PRESENT" ]; then
    CREDENTIAL_DIRECT_SET=true
    CREDENTIAL_DIRECT_VALUE="$CREDENTIAL_ENV_VALUE"
  fi

  if [ -n "$CREDENTIAL_ENV_FILE_PRESENT" ]; then
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

# Mirrors the binary's .trim() so the empty/default checks below agree with
# what the server will actually use: strips CR (CRLF-edited files) and
# surrounding blanks. Comparison only — the value passed to the binary is
# untouched.
trim_credential() {
  printf '%s' "$1" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

# Credential policy: images ship no baked-in credentials, but default or
# missing credentials only WARN — the container still starts (the binary falls
# back to its built-in default, and warns when both keys end up default).
# Hard failures (ERROR, exit 1) are reserved for malformed configuration
# (conflicting sources, unreadable files, empty values). The binary also accepts credentials via
# legacy/compat envs (RUSTFS_ROOT_*, MINIO_*) that this script does not
# inspect, so the missing-credential warning is phrased conditionally.
validate_credential_source() {
  CREDENTIAL_NAME="$1"
  FILE_NAME="$2"
  ALIAS_HINT="$3"
  CREDENTIAL_SOURCE="$4"

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
      echo "WARNING: $CREDENTIAL_NAME or $FILE_NAME is not set; unless credentials are provided via another supported source (e.g. $ALIAS_HINT), rustfs falls back to its built-in default credential. Set non-default credentials for production deployments." >&2
      ;;
    value:*)
      CREDENTIAL_VALUE=$(trim_credential "${CREDENTIAL_SOURCE#value:}")
      if [ -z "$CREDENTIAL_VALUE" ]; then
        echo "ERROR: $CREDENTIAL_NAME must not be empty." >&2
        exit 1
      fi
      if [ "$CREDENTIAL_VALUE" = "$DEFAULT_ROOT_CREDENTIAL" ]; then
        echo "WARNING: $CREDENTIAL_NAME uses the default $DEFAULT_ROOT_CREDENTIAL credential. Set non-default credentials for production deployments; with an all-default pair, multi-node clusters additionally need RUSTFS_RPC_SECRET to derive internode RPC auth." >&2
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
      # `read` fails at EOF-without-newline but still fills the variable;
      # keep the partial line so newline-less secret files stay valid.
      IFS= read -r CREDENTIAL_FILE_CONTENT < "$CREDENTIAL_FILE" || :
      CREDENTIAL_FILE_CONTENT=$(trim_credential "$CREDENTIAL_FILE_CONTENT")
      if [ -z "$CREDENTIAL_FILE_CONTENT" ]; then
        echo "ERROR: $FILE_NAME must not be empty." >&2
        exit 1
      fi
      if [ "$CREDENTIAL_FILE_CONTENT" = "$DEFAULT_ROOT_CREDENTIAL" ]; then
        echo "WARNING: $FILE_NAME contains the default $DEFAULT_ROOT_CREDENTIAL credential. Set non-default credentials for production deployments; with an all-default pair, multi-node clusters additionally need RUSTFS_RPC_SECRET to derive internode RPC auth." >&2
      fi
      ;;
  esac
}

if [ "$1" = "/usr/bin/rustfs" ]; then
  ACCESS_SOURCE=$(resolve_credential_source "RUSTFS_ACCESS_KEY" "RUSTFS_ACCESS_KEY_FILE" "access-key" "access-key-file" "$@")
  SECRET_SOURCE=$(resolve_credential_source "RUSTFS_SECRET_KEY" "RUSTFS_SECRET_KEY_FILE" "secret-key" "secret-key-file" "$@")
  validate_credential_source "RUSTFS_ACCESS_KEY" "RUSTFS_ACCESS_KEY_FILE" "RUSTFS_ROOT_USER or MINIO_ROOT_USER" "$ACCESS_SOURCE"
  validate_credential_source "RUSTFS_SECRET_KEY" "RUSTFS_SECRET_KEY_FILE" "RUSTFS_ROOT_PASSWORD or MINIO_ROOT_PASSWORD" "$SECRET_SOURCE"
fi

# Existing directories need no ownership scan; newly created directories get
# only a best-effort, non-recursive owner change, including on large mounts.
ensure_local_directory() {
  [ -d "$1" ] && return 0

  echo "  mkdir -p $1"
  if ! mkdir -p "$1"; then
    echo "ERROR: Cannot create $2. Check the configured path and write permissions for the container user (10001:10001 in official images)." >&2
    if [ "$2" = "RUSTFS_OBS_LOG_DIRECTORY" ]; then
      case "$1" in
        \"*\"|\'*\')
          echo 'HINT: RUSTFS_OBS_LOG_DIRECTORY contains literal surrounding quotes. In Docker Compose, use map syntax: RUSTFS_OBS_LOG_DIRECTORY: "/var/log/rustfs/", or list syntax without inner quotes: - RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs/' >&2
          ;;
      esac
    fi
    return 1
  fi

  if [ -n "$RUSTFS_UID" ] && [ -n "$RUSTFS_GID" ]; then
    chown "$RUSTFS_UID:$RUSTFS_GID" "$1" 2>/dev/null || true
  elif [ -n "$RUSTFS_USERNAME" ] && [ -n "$RUSTFS_GROUPNAME" ]; then
    chown "$RUSTFS_USERNAME:$RUSTFS_GROUPNAME" "$1" 2>/dev/null || true
  fi
}

has_local_pool_list() {
  pool_count=0
  for pool in $1; do
    case "$pool" in
      /*'{'*'...'*'}'*) pool_count=$((pool_count + 1)) ;;
      *) return 1 ;;
    esac
  done
  [ "$pool_count" -gt 1 ]
}

# RustFS pads every drive number in a padded pool expression. Legacy path
# lists retain the shell's original numbering for existing drive directories.
volume_range_numbers() {
  if [ "$3" = true ]; then
    case "$1" in
      0?*) seq -f "%0${#2}.0f" "$1" "$2"; return ;;
    esac
  fi
  number=$1
  while [ "$number" -le "$2" ]; do
    printf '%s\n' "$number"
    number=$((number + 1))
  done
}

# Expand the ranges in source order to preserve drive ordering in legacy lists.
expand_volume_paths() {
  expanded_paths=$1
  case "$1" in
    *'{'*'}'*) ;;
    *) return 0 ;;
  esac
  # grep returns 1 for literal braces that contain no numeric range.
  ranges=$(printf '%s\n' "$1" | grep -Eo '\{[0-9]+\.\.\.?[0-9]+\}') || [ "$?" -eq 1 ] || return 1
  for range in $ranges; do
    bounds=${range#\{}
    bounds=${bounds%\}}
    numbers=$(volume_range_numbers "${bounds%%.*}" "${bounds##*.}" "$2") || return 1
    next_paths=""
    for path in $expanded_paths; do
      prefix=${path%%"$range"*}
      suffix=${path#*"$range"}
      for number in $numbers; do
        next_paths="$next_paths $prefix$number$suffix"
      done
    done
    expanded_paths=${next_paths# }
  done
}

DATA_VOLUMES=""
process_data_volumes() {
  volumes="${RUSTFS_VOLUMES:-/data}"
  case "$volumes" in
    *','*|*'	'*) volumes=$(printf '%s\n' "$volumes" | tr ',\t' ' ') ;;
  esac
  preserve_pools=false
  if has_local_pool_list "$volumes"; then
    preserve_pools=true
  fi

  create_dirs=""
  for volume in $volumes; do
    case "$volume" in
      /*)
        expand_volume_paths "$volume" "$preserve_pools"
        if [ "$preserve_pools" = true ]; then
          DATA_VOLUMES="$DATA_VOLUMES $volume"
        else
          DATA_VOLUMES="$DATA_VOLUMES $expanded_paths"
        fi
        ;;
      *://*/*)
        # RustFS reads URL endpoints from the environment. Prepare their local directories.
        host_and_path=${volume#*://}
        expand_volume_paths "/${host_and_path#*/}" false
        ;;
      *) continue ;;
    esac
    create_dirs="$create_dirs $expanded_paths"
  done

  echo "Initializing data directories:$create_dirs"
  for path in $create_dirs; do
    ensure_local_directory "$path" RUSTFS_VOLUMES
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
  ensure_local_directory "$LOG_DIR" RUSTFS_OBS_LOG_DIRECTORY
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

BOOTSTRAP_DIR="${RUSTFS_BOOTSTRAP_DIR:-/etc/rustfs/bootstrap}"
BUCKETS_FILE="$BOOTSTRAP_DIR/buckets.json"

bootstrap_credential() {
  SOURCE="$1"
  LABEL="$2"
  case "$SOURCE" in
    value:*) printf '%s' "${SOURCE#value:}" ;;
    file:*)
      FILE="${SOURCE#file:}"
      IFS= read -r VALUE < "$FILE" || :
      trim_credential "$VALUE"
      ;;
    *)
      echo "ERROR: Bootstrap requires an explicit $LABEL or its _FILE variant." >&2
      return 1
      ;;
  esac
}

bootstrap_bucket_config() {
  BUCKET="$1"
  CONFIG_DIR="$2"
  CONFIG_FILE="$CONFIG_DIR/$BUCKET.json"
  [ -f "$CONFIG_FILE" ] || return 0
  if ! jq -e --arg bucket "$BUCKET" 'type == "array" and index($bucket) != null' "$BUCKETS_FILE" >/dev/null; then
    echo "ERROR: Bootstrap configuration '$CONFIG_FILE' refers to a bucket absent from buckets.json." >&2
    return 1
  fi
  return 0
}

cleanup_bootstrap() {
  if [ -n "${RUSTFS_PID:-}" ]; then
    kill "$RUSTFS_PID" 2>/dev/null || true
    wait "$RUSTFS_PID" 2>/dev/null || true
  fi
  if [ -n "${RC_CONFIG_DIR:-}" ]; then
    rm -rf "$RC_CONFIG_DIR" 2>/dev/null || true
  fi
}

apply_bucket_bootstrap() {
  BOOTSTRAP_ENDPOINT="${RUSTFS_BOOTSTRAP_ENDPOINT:-}"
  case "$BOOTSTRAP_ENDPOINT" in
    http://*|https://*) ;;
    *) echo "ERROR: RUSTFS_BOOTSTRAP_ENDPOINT must be an http(s) S3 endpoint when buckets are configured." >&2; return 1 ;;
  esac

  BOOTSTRAP_ACCESS_KEY=$(bootstrap_credential "$ACCESS_SOURCE" RUSTFS_ACCESS_KEY) || return 1
  BOOTSTRAP_SECRET_KEY=$(bootstrap_credential "$SECRET_SOURCE" RUSTFS_SECRET_KEY) || return 1
  ENCODED_ACCESS_KEY=$(jq -rn --arg value "$BOOTSTRAP_ACCESS_KEY" '$value | @uri') || return 1
  ENCODED_SECRET_KEY=$(jq -rn --arg value "$BOOTSTRAP_SECRET_KEY" '$value | @uri') || return 1
  ENDPOINT_AUTHORITY="${BOOTSTRAP_ENDPOINT#*://}"
  case "$ENDPOINT_AUTHORITY" in
    */*) ENDPOINT_AUTHORITY="${ENDPOINT_AUTHORITY%%/*}" ;;
  esac
  case "$BOOTSTRAP_ENDPOINT" in
    https://*) RC_HOST_bootstrap="https://${ENCODED_ACCESS_KEY}:${ENCODED_SECRET_KEY}@${ENDPOINT_AUTHORITY}" ;;
    http://*) RC_HOST_bootstrap="http://${ENCODED_ACCESS_KEY}:${ENCODED_SECRET_KEY}@${ENDPOINT_AUTHORITY}" ;;
  esac
  export RC_HOST_bootstrap

  RC_CONFIG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rustfs-bootstrap.XXXXXX") || return 1
  export RC_CONFIG_DIR

  BUCKETS=$(jq -r '.[]' "$BUCKETS_FILE")
  for bucket in $BUCKETS; do
    echo "Ensuring bootstrap bucket: $bucket"
    /usr/bin/rc bucket create "bootstrap/$bucket" --ignore-existing || return 1
    /usr/bin/rc bucket public-access-block set "bootstrap/$bucket" "$BOOTSTRAP_DIR/public-access-block.json" || return 1
  done

  for bucket in $BUCKETS; do
    for config_kind in policies versioning lifecycle cors notifications; do
      CONFIG_FILE="$BOOTSTRAP_DIR/$config_kind/$bucket.json"
      [ -f "$CONFIG_FILE" ] || continue
      bootstrap_bucket_config "$bucket" "$BOOTSTRAP_DIR/$config_kind" || return 1
      case "$config_kind" in
        policies)
          /usr/bin/rc bucket anonymous set-json "$CONFIG_FILE" "bootstrap/$bucket" || return 1
          ;;
        versioning)
          VERSIONING=$(jq -r 'if type == "boolean" then . else error("expected a JSON boolean") end' "$CONFIG_FILE") || return 1
          if [ "$VERSIONING" = "true" ]; then
            /usr/bin/rc bucket version enable "bootstrap/$bucket" || return 1
          else
            /usr/bin/rc bucket version suspend "bootstrap/$bucket" || return 1
          fi
          ;;
        lifecycle)
          /usr/bin/rc bucket lifecycle rule import "bootstrap/$bucket" "$CONFIG_FILE" || return 1
          ;;
        cors)
          /usr/bin/rc bucket cors set "bootstrap/$bucket" "$CONFIG_FILE" || return 1
          ;;
        notifications)
          while IFS="$(printf '\t')" read -r arn events; do
            /usr/bin/rc bucket event add "bootstrap/$bucket" "$arn" --event "$events" || return 1
          done <<EOF
$(jq -r '.[] | [.arn, (.events | join(","))] | @tsv' "$CONFIG_FILE")
EOF
          ;;
      esac
    done
  done
  return 0
}

validate_bootstrap_assets() {
  for config_kind in policies versioning lifecycle cors notifications; do
    CONFIG_DIR="$BOOTSTRAP_DIR/$config_kind"
    for CONFIG_FILE in "$CONFIG_DIR"/*.json; do
      [ -f "$CONFIG_FILE" ] || continue
      CONFIG_BUCKET=${CONFIG_FILE##*/}
      CONFIG_BUCKET=${CONFIG_BUCKET%.json}
      bootstrap_bucket_config "$CONFIG_BUCKET" "$CONFIG_DIR" || return 1
      case "$config_kind" in
        versioning)
          jq -e 'type == "boolean"' "$CONFIG_FILE" >/dev/null || {
            echo "ERROR: Versioning configuration '$CONFIG_FILE' must contain a JSON boolean." >&2
            return 1
          }
          ;;
        notifications)
          jq -e 'type == "array" and all(.[]; keys == ["arn", "events"] and (.arn | type == "string" and length > 0) and (.events | type == "array" and length > 0 and all(.[]; type == "string" and length > 0))) and ((map(.arn) | unique | length) == length)' "$CONFIG_FILE" >/dev/null || {
            echo "ERROR: Notification configuration '$CONFIG_FILE' must be an array of unique arn/events objects." >&2
            return 1
          }
          ;;
      esac
    done
  done
}

if [ "$1" = "/usr/bin/rustfs" ]; then
  if [ ! -r "$BUCKETS_FILE" ] || ! jq -e '. as $buckets | (type == "array" and all(.[]; type == "string" and test("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$") and (contains("..") | not))) and ((unique | length) == ($buckets | length))' "$BUCKETS_FILE" >/dev/null; then
    echo "ERROR: $BUCKETS_FILE must be a JSON array of unique, valid S3 bucket names." >&2
    exit 1
  fi

  validate_bootstrap_assets || exit 1
  BOOTSTRAP_BUCKET_COUNT=$(jq 'length' "$BUCKETS_FILE")
  if [ "$BOOTSTRAP_BUCKET_COUNT" -gt 0 ]; then
    [ -r "$BOOTSTRAP_DIR/public-access-block.json" ] || {
      echo "ERROR: Missing $BOOTSTRAP_DIR/public-access-block.json." >&2
      exit 1
    }
    if ! jq -e 'type == "object" and keys == ["block_public_acls", "block_public_policy", "ignore_public_acls", "restrict_public_buckets"] and all(.[]; type == "boolean")' "$BOOTSTRAP_DIR/public-access-block.json" >/dev/null; then
      echo "ERROR: Public Access Block configuration must define all four boolean flags and no other fields." >&2
      exit 1
    fi
    BOOTSTRAP_ENDPOINT="${RUSTFS_BOOTSTRAP_ENDPOINT:-}"
    case "$BOOTSTRAP_ENDPOINT" in
      http://*|https://*) ;;
      *) echo "ERROR: RUSTFS_BOOTSTRAP_ENDPOINT is required when buckets are configured." >&2; exit 1 ;;
    esac
    ENDPOINT_AUTHORITY="${BOOTSTRAP_ENDPOINT#*://}"
    case "$ENDPOINT_AUTHORITY" in
      */*|*\?*|*\#*|*@*) echo "ERROR: RUSTFS_BOOTSTRAP_ENDPOINT must contain only the scheme, host, and optional port." >&2; exit 1 ;;
    esac
    bootstrap_credential "$ACCESS_SOURCE" RUSTFS_ACCESS_KEY >/dev/null || exit 1
    bootstrap_credential "$SECRET_SOURCE" RUSTFS_SECRET_KEY >/dev/null || exit 1
    /usr/bin/rc bucket public-access-block --help >/dev/null 2>&1 || {
      echo "ERROR: The bundled RustFS CLI does not support bucket Public Access Block; rebuild with an rc release that includes bootstrap support." >&2
      exit 1
    }
    BOOTSTRAP_TIMEOUT="${RUSTFS_BOOTSTRAP_TIMEOUT:-120}"
    case "$BOOTSTRAP_TIMEOUT" in
      ''|*[!0-9]*|0) echo "ERROR: RUSTFS_BOOTSTRAP_TIMEOUT must be a positive integer number of seconds." >&2; exit 1 ;;
    esac
    [ "$BOOTSTRAP_TIMEOUT" -le 3600 ] || { echo "ERROR: RUSTFS_BOOTSTRAP_TIMEOUT must not exceed 3600 seconds." >&2; exit 1; }
    MAX_BOOTSTRAP_ATTEMPTS=$(((BOOTSTRAP_TIMEOUT + 1) / 2))

    "$@" &
    RUSTFS_PID=$!
    trap cleanup_bootstrap EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM

    BOOTSTRAP_HEALTH_URL="${BOOTSTRAP_ENDPOINT%/}/health/ready"
    attempt=0
    while ! curl -fsS "$BOOTSTRAP_HEALTH_URL" >/dev/null 2>&1; do
      kill -0 "$RUSTFS_PID" 2>/dev/null || { echo "ERROR: RustFS exited before becoming ready for bootstrap." >&2; exit 1; }
      attempt=$((attempt + 1))
      [ "$attempt" -lt "$MAX_BOOTSTRAP_ATTEMPTS" ] || { echo "ERROR: RustFS did not become ready for bootstrap at RUSTFS_BOOTSTRAP_ENDPOINT within RUSTFS_BOOTSTRAP_TIMEOUT." >&2; exit 1; }
      sleep 2
    done

    set +e
    apply_bucket_bootstrap
    BOOTSTRAP_STATUS=$?
    set -e
    if [ "$BOOTSTRAP_STATUS" -ne 0 ]; then
      echo "ERROR: RustFS bootstrap failed." >&2
      exit "$BOOTSTRAP_STATUS"
    fi
    wait "$RUSTFS_PID"
    exit $?
  fi
fi

exec "$@"
