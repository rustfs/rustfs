#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(python3 -c 'import os,sys; print(os.path.dirname(os.path.realpath(sys.argv[1])))' "${BASH_SOURCE[0]}")"
exec "$SCRIPT_DIR/python_bin.sh" "$SCRIPT_DIR/inline_docker_lab.py" \
  --state-dir "${RUSTFS_INLINE_LAB_STATE:?set the owned inline lab state directory}" \
  mc --image "${RUSTFS_INLINE_MC_IMAGE:?set the pinned Linux mc image}" -- "$@"
