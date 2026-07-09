#!/usr/bin/env bash
set -euo pipefail

# Guard: tokio's io-uring runtime backend must stay disabled.
#
# Restricted Linux environments (Docker default seccomp, gVisor, proot,
# old kernels) reject io_uring_setup with EACCES/ENOSYS. With the global
# `--cfg tokio_unstable` in .cargo/config.toml, a tokio "io-uring" feature
# anywhere in the workspace silently switches every Linux build's file I/O
# onto that backend and turns the rejection into DiskAccessDenied at
# startup (backlog#890). Any future io_uring integration must go through
# an application-level, runtime-probed backend (backlog#894), never the
# tokio runtime feature — so only the tokio dependency line is banned
# here; an explicit `io-uring` crate dependency is allowed.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${CHECK_NO_TOKIO_IO_URING_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"

cd "$ROOT_DIR"

status=0

while IFS=: read -r file line content; do
    printf '%s:%s: tokio must not enable the "io-uring" runtime feature: %s\n' \
        "$file" "$line" "$content" >&2
    status=1
done < <(rg -n '^[[:space:]]*tokio[[:space:]]*=.*"io-uring"' --glob '**/Cargo.toml' . || true)

exit "$status"
