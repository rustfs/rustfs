# syntax=docker/dockerfile:1.6
# Copyright 2026 RustFS Team
# Licensed under the Apache License, Version 2.0.

# A compatible native runtime with Python 3.11+, getent and stat is required by
# the isolated fixture's staging/provenance checks. This is a test image.
ARG RUNTIME_BASE=python:3.13-slim-trixie
FROM ${RUNTIME_BASE}

ARG SOURCE_REVISION
ARG BINARY_SHA256
ARG BUILD_PROFILE=dev
COPY rustfs /usr/bin/rustfs
RUN python3 - "$SOURCE_REVISION" "$BINARY_SHA256" "$BUILD_PROFILE" <<'PY'
import hashlib
import os
import re
import shutil
import sys

source, digest, profile = sys.argv[1:]
if not re.fullmatch(r"[0-9a-f]{40}", source):
    raise SystemExit("invalid source revision")
if not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit("invalid binary digest")
if profile not in ("dev", "release"):
    raise SystemExit("invalid build profile")
with open("/usr/bin/rustfs", "rb") as binary:
    if hashlib.file_digest(binary, "sha256").hexdigest() != digest:
        raise SystemExit("binary digest mismatch")
for tool in ("getent", "stat"):
    if shutil.which(tool) is None:
        raise SystemExit(f"missing runtime tool: {tool}")
os.chmod("/usr/bin/rustfs", 0o755)
PY
LABEL io.rustfs.repro.source_revision="${SOURCE_REVISION}" \
      io.rustfs.repro.binary_sha256="${BINARY_SHA256}" \
      io.rustfs.repro.build_profile="${BUILD_PROFILE}"
USER 10001:10001
ENTRYPOINT ["/usr/bin/rustfs"]
