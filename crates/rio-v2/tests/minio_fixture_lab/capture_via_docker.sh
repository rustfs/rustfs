#!/usr/bin/env bash
# Generate real MinIO on-disk fixtures via Docker — no host `minio`/`mc` install.
#
# Builds a throwaway image carrying the official MinIO server binary plus the
# fixture lab, runs `lab.py capture-matrix` inside it, and writes the captured
# backend fixtures under crates/rio-v2/tests/fixtures/minio-generated (which is
# gitignored — regenerate as needed). The same script is used locally and by the
# nightly `minio-interop` CI workflow so both paths stay in sync.
#
# Usage:
#   ./capture_via_docker.sh                       # the two multipart cases the
#                                                 # ignored interop tests consume
#   ./capture_via_docker.sh sse-s3-singlepart-64k # specific case id(s)
#   ./capture_via_docker.sh all                   # full SSE/size matrix
set -euo pipefail

IMAGE="${MINIO_LAB_IMAGE:-rustfs-minio-lab:latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# repo root is four levels up: crates/rio-v2/tests/minio_fixture_lab -> repo
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
LAB_REL="crates/rio-v2/tests/minio_fixture_lab"
FIXTURE_REL="crates/rio-v2/tests/fixtures/minio-generated"

# Default to the two multipart cases the ignored round-trip tests consume; pass
# case ids to override, or "all" for the full default matrix.
cases=("$@")
if [ "${#cases[@]}" -eq 0 ]; then
  cases=(sse-s3-multipart-8m sse-kms-multipart-8m)
fi
case_args=()
if [ "${cases[0]}" != "all" ]; then
  for c in "${cases[@]}"; do
    case_args+=(--case-id "$c")
  done
fi

echo ">> building ${IMAGE}"
docker build -f "${SCRIPT_DIR}/Dockerfile" -t "${IMAGE}" "${SCRIPT_DIR}"

echo ">> capturing fixtures into ${FIXTURE_REL}"
docker run --rm -v "${REPO_ROOT}:/repo" "${IMAGE}" \
  python3 "/repo/${LAB_REL}/lab.py" capture-matrix \
    --root "/repo/${FIXTURE_REL}" \
    --work-root /tmp/minio-lab-work \
    --minio-binary /usr/local/bin/minio \
    "${case_args[@]}"

echo ">> done — fixtures under ${REPO_ROOT}/${FIXTURE_REL}/cases/"
