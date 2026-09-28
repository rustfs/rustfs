# Throwaway image for generating real MinIO on-disk fixtures without installing
# a MinIO server on the host. Multi-stage: extract the official MinIO server
# binary from its checksum-pinned GitHub release package, then drop it into a
# small Python image that runs the fixture lab (lab.py needs only python3 +
# openssl; it drives MinIO's S3 API directly, so no `mc` is required).
#
# The MinIO release is pinned so the captured fixture format is reproducible;
# this is the release the interop tests were validated against.
#
# MINIO_IMAGE can override the package stage with an accessible image carrying
# the same release at /usr/bin/minio. PYTHON_IMAGE can point at a Python mirror.
ARG MINIO_IMAGE=minio-release
ARG PYTHON_IMAGE=python:3.12-slim

FROM scratch AS minio-package-amd64
ADD --checksum=sha256:eeda08f699f6592d1b868ac8bda864ae2cacdb5ee1b888663366e8c8ff566249 https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio_20250907161309.0.0_amd64.deb /minio.deb

FROM scratch AS minio-package-arm64
ADD --checksum=sha256:f9466ed832ac4926c8870b3b8378d77861a87b41529fecae7789b9bc54acd78b https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio_20250907161309.0.0_arm64.deb /minio.deb

FROM scratch AS minio-package-ppc64le
ADD --checksum=sha256:39eec77281c552dfa0a92b5221c657dc58306ea8c4e92b79e9562cee61b59caa https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio_20250907161309.0.0_ppc64el.deb /minio.deb

FROM minio-package-${TARGETARCH} AS minio-package

FROM ${PYTHON_IMAGE} AS minio-release
COPY --from=minio-package /minio.deb /tmp/minio.deb
RUN dpkg-deb --extract /tmp/minio.deb /tmp/minio-package \
    && install -D /tmp/minio-package/usr/local/bin/minio /usr/bin/minio \
    && rm -rf /tmp/minio.deb /tmp/minio-package

FROM ${MINIO_IMAGE} AS minio

FROM ${PYTHON_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=minio /usr/bin/minio /usr/local/bin/minio
RUN chmod +x /usr/local/bin/minio && /usr/local/bin/minio --version
WORKDIR /repo
