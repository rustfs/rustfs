# Throwaway image for generating real MinIO on-disk fixtures without installing
# a MinIO server on the host. Multi-stage: pull the official MinIO server binary
# from the published image (linux, no dl.min.io download), then drop it into a
# small Python image that runs the fixture lab (lab.py needs only python3 +
# openssl; it drives MinIO's S3 API directly, so no `mc` is required).
#
# The MinIO release is pinned so the captured fixture format is reproducible;
# this is the release the interop tests were validated against.
#
# Both base images are build args so a network that cannot reach Docker Hub can
# point them at a mirror carrying the same content — quay.io publishes the MinIO
# releases, and public.ecr.aws mirrors the official Python images. CI keeps the
# Docker Hub defaults. Override with:
#   --build-arg MINIO_IMAGE=quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z \
#   --build-arg PYTHON_IMAGE=public.ecr.aws/docker/library/python:3.12-slim
ARG MINIO_IMAGE=minio/minio:RELEASE.2025-09-07T16-13-09Z
ARG PYTHON_IMAGE=python:3.12-slim
FROM ${MINIO_IMAGE} AS minio

FROM ${PYTHON_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=minio /usr/bin/minio /usr/local/bin/minio
RUN chmod +x /usr/local/bin/minio && /usr/local/bin/minio --version
WORKDIR /repo
