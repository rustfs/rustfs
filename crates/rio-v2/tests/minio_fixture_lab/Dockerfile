# Throwaway image for generating real MinIO on-disk fixtures without installing
# a MinIO server on the host. Multi-stage: pull the official MinIO server binary
# from the published image (linux, no dl.min.io download), then drop it into a
# small Python image that runs the fixture lab (lab.py needs only python3 +
# openssl; it drives MinIO's S3 API directly, so no `mc` is required).
#
# The MinIO release is pinned so the captured fixture format is reproducible;
# this is the release the interop tests were validated against.
FROM minio/minio:RELEASE.2025-09-07T16-13-09Z AS minio

FROM python:3.12-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=minio /usr/bin/minio /usr/local/bin/minio
RUN chmod +x /usr/local/bin/minio && /usr/local/bin/minio --version
WORKDIR /repo
