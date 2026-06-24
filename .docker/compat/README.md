# Rio compatibility compose files

These compose files prepare 4-node, 4-disk clusters for rio/rio-v2 storage format compatibility checks. All disks are bind-mounted under `.docker/compat/data` so the on-disk files remain available on the host.

## Clusters

```bash
docker compose -f .docker/compat/docker-compose.rustfs-beta5.yml up -d --build
docker compose -f .docker/compat/docker-compose.minio.yml up -d
docker compose -f .docker/compat/docker-compose.rustfs-rio-v2.yml up -d --build
```

Default API endpoints:

- RustFS `1.0.0-beta.5`: `http://127.0.0.1:9100`
- MinIO: `http://127.0.0.1:9200`
- current main with `rio-v2`: `http://127.0.0.1:9300`

## Reading old datasets with rio-v2

Stop the writer cluster before mounting its disks into the rio-v2 cluster.

```bash
docker compose -f .docker/compat/docker-compose.rustfs-beta5.yml down
RUSTFS_RIO_V2_DATASET=./data/rustfs-beta5 \
  docker compose -f .docker/compat/docker-compose.rustfs-rio-v2.yml up -d --build

docker compose -f .docker/compat/docker-compose.minio.yml down
RUSTFS_RIO_V2_DATASET=./data/minio \
  docker compose -f .docker/compat/docker-compose.rustfs-rio-v2.yml up -d --build
```

## 200G object mix

Use the same bucket/object matrix against the beta5 and MinIO endpoints, then read it back through the rio-v2 endpoint. A practical 200G mix is:

- 1 KiB x 1024
- 1 MiB x 1024
- 64 MiB x 512
- 1 GiB x 64
- 8 GiB x 12
- 6 GiB x 1

Compression is enabled by default for RustFS and MinIO. Server-side KMS/SSE settings are intentionally left to environment variables or mounted key directories so real key material is not committed. For SSE-C cases, run the clusters with TLS because MinIO requires HTTPS for SSE-C.

## High-concurrency write/read stress

Use `run_rw_compat_stress.sh` to generate a manifest on an old endpoint, then verify the same objects through the rio-v2 endpoint after mounting the old disks.

```bash
.docker/compat/run_rw_compat_stress.sh \
  --mode write \
  --endpoint http://127.0.0.1:9100 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket compat-beta5 \
  --concurrency 96

RUSTFS_RIO_V2_DATASET=./data/rustfs-beta5 \
  docker compose -f .docker/compat/docker-compose.rustfs-rio-v2.yml up -d --build

.docker/compat/run_rw_compat_stress.sh \
  --mode verify \
  --endpoint http://127.0.0.1:9300 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket compat-beta5 \
  --concurrency 96 \
  --manifest target/compat/rw-stress-YYYYmmdd-HHMMSS/manifest.csv
```

For encrypted datasets, add `--encryption sse-s3`, `--encryption sse-kms --sse-kms-key-id <key-id>`, or `--encryption sse-c --sse-c-key-file <raw-32-byte-key-file>` to both the write and verify commands.

## 5 GiB encrypted compatibility run

The `5g` profile covers 1 KiB, 1 MiB, 16 MiB, 64 MiB, and 1 GiB objects and totals exactly 5 GiB. Generate `compat-key.key` under `.docker/compat/kms/rustfs-compat`, enable local KMS on both RustFS clusters, then use the same encryption arguments while writing with beta5 and verifying with rio-v2. Set non-default local test credentials first because distributed listeners reject the built-in default credentials.

```bash
export COMPAT_ACCESS_KEY='<non-default-access-key>'
export COMPAT_SECRET_KEY='<non-default-secret-key>'

RUSTFS_ACCESS_KEY="$COMPAT_ACCESS_KEY" RUSTFS_SECRET_KEY="$COMPAT_SECRET_KEY" \
  RUSTFS_KMS_ENABLE=true RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true \
  docker compose -f .docker/compat/docker-compose.rustfs-beta5.yml up -d

.docker/compat/run_rw_compat_stress.sh \
  --mode write --endpoint http://127.0.0.1:9100 \
  --access-key "$COMPAT_ACCESS_KEY" --secret-key "$COMPAT_SECRET_KEY" \
  --bucket compat-beta5-sse-s3 --profile 5g --concurrency 16 \
  --encryption sse-s3
```
