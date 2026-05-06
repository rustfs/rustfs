# Issue 2815 Local Docker Verification

## Purpose

This directory contains the local distributed Docker verification assets used to validate issue `#2815` against the current source build.

The target behavior is:

- 4-node distributed cluster starts successfully
- `/health/ready` becomes reachable on each node
- logs no longer contain `storage_info failed: Io error: wrong msgpack marker FixArray(1)`
- internode RPC authentication succeeds with an explicit non-default RPC secret

## Files

- `docker-compose.yml`: 4-node distributed cluster using a locally built image

## Data Directories

Create the bind-mount directories before `docker compose up`:

```bash
mkdir -p .docker/test/issues-2815/data/rustfs{1..4}-disk{0..3}
```

## Build

Apple Silicon / arm64 host:

```bash
docker build --platform linux/arm64 -f Dockerfile.source -t rustfs-issue-2815-local .
```

If you intentionally want amd64 emulation:

```bash
docker build --platform linux/amd64 -f Dockerfile.source -t rustfs-issue-2815-local .
```

## Run

```bash
docker compose -f .docker/test/issues-2815/docker-compose.yml up -d
```

If the image platform is not `linux/arm64`, align compose explicitly:

```bash
RUSTFS_DOCKER_PLATFORM=linux/amd64 docker compose -f .docker/test/issues-2815/docker-compose.yml up -d
```

## Health Checks

Container-level healthcheck is now included and probes:

```bash
curl -fsS http://127.0.0.1:9000/health
```

Manual checks:

```bash
curl -i http://127.0.0.1:9101/health/ready
curl -i http://127.0.0.1:9102/health/ready
curl -i http://127.0.0.1:9103/health/ready
curl -i http://127.0.0.1:9104/health/ready
```

## RPC Secret Requirement

The current source build no longer reproduces the original `FixArray(1)` decode error from issue `#2815`.

Earlier local Docker attempts failed during erasure bootstrap with:

```text
No valid auth token
store init failed to load formats after 10 retries: erasure read quorum
```

Root cause:

- RPC authentication rejects the default secret `rustfsadmin`
- distributed local Docker validation therefore needs an explicit non-default secret

This compose now sets both:

- `RUSTFS_SECRET_KEY=issue-2815-secret`
- `RUSTFS_RPC_SECRET=issue-2815-rpc-secret`

With those values in place, the current 4-node local Docker cluster reaches healthy state and `/health/ready` returns `200`.

In other words:

- `RUSTFS_ACCESS_KEY` may still be `rustfsadmin` for local service credentials if desired
- `RUSTFS_SECRET_KEY` can still be used for service credentials
- but RPC authentication must not resolve to the default secret value `rustfsadmin`
- if `RUSTFS_RPC_SECRET` is unset, the code falls back to `RUSTFS_SECRET_KEY`
- so at least one of them must provide a non-default shared secret for internode RPC signing

## Suggested Debug Commands

```bash
docker compose -f .docker/test/issues-2815/docker-compose.yml ps
docker compose -f .docker/test/issues-2815/docker-compose.yml logs --no-color --tail=200
docker compose -f .docker/test/issues-2815/docker-compose.yml down -v
```
