# RustFS Local Development Guide

This guide explains how to set up and run a local development environment for RustFS using Docker. This approach allows you to build and run the code from source in a consistent environment without needing to install the Rust toolchain on your host machine.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Quick Start

The development environment is configured as a Docker Compose profile named `dev`.

### 1. Setup Console UI (Optional)

If you want to use the Console UI, you must download the static assets first. The default source checkout does not include them.

```bash
bash scripts/static.sh
```

### 2. Start the Environment

To start the development container:

```bash
docker compose --profile dev up -d rustfs-dev
```

**Note**: The first run will take some time (5-10 minutes) because it builds the docker image and compiles all Rust dependencies from source. Subsequent runs will be much faster.

### 3. View Logs

To follow the application logs:

```bash
docker compose --profile dev logs -f rustfs-dev
```

### 4. Access the Services

- **S3 API**: `http://localhost:9010`
- **Console UI**: `http://localhost:9011/rustfs/console/index.html`

## Workflow

### Making Changes
The source code from your local `rustfs` directory is mounted into the container at `/app`. You can edit files in your preferred IDE on your host machine.

### Applying Changes
Since the application runs via `cargo run`, you need to restart the container to pick up changes. Thanks to incremental compilation, this is fast.

```bash
docker compose --profile dev restart rustfs-dev
```

### Rebuilding Dependencies
If you modify `Cargo.toml` or `Cargo.lock`, you generally need to rebuild the Docker image to update the cached dependencies layer:

```bash
docker compose --profile dev build rustfs-dev
```

## Troubleshooting

### `VolumeNotFound` Error
If you see an error like `Error: Custom { kind: Other, error: VolumeNotFound }`, it means the `rustfs` binary was started without valid volume arguments.
The development image uses `entrypoint.sh` to parse the `RUSTFS_VOLUMES` environment variable (supporting `{N..M}` syntax), create the directories, and pass them to `cargo run`. Ensure your `RUSTFS_VOLUMES` variable is correctly formatted.

### Slow Initial Build
This is expected. The `dev` stage in `Dockerfile.source` compiles all dependencies from scratch. Because the `/usr/local/cargo/registry` is mounted as a volume, these compiled artifacts are preserved between restarts, making future builds fast.
