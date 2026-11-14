# Inline geometry Linux lab

**Use this when** validating inline placement, block geometry, Range reads and reader rollback in a task-owned local Docker cluster.

The adapter reuses the repository's four-node compose topology with 16 logical Docker volumes and EC12+4. It binds one S3 port to loopback, keeps strict durability and checks running binary hashes and persisted object geometry. Docker volumes share the host storage substrate. Performance observations describe this local virtualized lab.

## Build a frozen server image

Use the current stable toolchain pinned in the lab Dockerfile. Supply a Git bundle and the metadata inspector source as the build context. The server is compiled from a clean checkout before the diagnostic example is added.

```bash
set -euo pipefail
LAB_BUILD_DIR="$(mktemp -d)"
LAB_REVISION="$(git rev-parse HEAD)"
git bundle create "$LAB_BUILD_DIR/repository.bundle" HEAD
cp crates/filemeta/examples/inline_lab_inspect.rs "$LAB_BUILD_DIR/inspect_geometry.rs"
docker --context "$LAB_CONTEXT" build --target build-tools \
  -t rustfs-inline-build-tools:1.100 \
  -f .docker/inline-lab/Dockerfile .docker/inline-lab
docker --context "$LAB_CONTEXT" build \
  --build-arg SOURCE_REVISION="$LAB_REVISION" \
  -t rustfs-inline-validation:local \
  -f .docker/inline-lab/Dockerfile "$LAB_BUILD_DIR"
LAB_EXPORT_ID="$(docker --context "$LAB_CONTEXT" create \
  --label rustfs.validation=inline rustfs-inline-validation:local)"
docker --context "$LAB_CONTEXT" cp \
  "$LAB_EXPORT_ID:/usr/local/bin/rustfs" "$LAB_BUILD_DIR/rustfs-linux"
docker --context "$LAB_CONTEXT" rm "$LAB_EXPORT_ID"
```

## Configure and probe

Select a new compose project and a private state directory. The adapter generates test credentials there. Existing resources require matching ownership labels. Configure checks the image's source revision and pins immutable image IDs. Each operation takes an exclusive state lock.

```bash
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" configure \
  --context "$LAB_CONTEXT" --project "$LAB_PROJECT" \
  --image rustfs-inline-validation:local --revision "$LAB_REVISION" \
  --binary "$LAB_BUILD_DIR/rustfs-linux" --port 19080
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" deploy \
  --bucket inline-geometry-probe --shard-bytes 0
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" probe \
  --bucket inline-geometry-probe
```

The probe verifies complete bytes, HEAD sizes, Range contents, EC12+4 and the actual persisted block. It records inline markers for 4 KiB, 1 MiB, 1.25 MiB and 1.5 MiB objects in `probe.json`. The server-only image includes the existing filemeta decoder through a small geometry inspector. Console assets and the administration CLI are outside this lab image.

## ABBA deployment hook

Use `inline_docker_lab.py ... deploy` in the existing `run_hotpath_warp_abba.sh` external deploy hook. Export `RUSTFS_ERASURE_SHARD_BYTES` according to `HOTPATH_ABBA_PHASE`. The adapter uses the supplied leg, phase, bucket, binary and evidence-file context, resets only owned lab volumes, checks running configuration and binary hashes, creates the fresh bucket and writes the runner's deployment evidence.

Use the same frozen binary for both sides of a geometry experiment. Fix the inline budget and encoding mode, then compare zero with one candidate shard value. Include the CPU, memory and image identities from every node in each result. The state `config.json` supplies the generated test credentials for the load driver. Set `DOCKER_CONTEXT` to the selected context when the driver captures Docker resource snapshots.

For content compatibility across an image or geometry change, configure the next selected image and artifact using the same state, then deploy with `--keep-data` and an existing bucket. This retains owned volumes. Compare existing objects and version IDs with the manifest prepared before the change.

## Cleanup

```bash
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" down --purge
```

Cleanup checks ownership for containers, volumes and networks. A configuration cannot adopt another project's resources. Raw measurements, state, Git bundles and build outputs stay in task artifact directories.

Unit checks run with `scripts/python_bin.sh scripts/test_inline_docker_lab.py` and are included in `make script-tests`.

## Linux content-probe client

The PUT after-probe requires a working `mc`. For a macOS client affected by cgo initialization failures, compile the same frozen public mc module for Linux with CGO disabled, build it with `Dockerfile.mc`, and use `scripts/inline_docker_mc.sh` as the process-local `mc` command. Record the module version, checksum and resulting image ID with the run.

The adapter rewrites only the lab loopback alias endpoint to the private `node1` endpoint. Alias configuration stays in the probe's own `MC_CONFIG_DIR`, and `cat` streams raw bytes. The existing HEAD, GET and hash checks continue to execute.

```bash
export RUSTFS_INLINE_LAB_STATE="$LAB_STATE"
export RUSTFS_INLINE_MC_IMAGE="$LAB_MC_IMAGE"
mkdir -p "$LAB_TOOLS"
ln -s "$PWD/scripts/inline_docker_mc.sh" "$LAB_TOOLS/mc"
export PATH="$LAB_TOOLS:$PATH"
```
