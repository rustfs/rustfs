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

Select a new compose project and a private state directory. The adapter generates test credentials there. Existing resources require matching ownership labels. Configure checks the image's source revision and pins immutable image IDs. Each operation takes an exclusive state lock. Deployment waits for `/health/ready` on every node, covering storage, IAM, lock quorum and peer dependencies.

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

The probe verifies complete bytes, HEAD sizes, Range contents, EC12+4 and the actual persisted block. It records inline markers for 4 KiB, 1 MiB, 1 MiB + 1 byte, 1.25 MiB, 1,572,856 bytes, 1,572,857 bytes and 1.5 MiB objects in `probe.json`. The server-only image includes the existing filemeta decoder through a small geometry inspector. Console assets and the administration CLI are outside this lab image.

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

## Compare transport and placement independently

`deploy --rpc-mode json-compat` explicitly clears both MessagePack-only flags on every node. `deploy --rpc-mode msgpack-only` enables both flags for the selected homogeneous lab fleet. Deployment evidence records the mode and checks both running flags. The default is `json-compat`. The ABBA hook may select the same mode through `RUSTFS_INLINE_LAB_RPC_MODE`.

First compare the base and bounded-multi-block binaries at `--shard-bytes 0` in JSON compatibility mode. Then repeat with MessagePack-only on both sides. A same-binary transport ABBA isolates the encoding effect. Keep the 128 KiB budget, CPU sets, strict durability, load and object set fixed. Check actual markers and block sizes at the boundary sizes before timing. Retain baseline-drift and candidate gates, error logs and raw request distributions.

For a rolling downgrade, redeploy the current binary with `--keep-data --rpc-mode json-compat` before introducing an older node. Check old and new reads, version IDs and batch deletes against the persisted manifest. A running MessagePack-only flag pair establishes the lab configuration. Compatibility with a particular old reader requires the explicit reader test.

## Preserve a compatibility manifest

Run the seed once in a fresh lab bucket. It records full byte oracles and the observed version inventory, including any additional version matching a known body, and leaves a 5 MiB multipart part ready for the next reader. Each bucket has its own private `compat-<bucket>.json` manifest. Keep the same state and volumes across image changes. A fresh deployment or cleanup that resets volumes moves existing compatibility manifests into `compat-history`. Retained-data deployments preserve the active manifests.

```bash
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-seed --bucket inline-compat
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-check --bucket inline-compat
# Configure the next frozen image and binary, then retain the data.
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" deploy \
  --bucket inline-compat --shard-bytes 0 --rpc-mode json-compat --keep-data
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-check --bucket inline-compat
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-finish-mpu --bucket inline-compat
```

The check compares every persisted version ID, HEAD length, complete SHA256, cross-block Range and per-version geometry from `xl.meta`. The seed includes zero-length objects, null versions, the 192 KiB versioned admission boundary and the accumulated 128 KiB shard boundary. Seed a second bucket on the new writer before the downgrade, then check both manifests using the selected old reader. Complete an old upload on the new binary and a new upload on the old binary. After returning to the current fleet, `compat-delete --bucket <bucket>` deletes the recorded versions in one S3 batch and checks the exact remaining inventory and bytes.

Successful multipart-completion and batch-delete receipts are saved before subsequent content and geometry checks. If either check fails, rerun the same command with the same state directory to resume verification from the saved completion state. After a seed check fails with a saved manifest, use `compat-check` to resume verification.

For mixed-version testing, retain JSON compatibility mode throughout the rolling replacement. Verify each node's immutable image and binary, then perform manifest checks and new writes through both coordinator versions using `--coordinator node1` and `--coordinator node4`. The manifest records the actual writer coordinator revision from the running image. Stop one owned node to check read availability at EC12+4, restore it and check the manifests again. Record the exact oldest tested source revision, toolchain and build profile with the evidence. The version boundary is established by these executed cells.

An owned lab can replace just its fourth node through a compose image override. Select the frozen old image first and verify its revision label. Keep `compose.json` in JSON compatibility mode for this cell.

```bash
set -euo pipefail
OLD_IMAGE_ID="$(docker --context "$LAB_CONTEXT" image inspect "$OLD_IMAGE" --format '{{.Id}}')"
test "$(docker --context "$LAB_CONTEXT" image inspect "$OLD_IMAGE_ID" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$OLD_REVISION"
INLINE_OLD_IMAGE_ID="$OLD_IMAGE_ID" scripts/python_bin.sh - <<'PYTHON' > "$LAB_STATE/mixed-image.json"
import json
import os
import sys

override = {"services": {"node4": {"image": os.environ["INLINE_OLD_IMAGE_ID"]}}}
json.dump(override, sys.stdout)
PYTHON
docker --context "$LAB_CONTEXT" compose --project-name "$LAB_PROJECT" \
  --file "$LAB_STATE/compose.json" --file "$LAB_STATE/mixed-image.json" \
  up --detach --no-deps --no-build --pull never node4
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-check \
  --bucket inline-compat --coordinator node4
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-seed \
  --bucket inline-mixed-writer --coordinator node4
scripts/python_bin.sh scripts/inline_docker_lab.py --state-dir "$LAB_STATE" compat-check \
  --bucket inline-mixed-writer --coordinator node1
```

Inspect all four running image IDs and binary hashes before recording the mixed-fleet result. Redeploy the configured current image with `--keep-data --rpc-mode json-compat` to restore a homogeneous fleet. The same manifest checks apply after stopping and restarting one owned node through the base compose file.
