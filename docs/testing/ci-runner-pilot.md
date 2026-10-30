# Runner capability and storage pilot

**Use this when:** checking whether a runner can consume a build artifact, comparing workspace build storage, or preparing a limited local-SSD trial in the existing runner infrastructure.

`scripts/ci/runner_pilot.py` owns capability checks and isolated cold/warm build measurements. `.github/workflows/ci-runner-pilot.yml` is a manual entry using the existing hosted and ARC labels. It creates no cloud resources, changes no pool capacity and moves no required CI jobs. The ARC controller configuration, node images and storage provisioning live outside this repository, as documented by the existing runner hygiene workflow. A successful probe does not certify local physical storage or worker destruction.

## Probe and benchmark

Start with the `probe` workload. The JSON receipt records architecture, kernel/libc, effective CPU and memory limits, free disk and mount, Docker daemon availability, an actual minimal io_uring setup attempt, installed tool versions and available image identity. Required capabilities fail closed through the CLI options. Unrequested unsupported capabilities remain observations. Network reachability, dynamic-library compatibility of a specific artifact and physical storage locality are not established by this probe.

```bash
python3 scripts/ci/runner_pilot.py \
  --root /mnt/build-scratch \
  --output /tmp/runner-pilot-evidence \
  --architecture x86_64 --min-free-gib 90 --min-memory-gib 8
```

For a build, select `server` or `workspace-tests` in the manual workflow and supply an exact merged-main SHA and an exact Rust version. The workflow checks ancestry before executing the source, installs the requested compiler and the fixed protobuf/flatbuffer tool versions from its composite action, then invokes the tool. Run both workloads separately with the same source, toolchain and concurrency when comparing storage. Hosted runners may fail the disk preflight; this is an explicit capacity result, not a build failure to bypass.

The CLI's `--benchmark`, `--source-sha`, `--toolchain` and `--jobs` options provide the same entry locally on a prepared Linux worker. Build commands are defined by `build_commands`; the workspace workload compiles tests without executing them. This pilot does not replace functional acceptance or the dedicated compile-attribution work.

The cold build uses a new task-owned Cargo target directory with compiler wrappers and incremental compilation disabled. The warm build reuses only that target. Cargo registry downloads may already be warm, and the operating-system page cache is not dropped. GNU time records wall time, maximum RSS and filesystem I/O counters; these are not disk bandwidth or a simultaneous memory total across every process. Each attempt keeps logs and a JSON receipt. A failed or timed-out cold build stops the pair instead of producing a warm success. Ordinary termination kills the child process group and removes the task-owned target; a hard job timeout or lost node still requires infrastructure destruction. Queue, setup and evidence-upload costs come from the [resource accounting report](ci-resource-accounting.md).

## Controlled existing-infrastructure trial

An operator must make the actual ARC configuration change in its owning infrastructure repository. Use that deployment mechanism and retain its reviewed revision; do not install a second scheduler from this repository. Keep the current production pool available. Bound the candidate to one worker and one manual job at a time, isolate its scratch mount and credentials, and ensure the selected existing label is routed exclusively to the intended candidate during the trial. If that cannot be guaranteed without affecting ordinary jobs, use an operator-provisioned dedicated label and review its workflow/actionlint declaration before running a trial.

The image must have a pinned digest and the required tools or a recorded setup phase. A locally attached SSD/NVMe mount must be a task-local filesystem, never a persistent shared target between untrusted jobs. Do not infer physical locality from a disk's nonrotational flag: virtual network-backed disks can report the same property. For `local_ssd: true`, the operator provides a root-owned, non-group/world-writable `/etc/rustfs-runner/storage.json` and sets `RUSTFS_RUNNER_IMAGE_DIGEST` to the actual image digest:

```json
{
  "schema": 1,
  "storage": "local-ssd",
  "mount_source": "/dev/nvme0n1",
  "mount_target": "/mnt/build-scratch",
  "image_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

The tool requires that assertion to match the observed filesystem, mount and image digest. It reports the source as an operator attestation and keeps `storage_locality_verified` false. The operator must retain infrastructure evidence of device locality separately. Likewise, `worker_destruction_verified` stays false: directory cleanup is not a proof that a worker or credentials were destroyed.

Before admitting PR work, the infrastructure owner must execute two separate jobs and demonstrate that the second cannot read a marker or credential fixture left by the first. Repeat for cancellation and abnormal termination, retaining controller events and worker identities. Never put publication credentials in this pool. The monthly `scripts/ci/check_runner_ephemerality.sh` audit remains independent. No deployment or isolation result is implied by the fixtures in this repository.

## Artifact consumers and rollout decision

Run the existing artifact identity/ABI verifier before a real consumer trial. A matching architecture and glibc version alone is insufficient: execute the exact selected test set, verify required dynamic libraries and Docker/privilege requirements, and retain the archive's result contract. Rust fixtures can embed absolute build-time paths. Preserve the producer workspace path and required fixture files, or explicitly establish a supported relocation strategy before using a different runner layout. Required consumers keep their current runner until that acceptance is complete.

Repeat the same benchmark pair enough times to expose variability, including an empty target-cache case and a failed or cancelled run. Keep source, compiler, features, profile, concurrency and image fixed while changing storage. Compare queue, compilation, consumer execution, transfer and total occupied minutes; do not attribute simultaneous compiler or cache-policy changes to the SSD. Keep fixed-capacity charges separate from demand. Promotion needs measured total benefit and unchanged test responsibility; this tooling does not claim deployment, savings or a performance target.

Rollback is to stop the manual workflow and restore the infrastructure owner's previous image, scratch mount and capacity revision. No main CI placement changes need reversal. Keep receipts outside the disposable target. Local validation is `python3 -m unittest discover -s scripts/ci -p 'test_runner_pilot.py'`; deployment, real cloud storage, queueing, node destruction and artifact migration remain live acceptance work.
