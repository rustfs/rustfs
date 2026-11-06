# Docker rejoin readiness reproduction

**Use this when:** distinguishing restarted-node admission failures from acknowledged-overwrite peer GET failures in an HTTPS erasure set, or validating a change against fixed-source Docker binaries.

**Source of truth:** `scripts/repro_rejoin_readiness_docker.py` (`Experiment`, `GATE_PATHS`, `cleanup_resources`), `scripts/test_repro_rejoin_readiness_docker.py`, and `.docker/rejoin-readiness.Dockerfile`.

## Scope and prerequisites

The harness operates one isolated project on the selected Docker daemon. It records the client host, daemon architecture, kernel/storage driver, runtime distribution, data-volume filesystem, resource limits and binary identity. Matching Linux AMD64 and ARM64 daemon/image/binary combinations are accepted; a mismatch is refused before server startup. A successful run on a virtualized ARM64 Docker host is functional evidence for that environment, not native AMD64, independent-host durability, or production throughput evidence.

Use Docker, host curl with built-in `--aws-sigv4`, OpenSSL and Python. Run the client on the daemon host or a local Docker VM that forwards published loopback ports to it; selecting a remote Docker context alone does not forward those ports. The runtime image must provide `/bin/sh`, Python 3.11+, `getent`, `stat`, and a compatible `/usr/bin/rustfs`. The fixture keeps the root filesystem read-only, mounts owned Linux data/configuration volumes, verifies TLS, and publishes only loopback S3 ports. It explicitly creates and records a non-internal bridge network; include this difference when comparing it with an internal-network fixture. It does not use production credentials or data. See [probe semantics](../architecture/readiness-matrix.md#probe-semantics) and [recovered-backend routing](../operations/rolling-restart.md#returning-a-recovered-node-to-a-load-balancer).

## Prepare an immutable image

Build the selected source in an isolated checkout with a recorded compiler, target, features and profile. Reusing dependency caches is safe only when package identities and inputs remain correct; use a separate target directory for each source revision. If seeding it from a compiled cache, run `cargo clean --workspace` in the selected source before building; cached local path-package APIs must not cross revisions. Distinct container source paths alone do not establish freshness when checkout mtimes predate cached outputs. Do not label a dirty runtime build as an unmodified commit. Copy the completed binary into a private image context named `rustfs`, and record its actual SHA256. Use a libc-compatible native base with no inherited `RUSTFS_*`, `MINIO_*` compatibility aliases or `RUST_LOG` configuration; preflight rejects those overrides so each recorded control uses the fixture's explicit environment. The wrapper's default is an example GNU runtime, not a production-image recommendation.

```bash
REJOIN_SOURCE_SHA="$(git rev-parse HEAD)"
REJOIN_BINARY_SHA="$(openssl dgst -sha256 /path/to/context/rustfs | awk '{print $NF}')"
docker build --pull=false -f .docker/rejoin-readiness.Dockerfile \
  --build-arg RUNTIME_BASE='<compatible-native-base>@sha256:<digest>' \
  --build-arg SOURCE_REVISION="$REJOIN_SOURCE_SHA" \
  --build-arg BINARY_SHA256="$REJOIN_BINARY_SHA" \
  --build-arg BUILD_PROFILE=release \
  -t rustfs-rejoin:fixed /path/to/context
```

The wrapper validates its source/profile/digest arguments. The runner resolves the image to its immutable ID, validates labels, and independently verifies the binary digest and ELF architecture in the isolated staging container before executing it. Its evidence manifest records what was tested; an image name or `--version` string alone is insufficient provenance. An existing historical reproducer's revision checks must be updated together with its new binary and image labels; preserve the original fixture as the control.

## Run controlled cases

```bash
python3 scripts/repro_rejoin_readiness_docker.py run \
  --image rustfs-rejoin:fixed --expected-source "$REJOIN_SOURCE_SHA" \
  --build-profile release --out-dir /path/to/new-private-evidence \
  --gate node-write --payload-mode distinct --cleanup-on-success
```

Use a fresh output directory and project for each case. `--gate cluster` preserves the cluster-only recovery condition; `--gate node-write` additionally requires node readiness. Compare the gates on the same binary before attributing a change to code. Compare source revisions with the same architecture, runtime, profile, quotas, payload mode and workload. The default outage duration matches the historical reduced fixture; it is a fault interval, not an extra delay after the recovery gate. `--workers 2` is a separate worker-count experiment and must not silently change the default control.

`--payload-mode original` preserves identical overwrite bytes. The distinct mode retains the last acknowledged payload per key so an old matching version cannot masquerade as the latest overwrite. `--stress-iterations` increases same-key overwrite/peer-read pairs during each outage. Bucket creation may retry startup 503; object operations have no automatic retries. A first failed object response remains a failed result even if recovery or later observations succeed. Cluster-only health may pass before S3 admission; that observation is distinct from a survivor's namespace-lock timeout.

## Failure evidence and cleanup

The runner saves request timestamps, status, response bodies/headers, acknowledged-content identities, container generations, phase snapshots, Docker output and runtime logs. The warn/file profile archives live `/logs` through in-container Python with a 32 MiB/256-file budget; stopped tmpfs logs are explicitly unavailable. Docker copy can omit live tmpfs mounts and is not used for log capture. On a namespace-lock timeout it samples the read-only signed `/rustfs/admin/v3/top/locks` endpoint on the surviving nodes before restarting stopped peers. A missing/unsupported diagnostic endpoint is recorded, not interpreted as an empty lock table. No force-unlock is performed. See [distributed lock diagnostics](distributed-e2e.md) for the limits of guard-drop and physical-release observations.

Use `--diagnostics` only as a separately identified diagnostic run; trace overhead is not performance evidence. This profile uses stdout only and explicitly bounded Docker JSON logs (20 MiB per file, five retained files); capture includes the retained history across restart. `summary.log_captures` records sources and bytes, and final `diagnostic_coverage` requires acquired, guard-dropped and physical-release JSON event categories across the pool. Missing any category fails diagnostic coverage while retaining the independent HTTP results. Category coverage does not prove release of every owner or resolve a historical timeout. Keep initial failure, later convergence, and restarted-node checks separate. A retained run stops its owned nodes after capturing final evidence; its volumes and private files remain available for investigation. Successful runs can remove resources with `--cleanup-on-success`.

```bash
python3 scripts/repro_rejoin_readiness_docker.py cleanup \
  --out-dir /path/to/private-evidence
```

Every data/configuration volume is verified before staging mounts or changes it; mounts disable image-content copying. Cleanup verifies every extant resource against the manifest's project, source and ownership labels before deleting any resource. During a run, Ctrl-C and SIGTERM interrupt capture as failures and continue through shutdown and summary writing; interrupted retention is explicitly reported as unconfirmed. The CLI restores the previous SIGTERM handler when the run returns or raises an exception. It refuses foreign resources and does not prune the daemon. Output directories contain generated credentials, curl configurations and private CA/node keys: do not commit or upload them wholesale. Publish only a reviewed, redacted result summary with source/build identity and explicit environment limits.
