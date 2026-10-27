# Scanner memory acceptance

**Use this when:** validating scanner memory with large S3 namespaces, a memory-limited Linux process, interrupted scans, or a separate scanner-disabled request burst.
**Source of truth:** `scripts/scanner_memory_acceptance.py`; scanner status and usage contracts in `rustfs/src/admin/handlers/scanner.rs`, `rustfs/src/admin/handlers/account_info.rs`, and `crates/scanner-metrics/src/metrics.rs`.

This is an operator-run acceptance harness, not a CI gate or a throughput benchmark. It creates real S3 objects and preserves raw observations in a private run directory. It never starts, restarts, stops, or removes a service, container, volume, bucket, or base fixture object. Service lifecycle belongs to the operator. The only deletes are copies created under the current run's uniquely named burst prefix.

## Evidence and limits

Run wide and nested layouts as separate manifests. The defaults are 2,000,000 objects of 50 KiB, with 170,000 direct source prefixes for the wide layout. Nested keys put the same sources below `photos/xx/yy/`. Payloads are deterministic pseudorandom bytes, shared by all fixture objects; they are not a variable-size photo distribution. Each bucket also contains one small ownership marker. Exact listing and scanner usage checks include this marker separately, so expected scanner totals are **N + 1 objects**, with the marker's serialized bytes added to logical payload size.

Every live observation verifies the running executable digest, process start identity, cgroup v2 memory limit and S3 endpoint binding. Docker mode additionally verifies the container's run label, bound data source and, for named volumes, the volume's run label. The source revision is declared build provenance; a digest match proves which binary is running, not that the declared source was independently rebuilt by this script. Keep the exact source archive and build receipt beside the evidence.

Samples distinguish process RSS, anonymous RSS, `smaps_rollup` anonymous memory, cgroup current/peak usage, anonymous/file/slab/kernel accounting, swap and OOM events. A missing reading remains unknown. Optional Prometheus collection accepts live allocator requested bytes only when `rustfs_memory_allocator_malloc_requested_bytes_available` equals one. Reserved/committed bytes and allocation totals are not substitutes for active heap.

`summary.json` reports phase measurements and restart evidence, but deliberately does not automatically certify overall acceptance. A finite run cannot prove a universal memory bound, and RSS retention alone is not a memory-leak diagnosis. Inspect the trend across completed cycles, anonymous memory versus file/slab accounting, live heap when available, and the burst idle window. Record the actual object scale, duration, resource limit and unavailable measurements in the conclusion. A small pilot does not satisfy the 2M-object case.

## Prerequisites and ownership

Use Python 3.11+ with `boto3`, a disposable local RustFS instance, and disposable credentials supplied as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The runner bypasses HTTP proxies explicitly for localhost traffic. It accepts only literal `127.0.0.1` or `::1` endpoints with explicit ports; ambiguous `localhost` names are rejected. TCP bindings must match the endpoint address and IP family, or an explicitly observed wildcard in that same family. Credentials and raw SDK error messages are never written to evidence.

Docker containers need `sh`, `cat`, `stat`, `sha256sum`, `df`, and `tail`, a readable `/proc`, and cgroup v2. Bind the tested binary read-only. Use a four-GiB memory limit and, for an exact no-swap limit, matching `--memory-swap=4g`. The runner records and verifies the actual kernel limit rather than trusting a launch command. Native Linux `--pid PID` mode requires the current user to own the process, a dedicated cgroup containing only that PID, and the endpoint listening socket to belong to it.

`prepare` creates a new mode-0700 run directory and immutable manifest. It prints a generated run ID, bucket name and required label. The bucket is never selected by a user-supplied name. On every writing batch, the runner verifies its exact ownership marker and requires versioning to remain disabled. Resume markers bind to the manifest digest. Reusing an existing unmarked bucket, a foreign container, or a foreign volume fails closed.

The storage reserve is one GiB plus 1.2 times remaining payload and 16 KiB per-object metadata allowance. Bind/native modes check host free space at preparation. Named-volume mode defers the capacity check until the labelled container is running, and checks `df` inside the mounted volume before the first PUT. Batches recheck the sampled free-space allowance. This is a guard against an undersized test volume, not a promise about every filesystem's physical overhead.

## Prepare and run a small pilot

Use a new absolute output path. Values below are placeholders; source and binary digests must be taken from the exact build being tested. Keep binary/build artifacts outside the repository.

```sh
python3 scripts/scanner_memory_acceptance.py prepare /tmp/scanner-memory-wide-pilot \
  --endpoint http://127.0.0.1:19000 \
  --source-sha SOURCE_COMMIT_SHA \
  --binary-sha256 BINARY_SHA256 \
  --layout wide --objects 4096 --root-prefixes 256 --object-bytes 51200 \
  --data-volume scanner-memory-wide-pilot
```

Create that **new** named volume and the disposable service container with the printed `rustfs.scanner-memory.run-id=UUID` label. Start with `RUSTFS_SCANNER_ENABLED=false`, healing disabled, the chosen binary and the manifest's four-GiB limit. Publish the S3 port directly to the supplied loopback endpoint. Alternatively use `--data-root /absolute/owned/data` at preparation and bind that exact directory.

```sh
python3 scripts/scanner_memory_acceptance.py load /tmp/scanner-memory-wide-pilot \
  --container scanner-memory-wide-pilot --workers 48 --seconds 3600
python3 scripts/scanner_memory_acceptance.py verify /tmp/scanner-memory-wide-pilot
```

Only one action may hold a run directory at a time. Load checkpoints advance after a complete bounded batch. An interrupted batch can be safely repeated because fixture keys and bodies are deterministic and the owned bucket is unversioned. A timeout or failed request leaves progress for the next invocation and exits nonzero; it never produces passing load evidence.

For the full-scale matrix, create separate new manifests with the defaults, once with `--layout wide` and once with `--layout nested`. Do not change a pilot manifest's object count or repurpose its bucket. Allocate adequate disk and wall time; two million S3 PUTs are real work.

## Scanner cycles and external restart

Recreate or restart the owned service externally with scanner enabled, preserving the same volume and binary. Record the chosen runtime knobs, including `RUSTFS_SCANNER_CYCLE`, `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS`, `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS`, and the disk I/O budget. Runtime configuration samples capture those values. A change in binary requires a separate manifest.

```sh
python3 scripts/scanner_memory_acceptance.py probe /tmp/scanner-memory-wide-pilot \
  --container scanner-memory-wide-pilot --label before_restart \
  --seconds 600 --stop-after-progress 128
# Externally stop/restart the owned process while preserving its data.
python3 scripts/scanner_memory_acceptance.py probe /tmp/scanner-memory-wide-pilot \
  --container scanner-memory-wide-pilot --label resumed_cycles \
  --seconds 1800 --min-completed-cycles 2
```

`--stop-after-progress` ends observation after scanner status reports an active cycle with the requested amount of work. It does **not** cancel the scanner itself. Act on the external process promptly and retain the lifecycle event evidence; a completed cycle before the restart is not an interrupted-scan test. Each invocation has a unique phase ID and binds to one process identity through its final inventory and usage verification. It rejects a restart at any point in that phase. `--seconds` is the total deadline, including verification; a probe exits once its requested progress or new completed-cycle count is observed. A later successful probe on a different identity is recorded as a restart window, after exact S3 inventory and converged admin usage verification. Completion counts include only newly observed successful cycle-end timestamps, not a previous completion present when the probe began.

A budgeted partial sweep is a separate state from process cancellation. Pair real observations with the scanner checkpoint fixture described in [scanner-checkpoint-fixture.md](scanner-checkpoint-fixture.md); do not infer process-crash durability from an in-process test.

`restart_window_observed` alone does not prove checkpoint reuse: the new process could have rescanned the entire namespace. Checkpoint-reuse acceptance additionally needs persisted checkpoint identity/frontier evidence and metadata-visit evidence showing that completed prefixes were retained rather than reread. Likewise, a successful cycle can reuse cached usage; its success does not prove a full namespace walk. Keep the recorded object-visit counts and distinguish full-walk, cached-cycle and restart-reuse evidence in the conclusion.

## Scanner-disabled burst and idle window

Restart externally with scanner disabled, then run the separate request burst:

```sh
python3 scripts/scanner_memory_acceptance.py burst /tmp/scanner-memory-wide-pilot \
  --container scanner-memory-wide-pilot --workers 48 \
  --burst-objects 4096 --seconds 3600 --idle-seconds 1800
python3 scripts/scanner_memory_acceptance.py summary /tmp/scanner-memory-wide-pilot
```

Each operation copies a base fixture object to an owned scratch key, HEADs its length, and deletes that copy. Completed batches are resumable. Results distinguish the burst ID, initial/final cursors, newly completed objects and cumulative progress. Three logical requests per newly completed object do not count SDK retry attempts; observed HTTP request count remains unknown. An idle-only continuation reports zero new logical requests. `--seconds` includes the workload, inventory verification and the requested idle interval. The full inventory must return to the original exact keys and byte count before the idle window begins. No `delete_objects`, bucket deletion or broad prefix cleanup is performed. An aborted burst can leave owned scratch objects; resume the same burst to reconcile them rather than deleting unrelated keys.

Supply `--allocator-metrics-url http://127.0.0.1:PORT/metrics` to live phases only when that endpoint exports this process's allocator metrics. Without an authenticated binding between that metrics endpoint and the observed process, treat allocator attribution as operator-supplied context. The runner cannot fabricate live-heap evidence from process RSS.

## Artifacts and validation

- `manifest.json`: immutable workload, source declaration, binary digest, storage source and memory limit.
- `state.json`: manifest-bound load and burst resume positions.
- `observations.jsonl`: phase starts/results/errors, raw selected scanner status, process/cgroup samples and inventory oracles.
- `summary.json`: measurements grouped by invocation, process identity and workload/verification/idle stage, sampled-current and kernel-reported cgroup peaks, first/last RSS, allocator availability, operation results and restart-window status.

Raw artifacts may contain local paths, binary fingerprints and configuration. Keep them private; publish only a redacted conclusion and the measurements needed to understand it. There is no automatic GitHub write or cleanup step.

Offline harness tests run without Docker, networking or credentials:

```sh
python3 scripts/test_scanner_memory_acceptance.py
python3 -m py_compile scripts/scanner_memory_acceptance.py scripts/test_scanner_memory_acceptance.py
```

The structural characterization in `crates/scanner/src/scanner_folder/tests/checkpoint_fixture/memory_budget.rs` complements these measurements. Its `SCANNER_MEMORY_BUDGET` rows describe cache entries/capacities, a heap lower bound and wire size after a real small filesystem walk. The seeded coverage-gap case is synthetic and retains a frozen frontier; it is not a real fault injection, process RSS measurement, or 2M-object reproduction. Use it to explain retained structure, then use this harness to measure the running service.
