# Distributed 4-node 4-disk e2e

**Use this when:** adding or diagnosing GitHub Actions coverage for a 4-node cluster, or deciding whether a behaviour belongs in `e2e-distributed` versus the single-node `e2e-full` lane, the nightly cluster-fault lane, or the hardware functional chain.
**Source of truth:** `crates/e2e_test/src/distributed/`, `[profile.e2e-distributed]` in `.config/nextest.toml`, `.github/workflows/e2e-distributed.yml`.

## Topology

The in-tree harness runs every node on `127.0.0.1` with a distinct port. That matches `RustFSTestClusterEnvironment` in `crates/e2e_test/src/common.rs`:

| Layout | Constructor | Use |
|---|---|---|
| 4 nodes × 4 drives, one pool | `ClusterTopology::single_pool_multidrive(4, 4)` | S3, object lock, versioning, quota, observability, concurrency, chaos |
| 4 nodes × 1 drive, one pool | `ClusterTopology::single_pool(4)` | Two-site replication (8 processes total); direct/rolling upgrade from the pinned previous release |
| 1 single-node pool × 4 drives, then `append_single_node_pool` three times | expansion seed | Pool expand, then decommission / rebalance / integrity |

A multi-pool layout in which any pool spans several localhost ports is not expressible (`RUSTFS_VOLUMES` host ellipses would collide on disk paths). Multi-host striped expansion pools remain the hardware functional-chain / backlog #1313 / #1314 lane.

Data-movement cases fail closed. A decommission or rebalance test must observe a successful start response, an active state, a clean terminal state, non-zero movement counters, and post-operation object integrity. An unsupported response, HTTP 5xx, missing status fields, cleanup warning, or zero-progress terminal response fails the case; pre/post S3 availability alone is not evidence that movement ran.

The four expansion pools must report independent capacity. Four directories on one runner filesystem all return the same `statfs` totals, so RustFS correctly concludes that no pool is less free than the cluster average and performs no rebalance. The Actions job runs on GitHub-hosted `ubuntu-latest` and mounts four isolated 1 GiB tmpfs filesystems, then exports their absolute paths through `RUSTFS_E2E_POOL_ROOTS`. It does not use the self-hosted `sm-standard-4` ARC pods: those cannot create filesystems (`mount -o loop` fails with `No such file or directory`, and `mount -t tmpfs` fails with `cannot mount tmpfs read-only`). Sized tmpfs still reports a distinct `st_dev` and independent 1 GiB `statfs` capacity. The harness rejects missing, duplicate, relative, nonexistent, or same-device roots instead of allowing a vacuous movement pass. Planned pool additions stop every process with SIGTERM; hard process termination remains a chaos-only fault. After the fourth pool joins, the harness performs one full graceful persistent restart: this proves the expanded pool map survives restart and ensures movement begins only after every replica can load the converged metadata.

The expansion fixture is an all-current-binary fleet, so it initializes pool metadata with the documented V3 write and fleet-confirmation gates. Decommission cases write their baseline objects, version history, and multipart data into pool 0 before adding pools 1–3, then retire pool 0. This makes a passing result evidence of user-data movement rather than merely an internal-metadata counter changing.

## What this lane covers

`python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo nextest run --profile e2e-distributed -p e2e_test` selects `distributed::*`:

- S3 put / get / head / list / copy / rename / delete / presign, range and conditional reads, special keys, metadata, tags, pagination, empty objects, multipart complete and abort
- Object Lock COMPLIANCE, GOVERNANCE and bypass, legal hold, bucket default retention, and non-lock bucket rejection
- Versioning, exact historical reads, delete-marker removal, and suspended null-version overwrite semantics
- Bucket replication between two 4-node clusters, including metadata/tags and target-outage retry; hard quota admission and absence of rejected keys
- Ready/live probes on every node, exact 4-server/16-disk inventory, realtime metrics on every node, and correlated audit-webhook delivery. The observability case runs at WARN and suspends two node processes to model nonresponsive peers: cached drives become unknown immediately, exact per-node HTTP PUT deltas expose sub-quorum failures, the local metadata snapshot does not invent a write latch, and resumed peers allow new writes and byte-identical reads from every node. This models stalled processes, not a physical network partition.
- Pool expand, decommission, rebalance, checksum integrity, versioned and multipart data, and S3 during active movement
- Bidirectional site-replication convergence plus enabled/synchronized peer state on both sites
- A 24-worker mixed PUT/HEAD/GET/COPY/DELETE workload; concurrent PUT during active decommission
- Node kill/restart, full process restart, node-facing TCP blackhole/recovery, in-flight streaming GETs across a peer kill, and fresh-drive replacement verified by physical `xl.meta`/part-shard census
- Multipart, cross-node listing, list-buckets agreement
- Direct and rolling upgrade from the pinned previous release: historical objects, versioned history, and IAM user AK/SK still work afterwards
- The `scanner_heal_topology_` protection matrix: versioned, locked and multipart inventory verified through every node across expansion, rebalance stop/restart and queued decommission, with kill and commit-failure injection at proven movement windows; gate map and timeouts in [scanner-heal-topology-gates.md](scanner-heal-topology-gates.md)

## Existing Actions gaps this lane does not replace

Those suites stay in place; this lane fills the in-tree 4×4 hole they leave.

| Existing lane | Gap |
|---|---|
| `rustfs-*-test.yml` functional chain | Clones private `rustfs/auto-testing`, runs on three shared VMs (`vm000`–`vm002`), `continue-on-error: true`, not a merge signal, not 4 nodes. Hardware `rustfs-upgrade-test.yml` stays there |
| `e2e-upgrade.yml` | Single-node SSE/multipart/delete-marker contracts plus mixed-version listing; does not pin IAM user AK/SK on a 4-node cluster |
| `e2e-smoke` / `e2e-full` | Most selected cases are single-node; distributed modules are intentionally owned by this serialized lane |
| `e2e-nightly` | 4-node cluster faults and heal, not S3/lock/versioning/quota/decommission matrix |
| `e2e-repl-nightly` | Site and bucket replication on 1–3 *single-node* processes |
| `e2e-s3tests.yml` `multi` | Weekly ceph/s3-tests against Docker 4-node; not lock/WORM, decommission, chaos, or checksum integrity |
| `crates/e2e_test/src/chaos.rs` | Single-node disk faults only |

Hardware power-loss, physical NIC pull, authenticated inter-node partition, firmware/media errors, and replacement-server provisioning still belong on the hardware validation VMs. This lane provides deterministic process kill, fresh local-volume replacement, and node-facing TCP blackhole analogues; it does not claim physical fault certification.

## Run

```bash
python3 scripts/e2e_binary.py build --bins --features e2e-test-hooks
# Expansion/decommission/rebalance cases require four paths on distinct filesystems.
# If you do not already have four disks, sized tmpfs is enough:
#   for p in 0 1 2 3; do
#     sudo mkdir -p /mnt/rustfs-pool-$p
#     sudo mount -t tmpfs -o size=1G,nosuid,nodev,mode=1777 tmpfs /mnt/rustfs-pool-$p
#   done
export RUSTFS_E2E_POOL_ROOTS=/mnt/rustfs-pool-0:/mnt/rustfs-pool-1:/mnt/rustfs-pool-2:/mnt/rustfs-pool-3
# Upgrade cases require the pinned previous binary (CI downloads it).
export RUSTFS_UPGRADE_SOURCE_BINARY=/path/to/rustfs-1.0.0-rc.2
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo nextest run --profile e2e-distributed -p e2e_test
```

Without `RUSTFS_UPGRADE_SOURCE_BINARY` the two `distributed::upgrade_test::*` cases fail closed. Without four distinct `RUSTFS_E2E_POOL_ROOTS`, the expansion and data-movement cases fail closed. Filter upgrades out for a local run that is not checking upgrade:

```bash
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo nextest run --profile e2e-distributed -p e2e_test -E 'not test(/^distributed::upgrade_test::/)'
```

The general upgrade topology is `ClusterTopology::single_pool(4)` (4 nodes × 1 drive). That matches the mixed-version fixture in `upgrade_compatibility_test`; default same-device checks reject 4×4 localhost drives. The focused sixteen-drive MPU case explicitly uses the test harness's same-device bypass to model its erasure quorum, without claiming physical-drive independence.

Scanner/Heal G09 release evidence can be collected on Linux x86_64 with the
same pinned previous-release binary used by `e2e-upgrade.yml`:

```bash
scripts/run_scanner_heal_g09_upgrade_evidence.sh
```

The runner builds the current `rustfs` binary, downloads and verifies the
pinned previous release, runs the mixed-version rolling-upgrade and rollback
tests, and leaves the required raw G09 artifacts under
`target/scanner-heal-g09-upgrade-evidence/<timestamp>/`. These artifacts are
inputs for the Scanner/Heal release bundle gate; the runner does not mark the
full release matrix complete by itself.

The distributed Scanner/Heal EC8+4 restart case is registered as
`ec84-target-drive-restart` and selected by this profile:

```bash
scripts/run_scanner_heal_evidence_case.sh --case ec84-target-drive-restart
```

That command records the current build, runs exactly the registered
`distributed::heal_test` case, validates the JUnit/listing/oracle receipt, and
keeps the wider release gate blocked until the remaining release evidence lanes
have measured artifacts.

Membership is pinned by `.config/e2e-distributed-selection.txt`. Update the Linux and Darwin entries with `python3 ./scripts/check_test_wiring.py --update-profile e2e-distributed <listing.json> <platform>` after adding or renaming a case.

## Multipart fence fault acceptance

The `distributed::multipart_fence_acceptance_test` cases separate remote storage RPC delays from lock RPC delays through a method-selective proxy. They require the selected RPC to reach its pause before checking quorum, recovery, or cancellation. The proxy forwards opaque bodies and gRPC trailers and is confined to the test process. It does not change server timeouts, locking, retry policy, or the commit path.

After a quorum-minus-one Complete is rejected and faults are retired, ordinary Abort must succeed. `NoSuchUpload` is not accepted as recovery of that failed completion: it can hide staging metadata consumed by partial publication. Already completed or aborted sessions retain their existing missing-upload cleanup semantics. The ECStore multipart regressions assert unchanged per-disk authority bytes across primary/recovery locations, no duplicated disk votes, rejection of completed-upload resends after cleanup failure, orphan-cleanup refusal while authority remains, preservation of the previous object, and write-quorum completion replay. Scoped Abort faults decode the exact multipart volume, root path and recursive option without changing signed bytes. Post-fault read probes can wait within the existing recovery deadline for classified 503 responses, honoring Retry-After; injected writes and successful streaming bodies remain single-attempt.

Partial-Abort acceptance holds five recursive root Delete RPCs in EC12+4, observes the eleven completed deletions, then requires a typed quorum refusal after peer recovery while five original metadata copies and the committed object remain unchanged. The explicit late-delivery proxy mode retains bounded control packets beyond caller cancellation; dropping that mode discards them, and proxy teardown aborts its retained tasks. Late Complete acceptance delivers the original signed RenameData packets after successful Abort, staging GC and same-name bucket recreation, and requires an authenticated mutation refusal plus unchanged new-object bytes. This covers empty and non-inline parts; expired-packet discard remains a distinct fault model.

The pinned previous-release MPU recovery case seeds all sixteen disks before rolling one coordinator to the current build. Five held RenameData calls leave five legacy primary records and four current recovery records; their original bytes must agree with the seed. The previous coordinator's reply is recorded as an unsupported boundary and cannot count as successful recovery while those records remain. The current coordinator must reclaim all old/new roots and preserve an unrelated historical object. This is a fresh-key, one-current/three-previous matrix; it does not establish homogeneous downgrade or same-key overwrite behavior. The same case additionally runs for every verified source in the [upgrade compatibility workflow](../../.github/workflows/e2e-upgrade.yml), with an exact one-test listing check and per-version node/test logs. This supplements the distributed suite's historical source pin.

The resend-owner case replaces an acknowledged 9 MiB part with different bytes in EC:2. It pauses PreparePartTransaction on all three remote peers while terminal calls contend. Four pairwise rows cover Complete or Abort with a live or cancelled part response waiter. Two ungated simultaneous Complete/Abort/resend rows require both terminal coordinators' distinct scoped lock owners to reach a peer before the part resumes; one owner's retries cannot satisfy admission. Four additional ordered rows cover both terminal winners with live and cancelled part response waiters. Their independent lock gates hold only the losing coordinator on two remote peers, leaving it at most two of the required three lock votes. The preferred coordinator must actually reach a peer and the losing coordinator must actually hit a scoped lock gate before part release. Both S3 calls must remain pending while the part transaction is paused. The losing lock gates are released only after the preferred terminal call succeeds; these gates use ordinary caller lifetime and do not retain expired packets. Proxy regressions check owner/upload isolation, independent part/lock release, conflicting lock-rule rejection, packet/trailer preservation and discard without replay. Every row verifies complete object bytes from all four readers, terminal ListParts, no visible uploads and all four staging roots retired. An Abort winner requires typed NoSuchUpload from Complete and preservation of the prior object. A Complete winner requires the replacement body and permits typed NoSuchUpload from Abort only alongside that successful Complete. The ordered rows assert the requested winner explicitly. Client response cancellation does not prove server-task cancellation, and these EC:2 rows do not cover every layout or late-mutation family.

Lock-layer regressions also cover every four-node read-quorum ordering, an eight-node write quorum behind silent voters, and the initial fan-out bound for prompt replies. Distributed acquisition probes unstarted voters within its existing attempt budget so silent early peers cannot hide a reachable quorum. Vote thresholds, owner identities, heartbeat checks, and late-grant cleanup retain their existing contracts.

Faults match the test bucket or upload suffix in bounded control-packet bodies, preserving their signed bytes and trailers. Abort injection decodes only the unary Delete routing fields and matches the exact multipart root, excluding child transaction cleanup. Readiness requires an actual matching RPC, without waiting for voters that adaptive fan-out does not contact. Recovery cases release waiting packets while the request is active; quorum cases discard expired packets before restoring the peer. Replaying a packet after its caller has timed out is a separate late-delivery fault model. The injected S3 operation has SDK retries disabled. When the restored peers retain the metadata identity quorum, ordinary cleanup must recover within 45 seconds, allowing the existing lease and drive-health recovery intervals; only classified ServiceUnavailable responses are polled, and the server's Retry-After advice is honored. A quorum Abort can leave fewer identities on the peers whose Delete calls expired. Below identity quorum, the matrix instead requires typed storage_write_quorum refusal, zero further scoped Delete RPCs, unchanged surviving metadata bytes, and the prior committed body on every client. It cannot infer cleanup authority from missing metadata. The fault-free eight-node workload keeps single-attempt APIs throughout. Failed Complete uses the best-effort rollback contract in [erasure-coding.md](../architecture/erasure-coding.md#7-write-path-and-write-quorum). A below-quorum Abort can also leave partially deleted staging. Each matrix reuses one cluster and bucket, requiring fresh responses from every lock peer before the next row so circuit-breaker history cannot reduce its available votes. Recoverable rows require ordinary Abort success or an already retired session, terminal ListParts, and no visible uploads. Minority-authority rows retain their physical evidence rather than claiming residue-free cleanup; active-interface absence is not proof that those roots were reclaimed. This verifies the Abort recovery merged in #8904 without weakening the delete quorum; it does not claim residue-free rollback of a failed Complete. Bucket-generation recreation is covered by the cancellation case.

The four-node EC:2 cases cover exactly quorum and quorum-minus-one for storage writes and exclusive locks (three votes) and ordinary Create shared locks (two votes), error-cause and retry headers, each multipart phase, and bucket recreation after client cancellation. A sixteen-shard EC12+4 case repeats all four metadata commit phases at exactly twelve and eleven available shards. The eight-node, eight-drive case checks the server-reported one-pool/four-set EC12+4 geometry, uploads 45 MiB objects using 8 MiB parts with two files and four parts in flight, verifies complete bytes through every node, and checks aborted and completed uploads leave no visible sessions.

```bash
python3 scripts/e2e_binary.py build --features e2e-test-hooks
RUSTFS_E2E_LOG_DIR=/path/to/isolated-run/logs \
RUSTFS_E2E_MPU_LOAD_ROUNDS=12 \
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- \
  cargo test -p e2e_test distributed::multipart_fence_acceptance_test -- --test-threads=1 --nocapture
```

`RUSTFS_E2E_LOG_FILTER` optionally overrides node log filtering for distributed fixtures; its absence preserves each fixture's existing filter. Use a narrow module filter when diagnosing RPC responses and keep logs with the matching server provenance.

`RUSTFS_E2E_MPU_LOAD_ROUNDS` accepts 1–256 and defaults to three rounds. Allocate a private target directory and bounded CPU/memory resources before running the eight-process case. Temporary drive directories follow `TMPDIR`; use a task-owned directory on the intended filesystem. These are plaintext loopback processes and logical drives on one host, so passing them does not establish physical-drive independence, TLS behavior, the original incident cause, or a throughput improvement. Performance comparisons require a separate controlled baseline and drift-checked measurement.
