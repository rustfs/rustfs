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

The four expansion pools must report independent capacity. Four directories on one runner filesystem all return the same `statfs` totals, so RustFS correctly concludes that no pool is less free than the cluster average and performs no rebalance. The Actions job mounts four isolated ext4 loopback filesystems and exports their absolute paths through `RUSTFS_E2E_POOL_ROOTS`. The harness rejects missing, duplicate, relative, nonexistent, or same-device roots instead of allowing a vacuous movement pass. Planned pool additions stop every process with SIGTERM; hard process termination remains a chaos-only fault. After the fourth pool joins, the harness performs one full graceful persistent restart: this proves the expanded pool map survives restart and ensures movement begins only after every replica can load the converged metadata.

The expansion fixture is an all-current-binary fleet, so it initializes pool metadata with the documented V3 write and fleet-confirmation gates. Decommission cases write their baseline objects, version history, and multipart data into pool 0 before adding pools 1–3, then retire pool 0. This makes a passing result evidence of user-data movement rather than merely an internal-metadata counter changing.

## What this lane covers

`cargo nextest run --profile e2e-distributed -p e2e_test` selects `distributed::*`:

- S3 put / get / head / list / copy / rename / delete / presign, range and conditional reads, special keys, metadata, tags, pagination, empty objects, multipart complete and abort
- Object Lock COMPLIANCE, GOVERNANCE and bypass, legal hold, bucket default retention, and non-lock bucket rejection
- Versioning, exact historical reads, delete-marker removal, and suspended null-version overwrite semantics
- Bucket replication between two 4-node clusters, including metadata/tags and target-outage retry; hard quota admission and absence of rejected keys
- Ready/live probes on every node, exact 4-server/16-disk inventory, realtime metrics on every node, and correlated audit-webhook delivery
- Pool expand, decommission, rebalance, checksum integrity, versioned and multipart data, and S3 during active movement
- Bidirectional site-replication convergence plus enabled/synchronized peer state on both sites
- A 24-worker mixed PUT/HEAD/GET/COPY/DELETE workload; concurrent PUT during active decommission
- Node kill/restart, full process restart, node-facing TCP blackhole/recovery, in-flight streaming GETs across a peer kill, and fresh-drive replacement verified by physical `xl.meta`/part-shard census
- Multipart, cross-node listing, list-buckets agreement
- Direct and rolling upgrade from the pinned previous release: historical objects, versioned history, and IAM user AK/SK still work afterwards

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
cargo build -p rustfs --bins
# Expansion/decommission/rebalance cases require four paths on distinct filesystems.
export RUSTFS_E2E_POOL_ROOTS=/mnt/rustfs-pool-0:/mnt/rustfs-pool-1:/mnt/rustfs-pool-2:/mnt/rustfs-pool-3
# Upgrade cases require the pinned previous binary (CI downloads it).
export RUSTFS_UPGRADE_SOURCE_BINARY=/path/to/rustfs-1.0.0-rc.2
cargo nextest run --profile e2e-distributed -p e2e_test
```

Without `RUSTFS_UPGRADE_SOURCE_BINARY` the two `distributed::upgrade_test::*` cases fail closed. Without four distinct `RUSTFS_E2E_POOL_ROOTS`, the expansion and data-movement cases fail closed. Filter upgrades out for a local run that is not checking upgrade:

```bash
cargo nextest run --profile e2e-distributed -p e2e_test -E 'not test(/^distributed::upgrade_test::/)'
```

The upgrade topology is `ClusterTopology::single_pool(4)` (4 nodes × 1 drive). That matches the proven mixed-version fixture in `upgrade_compatibility_test`; 4×4 localhost drives are rejected by the previous release's same-device disk check.

Membership is pinned by `.config/e2e-distributed-selection.txt`. Update the Linux and Darwin entries with `python3 ./scripts/check_test_wiring.py --update-profile e2e-distributed <listing.json> <platform>` after adding or renaming a case.
