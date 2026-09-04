# Distributed 4-node 4-disk e2e

**Use this when:** adding or diagnosing GitHub Actions coverage for a 4-node cluster, or deciding whether a behaviour belongs in `e2e-distributed` versus the single-node `e2e-full` lane, the nightly cluster-fault lane, or the hardware functional chain.
**Source of truth:** `crates/e2e_test/src/distributed/`, `[profile.e2e-distributed]` in `.config/nextest.toml`, `.github/workflows/e2e-distributed.yml`.

## Topology

The in-tree harness runs every node on `127.0.0.1` with a distinct port. That matches `RustFSTestClusterEnvironment` in `crates/e2e_test/src/common.rs`:

| Layout | Constructor | Use |
|---|---|---|
| 4 nodes × 4 drives, one pool | `ClusterTopology::single_pool_multidrive(4, 4)` | S3, object lock, versioning, quota, observability, concurrency, chaos |
| 4 nodes × 1 drive, one pool | `ClusterTopology::single_pool(4)` | Two-site replication (8 processes total) |
| 2 single-node pools × 4 drives, then `append_single_node_pool` twice | expansion seed | Pool expand, then decommission / rebalance / integrity |

A pool striped across several localhost ports is not expressible (`RUSTFS_VOLUMES` host ellipses would collide on disk paths). Multi-host striped pools remain the hardware functional-chain / backlog #1313 / #1314 lane.

Decommission and rebalance POST currently 500 on localhost DistErasure multi-pool when pool.bin writes are fenced (`pool metadata writes remain blocked` / missing fleet capability proof). Those cases still assert object bytes and SHA-256; when the API starts they wait for completion and assert post-move integrity. They do not treat the fence as a successful move.

## What this lane covers

`cargo nextest run --profile e2e-distributed -p e2e_test` selects `distributed::*`:

- S3 put / get / head / list / copy / rename / delete / presign / empty object
- Object Lock COMPLIANCE, GOVERNANCE (with bypass), legal hold
- Versioning, version GET, delete marker
- Bucket replication between two 4-node clusters; hard quota
- Health / admin info / storageinfo / audit target list
- Pool expand, decommission, rebalance, checksum integrity, S3 during move
- Site replication object convergence
- High-concurrency PUT/GET; concurrent PUT during decommission
- Node kill/restart, full process restart, drive offline (4×4). Volume-proxy blackhole stays in `cluster_volume_fault_proxy_pass_smoke` (2×2); a 4-node volume proxy cannot format because RPC audience is the listen port
- Multipart, cross-node listing, list-buckets agreement, delete+recreate bucket
- Concurrent GET while a peer node is killed

## Existing Actions gaps this lane does not replace

Those suites stay in place; this lane fills the in-tree 4×4 hole they leave.

| Existing lane | Gap |
|---|---|
| `rustfs-*-test.yml` functional chain | Clones private `rustfs/auto-testing`, runs on three shared VMs (`vm000`–`vm002`), `continue-on-error: true`, not a merge signal, not 4 nodes |
| `e2e-smoke` / `e2e-full` | Almost all cases are single-node |
| `e2e-nightly` | 4-node cluster faults and heal, not S3/lock/versioning/quota/decommission matrix |
| `e2e-repl-nightly` | Site and bucket replication on 1–3 *single-node* processes |
| `e2e-s3tests.yml` `multi` | Weekly ceph/s3-tests against Docker 4-node; not lock/WORM, decommission, chaos, or checksum integrity |
| `crates/e2e_test/src/chaos.rs` | Single-node disk faults only |

Hardware power-loss, NIC pull, and real disk replacement still belong on the smoke-testing VMs. This lane simulates those with SIGKILL, directory rename, and `FaultProxy` blackhole.

## Run

```bash
cargo build -p rustfs --bins
cargo nextest run --profile e2e-distributed -p e2e_test
```

Membership is pinned by `.config/e2e-distributed-selection.txt`. Update it with `python3 ./scripts/check_test_wiring.py --update-profile e2e-distributed <listing.json> linux` after adding or renaming a case.
