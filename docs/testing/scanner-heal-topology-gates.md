# Scanner/Heal topology protection gates

**Use this when:** a heal, scanner, placement or data-movement change must run the S00 protection gates (G-EXP, G-REB, G-DEC and their fault windows) before delivery, or you need to know which `scanner_heal_topology_` case proves which gate, how its inventory is recorded, and why its timeouts are what they are.
**Source of truth:** `crates/e2e_test/src/distributed/scanner_heal_topology_test.rs` (cases), `crates/e2e_test/src/distributed/scanner_heal_topology_fixture.rs` (inventory, manifest, placement census, admin and barrier drivers), `crates/ecstore/src/data_movement/e2e_barrier.rs` (test-only barrier, wired into data movement and the decommission start), `[profile.e2e-distributed]` in `.config/nextest.toml` (lane and per-test budget).

## Gate map

Gate names come from the Scanner/Heal program (rustfs/backlog#2872); the quick matrix below is the protection floor that every heal or scanner delivery runs. The fault-qualification matrix (long-window, mixed-version, physical faults) stays with the release candidate run and is not duplicated here.

| Gate | Test (nextest id under `package(e2e_test)`) | Proves |
|---|---|---|
| G-EXP | `distributed::scanner_heal_topology_test::scanner_heal_topology_expand_one_to_four_pools_preserves_versions_and_placement` | 1→2→4 pools with scanner and heal enabled: every recorded version, listing, marker and lock survives on every node after each step and after a full restart; seed-pool shards and erasure placement are byte-identical; new writes land in exactly one pool; the pool map reports four pools after restart |
| G-EXP (existing baseline) | `distributed::expand_decommission_rebalance_test::four_node_pool_expand_preserves_objects_then_rebalance` | 64 unversioned objects survive expansion and a completed rebalance |
| G-REB | `distributed::scanner_heal_topology_test::scanner_heal_topology_rebalance_survives_live_writes_stop_and_restart` | rebalance with concurrent PUT/GET/DELETE and the scanner running: no content error, acknowledged writes and deletes hold on every node, a stop leaves every version readable and held by at least one pool (a committed target beside its retained source is allowed), writes issued while the seed pool rebalances avoid it, every node serves after a restart, and a second run completes |
| G-REB fault | `distributed::scanner_heal_topology_test::scanner_heal_topology_rebalance_kill_before_source_cleanup_resumes_without_loss` | SIGKILL of the rebalancing node at `before_source_cleanup` (target committed, source retained): the resumed run completes without loss or duplication |
| G-DEC | `distributed::scanner_heal_topology_test::scanner_heal_topology_decommission_queue_cancel_resume_and_last_pool_guard` | cancel of an active decommission keeps every version in exactly one pool; a two-pool queue retires pools 0 and 1 in order with pool 1 reported `queued`; versions, markers, listings, legal holds and compliance retention are intact on the surviving pools and after a restart that removes the retired pools from the command line; retiring the last active pools is rejected with a client error |
| G-DEC (existing baseline) | `distributed::expand_decommission_rebalance_test::four_pool_decommission_moves_objects_without_loss` | 96 unversioned objects survive retiring the seed pool |
| G-DEC fault: kill before source cleanup | `distributed::scanner_heal_topology_test::scanner_heal_topology_decommission_kill_before_source_cleanup_resumes_without_loss` | SIGKILL of the decommission leader at `before_source_cleanup`; the persisted decommission resumes and completes without loss or duplication |
| G-DEC fault: target commit failure | `distributed::scanner_heal_topology_test::scanner_heal_topology_decommission_target_commit_failure_retains_source` | an injected failure at `before_target_commit` of a multipart copy leaves the source authoritative (no target published) and a later attempt completes |
| G-DEC fault: kill after responsibility persisted, before ACK | `distributed::scanner_heal_topology_test::scanner_heal_topology_decommission_kill_after_responsibility_persisted_before_ack_resumes` | SIGKILL of the leader after the decommission start transaction is durable in `pool.bin`, before any worker starts and before the admin request is acknowledged (the request must not report success); the responsibility resumes on restart |
| G-FAULT | `distributed::heal_test::three_node_four_drive_ec8_4_root_heal_rebuilds_replaced_drive_after_restart`, `distributed::chaos_test::*`, `heal_erasure_disk_rebuild_test::*` | existing drive replacement, kill/restart and heal cases; the TCP blackhole case needs `iptables` and is Linux-only |
| G-UPG | `distributed::upgrade_test::*`, `upgrade_compatibility_test::*` | existing pinned-previous-release cases; they fail closed without `RUSTFS_UPGRADE_SOURCE_BINARY` |
| G-TIER | `tier_transition_regression_test::*`, `tier_stats_cluster_test::*` | existing tier fixtures; no quick-matrix case moves transitioned versions through rebalance or decommission yet |

A missing environment input (four distinct pool filesystems, the pinned upgrade binary, `iptables`) blocks the lane; it never passes it.

## Inventory and manifest

`seed_manifest` writes a deterministic inventory into the seed pool before any pool is added and records every version as a manifest entry: bucket, key, shape, the version id returned by the write, the version id readers must request, delete-marker flag, byte length and SHA-256. The manifest is the comparison baseline; the current listing is never used as the history inventory.

| Bucket | Content |
|---|---|
| plain (unversioned) | 8 empty objects, 24 inline 1 KiB objects, 16 external 1 MiB objects, 3 multipart objects (5 MiB + 1 MiB parts), 10 keys with spaces, punctuation and Unicode, 4 parent/child key pairs |
| versioned | 8 keys, each with an explicit null version written before versioning was enabled, three history versions and a current delete marker |
| locked (Object Lock) | 2 objects under legal hold, 2 objects under COMPLIANCE retention |

`verify_manifest` runs through one client and `verify_from_every_node` repeats it on every node: the exact set of `(key, version id, marker)` per bucket must equal the manifest, every non-marker version must return its recorded length and SHA-256, a marker-covered key must return `NoSuchKey`, lock state must be unchanged and versioned deletes of locked versions must still be denied, and the plain listing must equal the manifest keys.

Physical placement comes from `xl.meta` on the pool drives (`pools_holding`, `capture_seed_pool_census`): expansion must leave every seed-pool shard byte-identical, and after any movement every version must be held by exactly one pool. The `.rustfs.sys/buckets/<bucket>/.bucket-incarnation` sidecar is censused per pool drive; expansion and restart must leave the seed-pool census unchanged and decommission must leave a sidecar on a target pool.

Durability is read back, not assumed: the fixture starts every node with `RUSTFS_DURABILITY_MODE=strict` and `RUSTFS_NEW_BUCKET_DURABILITY_MODE=strict`, confirms each bucket through `GET /rustfs/admin/v3/bucket-durability/<bucket>`, and parses the `disk_local_durability_mode` startup event from the captured node logs.

## Fault windows

The movement windows are file barriers compiled only with `e2e-test-hooks`; a production build carries none of them. The four fault cases therefore require a server built with that feature and fail immediately with a prerequisite error otherwise; they are never skipped. The fixture arms a window for one object, waits for the server's `reached.*.json` observation, and only then injects. An unreached window fails the case instead of counting as a fault test.

| Window | Server location | Env / arm | Injection | Evidence |
|---|---|---|---|---|
| `before_target_commit` | `migrate_object_inner` before the data-movement multipart complete | `RUSTFS_E2E_DATA_MOVEMENT_BARRIER_DIR`, `arm.json` with nonce, bucket, object, stage, operation | `release.<nonce>` body `io_error` fails the commit; the source stays authoritative | `reached.<nonce>.<pid>.*.json` |
| `before_source_cleanup` | `cleanup_source_entry_if_unchanged` after the source preflight, before the delete | same | SIGKILL of the mover while held | same |
| `after_responsibility_persisted` | `start_decommission_inner` after the durable `pool.bin` start save, before peer reload, worker start and the admin response | same env; arm with operation `decommission_start`, bucket `.rustfs.sys`, object `pool.bin` | SIGKILL of the leader while held | same |

The barrier directory is set for every node of a run; only the process whose operation, bucket, object and stage match the arm blocks. Mover operations match by prefix, so an arm for `decommission` also matches the `decommission_object` copy stage.

## Timeouts

| Budget | Value | Basis |
|---|---|---|
| movement active | 30 s | matches the existing decommission and rebalance cases |
| movement complete | 240 s | the existing cases allow 180 s for 96 objects of 128-256 KiB; this inventory adds 1 MiB externals, multipart and version history (about 45 MiB) |
| movement barrier reach | 240 s (the movement budget) | a window can sit deep in the object order; on Linux CI a busy target capacity gate (160 inline retries, entries deferred to round end) delayed the first fault object by about 2 minutes. The wait fails at once if the operation completes, fails or is canceled without entering the window. Each status probe is bounded to 5 s and a slow probe counts as "still running", because the admin status request can wait on the held mover until the barrier itself times out. The server releases a held barrier on its own after 120 s |
| pre-ACK window reach | 60 s | the decommission start normally reaches its durable save in about a second (POST to response ~0.8 s locally); exceeding 60 s is reported as a stalled start path, not retried with a longer budget |
| cancel, stop and terminal transitions | 90 s | stop and cancel are acknowledged before every in-flight entry finishes its cleanup, so terminal state and placement are polled within this budget |
| decommission start retry | 503 within the movement budget; fleet-proof 500 for 45 s | a start that meets a busy movement gate answers a retryable 503 instead of waiting; any other status is fatal |
| per test (lane) | 6 × 120 s | `slow-timeout` of `[profile.e2e-distributed]`, which also serializes the lane |

## Run

```bash
export RUSTUP_TOOLCHAIN=stable
export NO_PROXY=127.0.0.1,localhost
export RUSTFS_E2E_POOL_ROOTS=/mnt/pool0:/mnt/pool1:/mnt/pool2:/mnt/pool3   # four distinct filesystems
python3 scripts/e2e_binary.py build --features e2e-test-hooks
cargo nextest list -p e2e_test -E 'test(scanner_heal_topology_)'
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo nextest run -p e2e_test -E 'test(scanner_heal_topology_)' --test-threads 1
```

Set `RUSTFS_SCANNER_HEAL_TOPOLOGY_EVIDENCE_DIR` to keep node logs, barrier observations and the per-case evidence JSON after the run; without it the fixture keeps them under `RUSTFS_E2E_LOG_DIR`, which the distributed workflow uploads. On macOS, `hdiutil attach -nomount ram://2097152` followed by `diskutil erasevolume APFS` yields the distinct filesystems that `RUSTFS_E2E_POOL_ROOTS` requires.
