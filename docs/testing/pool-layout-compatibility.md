# Pool Layout Compatibility and Regression Tests

**Use this when:** configuring `RUSTFS_VOLUMES` for expansion, investigating issue #6186, or changing pool admission and its regression tests.
**Source of truth:** `DisksLayout::from_volumes` and `get_set_indexes` in `crates/ecstore/src/layout/disks_layout.rs`, `EndpointServerPools::create_server_endpoints` in `crates/ecstore/src/layout/endpoints.rs`, startup format validation in `crates/ecstore/src/store/init_format.rs`, and `lookup_config_for_pools` in `crates/ecstore/src/config/storageclass.rs`. Geometry and parity invariants are owned by [erasure-coding.md](../architecture/erasure-coding.md).

## Notice: count drives, not just nodes

An erasure pool requires at least two drive endpoints. There is no additional admission rule requiring two nodes per pool or two drives per node. A single-node multi-drive pool and a multi-node pool with one drive per node may both be valid.

For command-line / `RUSTFS_VOLUMES` expansion:

- If any volume argument contains an ellipsis expression, each argument describes a separate pool and must contain an ellipsis expression. Each pool must expand to at least two distinct drive endpoints and form a valid set layout.
- A singleton range such as `http://node{3...3}:9000/data` still describes only one drive. It cannot bypass the minimum drive count.
- Without ellipses, all explicit endpoints describe one pool, not one pool per endpoint.
- A single local path such as `/data` remains a supported standalone single-drive deployment. A single URL endpoint is not a valid standalone single-drive endpoint, and a single-drive pool cannot be appended to a multi-pool deployment.
- An initialized single-node single-drive (SNSD) deployment cannot expand in place by adding endpoints or pools. Create a new multi-drive deployment and migrate data through S3 instead. Increasing the capacity of its underlying filesystem is not a pool-topology expansion and adds no redundancy.
- An existing multi-drive pool's drive count and set width are immutable. Preserve its original endpoints and `RUSTFS_ERASURE_SET_DRIVE_COUNT` setting, then append a new pool. Changing `/data{1...4}` to `/data{1...8}` resizes the old pool; appending `/other-data{1...4}` creates a new one.
- Multi-drive sets contain 2 through 16 drives. A pool may contain multiple sets; 16 is not a limit on total drives in a pool. Set divisibility, automatic layout symmetry, duplicate endpoints, endpoint locality, physical-disk validation, and storage-class validation still apply.
- An explicit storage-class parity must fit every pool's set width: `parity <= drives_per_set / 2`, with `STANDARD parity >= RRS parity`. Do not silently lower an explicit parity to admit a smaller pool.

Topology acceptance is not a high-availability guarantee. Losing the only host of a single-node pool loses access to every shard in that pool. With a two-drive set at `EC:1`, losing one drive leaves read quorum but not write quorum. Plan failure domains and quorum separately from admission.

These are valid four-drive-per-set topology examples, subject to the remaining startup checks:

```text
# Two pools, each with four nodes and one drive per node.
RUSTFS_VOLUMES="http://node{1...4}:9000/data http://node{5...8}:9000/data"

# A four-node pool plus a single-node, four-drive pool.
RUSTFS_VOLUMES="http://node{1...4}:9000/data http://node5:9000/data{1...4}"
```

## Rejection and recovery

Invalid single-drive expansion arguments fail during layout parsing. When a syntactically valid layout tries to resize an initialized pool, startup compares the stored format with the configured drive count and set width before initializing or migrating formats for that pool:

- `UnsupportedSnsdExpansion` explains that SNSD cannot expand in place and directs the operator to restore the single local path or migrate through S3 to a new deployment.
- `PoolTopologyMismatch` reports stored and configured drive counts and set widths, and directs the operator to restore the original pool and append a new pool instead.

These are permanent startup errors, not retryable quorum failures. Rejection does not rewrite the affected pool's old format or initialize its new drives. Do not delete `format.json` to bypass it. This is a per-pool check, not an atomic, read-only preflight across every pool in the deployment.

A healthy format quorum remains authoritative; a foreign or malformed minority is quarantined as before. Without a quorum, an unambiguous, valid observed layout can identify a topology mismatch before the wait/retry path. Conflicting observed layouts are not treated as proof of expansion. Missing disks and transient network failures alone do not establish a topology change and retain their existing handling.

## MinIO comparison boundary

The reference is MinIO Community source at commit `7aac2a2c5b7c882e68c1ce017d8256be2feea27f`, not an unversioned claim about all MinIO products or releases:

- [Endpoint expansion](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/endpoint-ellipses.go): `mergeDisksLayoutFromArgs` requires ellipses on every expansion argument, and `getSetIndexes` rejects fewer than two endpoints.
- [Endpoint admission](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/endpoint.go): `CreatePoolEndpoints` does not require two nodes per pool; its standalone single-drive special case requires a local path.
- [Pool initialization](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/erasure-server-pool.go): `newErasureServerPools` checks a common parity against every pool.
- [Storage preparation](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/prepare-storage.go) and [format validation](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/format-erasure.go): persisted drive counts and set widths must match the configured pool; format-layout errors are not ordinary quorum-wait conditions. RustFS keeps its existing majority/minority handling rather than adopting MinIO's all-format validation order.

The node/drive admission rules above match this baseline. This reference does not claim complete startup or storage-class equivalence:

- RustFS resolves automatic parity independently for each pool's set width. For widths `[4, 2]`, automatic STANDARD parity resolves to `[2, 1]`. MinIO uses a common parity, initially selected from the first pool when no value is configured, and rejects a later pool that cannot accommodate it. RustFS's existing automatic policy is not changed by these regression tests.
- An explicit STANDARD `EC:2` rejects a two- or three-drive set in RustFS; `EC:1` fits both. Explicit configuration is shared, not a user-configurable per-pool override.
- RustFS also checks symmetry when `RUSTFS_ERASURE_SET_DRIVE_COUNT` is explicitly set. The MinIO baseline skips automatic symmetry selection for an explicit set width. The topology tests below do not establish equivalence for every explicit-width layout.

## Regression matrix

Layout tests use symbolic endpoints and fixed set-count inputs. Startup tests use temporary local drives and the production format-loading path, comparing format bytes before and after rejection. They do not require production disks, DNS records, or a running MinIO server. Storage-class tests inject configuration directly rather than mutating the process environment.

| Scenario | Expected result | Regression guard |
|---|---|---|
| Standalone `/data` | One single-drive layout | `standalone_single_drive_path_remains_supported` |
| Standalone single URL endpoint | Reject; single-drive mode requires a local path | `test_create_pool_endpoints` |
| Two explicit URLs, no ellipses | One pool containing both drives | `explicit_endpoints_without_ellipses_form_one_pool` |
| Two single-node pools, each with 2 or 4 drives | Two valid pools | `pool_expansion_accepts_single_node_multi_drive_pools` |
| Four-node, one-drive-per-node pool mixed with a single-node, four-drive pool, in either order | Both pool boundaries and set widths preserved | `pool_expansion_accepts_single_node_multi_drive_pools` |
| Two pools with 2, 3, or 4 nodes per pool and one drive per node | One set per pool; every drive retained in its pool | `pool_expansion_accepts_multi_node_single_drive_pools` |
| Ellipsis pool mixed with a plain single-drive endpoint, in either order | Reject with the ellipsis requirement and minimum-drive notice | `pool_expansion_rejects_plain_single_drive_pool_with_notice` |
| Singleton host or drive range, alone or before/after another pool | Reject with the minimum-drive notice and standalone-path guidance | `pool_expansion_rejects_singleton_ellipsis_pool_with_notice` |
| Four drives on one node or four nodes, explicit set width 2 | Two two-drive sets | `explicit_set_size_counts_drives_not_nodes` |
| Two-drive pool, explicit set width 4 | Reject and identify the requested set width | `undersized_pool_error_identifies_requested_set_size` |
| Credentials in rejected plain or singleton pool endpoints | Errors do not echo secrets | `layout_errors_do_not_echo_url_credentials` |
| Mixed single-node multi-drive / multi-node single-drive pools through endpoint resolution | Distributed setup, correct node count and pool/set/disk indices | `pool_expansion_resolves_single_node_multi_drive_and_multi_node_single_drive_pools` |
| Additional set width 2 or 3, explicit STANDARD `EC:2` | Reject and identify the incompatible pool | `explicit_standard_parity_is_validated_against_every_pool` |
| Set widths `[4, 4]` with `EC:2`, or `[4, 2/3/4]` with `EC:1` | Shared explicit parity accepted | `explicit_standard_parity_is_validated_against_every_pool` |
| Explicit environment STANDARD `EC:2`, widths `[4, 2]` | Reject; do not clamp parity | `explicit_environment_standard_parity_is_not_clamped` |
| Automatic parity, widths `[4, 2]` | Preserve RustFS's existing per-pool `[2, 1]` policy | `automatic_parity_is_resolved_per_pool` |
| Existing SNSD plus new drives, on first/non-first server | Reject with SNSD migration guidance; old format unchanged and new drives unformatted | `single_drive_format_rejects_in_place_expansion_without_writes` |
| Existing four-drive pool resized to 2, 6, or 8 drives, or regrouped between one four-drive set and two two-drive sets | Reject with stored/configured geometry and append-pool guidance; no format writes | `existing_pool_rejects_drive_count_or_set_width_changes_without_writes` |
| Existing four-drive pool with only one drive reachable | Retain quorum failure, not an expansion error | `subquorum_existing_layout_with_missing_drives_is_not_expansion` |
| Conflicting four-drive and two-drive formats without a quorum | Retain quorum failure; do not infer the original topology | `conflicting_layouts_without_quorum_are_not_expansion_proof` |
| Healthy three-drive majority with a foreign SNSD minority | Start with the majority and quarantine the outlier | `existing_format_quorum_ignores_single_drive_outlier` |
| New four-drive pool alongside an initialized four-drive pool | Preserve the deployment ID and original format; original pool restarts | `multi_drive_pool_expansion_preserves_existing_format` |
| Typed SNSD/topology errors versus missing-disk, network, and quorum errors | Only permanent topology/corruption errors bypass the format retry loop | `test_should_retry_format_load_rejects_permanent_topology_errors` |
| Full store startup, SNSD to four drives or four-drive pool to eight | Return the typed topology error before retry backoff; no format writes | `store_startup_rejects_pool_resize_before_retry_loop` |
| Startup topology error cloning and I/O wrapping | Retain error type and guidance; do not narrow into a disk/quorum error | `startup_topology_errors_preserve_identity_and_guidance` |

Layout and endpoint guards live in the layout source files above; parity guards live in the storage-class module. The existing `test_get_set_indexes` and `test_into_endpoint_set` tables cover larger, multi-set layouts and malformed ranges.

Run the focused crate tests:

```bash
cargo nextest run -p rustfs-ecstore --lib \
  -E 'test(layout::disks_layout::) | test(layout::endpoints::) | test(config::storageclass::) | test(store::init_format::) | test(test_should_retry_format_load) | test(error::)'
```

## Runtime coverage

Keep the existing single-node multi-drive pool scenarios. They are valid topologies, not exceptions that need a node-count bypass:

- `cluster_two_pool_smoke` in `crates/e2e_test/src/cluster_multidrive_pool_test.rs` exercises real S3 traffic against two pools.
- `four_node_pool_expand_preserves_objects_then_rebalance` in `crates/e2e_test/src/distributed/expand_decommission_rebalance_test.rs` appends pools, verifies existing objects, restarts, and exercises rebalance.

The localhost harness uses separate processes and ports; it does not prove independent physical-host failure tolerance. See [distributed-e2e.md](distributed-e2e.md) for the binary, filesystem, and execution requirements before running expansion tests. Parser and endpoint unit tests establish admission, not persistent-data migration safety or production availability.
