# Distributed multipart RPC delay gates

**Use this when:** validating multipart behavior under remote metadata or lifecycle-lock latency, peer loss after RPC entry, or cancellation before an incarnation read completes.
**Source of truth:** [the RPC hook](../../rustfs/src/storage/rpc/node_service/e2e_rpc_delay.rs), especially `Arm`, `Scope` and `ExitObservation`; [the distributed cases](../../crates/e2e_test/src/distributed/multipart_rpc_delay_test.rs), especially `run_matrix` and `Session`; `[profile.e2e-distributed]` in [nextest configuration](../../.config/nextest.toml).

## Contract

The hook exists only in builds with `e2e-test-hooks`. Without `RUSTFS_E2E_RPC_DELAY_DIR`, it performs no filesystem access. Each test node uses its own control directory. A bounded, strictly decoded arm selects a non-nil nonce, a finite RPC method set and an exact test resource. It pauses only after authentication and decoding, before the real storage or lock call. Unlock, refresh and mixed-resource lock batches continue normally. Existing production operation budgets, fanout and quorum rules remain active.

Before arming a window or checking recovery, every coordinator must report its own `local_runtime` storage inventory with write quorum, no unavailable or missing drive slots, and ready pool metadata and locks. HTTP 200 alone can supply only read quorum while returning disk handles still reject writes. The harness records these real readiness payloads; it does not retry the old-part completion write to hide a recovery failure.

A short delay with every node healthy tests latency in the selected plane. Peer-loss cases first prove that a remote RPC reached the selected hook, then stop peers and release surviving calls. These are post-entry peer-loss checks: stopping a peer affects both its metadata and lock services, so the final failure cannot be attributed exclusively to one plane.

The erasure write quorum and the lifecycle shared-lock read quorum are different. The harness observes peers actually scheduled by bounded fanout; it must not require an unused client to start or expand production fanout to satisfy a marker count.

## Oracles

Successful writes are read through every restored node. A failed part replacement must preserve the previously acknowledged payload, not merely its ETag; the prior part is completed and read before a later upload can overwrite it. Complete and Abort checks retain the acknowledged object and upload-cleanup contracts.

A client task abort does not establish server cancellation. The cancellation case uses actual RPC entry, lifecycle-writer completion, lock-table drain and old-upload isolation after bucket recreation. An exited hook marker alone does not prove that remote locks were released. The existing ECStore post-commit cancellation tests remain necessary for ownership of detached commit tails.

## Run

```bash
export RUSTUP_TOOLCHAIN=stable
export NO_PROXY=127.0.0.1,localhost,::1
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
python3 scripts/e2e_binary.py build --features e2e-test-hooks
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo nextest run -p e2e_test --profile e2e-distributed --test-threads 1 -E 'test(/^distributed::multipart_rpc_delay_test::/)'
```

The binary wrapper verifies the source, features and executable identity. Configuration and resource-matcher unit tests are selected with `test(/e2e_rpc_delay::tests::/)` in the `rustfs` library target with the same feature.

## Evidence limits

Keep the actual matched-method and reached/exited records, S3 response headers, node logs and lock-table snapshots. Correlate only identifiers actually present; an RPC identifier is not assumed to equal an S3 request identifier. Missing cause headers are not synthesized, and `Retry-After` assertions follow the existing error contract.

These local cases do not establish production throughput, the original eight-node incident's cause, physical scratch cleanup, complete owner intervals or exported lock counters. Counter acceptance needs a separately configured and captured real metrics exporter; logs are not counter samples.
