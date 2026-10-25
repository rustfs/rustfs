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

## Retryable Complete publication

Complete preserves the original shards, selected native part metadata and integrity proofs until global success cleanup. Each attempt uses private hardlinks and one fresh final data-directory identity shared by its participants; it never copies payload bytes. Original source metadata backups use independent inodes because preparing a final head must not change the backup through a hardlink. On Linux, payload and canonical-head publication pin their distinct source parents while sharing the same pinned destination. Physical object serialization uses configured path identity across disk-handle reopens; procfs aliases remain I/O paths.

Each actual native publisher also writes a small, version-owned retirement anchor under its final data directory. The anchor binds its own physical disk and source validation; a donor anchor copied by heal cannot prove local publication. It survives normal source-root cleanup, so a cold replay can verify provenance without retaining upload tombstones. Zero-byte MPU also publishes this directory and its selected native records; it remains a normal parts-backed FileMeta version. Inherited inline controls are cleared rather than extending the FileMeta format.

A native publication atomically moves the canonical prepared upload head to the destination metadata head. This same operation consumes the upload head. Its acknowledgement includes both destination and source-parent durability barriers. A write-quorum completion therefore cannot leave a reusable source quorum that later overwrites a successor PUT or resurrects a deleted object. Prepared and recovering heads are blocked states, not ordinary active upload metadata.

The request captures the source generation before changing its version, layout or final data directory. Native publication, compensation and recovery validate that generation and the complete expected local state under upload ownership. An old callback cannot adopt a new generation. Failed-operation compensation requires its original response capability and conditionally undoes only its exact target publication; inspection and duplicate requests cannot reconstruct that capability. Successful completion irrevocably discards compensation authority, including late results. Only this validated success releases the shared logical object/upload guards. Physical source and target leases, commit accounting and the original resource permit remain owned until their actual filesystem callbacks finish, so a slow tail does not block reads or copies after a successful response.

Every replay requires the same completion identity, version, layout, selected parts and actual physical slots on write quorum. A native-protocol publication additionally requires fresh durable terminal confirmations on write quorum before returning success. Historical completions without native publication controls retain the strict write-quorum replay path because they never issued the new source-compensation capability; malformed or conflicting native controls cannot fall back to that legacy path. These confirmations revoke old compensation authority and wait for real owned filesystem callbacks. An earlier read quorum, a stale target observation or best-effort source cleanup cannot authorize replay. A malformed metadata copy is unknown evidence; independent valid copies can still establish quorum.

Metadata-only and materialized Copy can retain a historical native publication control while changing the current version, modification time or placement. Data-movement equivalence validates that control as historical metadata and still compares the actual current object identity, parts, checksums and user metadata. Ordinary heal preserves an already valid, physically owned retirement record byte for byte, including when the incoming copy omits the historical control. It never imports a donor record or creates new publication authority. Terminal confirmation still requires the exact current version and fresh native proof.

Exceptional recovery uses a fenced negative proof from `N - Q + 1` distinct physical participants, where `Q` remains the ordinary write quorum. Only verified present active/prepared/recovering heads or the same failed owner's exact conditional compensation can contribute. Recovery advances a common generation, durably restores blocked heads on all `N` participants, then activates at least `Q` using the common all-participant evidence. It rereads ordinary source metadata and parts before another publication. A matching partial local terminal seal can enter the new blocked generation only with this exact negative proof; it never regains old compensation authority. A global terminal certificate forbids restoration.

This recovery deliberately waits for every original source/backup participant. It cannot reconstruct an original copy that never existed. A legal Create/Part accepted on only `Q` disks may remain permanently indeterminate after minority publication and coordinator loss, even when the previously missing disk returns. Disjoint minority attempts that consume every source head can also lack a safe recovery witness. These states retain data and return a retryable recovery error; they must not be described as successful retry or affirmative NoSuchUpload. An overwritten or deleted target alone cannot authorize source reconstruction or prove terminal absence.

Multipart publication uses the additive `RenameMultipartData` RPC; recovery and terminal confirmation use `RecoverMultipartSource`. Their payloads are strict, bounded and body authenticated. Older peers return unsupported before mutation, with no legacy publication fallback. Pause all multipart mutations during a mixed-version rollout and upgrade participating storage nodes and coordinators together before resuming them; old coordinators cannot supply a recovered source generation. Existing payload formats and ordinary Create, Part and Complete quorums remain unchanged. Before downgrading coordinators or storage nodes, settle or abort uploads with native prepared/recovering state using the upgraded code; older code does not understand those blocked states. Do not use a downgrade as a recovery procedure for an indeterminate upload.

The private staging adds O(selected parts plus native metadata and integrity sidecars) filesystem metadata operations, an independent source-metadata snapshot, protected state writes and their required parent barriers, plus one small retirement record write/sync. The normal native path semantically validates the encoded target metadata once and carries a private in-memory receipt; later completion/readback checks the exact published bytes and publication state. Cold recovery and compensation keep their full validation. RootMissing confirmation reads that bounded record from the current version directory. Normal Complete uses one publication fanout, waits for its logical tails, and makes an irreversible success decision before later quota or cleanup work. Native-protocol replay adds metadata verification and terminal confirmation; exceptional recovery adds inspection, generation claims, all-participant restoration and activation barriers. An interrupted round can resume from its verified durable ready certificate. Upload-scoped read locks allow independent Part commits together, while Complete and Abort exclude source mutation. The registry retires its own key without scanning other uploads, and noncanonical physical aliases fail before I/O. Attempt cleanup retains ownership until blocking work ends. These mechanisms bound overhead and avoid payload copying; they are not measurements of Complete throughput.

Fully local stores use private, instance-bound proof authority after validating the complete local endpoint topology; they do not require a network cluster credential. Reinitialization retains the same instance authority while registering the validated local topology. Deserialization, a different store instance, a remote mode or changed proof contents cannot inherit that authority. RPC proofs continue to require authenticated wire material.

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
