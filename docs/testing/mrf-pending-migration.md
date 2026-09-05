# Pending MRF Migration

`heal::mrf_queue::snapshot::migration` exposes explicit capture, staging, and readback of pending legacy responsibility evidence. Nothing invokes it from the production MRF consumer. It does not enable the committed-snapshot writer, freeze legacy ingress, acknowledge durable admission, or authorize source garbage collection.

The caller supplies every configured local disk slot, including missing slots. Missing/unformatted/duplicate disks, unavailable metadata volumes, invalid records, empty evidence, and aggregate byte/record overflow fail closed. Both legacy paths retain their original bytes, disk identity, absent-versus-empty state, and SHA-256 digest. Complete subset/superset replicas become conservative pending evidence, never a claimed newest legacy snapshot. Raw record replay preserves kind, scope and nil/absent version encodings; unknown incarnation stays unknown.

Staging writes only `.heal-mrf-import-pending.{0,1}.bin`, `.heal-mrf-import-commit.{0,1}.bin`, and `.heal-mrf-import-claim.bin` under the metadata volume. It reuses the committed reader's manifest codec and the storage owner's conditional-file operation, including the configured metadata durability policy. A candidate is written before sources are revalidated, its manifest is then committed, and committed bytes plus source coverage are read back before success. Success is pending staging evidence, not a power-loss or cluster-quorum durability receipt.

A successor inherits prior source bytes even if a replay consumer has already read or admitted their records. Size limits include inherited evidence. There is no completion-based pruning. A changed source blocks recovery of that pending generation; an explicit new capture can stage a successor that retains both the previous and current responsibilities. The inactive slot is replaced while the preceding committed slot remains intact.

All participating disks are claimed in disk-identity order through CAS. Normal completion conditionally releases only the current invocation's claim. Cancellation, process death, or an ambiguous claim/release I/O failure may leave a claim behind. Read-only recovery remains available, but further staging is blocked until a separate storage-fenced recovery procedure is implemented. Process liveness and the legacy ingress lease do not authorize taking over or deleting a claim.

Run the focused fixtures with a nonzero test count:

```sh
cargo test -p rustfs-heal --lib heal::mrf_queue::snapshot::migration::tests
```

Fixtures use real local disks and the production CAS/readback path. They cover disk-order independence, retained raw identities, source change after candidate write, capacity rejection, interrupted commit boundaries, lost responses, torn inactive slots, and refusal to take over an interrupted claim. Boundary injection and same-process reopen are not process-kill, directory-fsync failure, disk-full, mixed-version, or power-loss tests. The actual manager Full/Accepted-to-crash pipeline remains outside this staged API.

Rollback leaves all pending and legacy artifacts untouched. Activation still requires legacy-writer coordination, bounded recoverable ingress, exact object-disposition/successor receipts, and the W14/W21 process-crash and compatibility gates. The production legacy replay deletion window remains unresolved by this staging-only phase.
