# KMS disaster-recovery drill

A KMS backup that has never been restored is a hypothesis. This runbook turns it into evidence: it rehearses the complete loop — back up, lose the persistence layer, preflight, restore, and read historical objects again — and files a machine-readable evidence bundle for each run. For what each backend's backup actually covers, see [KMS backend security properties](kms-backend-security.md); for the metrics and alerts around KMS operations, see the [KMS observability runbook](kms-observability-runbook.md).

The acceptance criterion of a drill is not that files came back. It is that objects encrypted before the disaster decrypt after the restore. The harness keeps the ciphertext and encryption metadata of every object it sealed before the disaster and, once the restore is complete, decrypts each one through a freshly opened backend and compares against the pre-disaster digest. Anything less proves only that a bundle is well formed.

## Scope

The drill covers the **Local** backend, which is the only backend RustFS produces a full-material bundle for. The responsibility split is deliberate and is described in `crates/kms/src/backup/capability.rs`:

| Backend | What a RustFS bundle carries | What restores it |
| --- | --- | --- |
| Local | Key records, all stored versions, the KDF salt, sanitized configuration | The RustFS restore in this runbook |
| Static | Non-sensitive references only | The operator re-supplies the secret out of band |
| Vault KV2 + Transit | KV metadata and Transit ciphertext references | Vault's native snapshot restore, then the RustFS orchestration |
| Vault Transit | Metadata, configuration references, verification data | Vault's native snapshot restore, then the RustFS orchestration |

For the Vault backends there is no RustFS-side export, so there is no loop for a drill to close end to end: the cryptographic root is non-exportable and comes back through Vault's own disaster-recovery flow. What RustFS owns there is the refusal to proceed before that has happened, plus the ordering of everything after it. Rehearse it with the Vault section below.

## What the drill measures

**Recovery point (RPO).** The harness creates one key *after* the export fence closed and seals objects under it. After the restore that key is absent and its objects do not decrypt — as they must not. The recovery point of a KMS backup is therefore the snapshot generation of the last bundle, not the moment of the disaster, and `rpo.rpo_window_millis` in the evidence is the width of that window in the rehearsal. In production the same window is the age of your newest bundle, which is what your backup schedule sets.

**Recovery time (RTO).** `rto_millis` is the sum of the phases an operator waits on: quarantine, preflight, restore, and verification. Seeding, sealing and the disaster itself are drill scaffolding and are excluded. Human decision time is excluded too and belongs to your incident process, not to this number. Per-phase costs are in `timings`.

An RTO figure only transfers to production if the rehearsed deployment is comparable, so size `RUSTFS_KMS_DRILL_KEYS` to the number of master keys the real deployment holds. Both numbers are recorded in the evidence (`dataset`), so a stale measurement is visibly stale.

## Running a drill

```bash
export RUSTFS_KMS_DRILL_WORKSPACE=/var/lib/rustfs/dr-drill/$(date -u +%Y%m%dT%H%M%SZ)
export RUSTFS_KMS_DRILL_MASTER_KEY='<throwaway at-rest key for the rehearsal deployment>'
export RUSTFS_KMS_BACKUP_KEK='<base64 of the 32-byte backup KEK>'
export RUSTFS_KMS_BACKUP_KEK_ID='<kek identifier>'
export RUSTFS_KMS_DRILL_KEYS=64

cargo run -q -p rustfs-kms --example kms_dr_drill
```

The workspace must be an absolute path that does not already hold a rehearsal; give each run its own directory so the evidence and the quarantined state stay side by side. The runner exits 0 only when every check held, so a scheduled drill fails its job on a bad result instead of quietly filing a bad report.

`RUSTFS_KMS_BACKUP_KEK` and `RUSTFS_KMS_BACKUP_KEK_ID` are the same variables the admin backup API reads. Use the KEK real bundles are sealed under: retrieving it is the one part of a recovery no bundle can attest to, and a drill that invents its own KEK does not test it. The rehearsal deployment's at-rest master key is separate and throwaway — the drill creates and destroys that deployment itself and never touches the running KMS.

Optional variables: `RUSTFS_KMS_DRILL_DISASTER` (see below), `RUSTFS_KMS_DRILL_ID`, `RUSTFS_KMS_DRILL_DEPLOYMENT`, `RUSTFS_KMS_DRILL_OBJECTS_PER_KEY`, `RUSTFS_KMS_DRILL_OBJECT_BYTES`, `RUSTFS_KMS_DRILL_FILE_PERMISSIONS`, `RUSTFS_KMS_BACKUP_KEK_VERSION`, and `RUSTFS_KMS_DRILL_EVIDENCE` (evidence path, default `<workspace>/evidence.json`).

## Disaster matrix

Run all three; they exercise different failure surfaces and converge on the same procedure, which is the point — an operator does not have to diagnose the failure mode before acting.

| `RUSTFS_KMS_DRILL_DISASTER` | Simulates |
| --- | --- |
| `key-directory-lost` (default) | The whole key directory is gone: lost volume, wiped host |
| `master-key-salt-lost` | Records survive but the KDF salt is gone, so no material unwraps |
| `key-record-corrupted` | One key record is truncated in place: torn write, partial media failure |

Bundle-side faults — tampered, truncated, wrong-KEK, incomplete, unknown-version — are not drill scenarios. They are covered exhaustively and deterministically by the unit tests of `crates/kms/src/backup/local_export.rs` and `crates/kms/src/backup/local_restore.rs`, and re-running them as a drill would add fixtures without adding evidence.

## Recovery procedure

The drill executes exactly this sequence; running it by hand against a real deployment is the same procedure with your own paths.

1. **Stop the KMS.** A restore must never race a running backend.
2. **Quarantine, do not delete.** Move the damaged key directory aside rather than clearing it. The drill records the quarantine path and its contents in the evidence, and nothing damaged is ever destroyed: a restore that turns out to be the wrong call has to stay reversible, and forensics need the original bytes. Restore refuses a non-empty target anyway, including one holding only an orphan salt.
3. **Preflight.** A dry-run decodes the whole bundle, checks the KDF descriptor against this build, verifies the operator-supplied master key against the bundle's one-way verifier, and enumerates conflicts. It writes nothing; the drill proves that by digesting the target before and after. Read every blocker before going further.
4. **Restore.** Artifacts are staged inside the target, decryption-probed, then published through a commit marker and an atomic cutover.
5. **Verify.** Open the restored directory and decrypt historical objects. This is the acceptance criterion, not step 4.
6. **Observation period.** Keep the quarantined directory and the bundle until you have observed the recovered deployment serving reads. Nothing in the restore path deletes old material for you.

## Reading the evidence

`evidence.json` is `DrillEvidence` (`format_version` 1). It carries identifiers, digests and durations only: no key material, no master key, no plaintext, no ciphertext. Archive it next to the incident or audit record.

| Field | Why it matters |
| --- | --- |
| `verdict`, `findings` | The result and every check that did not hold |
| `envelope_probes[].verified` | Per-object proof that a historical data key still unwraps |
| `rpo.post_snapshot_objects_recovered` | Must be `0`; anything else means the bundle was not a point-in-time snapshot |
| `bundle.manifest_digest` vs `manifest_digest_after_recovery`, `bundle.source_unchanged` | The restore treated its bundle as strictly read-only |
| `recovery.dry_run_zero_write` | The preflight wrote nothing to the target |
| `recovery.commit_marker_cleared` | The cutover completed rather than leaving the target mid-restore |
| `recovery.repeat_restore_refused` and `..._left_target_unchanged` | Re-running the procedure cannot damage a healthy deployment |
| `dataset`, `timings`, `rto_millis`, `rpo.rpo_window_millis` | The measurement, and the deployment size it is valid for |

## Interruptions and re-entry

A restore has exactly one commit point: the durably published `.restore-commit.json` marker. Before it, the target's top level is untouched and re-running starts over. With it published, the backend refuses to start — a key directory mid-cutover must not serve requests — and you have two ways out:

- **Roll forward**: re-run the restore with the same bundle. The marker is bound to the bundle by backup id and manifest digest, so a different bundle is refused rather than merged.
- **Roll back**: abort the restore, which takes back exactly the files the marker names, then the marker, then the staged state. The target returns to its pre-restore state and a fresh restore can start.

Both paths are exercised by the drill's interrupted-cutover tests in `crates/kms/src/backup/drill.rs`, which crash a restore precisely at its commit point rather than simulating the resulting state. Every interruption converges on the complete old state or the complete new state; there is no half-activated outcome.

## Vault backends

There is no RustFS-side Vault export, so a Vault drill is not the same closed loop. Rehearse it as:

1. Restore the Vault cluster from its own native snapshot, following your Vault runbook. Transit keys are non-exportable and RustFS never attempts to import them.
2. Run the RustFS preflight against the recovered cluster. It verifies cluster, namespace, mounts, the Transit key's identity and its version window, and the KV generation, and refuses on the first mismatch. A Transit key that was not restored, a `min_decryption_version` raised above what the bundle still needs, or a version history restored below the bundle's snapshot point are each reported as a distinct mismatch.
3. Only then let the restore publish KV records, in the order material and versions, then metadata, then configuration and cutover.

`crates/kms/src/backup/drill.rs` carries an `#[ignore]`d leg for step 2 that drills the refusal against a real server. It needs a Vault with the transit engine enabled and reads `RUSTFS_KMS_VAULT_ADDR` and `RUSTFS_KMS_VAULT_TOKEN`:

```bash
cargo test -p rustfs-kms --lib backup::drill -- --ignored --nocapture
```

## Running the drill matrix as tests

```bash
cargo test -p rustfs-kms --lib backup::drill
```

This runs the offline matrix — all three disasters, the evidence contract, and both interrupted-cutover outcomes — with no external dependencies. Use it in CI to keep the harness itself honest; use the operator entry point above to produce evidence for a real deployment size.
