# KMS key destruction

English | [简体中文](kms-key-destruction.zh-CN.md)

**Use this when:** retiring a KMS key, deciding whether data protected by a key can be made permanently unrecoverable, answering an erasure or key-compromise request, or explaining why a key scheduled for deletion still exists.
**Source of truth:** `crates/kms/src/manager.rs` (`delete_key_inner`, `check_deletion_request`, `refuse_referenced_immediate_deletion`), `crates/kms/src/deletion_worker.rs` (the sweep and its metrics), `crates/kms/src/types.rs` (`*_PENDING_DELETION_WINDOW_DAYS`), `crates/kms/src/backends/{local,vault,vault_transit,aws}.rs` (`delete_key`, `remove_expired_key`), `rustfs/src/kms_deletion_gate.rs` (configuration references), `rustfs/src/admin/handlers/kms_keys.rs` (`DeleteKmsKeyHandler`).

Destroying a KMS key is a product feature, not a maintenance side effect: it is how RustFS makes data permanently unrecoverable without rewriting the data itself. This page states what the feature guarantees, the steps a key goes through, and the safeguards around it. Related: [KMS admin API contract](kms-admin-contract.md) (routes and IAM actions), [KMS backend security properties](kms-backend-security.md) (where key material lives, version retention), [Per-key KMS authorization](kms-per-key-authorization.md), [KMS observability runbook](kms-observability-runbook.md).

## What destruction means

Every SSE-KMS object, and every SSE-S3 object on a deployment whose SSE-S3 keys come from the KMS, is encrypted with its own data key, and that data key is stored next to the object only in wrapped form — encrypted under a KMS key. Destroying the KMS key removes the only means of unwrapping those data keys. The object bytes stay on disk, and stay replicated and tiered wherever they already are, but nothing can decrypt them again. This is cryptographic erasure.

The feature serves three needs that logical deletion cannot:

- **Erasure obligations.** A tenant leaves, or a deletion request has to cover every copy, including versions, replicas and tiered data that are expensive or impossible to locate and overwrite one by one.
- **Key compromise.** Material that may have leaked is retired for good once the data under it has been rewrapped or written off.
- **Decommissioning.** Media that cannot be wiped block by block is made unreadable by destroying the keys instead.

Because one key usually protects many objects, destroying it is the most consequential operation the KMS admin API offers. Every rule below follows from that.

## Guarantees

- **A key is never destroyed in one step.** Destruction always passes through `PendingDeletion`, a state in which the key still exists, objects under it are still readable, and the deletion can still be cancelled. No single request takes an `Enabled` or `Disabled` key to destroyed.
- **Success means what it says.** A scheduling request that succeeds has destroyed nothing. A destruction request that succeeds has destroyed the key: the key record and every retained version of its material are gone from the backend when the response is returned. A request that cannot do what it was asked is refused; it is never quietly turned into the other kind.
- **Destruction is final.** RustFS keeps no copy of destroyed material and offers no undelete. Recovery after destruction is possible only from a backup taken outside this feature (see [What destruction does not cover](#what-destruction-does-not-cover)).
- **Configuration that still names the key blocks destruction.** A key that is the KMS service default key, or that any bucket's default encryption configuration names, is not destroyed — neither by the background sweep nor on request.
- **Every decision is recorded.** Scheduling, cancellation, destruction and each refusal are written to the KMS audit log with the caller's identity; destruction carried out by the background sweep is recorded as an internal operation.

## Lifecycle

| State | Entered by | New encryption | Reads of existing objects | Leaves by |
| --- | --- | --- | --- | --- |
| `Enabled` | Key creation, enable, cancelled deletion | Allowed | Allowed | Disable, schedule deletion |
| `Disabled` | Disable | Refused | Allowed | Enable, schedule deletion |
| `PendingDeletion` | Schedule deletion | Refused | Allowed | Cancel deletion, destruction |
| Destroyed | Window elapsed, or expedited destruction | — | Fail permanently | — |

Reads keep working through `Disabled` and `PendingDeletion` on purpose: the waiting window is only a safety net if the data is still there to notice missing. The AWS KMS backend is the exception — AWS itself refuses decryption for disabled and pending keys; see [Backend differences](#backend-differences).

After destruction, any read of an object encrypted under the key fails with `400 KMS.NotFoundException`, and the key id answers `404` on the admin API. The name is free again, but a key created under it later is a new key with new material: it does not make the old objects readable.

## Step 1: schedule deletion

`DELETE /rustfs/admin/v3/kms/keys/delete` with a JSON body naming `key_id` moves an `Enabled` or `Disabled` key to `PendingDeletion` and records a deadline.

- `pending_window_in_days` sets the waiting window. It must be between 7 and 30 days and defaults to 30; anything outside that range is refused with `400`.
- The response carries `deletion_date`, the deadline after which the key becomes eligible for destruction, and `impact`, the configuration references found at that moment.
- Scheduling a key that configuration still references is allowed. It destroys nothing, and the references are checked again before anything is destroyed. Treat a non-empty `impact` as a list of things to clear before the deadline.
- Scheduling a key that is already `PendingDeletion` is refused with `400`; cancel first to change the deadline.

Use the window to confirm nothing still depends on the key: watch for reads that would fail without it, move bucket defaults to another key, and run the [bulk rekey sweep](../architecture/kms-bulk-rekey-contract.md) if the data must outlive the key.

## Step 2: cancel, or let the window elapse

`POST /rustfs/admin/v3/kms/keys/cancel-deletion` returns a `PendingDeletion` key to `Enabled` and clears the deadline. It works at any point before the key is destroyed, and fails once destruction has begun.

If the deadline passes without a cancellation, the background sweep destroys the key. The sweep runs on every node about once a minute, needs no operator action, and is safe to run concurrently and to repeat after a restart. For each expired key it re-checks the configuration references and the key's state at that moment:

- **Still referenced:** the key is left in `PendingDeletion`, still readable and still cancellable, and is reconsidered on every later sweep. It is destroyed once the references are gone. A key that never gets destroyed is therefore a signal to act on, not a failure to retry.
- **Cancelled in the meantime:** the cancellation wins and nothing is destroyed.
- **Destruction interrupted** (crash, backend outage): the key is left unusable rather than half-present, and the next sweep completes the removal.

## Expedited destruction

When waiting for the deadline is not acceptable — an erasure deadline, a confirmed compromise — a key that is already `PendingDeletion` can be destroyed on request, without waiting for its deadline. The same `DELETE` route takes `force_immediate: true`.

Expedited destruction is refused unless every one of these holds:

| Condition | Refusal |
| --- | --- |
| The server runs with `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION=true` | `400` |
| The request is a JSON body; the query-string form is not accepted | `400` |
| `confirm_key_id` repeats `key_id` exactly | `400` |
| The key is already `PendingDeletion` | `400`; schedule the deletion first |
| No bucket default encryption configuration names the key, and it is not the KMS service default key | `409 Conflict` |
| The backend can physically delete keys | `501` |

Expedited destruction shortens the wait; it does not remove the cancellable step. The scheduling request and the destruction request are two separate, audited actions, so a single mistaken or malicious call can at worst park a key in a state that `cancel-deletion` undoes.

Keep `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION` off in normal operation and turn it on only for the duration of a planned destruction. With the gate off, the waiting window is the only path to destruction, whatever the caller's permissions.

## Safeguards and their limits

| Safeguard | What it protects against | What it does not do |
| --- | --- | --- |
| Mandatory `PendingDeletion` step | A single request destroying data | Stop two deliberate requests |
| Waiting window (7–30 days) | Discovering a dependency only after the key is gone | Tell you which objects use the key |
| Configuration reference check | Destroying a key that new writes are still directed at | See objects already encrypted under the key |
| Server-side gate for expedited destruction | A credential with `kms:DeleteKey` skipping the window on its own | Apply once an operator has turned it on |
| `confirm_key_id` | Acting on the wrong key through a mistyped or replayed request | Authorize anything by itself |
| `kms:DeleteKey`, checked against the named key | Callers without a grant on that key | Separate scheduling from destroying; both, and cancellation, use the same action |

The reference check is deliberately limited to configuration. RustFS does not keep an inventory of which objects are encrypted under which key, so an empty `impact` report never means the key is unused. The objects already written under a key are exactly what destruction is meant to make unreadable; whether that is the intended outcome is the operator's decision, and the waiting window is the time to verify it.

## What destruction does not cover

- **Copies of the key outside RustFS's control.** Vault snapshots and storage backups, backups of the Local backend's key directory, and KMS backups taken through the admin API may still hold the material. Erasure is only complete once those copies are expired or destroyed under their own procedures.
- **Other keys.** Objects rewrapped onto another key by the rekey sweep before destruction remain readable under that key. Each replication site holds its own wrapped data keys and its own KMS; destroying a key at one site does not affect another.
- **Data keys already in use.** A request that had unwrapped its data key before the destruction completes can finish. Nodes may also serve cached key metadata for a short time; clear the KMS cache when the cut-off must be immediate.
- **Plaintext and other encryption modes.** SSE-C objects depend on client-held keys, and unencrypted objects on no key at all.
- **The bytes on disk.** Ciphertext is not removed. Delete the objects as usual to reclaim space.

## Backend differences

| Backend | Schedule and cancel | Destruction after the window | Expedited destruction | What is removed |
| --- | --- | --- | --- | --- |
| Local | Yes | Background sweep | Yes | The key file |
| Vault KV2 | Yes | Background sweep | Yes | The key record and every version record under it |
| Vault Transit | Yes | Background sweep | Yes | The Transit key and its metadata record; RustFS sets `deletion_allowed` on the Transit key immediately before deleting it |
| AWS KMS | Yes; AWS owns the window | Performed by AWS | No (`501`) | Whatever AWS removes; a cancelled key returns as `Disabled`, and reads are refused for the whole pending window |
| Static | No | — | — | Nothing; the key is configuration, not a managed key |

On the Vault backends the RustFS credential needs delete permission on the key paths for destruction to succeed; see [KMS backend security properties](kms-backend-security.md). Without it, scheduling works and destruction fails at the sweep, which keeps retrying.

## Observing it

- `rustfs_kms_pending_deletion_keys` — keys scheduled and not yet due. A value that never falls after a deadline means destruction is blocked or failing.
- `rustfs_kms_deletion_tombstone_keys` — keys whose destruction was interrupted and awaits the next sweep. Expected to return to zero within a few sweeps.
- `rustfs_kms_deletion_sweep_keys_total{outcome}` — `removed`, `blocked` (still referenced), `skipped` (cancelled or not yet due), `failed`, `unreadable`. Alert on sustained `blocked` or `failed`.

The server log names the key and the references whenever the sweep refuses to destroy an expired key. See the [KMS observability runbook](kms-observability-runbook.md) for how the gauges behave when part of the key set is unreadable.

## Procedures

**Retire a key that is no longer needed.** Move every bucket default off the key and change the service default if it is this key. Rekey the data that must survive. Schedule the deletion and read `impact` in the response. Watch for failed reads during the window. Let the sweep destroy the key, then confirm the key id answers `404`.

**Erase data by a deadline.** Schedule the deletion as above and clear every reference. On the day, enable `RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION`, send the expedited request with `confirm_key_id`, confirm the key id answers `404` and that a read of an affected object fails with `KMS.NotFoundException`, then turn the gate off again. Expire the backups that still hold the key.

**Undo a scheduled deletion.** Send `cancel-deletion` before the deadline. On AWS KMS, enable the key afterwards.
