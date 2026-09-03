# Postmortem: replication checksum default flipped, Object Lock targets rejected every PUT (rustfs#7082)

**Use this when:** you change what the replication or migration client sends to a remote target by default, you are reviewing such a change, or you are triaging a report where a fix for one target class broke another.
**Source of truth:** `crates/ecstore/src/bucket/remote_s3_client.rs` (`replication_request_checksum_calculation`), `crates/e2e_test/src/replication_target_matrix_test.rs` (the outbound target matrix), `crates/e2e_test/src/fake_s3_target/mod.rs` (target failure modes).

## Summary

rustfs/rustfs#6895 fixed rustfs/rustfs#6853 (a target that stores `aws-chunked` framing verbatim) by switching the outbound SDK checksum policy from `WhenSupported` to `WhenRequired` for every replication target. The SDK-computed CRC32 had been doing a second job nobody had written down: it satisfied the S3 rule that a PutObject carrying `x-amz-object-lock-*` headers must include `Content-MD5` or an `x-amz-checksum-*` header. With the checksum gone, every replication PUT of an object with a retention period or legal hold was rejected by AWS-compatible Object Lock targets. The regression shipped in `1.0.0-rc.5` and was reported by a customer two days later.

## Timeline (UTC)

| When | Event |
| --- | --- |
| 2026-08-29 14:49 | rustfs#6853 filed from the round-2 real-VM lab (SeaweedFS stored `aws-chunked` frames). |
| 2026-08-30 14:24 | PR rustfs#6895 opened. |
| 2026-08-30 17:43 | PR merged. Zero GitHub reviews; adversarial validation ran in-session. |
| 2026-08-31 | `1.0.0-rc.5-preview.1` tagged. |
| 2026-09-01 17:35 | `1.0.0-rc.5` tagged. |
| 2026-09-02 20:29 | rustfs#7082 filed by a customer replicating to an Object Lock bucket on Impossible Cloud. |

Three days from an own-lab finding to a public release candidate, with no Object Lock target exercised in between.

## Root cause

`RequestChecksumCalculation::WhenRequired` makes the SDK add a checksum only to operations whose model marks one as required. PutObject is not such an operation, so the client now sends no integrity header at all. AWS S3, MinIO and most compatible stores enforce a server-side rule the SDK model does not know about: a PutObject with Object Lock parameters needs `Content-MD5` or `x-amz-checksum-*`. RustFS itself does not enforce that rule (the matching error constant in `crates/ecstore/src/bucket/object_lock/objectlock.rs` is unused), so RustFS-to-RustFS site replication was unaffected and the gap was invisible in every in-tree test.

## Why four layers of defense all missed it

The same thing was missing at every layer: an inventory of what remote targets may require of an outbound request.

- **Change design.** A default that applies to every target was changed to satisfy one target class. The PR text considered checksums only for objects that already carried one ("object-level `x-amz-checksum-*` forwarding is unaffected") and never asked the inverse question: which target-side rules depend on a checksum being present.
- **Tests.** The new unit test asserted the fix ("no `x-amz-trailer`"), not the contract ("the target accepted the PUT"). The fake target modeled neither failure mode: it did not store `aws-chunked` verbatim and it did not demand a checksum on locked PUTs. `replication-check` has an ObjectLock phase, but its probe PUT carries no retention headers, so it would have reported OK against the customer's target.
- **Review.** The compatibility lens (`.agents/skills/adversarial-validation/references/compatibility.md`) listed only inbound and on-disk concerns: `xl.meta`, proto, MinIO fixtures. It contained no line about outbound target behavior, so the lens passed while blind to the class.
- **Release.** Two new environment escape hatches shipped undocumented. The customer's second question in rustfs#7082 was whether a configuration option exists; it did, and it was unfindable.

## Corrective actions

| Action | Where |
| --- | --- |
| Outbound target matrix e2e: every target failure mode times every object shape, with an explicit expectation table. Known-red cells are pinned to an open issue and fail loudly when they start passing. | `crates/e2e_test/src/replication_target_matrix_test.rs` |
| Fake target gains the three failure modes a real fleet has shown: rejects `aws-chunked` uploads, requires a checksum on Object Lock PUTs (and verifies `Content-MD5`), mints its own version ids. | `crates/e2e_test/src/fake_s3_target/mod.rs` |
| Compatibility lens gains an outbound target section. | `.agents/skills/adversarial-validation/references/compatibility.md` |
| `AGENTS.md` names outbound client defaults as high risk and requires the matrix plus documented escape hatches. | `AGENTS.md`, Adversarial Validation |
| The two escape hatches from rustfs#6895 are documented. | `docs/operations/replication-outbound-transport.md` |
| Product fix for rustfs#7082: derive `Content-MD5` from the source ETag on locked PUTs. | tracked in rustfs#7082 |
| `replication-check` probe PUT carries retention headers when the target bucket has Object Lock. | tracked in rustfs#7082 |

## SOP: changing an outbound client default

Applies to any change in what `TargetClient`, `PutObjectOptions`, the remote SDK configuration, or the outbound header set sends by default. Treat the change as high risk for every target class, even when it fixes one.

1. **Inventory the dependents.** Before editing, list every target-side rule that the current default satisfies, not only the one the change targets. Done when the PR body names each rule and which target classes enforce it.
2. **Run the matrix.** Run `replication_target_matrix_test` locally against the built binary. Done when every cell matches its expectation and no expectation was edited to make it pass.
3. **Extend the matrix for the new failure mode.** If the change is motivated by a target behavior the fake does not model, add the mode to the fake first and add a cell that is red before the fix. Done when the cell is green only with the fix applied.
4. **Document every escape hatch in the same PR.** Each new environment knob gets an entry in `docs/operations/replication-outbound-transport.md`. Done when the knob name appears there.
5. **Record the coverage.** The PR Impact section lists the target classes verified and, explicitly, the ones not verified. Done when a reader can tell which cells were never run.
6. **Soak before a release candidate.** A fix that changes outbound behavior runs once through the real-target lab, which must include an Object Lock target and an AWS or MinIO target, before it is tagged into a release candidate.

## Related

- rustfs/backlog#2085 — product boundary for generic S3 targets (version identity).
- `docs/operations/replication-object-size-limits.md` — single PUT and multipart limits on generic targets.
- `docs/operations/replication-check.md` — the probe phases and their contract.
