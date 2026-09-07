# Adversarial Review Shape

Use when root `AGENTS.md` triggers adversarial validation or for a substantial PR
review. Paths below are repository-relative. The root finding standard and
completion rule apply; selecting a lens does not require finding a defect.

Risk and review shape:

- **Exempt:** documentation, comments, formatting, or typos with no runtime,
  build, test, or agent-execution effect.
- **Mechanical:** renames, moves, test/tooling-only changes, and agent-rule
  changes. Run correctness and simplicity lenses.
- **Standard:** localized behavior changes. Run one integrated final-diff pass
  covering correctness, simplicity, and test coverage; add only domain lenses
  matched by the diff.
- **High risk / substantial PR review:** high risk includes locking,
  erasure/quorum/heal, replication, multipart, RPC, lifecycle/tiering,
  persistence/fsync, IAM/KMS/auth, cryptography, on-disk/on-wire formats, and
  S3-visible semantics. Cover all applicable lenses using exactly two
  independent reviewers when delegation is explicitly authorized. Split the
  lenses between them. Otherwise perform two fresh sequential passes.
- **Outbound client defaults:** what `TargetClient`, `PutObjectOptions`, or
  the remote SDK configuration sends to every replication or migration target
  is high risk for every target class even when the change fixes one. Follow
  the SOP in `docs/postmortems/2026-09-03-replication-checksum-default-regression.md`:
  run the outbound target matrix, document each new env knob in the same PR,
  and list verified and unverified target classes in the PR Impact section.

Available domain lenses are security, concurrency/durability, compatibility,
and performance. Select `.agents/skills/adversarial-validation/SKILL.md` for an
explicit adversarial request, a high-risk change, or a substantial PR review;
then read only its matching role references. A routine standard pass does not
load the playbook unless the reviewer needs a RustFS-specific probe.
