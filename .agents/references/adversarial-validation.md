# Adversarial Review Shape

Use when root `AGENTS.md` triggers adversarial validation or for a substantial PR review. Paths below are repository-relative. The root finding standard and completion rule apply; selecting a lens does not require finding a defect.

Risk and review shape:

- **Exempt:** documentation, comments, formatting, or typos with no runtime, build, test, or agent-execution effect.
- **Mechanical:** changes shown to preserve behavior, such as a symbol rename with unchanged callers or a file move with equivalent wiring. Run correctness and simplicity lenses. Classify tests, tooling, CI, and agent instructions by the behavior and authority they change, not their file type; deleting regression protection or changing failure propagation, publishing, or destructive actions can be high risk.
- **Standard:** localized behavior changes. Run one integrated final-diff pass covering correctness, simplicity, and test coverage; add only domain lenses matched by the diff.
- **High risk / substantial PR review:** high risk includes locking, erasure/quorum/heal, replication, multipart, RPC, lifecycle/tiering, persistence/fsync, IAM/KMS/auth, cryptography, on-disk/on-wire formats, and S3-visible semantics. Cover the applicable lenses from two perspectives: first trace the intended contract and implementation, then independently challenge its assumptions, failure paths, and evidence. Use separate reviewers when delegation is authorized and useful, or fresh sequential passes otherwise. Choose staffing and split lenses to fit the risk; a fixed reviewer count or duplicated review is not the goal.
- **Outbound client defaults:** what `TargetClient`, `PutObjectOptions`, or the remote SDK configuration sends to every replication or migration target is high risk for every target class even when the change fixes one. Follow the SOP in `docs/postmortems/2026-09-03-replication-checksum-default-regression.md`: run the outbound target matrix, document each new env knob in the same PR, and list verified and unverified target classes in the PR Impact section.

Available domain lenses are security, concurrency/durability, compatibility, and performance; select only those matched by the change.
