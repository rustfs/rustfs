---
name: adversarial-validation
description: Review a final RustFS diff adversarially when the user requests adversarial review, the root AGENTS.md classifies the change as high risk, or a substantial PR is being reviewed. Do not use for ordinary questions, diagnosis, planning, status, documentation-only work, or routine low-risk implementation.
---

# RustFS Adversarial Validation

Use the risk tier and review shape defined in the root `AGENTS.md`. This skill
routes a review to RustFS-specific probes without loading unrelated domains.

## Select Lenses

Read only the references required by the diff:

| Lens | When to read |
|---|---|
| [Correctness](references/correctness.md) | Every non-exempt adversarial review |
| [Simplicity](references/simplicity.md) | Mechanical/standard changes and production growth |
| [Test coverage](references/test-coverage.md) | Behavior or test changes |
| [Security](references/security.md) | Authn/authz, IAM, RPC trust, paths, secrets, parsing, browser, encryption |
| [Concurrency/durability](references/concurrency-durability.md) | Async shared state, locks, storage commit, cancellation, persisted queues |
| [Compatibility](references/compatibility.md) | S3 surface, MinIO interop, metadata, wire/disk formats, mixed versions |
| [Performance](references/performance.md) | Request/object hot paths, allocation, blocking work, fsync, fan-out |

Do not read all references as a precaution. A path name alone is insufficient;
the changed behavior must touch the lens's domain.

For a dedicated security audit or advisory analysis, use
`security-advisory-lessons` instead of loading it automatically during every
adversarial review.

## Review Protocol

1. Freeze the exact final diff/head and list the selected lenses.
2. Run the review shape required by root `AGENTS.md`.
3. For each selected lens, either report a concrete finding or a null verdict
   naming the attacks performed.
4. A finding needs `file:line`, a triggering input/state/interleaving, the wrong
   outcome, and a focused fix or missing regression check.
5. Fix or rebut every finding with code-path, test, or invariant evidence.
6. After a non-trivial edit, rerun only lenses affected by that edit against the
   new exact diff.

Do not turn a null verdict into a long checklist. Record concise evidence that
the relevant failure classes were attacked.
