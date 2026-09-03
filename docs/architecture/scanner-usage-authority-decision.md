# Scanner Usage Authority Decision

**Use this when:** deciding whether quota admission depends on scanner data usage, removing scanner publication layers, or designing a scanner storage boundary.
**Source of truth:** [scanner-usage-publication.md](scanner-usage-publication.md), `crates/ecstore/src/bucket/quota/checker.rs`, and the scanner publication state under `crates/scanner/src/scanner/`.

## Decision

Date: 2026-09-03

RustFS keeps scanner data usage as authoritative for quota admission.

This selects option A from the backlog decision record: the scanner publication protocol remains necessary while quota admission consumes scanner usage. The cycle epoch, publication CAS, data-movement fence, tier-registry fence, observed snapshot layer, and persisted usage floor are retained and documented as protocol invariants rather than treated as removable compatibility clutter.

## Rationale

Quota is a write-path admission decision, so serving quota from best-effort scanner data would turn temporary scanner lag into under-enforcement. The current design therefore needs an availability story for authoritative usage instead of deleting the proof layers that make it authoritative.

The scanner usage floor provides that availability story. It is a lower bound used when a complete authoritative snapshot is not available, including cold startup, upgrade recovery, and incomplete-cycle repair. Observed snapshots remain useful for admin and observability, but they do not become quota authority.

## Consequences

#2214 is the hard design input for future usage-publication changes. A future proposal may still choose soft quota and MinIO-style best-effort usage, but that would be a product change with its own staged compatibility plan for persisted artifacts.

#2219 may design the scanner storage boundary against the current authoritative protocol. The interface must include the CAS key-value, cycle lock, usage-floor, observed-snapshot, and recovery-marker capabilities needed by [scanner-usage-publication.md](scanner-usage-publication.md); it must not hide those proof obligations behind a generic object-store trait.
