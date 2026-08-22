# Test-Coverage Lens

- For every behavior claim, name the focused test/check that fails if the
  changed hunk is reverted. If none is practical, require the reason and
  residual risk.
- Confirm tests exercise the real production path and assert returned values,
  exact bytes, stored state, or the specific error variant—not only success,
  `is_err()`, or no panic.
- For new flags/modes, verify each branch and ask which test fails if the branch
  is inverted.
- For new error propagation, inject the failure and assert the caller observes
  it; mentally replacing `?`/`return Err` with success must break a test.
- Streaming GET tests assert the complete body and length under degraded reads.
- Disk/wire-format tests use pinned foreign/legacy fixtures; same-code
  round-trips are insufficient for compatibility.
- Concurrency tests use readiness polling, isolate global state, and avoid fixed
  sleeps or unrealistically short timeouts. Use nextest groups when process-level
  serialization is required.
- Internal metadata tests assert both RustFS and MinIO keys, not only read-back
  through a helper that prefers one key.
- Boundary companions are distinct coverage: `n == max` vs `max + 1`, and
  absent vs empty vs nil UUID.
- A focused test proves only the targets/features it builds. Add compilation or
  Clippy only for uncovered changed targets.
