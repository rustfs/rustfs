# Performance Lens

- For added clones/allocations on request/object/block paths, quantify copied
  data and frequency. Recommend borrowing, move, `Bytes`/`Arc`, `Cow`, or
  capacity reservation only for a concrete repeated cost.
- Route every new sync/flush through the durability-mode and bucket override
  gates; mode `none` must not pay the new fsync.
- Keep blocking filesystem/CPU work off async runtime threads, but do not split
  one small operation into many `spawn_blocking` round trips.
- Measure lock hold time across I/O and compare acquisition order for ABBA.
- Keep cleanup, extra stat/rename, and diagnostics out of the PUT commit critical
  section when they need not be there.
- Detect per-item serial I/O/RPC in batch APIs and accidental quadratic scans;
  use a gate or bounded concurrency when the concrete fan-out warrants it.
- Count buffer growth and byte copies in EC/bitrot paths; preserve pool gauge
  balance and avoid repeated metadata decode/fetch per object.
- Repetitive success logs stay at `trace`; metrics/instrumentation on hot paths
  require an existing gate.
- Claims of no impact on PUT/GET/commit/erasure paths need relevant benchmark or
  A/B evidence, especially for 4 KiB objects.
