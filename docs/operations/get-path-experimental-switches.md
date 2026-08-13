# GET Path Experimental Performance Switches

This document records two experimental environment switches on the object GET
path. Both default to **off**, are read once at startup, and exist to support
staged performance work — they are not general tuning knobs. Until this
document existed they were referenced only by performance harness scripts,
which made them look like orphans during dead-code sweeps; they are kept
deliberately (rustfs/backlog#1832).

## RUSTFS_GET_SEEK_BUFFER_ENABLE

- Type: boolean (`true`/`false`), default `false`.
- Read once at startup in `rustfs/src/app/object_usecase.rs`.
- When enabled, small GET responses may be served through an in-memory seek
  buffer, providing seek support without re-reading the object. The seek-buffer
  code path is unit-test gated; whether the path stays or graduates to default
  is a post-1.0 maintainer decision — do not remove either the switch or the
  gated path as dead code.

## RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE

- Type: boolean (`true`/`false`), default `false`.
- Read once at startup in `rustfs/src/app/object_usecase.rs`.
- When enabled, GET responses attribute output-handoff stage timing in the GET
  stage metrics, at a small per-request bookkeeping cost. Used by the A/B
  performance runbooks (`scripts/run_get_codec_streaming_smoke.sh`,
  `scripts/test_get_1mib_abba_stage_metrics.sh`) to compare handoff cost
  between configurations.

## Operational guidance

Leave both switches unset in production. Enable them only when following a
performance runbook that asks for them, and unset them afterwards — both are
startup-latched, so changing a value requires a process restart to take
effect.
