---
name: rustfs-logging-governance
description: Add or review RustFS `tracing` events with the repository field shape, level policy, privacy boundaries, and guardrails. Use when a change adds or edits a tracing macro/instrumentation site or the logging guardrail script.
---

# RustFS Logging Governance

Apply this skill only to changed logging sites; do not turn a local log edit into
a broad logging cleanup.

## Workflow

1. Read the changed function/module context and classify the site as lifecycle,
   request/hot path, fallback, external fetch, or summary.
2. Match neighboring structured events and reuse existing `EVENT_*`,
   `LOG_COMPONENT_*`, and `LOG_SUBSYSTEM_*` constants.
3. Put stable fields first (`event`, `component`, `subsystem`, `state`/`result`,
   then context) and a short label last.
4. Select the level by operational meaning:
   - `error`: behavior/security-affecting failure;
   - `warn`: degraded/fallback/operator-actionable state;
   - `info`: low-frequency lifecycle/mode change;
   - `debug`: targeted diagnostics;
   - `trace`: repetitive request/object/shard success paths.
5. Never log secrets, tokens, auth headers, credential payloads, raw
   attacker-controlled bodies, or merged config dumps. Error strings and
   `Debug` output are log surfaces too.
6. Prefer one aggregate summary over inventories or startup banners.
7. Run `./scripts/check_logging_guardrails.sh` and the checks selected by root
   `AGENTS.md`.

Read [logging-governance.md](references/logging-governance.md) only for a broad
logging audit, event-model migration, or guardrail expansion. Ordinary single-
site edits do not require the full workspace scope map.
