# RustFS Connect Agent v1 — schema and fixture regression

This directory contains the Agent v1 JSON Schema files and the original protocol
fixtures. The accompanying scripts validate the two schemas that are present in
this repository snapshot and the diagnostic-scheduler vectors that reference
them. Fixture families without a checked-in schema are covered only by manifest
integrity checks; they are not claimed to have passed schema validation.

## Run locally

From the repository root:

```bash
python -m pip install -r protocol/agent/v1/requirements.txt
scripts/run_agent_v1_regression.sh
```

The regression suite:

1. Validates the Draft 2020-12 Schema documents and the existing Accept vectors.
2. Validates each Reject vector against the Schema referenced by its own base
   Accept vector.
3. Fails on unknown bases, duplicate IDs, malformed JSON Pointers, unsafe paths,
   stale allowlist entries, and unapproved Schema-accepted Reject vectors.
4. Enables `FormatChecker` so `format: date-time` is asserted.
5. Checks the job-envelope Schema limits against the corresponding Rust runtime
   constants in `rustfs/src/connect/diagnostics/job.rs`.
6. Verifies the nested and top-level SHA-256 fixture manifest chain.

## Runtime-only semantic checks

Exactly two existing Reject vectors are allowlisted because the corresponding
rules require runtime context rather than structural JSON Schema validation:

- `health-service.job.wrong-target`: organization, cluster, and device identity
  must match the enrolled Agent.
- `health-service.job.revoked-consent`: `consentExpiresAt` must not precede the
  job's `expireTime`.

These checks remain mandatory in the Rust runtime validator. Every allowlist
entry must have a matching Reject vector and an explicit Rust runtime test;
unused allowlist entries fail CI.

## Scope boundary

Only `job-envelope.schema.json` and `health-service-result.schema.json` are
checked against JSON Schema in this source snapshot. Other fixture families do
not have corresponding checked-in schemas here. The Python suite is not a
replacement for the Rust Workspace unit/integration tests.
