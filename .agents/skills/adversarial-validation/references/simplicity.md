# Simplicity Lens

- Compare the production diff with the smallest equivalent local edit. Fewer
  lines alone are not evidence; the replacement must preserve correctness,
  compatibility, readability, and real boundaries.
- Search the touched crate, domain owner, `crates/utils`, `crates/common`, and
  relevant dependencies for each new helper, constant, wrapper, or fixture.
- Reject forced reuse when normalization, error, deadline, or durability
  semantics differ.
- Require a concrete trigger for every new defensive branch. Keep boundary
  checks for disk/RPC/version data and checks immediately before destructive
  actions.
- Flag one-caller helpers only when they merely forward or split a short linear
  flow without adding domain naming, invariant isolation, or useful context.
- Ensure a replacement removes the superseded in-scope path or keeps one
  canonical core behind a documented compatibility adapter.
- Remove narration/change-history comments; preserve concise safety, lock,
  durability, and compatibility invariants.
- Treat tests, fixtures, generated code, and documentation separately from
  production growth. Do not optimize away meaningful regression coverage.

A finding must include a concrete smaller design, not a style preference.
