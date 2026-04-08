# App Module Instructions

Applies to `rustfs/src/app/`.

## Orchestration First

- Keep application entrypoints and their primary flow files readable from top to bottom.
- A reader should be able to follow a GET/PUT happy path without jumping across a stack of one-off plumbing modules.
- Preserve real algorithm boundaries, but prefer local code when a helper only rearranges parameters for the next call.

## Helper and Module Extraction

- Do not add catch-all files such as `adapters.rs`, `types.rs`, or `context.rs` for single-caller plumbing.
- Extract a private helper only when it is reused or hides a genuinely non-trivial algorithm, validation, or protocol rule.
- If a helper has one caller and its body mostly clones fields, repacks inputs, or forwards to one callee, inline it back into the call site.
- Test-only request builders or setup helpers are acceptable under `#[cfg(test)]` when they remove repeated fixture code.

## Context and Data Carriers

- Avoid request/context/result structs that exist only to bypass argument-count linting.
- Keep shared context structs only when they carry stable semantics across multiple steps in the flow.
- When a struct is used by one step only and mainly mirrors another type's fields, prefer direct locals.

## Change Scope

- Keep refactors behavior-preserving unless the task explicitly requests a semantic change.
- Do not mix readability cleanup with unrelated flow rewrites in object, auth, quota, metadata, or notification paths.
- When removing an internal helper, keep or replace the behavior coverage at the boundary that still exists after the refactor.

## Suggested Validation

- Targeted tests for the touched flow under `rustfs/src/app/`
- Full gate before commit: `make pre-commit`
