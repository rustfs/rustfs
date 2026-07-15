# RustFS — CLAUDE.md

S3-compatible object store in Rust, derived from MinIO. Erasure-coded,
multi-pool, supports ILM tiering/lifecycle.

Agent rules (change style, verification, PR conventions, scoped per-crate
guidance) live in [AGENTS.md](AGENTS.md) — follow it. This file only adds
what Claude Code needs on top: commands and pointers.

## Commands

```bash
cargo build --release --bin rustfs   # production binary
cargo check -p <crate>               # fast type-check one crate
cargo test -p <crate>                # test one crate
cargo fmt --all                      # format (required before PR)
make pre-commit                      # fast gate: fmt + arch checks + quick-check (NO clippy/tests)
make pre-pr                          # full pre-PR gate: fmt + arch checks + clippy + tests
make build-docker BUILD_OS=ubuntu22.04
```

> **Docker build note**: `buildx build` without `--load` keeps the image in
> the buildx cache only — `docker run` will use a stale local image. The
> Makefile already includes `--load`; if you suspect a stale binary, add
> `--no-cache` to the `buildx build` invocation inside
> `.config/make/build-docker.mak`.

## Where to look (do not duplicate here)

- Crate membership: `Cargo.toml` `[workspace].members`
- Architecture, layering, crate map: [ARCHITECTURE.md](ARCHITECTURE.md)
- Migration guardrails & readiness contracts: [docs/architecture/](docs/architecture/README.md)
- CI gates: `.github/workflows/ci.yml` (source of truth; never copy its steps into docs)
- Test-layer taxonomy, per-layer entry commands, serial/nextest rules, flake
  policy: [docs/testing/README.md](docs/testing/README.md)
- Tier/ILM transition debugging (xl.meta inspection, versionId tracing):
  [docs/operations/tier-ilm-debugging.md](docs/operations/tier-ilm-debugging.md)

## Domain conventions worth knowing up front

Repo-wide domain invariants (dual internal metadata keys, defensive UUID
reads, unversioned tier buckets) live in [AGENTS.md](AGENTS.md) under
"Cross-Cutting Domain Invariants" — read them before touching metadata or
tiering code.
