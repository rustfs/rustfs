# Fixed zstd dependency adapter

**Use this when:** maintaining the dependency bridge for consumers that still require zstd 0.13.

This private package satisfies the version constraints of Avro, Arrow IPC, Parquet, and Kafka. Its version is a resolver compatibility identifier. Its entire library is a re-export of the exact registry `zstd 0.14.3` implementation, including that implementation's `zstd-safe` types. The original zstd 0.13 implementation and zstd-safe 7.x are removed from the resolved graph. Every old feature and the default feature set is forwarded.

The fixed implementation closes RUSTSEC-2026-0391, RUSTSEC-2026-0398, and RUSTSEC-2026-0403. A successful advisory scan alone is insufficient evidence: cargo-deny skips local path packages. The dependency-graph test therefore requires this adapter's sole runtime dependency to be the exact fixed registry implementation and rejects vulnerable registry codec versions. Security tests exercise abandoned stream contexts, malformed legacy frames, and seekable offsets past EOF. Consumer tests retain zstandard support in all four upstream formats.

Run `cargo nextest run --locked -p zstd@0.13.4` for the focused tests. The seekable feature is enabled by a test-only dependency to cover the optional vulnerable API without enabling it in production.

Remove this package and the root Cargo patch when all consumers accept the fixed zstd release line directly. The cleanup marker is registered in the architecture compatibility register.
