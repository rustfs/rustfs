[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS S3Select Query - SQL Query Engine

<p align="center">
  <strong>Apache DataFusion-powered SQL query engine for RustFS S3 Select implementation</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS S3Select Query** provides Apache DataFusion-powered SQL query engine capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- Apache DataFusion integration for high-performance queries
- Vectorized processing with SIMD acceleration
- Parallel query execution across multiple threads
- Cost-based query optimization
- Support for complex SQL operations (joins, subqueries, window functions)
- Multiple data format support (Parquet, CSV, JSON, Arrow)

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## Resource limits

JSON LINES schema inference reads the first 1,000 nonblank records, preserving Arrow's type inference and source field order. It buffers at most 4 MiB of decompressed input. Reaching that byte limit cuts the sample at the last complete record, exactly as a shorter object would; it is an error only when no complete record fits. CSV inference reads only the first non-comment record (the header when enabled), with the same 4 MiB prefix limit. Quoting, escaping, comments, header modes and record delimiters still apply.

CSV schemas have at most 1,024 columns. JSON LINES schemas have at most 1,024 distinct fields across all nested objects in the inference sample. SQL expressions have at most 256 KiB and 1,024 projection expressions; wildcard expansion must also fit 1,024 output columns. Inputs exceeding these limits now return explicit errors, including inputs older versions attempted to process without these bounds. Parquet schema inference is unchanged apart from reading string and binary columns as view types; Parquet decoding and downstream execution use one-row batches on a single partition, with batch coalescing disabled to bound dictionary expansion during casts. JSON DOCUMENT input handling is unchanged.

CSV decoding uses at most 256 rows per batch, reduced further for smaller configured query memory limits. At 1,024 columns, Arrow's two-offset-per-cell reservation is at most 4 MiB on 64-bit systems and at most one sixteenth of the configured query budget (with a minimum one-row batch). Schema prefix storage and temporary inference work are reserved against the existing query memory pool. The configured memory limit can reject a sample before the 4 MiB hard prefix limit.

SQL parsing, schema inference for CSV and JSON LINES, and SQL planning run on blocking workers after query admission. Queued and running workers retain that admission until they finish, even if the HTTP request is canceled or its query deadline expires. Request cancellation stops waiting immediately; it cannot forcibly interrupt synchronous third-party code or admit replacement CPU work before that code exits.

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
