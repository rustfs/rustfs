[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Zip - Archive And Compression Primitives

<p align="center">
  <strong>High-performance compression and archiving for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Zip** provides archive and compression primitives for the [RustFS](https://rustfs.com) distributed object storage system. Today it is primarily used by RustFS archive extract flows to:

- identify archive/compression formats by extension
- stream tar and tar+compression inputs through async decoders
- provide small ZIP read/write helpers for local archive workflows

## Current Features

- A clearer type model with:
  - `CompressionCodec` for stream codecs
  - `ArchiveKind` for container families
  - `ArchiveFormat` for concrete archive/container combinations
- Async stream codecs for `gzip`, `bzip2`, `zlib`, `xz`, and `zstd`
- Tar archive iteration over async readers through `read_archive_entries()` / `extract_tar_entries()`
- Archive guardrails through `ArchiveLimits` for entry count, entry size, total unpacked size, and path length
- In-memory compression helpers for payload round-trip workflows
- Blocking ZIP create/extract helpers for local archive files
- ZIP helper metadata via `ZipEntry`, including:
  - `compression_method`
  - `archive_kind`
  - `format`
  - `unix_mode`
- ZIP helper options via `ZipWriteOptions`, including:
  - `compression_level`
  - `create_directory_entries`

## Compatibility

- `CompressionFormat` is retained as a compatibility layer for existing callers
- New code should prefer `ArchiveFormat`, `ArchiveKind`, and `CompressionCodec` when expressing archive semantics

## ZIP Helper Scope

The file-based ZIP helper APIs are best suited for:

- local archive import/export flows
- admin-side packaging helpers
- test fixtures and tooling

They are not intended to be a remote streaming ZIP access engine.

## Current Boundaries

- ZIP is supported via file-based helper APIs, not the tar-family async stream APIs
- Tar-family stream APIs are intended for `tar`, `tar.gz`, `tar.bz2`, `tar.xz`, `tar.zst`, and similar compressed tar flows
- Default archive guardrails are intentionally conservative and do not replace higher-level RustFS object-path validation
- This crate does not currently implement a general-purpose parallel archive engine
- Archive extraction safety policy remains the responsibility of the RustFS caller for object-store flows

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
