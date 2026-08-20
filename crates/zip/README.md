[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Zip - Archive Format Detection And Stream Decoding

<p align="center">
  <strong>Archive format detection and async stream decoders for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Zip** provides the archive primitives used by the [RustFS](https://rustfs.com) archive extract flow:

- identify a compression format from an archive extension
- wrap an async reader in the matching stream decoder
- carry the shared default archive guardrails

## Current Features

- `CompressionFormat::from_extension()` for extension-based format detection, including tar-family suffixes such as `tgz`, `tbz2`, `txz`, and `tzst`
- `CompressionFormat::get_decoder()` for async stream decoding of `gzip`, `bzip2`, `zlib`, `xz`, and `zstd`, plus a pass-through reader for plain `tar`
- `ArchiveLimits` with the default entry count, entry size, total unpacked size, and path length guardrails

## Current Boundaries

- ZIP has no stream decoder: `get_decoder()` rejects `CompressionFormat::Zip`, because ZIP needs central-directory semantics that a forward-only stream cannot provide
- This crate detects formats and hands back decoders; archive iteration, entry writing, and extraction to disk belong to the caller
- `ArchiveLimits` carries the values only; enforcement and the resulting protocol error belong to the caller
- Archive extraction safety policy remains the responsibility of the RustFS caller for object-store flows

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
