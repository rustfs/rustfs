[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS ECStore - Erasure Coding Storage

<p align="center">
  <strong>High-performance erasure coding storage engine for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS ECStore** provides erasure coding storage capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- Reed-Solomon erasure coding implementation
- Configurable redundancy levels (N+K schemes)
- Automatic data healing and reconstruction
- Multi-drive support with intelligent placement
- Parallel encoding/decoding for performance
- Efficient disk space utilization

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## 📈 Benchmarks

ECStore ships several Criterion benchmarks under [`crates/ecstore/benches/`](./benches/).

### Direct Chunk Path

Use the direct chunk benchmark to compare the current slice-forwarding path against the previous assembled-copy path:

```bash
cargo bench -p rustfs-ecstore --bench direct_chunk_benchmark
```

To run only the end-to-end ECStore range-read benchmark:

```bash
cargo bench -p rustfs-ecstore --bench direct_chunk_benchmark ecstore_get_object_chunks
```

To run the reconstructed multi-disk range-read benchmark:

```bash
cargo bench -p rustfs-ecstore --bench reconstructed_chunk_benchmark
```

### Saved Comparison Points

Latest local measurements on this branch:

- `direct_chunk_path/slice_forwarding/single_block_aligned`: about `477 ns`
- `direct_chunk_path/assembled_copy/single_block_aligned`: about `3.25 us`
- `direct_chunk_path/slice_forwarding/multi_block_unaligned`: about `963 ns`
- `direct_chunk_path/assembled_copy/multi_block_unaligned`: about `7.25 us`
- `ecstore_get_object_chunks/drain/multi_disk_range`: about `644-654 us`, throughput about `2.86-2.90 GiB/s`
- `reconstructed_chunk_path/drain/multi_disk_missing_shard`: about `1.292-1.304 ms`, throughput about `1.43-1.45 GiB/s`

These numbers are intended as branch-local reference points. Re-run the benchmark on your target machine before treating them as a regression baseline.

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.

```
Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with ❤️ by the RustFS Storage Team
</p>
