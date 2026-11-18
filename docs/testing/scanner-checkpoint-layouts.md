# Checkpoint layout cost experiment

[Backlog #3271 C](https://github.com/rustfs/backlog/issues/3271) asks whether a bounded segment chain is sufficient before introducing a paged index. [The runner](../../tools/bench/scanner_checkpoint.py) compares the bytes three layouts actually read and write for exactly the same recovered coverage. This is an on-demand Python 3.10+ experiment with private temporary files. For measurements of the production `DataUsageCache` MessagePack codec and revision-aware save path, use the separate [scanner cache cost profile](scanner-cache-cost.md). Neither those numbers nor an estimated Rust struct size are inputs to this experiment.

```sh
python3 -B -m unittest discover -s tools/bench -p test_scanner_checkpoint.py -v
python3 tools/bench/scanner_checkpoint.py > checkpoint-layouts.jsonl
jq -s '.[1:] | group_by(.layout)[] | sort_by(.history_rows) |
  {layout: .[0].layout, ordinary_write_ratio: (.[1].ordinary_write_bytes_p50 / .[0].ordinary_write_bytes_p50),
   large_history_io: .[1].total_encoded_io_bytes, large_history_recovery: .[1].cold}' checkpoint-layouts.jsonl
```

Run outside shared benchmark loads when comparing times. Check the runner's exit status before using the output. Success produces one provenance line (runner and delta-trace SHA-256, platform, interpreter, workload settings) and six result lines. Archive these with `git rev-parse HEAD` outside the source tree. Any recovery mismatch is an error; the process exit status does not depend on wall times or favorable performance ratios.

## Input and oracle

There is one writer, one synthetic scope, and one ordered input ledger. Each row contains a key, version count, delete-marker count, and logical bytes; a row is a fixture observation, not an S3 object. Seed 4,096 or 40,960 such rows, then commit the **same** 64 pages of 32 new rows, one page per revision, including at the larger scale. A writer gets private copies of its inputs. After each checkpoint a separate reader reopens the visible root and checks the exact revision and every page, row, and field against the ledger prefix. Aggregate agreement alone cannot pass. At the end, three new interpreter processes each recover the whole ledger from files alone. Their revision and ordered-state digest must match the expected input. All six cells also reopen their final state through the full-layout reader.

The shared codec is compact, uncompressed JSON. Each layout uses one copy of each reachable blob, SHA-256 content references, and a short root replaced by rename after writing the blobs. The full control keeps its input in memory and rewrites the whole snapshot. Segments append one immutable delta and rewrite the bounded list of references in the root. Before accepting a 33rd segment the writer blocks, rereads the base and 32 segments, and writes their merged base. Revision 64 therefore measures recovery at maximum replay depth, not just after compaction. The paged layout appends an immutable leaf and copies only its index path, using fanout eight. Its root stores one reference and a height, not a list of all leaf digests. Distinct keys always append; there is no overwrite, invalidation of an earlier page, or lazy-recovery fast path in this trace.

## What the counters include

- `seed_writes` and `checkpoint_writes` count encoded bytes written to files by kind, including every replacement root, segment reference, index node and compacted base. `ordinary_write_bytes_p50` excludes seed and compaction commits; `compactions` reports each excluded commit's entire write and read cost. Read bytes for the writer's index lookups and compactions appear in `checkpoint_read_bytes`.
- `cold` reports files opened, encoded bytes read, and elapsed nanoseconds from reading the root through digest verification, decode and full materialization. Child startup and the final input-ledger comparison are outside the timer. **Process-cold only:** in-process verification has already reopened all files; the OS cache is not dropped.
- `retained_bytes` is the sum of final file lengths before export. `peak_checkpoint_bytes` includes staging the next root and coexisting old/new generations. `checkpoint_unlinks` counts superseded blob removals; root replacements are separate. This immediate reclamation is valid only for the fixture's single writer with no concurrent readers or retained old roots. Filename, allocation-block, filesystem metadata, GC with reader pins and device-level I/O costs are not measured.
- `export_read_bytes` and `export_write_bytes` include restoring the final state in full-layout form and verifying it. The full control already has that form, so it only pays the verification read. `total_encoded_io_bytes` sums seed writes, all checkpoint reads and writes, **one** full recovery, and the export/verification I/O. The other two timing repetitions and out-of-band per-commit oracle reads are excluded. This export is to the surrogate full codec, not to a mixed-version RustFS reader.

## Decision boundary

Reject a candidate with any oracle mismatch. With equal deltas, compare ordinary median write bytes at 10× vs 1× history (target **<2×**) and require lower total encoded I/O than the full control, including compaction and export. Inspect the largest compaction commit and the cold-recovery file count as well as the medians: a bounded number of segments does not bound the bytes in a compacted base, and many small index reads can cost more than one sequential read. If segments meet the real checkpoint, recovery, and rollback budgets, prefer them over a tree. If the full control already meets the target deployment's budgets, retain it.

Passing this experiment only justifies a closer test with real scanner inputs and serialization. A format switch still needs the issue's correctness, stale-writer/upgrade/rollback and durability gates, plus isolated target-scale HDD tests (at least three ABBA groups, cold and warm caches, mixed foreground load, total physical I/O, and foreground p99 regression ≤10%). The fixture neither calls the scanner nor fsyncs, exercises EC/quorum, drives ILM/Heal, publishes usage, or ACKs dirty work. Failure before root rename, a missing leaf, and corrupted data have local negative controls; these are not power-loss, failover or mixed-version evidence.

## Rust scanner codec probe: full vs segments

```sh
RUST_MIN_STACK=4194304 cargo nextest run -p rustfs-scanner --lib --no-tests fail --success-output immediate \
  -E 'test(=data_usage_define::tests::checkpoint_layouts::checkpoint_segments_replay_scanner_observations)'
```

The [Rust probe](../../crates/scanner/src/data_usage_define/tests/checkpoint_layouts.rs) runs `scan_data_folder` against 24 versioned, one-byte `xl.meta` fixtures, with budgets of four objects and four raw entries per round. The walk must produce partial checkpoints with a usable raw enumeration page index, then complete with exactly 24 objects, 24 versions and 24 logical bytes. The probe captures that trace **once**. For the byte comparison it attaches 32 or 320 synthetic untouched entries below one sibling; those entries did not come from the walker. A segment contains all observed active entries, links, removed keys and all checkpoint info, rather than an idealized diff of changed fields. The exact same encoded segments feed both history sizes. Knowing the frozen subtree is a preprocessing assumption: the experiment neither derives this partition from production invalidation events nor feeds the synthetic history to the walker.

Each revision must recover every persisted field and entry of the expected cache through the full codec and through replay; full base, compaction and export use `DataUsageCache::marshal_msg` / `unmarshal`. Segment bodies use the same MessagePack serializers for cache info and entries. A test-only in-memory root holds first and last revision; the single writer can address consecutive blobs by revision. A cap of **four** forces compaction in a short real scan trace (the Python experiment separately exercises 32). Before admitting the fifth segment the probe decodes the previous chain and encodes a new full base. Final export must reload through the existing full-cache reader.

The two `CHECKPOINT_CODEC_COST` rows report actual single-copy encoded buffer lengths. `segment_plus_root_bytes` excludes the compacted base; `segment_total_write_bytes` adds seed, every root, every segment, and every replacement base. `compaction_read_bytes`, `final_segment_recovery_read_bytes`, and `full_export_bytes` expose costs that a small ordinary append does not remove. Compare paired revisions and total bytes, including reads and export, before pursuing storage benchmarks. No backend save, second copy, CAS, fsync, fresh-process latency, or production dirty-subtree detection is measured by this probe.
