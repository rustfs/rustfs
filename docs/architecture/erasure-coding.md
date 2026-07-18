# Erasure Coding — Normative Algorithm & On-Disk Compatibility Contract

Status: normative. This document is the source of truth for how RustFS erasure-codes, stores, reads, reconstructs, and heals user data, and for the on-disk / on-wire compatibility contract that every future change must preserve. It governs the highest-risk code in the system: a regression here can silently corrupt or lose all user data, or make existing (and MinIO-migrated) objects permanently unreadable.

Erasure coding, quorum/heal, and metadata/on-disk formats are **High-risk** per [AGENTS.md](../../AGENTS.md) ("Risk tiers"). Any behavior-affecting change to code this document governs requires the full seven-role adversarial validation and, for anything touching decode or the on-disk format, a regression test against real on-disk and MinIO-migrated samples before merge.

This document describes the baseline (`main`) algorithm. Where the baseline has a known defect that a specific change corrects, that is called out inline; the *invariant* stated is always the correct rule the code must converge to, never the defect.

## How to use this document

- Before changing anything under `crates/ecstore/src/erasure/`, `crates/filemeta/`, `crates/ecstore/src/set_disk/`, the storage-class / layout code, or any decode boundary, read §12 (Invariants checklist) and §13 (Change procedure) first.
- This spec is normative for the algorithm and the compatibility contract. It **defers** to, and must not duplicate, these existing documents (cross-link, do not restate):
  - [minio-file-format-compat.md](minio-file-format-compat.md) — owns the `xl.meta` / `.metadata.bin` MinIO interop gap-analysis, the real-MinIO fixture proof, the version-support matrix, and the out-of-scope list.
  - [placement-repair-invariants.md](placement-repair-invariants.md) — owns object-to-**set** placement (which erasure set a key lands in), per-set readiness/lock quorum, scanner/heal admission, and the behavior-change gates.
  - [ecstore-layout-boundary.md](ecstore-layout-boundary.md) — owns the ecstore module ownership map, `FormatV3` set-ordering and disk-UUID-position invariants, and where the erasure engine physically lives.
  - [decommission-compatibility.md](decommission-compatibility.md) — owns moving encoded objects between pools (decommission/rebalance) and its persisted `PoolMeta` contract.
  - [../operations/tier-ilm-debugging.md](../operations/tier-ilm-debugging.md) — owns the ILM/tier runtime runbook, the dual internal-metadata-key table, the defensive binary-UUID read pattern, and the `xl.meta` inspection tooling (`dump_fileinfo` / `dump_versions`).
  - [AGENTS.md](../../AGENTS.md) "Cross-Cutting Domain Invariants" — dual internal metadata keys, defensive UUID reads, unversioned tier buckets.
  - [../../ARCHITECTURE.md](../../ARCHITECTURE.md) — crate roles (`ecstore`, `filemeta`, the `rustfs-erasure-codec` codec fork, `heal`, `scanner`).

## Table of contents

1. Model and industry standard
2. Geometry: pools, sets, drives, parity
3. Distribution: shard placement within a set
4. Encoding algorithm
5. Bitrot protection
6. On-disk format (`xl.meta`)
7. Write path and write quorum
8. Read path and read quorum
9. Healing
10. Quorum rules (summary)
11. Compatibility contract and decode tolerance
12. Invariants checklist (the frozen contract)
13. Change procedure and guardrails
14. References

---

## 1. Model and industry standard

RustFS stores every object as a Reed–Solomon erasure code across the drives of one erasure set. A set of `N` drives is split into `data_blocks` data shards and `parity_blocks` parity shards, `N = data_blocks + parity_blocks`. The code is **MDS** (Maximum Distance Separable): the object is reconstructable from **any** `data_blocks` of the `N` shards, and it tolerates the loss of up to `parity_blocks` drives per set.

Two codec backends exist, selected per object from its metadata (never a runtime toggle):

- **Modern backend** — Reed–Solomon over **GF(2⁸)** using `rustfs-erasure-codec` (a RustFS fork of `reed-solomon-erasure` v8, `Cargo.toml:279`), imported as `reed_solomon_erasure::galois_8::ReedSolomon` ([erasure.rs:20](../../crates/ecstore/src/erasure/coding/erasure.rs)). Vandermonde generator matrix; algorithm string `"rs-vandermonde"` ([object_api/mod.rs:52](../../crates/ecstore/src/object_api/mod.rs)). Field order is 256, so total shards per set is capped at 256 by the codec — well above the geometry cap of 16 (§2). Used for **all new writes**.
- **Legacy backend** — Reed–Solomon over **GF(2¹⁶)** using `reed-solomon-simd` v3.1 (`Cargo.toml:280`), imported at [erasure.rs:21](../../crates/ecstore/src/erasure/coding/erasure.rs). Used **only** to read and heal objects written in the older MinIO-lineage format.

Industry alignment (all confirmed in code): byte-oriented RS over GF(2⁸) with a Vandermonde matrix, 1 MiB erasure block, and HighwayHash-256 bitrot checksums with a π-derived key — the same family and defaults MinIO uses. This is what makes byte-level `xl.meta` interoperability with MinIO possible (see [minio-file-format-compat.md](minio-file-format-compat.md)).

Where the code lives: the erasure engine is owned by `crates/ecstore/src/erasure/` and is crate-private; the `xl.meta` model is owned by `crates/filemeta`. See [ecstore-layout-boundary.md](ecstore-layout-boundary.md).

---

## 2. Geometry: pools, sets, drives, parity

### 2.1 Pools → sets → drives

A deployment is a list of **pools**; each pool's drives are partitioned into equal-size **erasure sets**; each set has `N` **drives**. Set-size selection lives in [disks_layout.rs](../../crates/ecstore/src/layout/disks_layout.rs): the chosen set size is the largest member of `SET_SIZES` that divides the GCD of the pool sizes and is symmetric across the ellipsis patterns, preferring the fewest sets (`get_set_indexes`, `common_set_drive_count`, `possible_set_counts_with_symmetry`).

- **INVARIANT — set size.** `SET_SIZES = [2, 3, …, 16]` ([disks_layout.rs:24](../../crates/ecstore/src/layout/disks_layout.rs)); `is_valid_set_size` requires `2 ≤ N ≤ 16` ([disks_layout.rs:329](../../crates/ecstore/src/layout/disks_layout.rs)). Every erasure set has `N ∈ 2..=16` drives. This bounds `data_blocks + parity_blocks ≤ 16`, which downstream metadata validation may rely on.
- The `RUSTFS_ERASURE_SET_DRIVE_COUNT` override may pin the set size but only to a value that appears in the symmetric divisor set and still passes `is_valid_set_size` (≤ 16). It is a TUNABLE, not a way past the cap.
- Runtime geometry is read back from the on-disk format: `set_drive_count = format.erasure.sets[0].len()`. The disk-UUID position within `format.erasure.sets` must not change — see [ecstore-layout-boundary.md](ecstore-layout-boundary.md).

Which **set** a key lands in (object-to-set placement) is a separate hash, owned by [placement-repair-invariants.md](placement-repair-invariants.md) (`get_hashed_set_index`; V1 `crc_hash`, V2/V3 `sip_hash` seeded with the format ID). Do not conflate it with the intra-set distribution in §3.

### 2.2 Parity selection

Default parity by drive count — `default_parity_count(N)` ([storageclass.rs:24](../../crates/ecstore/src/config/storageclass.rs)):

| N | 1 | 2–3 | 4–5 | 6–7 | ≥8 |
|---|---|-----|-----|-----|----|
| default parity | 0 | 1 | 2 | 3 | 4 |

Two storage classes: `STANDARD` (SC) and `REDUCED_REDUNDANCY` (RRS) ([storageclass.rs](../../crates/ecstore/src/config/storageclass.rs)), configured as `"EC:<parity>"` via the `standard` / `rrs` config keys or the `RUSTFS_STORAGE_CLASS_STANDARD` / `RUSTFS_STORAGE_CLASS_RRS` env overrides. Absent config falls back to `default_parity_count` (SC) and `1`, or `0` on a single drive (RRS).

- **INVARIANT — parity bounds.** `validate_parity_inner` requires `parity ≤ N/2` for both classes and `SC parity ≥ RRS parity` when both are non-zero ([storageclass.rs](../../crates/ecstore/src/config/storageclass.rs), `validate_parity` / `validate_parity_inner`). Parity `0` is permitted (single-drive / capacity setups); there is no non-zero minimum.
- **INVARIANT — per-pool validity.** Each pool's resolved parity must be valid for **that pool's own drive count**. A heterogeneous deployment (pools of different widths) must resolve parity per pool; applying one pool's parity to a narrower pool can drive `data_blocks = N − parity` to `0` and make encoding impossible.
  - Baseline defect: `main` computes `common_parity_drives` from the **first** pool only and applies it to every pool ([store/init.rs](../../crates/ecstore/src/store/init.rs), `ec_drives_no_config` at [store/init_format.rs](../../crates/ecstore/src/store/init_format.rs)); this is issue #4801 (a smaller later pool panics with `TooFewDataShards`). The correct rule is per-pool resolution.

Per-write layout (the numbers that go into `xl.meta`), from the storage class or `default_parity_count`, with `opts.max_parity` forcing `N/2` for internal writes ([set_disk/ops/object.rs](../../crates/ecstore/src/set_disk/ops/object.rs)):

```
parity_drives = storage_class_parity(x-amz-storage-class) or set default_parity_count
if opts.max_parity: parity_drives = N / 2
data_drives  = N − parity_drives
write_quorum = data_drives ; if data_drives == parity_drives: write_quorum += 1
```

- **INVARIANT — layout arithmetic.** `data_blocks = N − parity`; `read_quorum = data_blocks`; `write_quorum = data_blocks`, bumped to `data_blocks + 1` iff `data_blocks == parity_blocks` (so a symmetric split cannot commit on a bare data-quorum). See [core/io_primitives.rs](../../crates/ecstore/src/set_disk/core/io_primitives.rs) (`default_read_quorum`, `default_write_quorum`).

---

## 3. Distribution: shard placement within a set

Within a set, the `N` shards of an object are assigned to drives by a **distribution vector** that is a permutation of `1..=N`, derived from the object key — `FileInfo::new` ([fileinfo.rs:277](../../crates/filemeta/src/fileinfo.rs)):

```
N       = data_blocks + parity_blocks
key_crc = CRC32/ISO-HDLC( "bucket/object" bytes )
start   = key_crc % N
distribution[i-1] = 1 + ((start + i) % N)   for i in 1..=N   // a cyclic rotation of 1..=N
```

- **INVARIANT — distribution is a permutation of `1..=N`.** `is_valid_distribution` requires exactly `N` entries, each in `1..=N`, no duplicates ([fileinfo.rs:254](../../crates/filemeta/src/fileinfo.rs)). Values of `0` or `> N` are used as `distribution[k] − 1` slot indices and would underflow / index out of bounds; the shuffle helpers additionally `checked_sub(1)` and bounds-filter defensively ([set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs)).
- **INVARIANT — key-only, version-independent.** The distribution depends only on the object key (`[bucket, object].join("/")`), so all versions of a key share one distribution. Changing the derivation would misplace every existing object's shards. See also the placement-algorithm-preservation gate in [placement-repair-invariants.md](placement-repair-invariants.md).
- `erasure.index` on a per-disk `FileInfo` is that disk's 1-based canonical shard slot. On write, disk `k` is placed at slot `distribution[k] − 1` and the shard written there records `erasure.index = slot + 1` ([set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs), `shuffle_disks_and_parts_metadata`). On read/heal, placement is re-derived by matching `distribution[k] == parts_metadata[k].erasure.index`, with a mod-time fallback when too many indices are inconsistent.

---

## 4. Encoding algorithm

### 4.1 Block size and shard sizing

- **INVARIANT — block size.** New objects use `BLOCK_SIZE_V2 = 1 MiB` ([object_api/mod.rs:53](../../crates/ecstore/src/object_api/mod.rs)); it is stored per version in `ErasureInfo.block_size` and must be read back from metadata, never assumed.
- **INVARIANT — shard-size formulas (frozen for on-disk compatibility).** Two formulas exist and must both be preserved:
  - Modern ([erasure.rs:456](../../crates/ecstore/src/erasure/coding/erasure.rs)): `calc_shard_size(block_size, data_shards) = block_size.div_ceil(data_shards)`.
  - Legacy ([erasure.rs:30](../../crates/ecstore/src/erasure/coding/erasure.rs)): `calc_shard_size_legacy = (block_size.div_ceil(data_shards) + 1) & !1` — round the ceiling **up to the nearest even number**, byte-identical to MinIO's even-padding and to `filemeta`'s own `calc_shard_size` ([fileinfo.rs:136](../../crates/filemeta/src/fileinfo.rs)).
  - The runtime codec picks the formula from `uses_legacy` ([erasure.rs:820](../../crates/ecstore/src/erasure/coding/erasure.rs)); the metadata layer `ErasureInfo::shard_size` always uses the even-padded form. Modern reads/writes drive off `Erasure::shard_size`.
- Whole-file logical shard size — `shard_file_size(total_length)` ([erasure.rs:829](../../crates/ecstore/src/erasure/coding/erasure.rs)): `0` for empty, pass-through for negative, else `full_blocks * shard_size() + shard_size_fn(last_block_size, data_shards)`. This is the pre-bitrot size; the on-disk file is larger by the per-block hash bytes (§5).

### 4.2 Per-block encode

`Erasure::encode_data(data)` ([erasure.rs:508](../../crates/ecstore/src/erasure/coding/erasure.rs)):

1. `per_shard_size = shard_size_fn(data.len(), data_shards)`; empty ⇒ emit `N` empty shards.
2. Copy `data` into a buffer and **zero-pad** the tail to `per_shard_size * N`.
3. Split into `N` equal `per_shard_size` chunks (first `data_shards` are data, rest are parity).
4. If `parity_shards > 0`, RS-encode in place to fill the parity chunks (legacy or modern encoder).
5. Emit `N` shard byte-slices (zero-copy views into the one buffer).

- **INVARIANT — zero-padding of the final block.** The last (short) block is padded to `per_shard_size` before encoding; this is what makes `shard_file_size` exactly reversible on read. Two allocation-optimized variants (`encode_data_owned`, `encode_data_bytes_mut`) produce byte-identical output (asserted by tests in [erasure.rs](../../crates/ecstore/src/erasure/coding/erasure.rs)).

### 4.3 Streaming pipeline and fan-out

`Erasure::encode` / `encode_batched` ([encode.rs](../../crates/ecstore/src/erasure/coding/encode.rs)) read the source in `block_size` chunks on a producer task, encode each block, and stream the `N` shards of each block over a bounded channel to a consumer that fans them out through `MultiWriter` to the `N` shard writers. `block_size == 0` is rejected up front (`InvalidInput`). In-flight memory is bounded by the channel depth (default budget 32 MiB). A clean EOF stops the loop; the final partial block is padded per §4.2.

- **INVARIANT — write-quorum enforcement during encode.** `MultiWriter` writes shard `i` to writer `i`, drops any stalled/failed/short writer (sets it to `None`), and after each block requires `nil_count ≥ write_quorum`, else fails with a reduced-write-quorum error ([encode.rs](../../crates/ecstore/src/erasure/coding/encode.rs)). Shard index → writer index → on-disk position is fixed.

---

## 5. Bitrot protection

Each shard file is self-verifying against silent disk corruption.

- **INVARIANT — hash algorithm.** The production bitrot hash is `HighwayHash256S` (streaming HighwayHash-256, 32-byte digest), the default of `HashAlgorithm` ([hash.rs:42](../../crates/utils/src/hash.rs)); `ErasureInfo::get_checksum_info` defaults an unspecified part to `HighwayHash256S` ([fileinfo.rs:141](../../crates/filemeta/src/fileinfo.rs)). Legacy files recorded as `HighwayHash256S` are verified with the legacy fixed-key variant `HighwayHash256SLegacy` (key `[3,4,2,1]`), selected on read when `fi.uses_legacy_checksum` ([set_disk/read.rs](../../crates/ecstore/src/set_disk/read.rs)). The modern key is the π-derived `MAGIC_HIGHWAY_HASH256_KEY` ([hash.rs](../../crates/utils/src/hash.rs)).
- **INVARIANT — on-disk shard layout is interleaved `[hash][data]` per block.** `BitrotWriter::write` prepends `hash_algo.hash_encode(block)` before each block, written in one vectored write ([bitrot.rs:307](../../crates/ecstore/src/erasure/coding/bitrot.rs)). On-disk shard file size — `bitrot_shard_file_size(size, shard_size, algo)` ([bitrot.rs:436](../../crates/ecstore/src/erasure/coding/bitrot.rs)): for the two streaming Highway variants `= size.div_ceil(shard_size) * 32 + size` (one 32-byte hash per block); for any other algorithm (whole-file bitrot) `= size`.
- **INVARIANT — verify before use.** `BitrotReader` reads `[hash][data]` in one pass, recomputes the hash, and returns `InvalidData "bitrot hash mismatch"` on mismatch; the data is handed to the caller **only after** verification passes. A short/truncated shard returns `UnexpectedEof` even under `skip_verify` ([bitrot.rs](../../crates/ecstore/src/erasure/coding/bitrot.rs)).
- Only the streaming interleaved layout is written/verified; it is self-consistent only for `HighwayHash256S` / `HighwayHash256SLegacy`. The default resolving to `HighwayHash256S` is load-bearing (backlog#959, documented at [bitrot.rs](../../crates/ecstore/src/erasure/coding/bitrot.rs)).

---

## 6. On-disk format (`xl.meta`)

This section is the load-bearing compatibility contract for stored metadata. It is byte-compatible with MinIO's `xl.meta`; interop proof, the fixture corpus, and the out-of-scope list are owned by [minio-file-format-compat.md](minio-file-format-compat.md) — cite it, do not re-derive interop claims.

### 6.1 Container

Produced by `FileMeta::marshal_msg` ([codec.rs](../../crates/filemeta/src/filemeta/codec.rs)), constants in [filemeta.rs](../../crates/filemeta/src/filemeta.rs):

| Offset | Bytes | Content |
|--------|-------|---------|
| 0 | 4 | Magic `XL_FILE_HEADER = "XL2 "` |
| 4 | 2 | `major = 1`, little-endian u16 |
| 6 | 2 | `minor = 3`, little-endian u16 |
| 8 | 5 | msgpack bin32 marker `0xc6` + big-endian u32 length of the meta blob |
| 13 | N | meta blob (msgpack; the versions array) |
| 13+N | 5 | `0xce` (msgpack uint32) + big-endian u32 = `xxh64(meta, seed=0)` truncated to u32 |
| 13+N+5 | … | inline data blob, appended verbatim |

- **INVARIANT.** Magic `"XL2 "`; LE u16 major/minor; the bin32-with-BE-length meta framing; the trailing `0xce`+BE-u32 xxh64 CRC with `XXHASH_SEED = 0`. CRC mismatch is fatal. `is_indexed_meta` gates inline/indexed layout on `major == 1 && minor ≥ 3`.
- Meta blob starts with three msgpack ints: `header_ver` (≤ 3), `meta_ver` (≤ 3), `versions_len`; then per version **two msgpack `bin` blobs**: the marshaled `FileMetaVersionHeader` and the opaque marshaled `FileMetaVersion` body (`FileMetaShallowVersion { header, meta }`, [version.rs](../../crates/filemeta/src/filemeta/version.rs)). The body is parsed lazily.
- Constants: `XL_FILE_VERSION_MAJOR = 1`, `XL_FILE_VERSION_MINOR = 3`, `XL_HEADER_VERSION = 3`, `XL_META_VERSION = 3` ([filemeta.rs](../../crates/filemeta/src/filemeta.rs)).

### 6.2 Shallow header (`FileMetaVersionHeader`)

Fields: `version_id`, `mod_time`, `signature: [u8;4]`, `version_type`, `flags: u8`, `ec_n: u8`, `ec_m: u8` ([version.rs](../../crates/filemeta/src/filemeta/version.rs)). Three wire versions dispatched by array length:

| header_ver | array len | fields (in order) |
|------------|-----------|-------------------|
| 1 | 4 | version_id, mod_time, type, flags |
| 2 | 5 | version_id, mod_time, signature, type, flags |
| 3 (current) | 7 | version_id, mod_time, signature, type, flags, `ec_n`, `ec_m` |

- **INVARIANT.** Array length discriminates the header format; a reader must branch on `header_ver`. Writes always emit v3 (len 7). `ec_m = data`, `ec_n = parity` (header order is `ec_n` then `ec_m`) — a mirror of the geometry for quorum decisions without parsing the body.
- **INVARIANT — flags.** `FreeVersion = 1<<0`, `UsesDataDir = 1<<1`, `InlineData = 1<<2` ([version.rs](../../crates/filemeta/src/filemeta/version.rs)).
- The v3 header intentionally keeps a null version id as `Some(nil)` (null-version disambiguation via mod_time), unlike the body decoders which fold nil → None.
- `signature` is a RustFS-internal content hash for divergence/heal detection; it is recomputed on write and never compared byte-wise against MinIO.

### 6.3 Version body

`VersionType` — **INVARIANT (numeric values)**: `Invalid = 0`, `Object = 1`, `Delete = 2`, `Legacy = 3`. The body wrapper `FileMetaVersion` is a msgpack map with **INVARIANT keys** `Type`, `V1Obj`, `V2Obj`, `DelObj`, `v` (write generation); a present-but-nil body is `0xc0`; unknown keys are skipped for forward-compat.

**`MetaObject` (V2Obj)** — msgpack map, keys (**INVARIANT**): `ID`, `DDir`, `EcAlgo`, `EcM`, `EcN`, `EcBSize`, `EcIndex`, `EcDist`, `CSumAlgo`, `PartNums`, `PartETags`, `PartSizes`, `PartASizes`, `PartIdx`, `Size`, `MTime`, `MetaSys`, `MetaUsr` ([version.rs](../../crates/filemeta/src/filemeta/version.rs)). Notes:
- `ID` / `DDir` are **16 raw UUID bytes**; nil ⇒ `None` on decode, `None` ⇒ 16 zero bytes on write.
- `MTime` is **unix-nanos** sint; `UNIX_EPOCH` ⇒ `None`, and `None` is omitted on write so it never round-trips to `Some(epoch)`.
- `EcDist` is an **array** of per-shard slot values (not a bin blob). V2Obj does **not** store per-part bitrot checksums (only legacy V1 does).
- `PartETags` / `PartASizes` / `MetaSys` / `MetaUsr` are written as msgpack nil when empty; **a reader must treat nil and empty identically**. `PartIdx` is omitted entirely when empty.
- Part arrays are **parallel and index-aligned**: `PartNums` / `PartSizes` / `PartASizes` must be equal length (mismatch ⇒ `FileCorrupt`, because indexing would panic or miscompute Content-Length/Range); `PartETags` / `PartIdx` are soft-guarded (applied only if length matches, empty index ⇒ None).
- **INVARIANT — negative `part.actual_size` is a valid sentinel** for "compressed, actual size unknown" ([fileinfo.rs:52](../../crates/filemeta/src/fileinfo.rs)); it is carried verbatim and must not be rejected on decode (see §11).

**`MetaDeleteMarker` (DelObj)** — msgpack map, always 3 keys `ID`, `MTime`, `MetaSys` ([version.rs](../../crates/filemeta/src/filemeta/version.rs)); `MetaSys` is always written even when empty. Decode folds nil `ID` ⇒ None and epoch `MTime` ⇒ None, and skips unknown keys.

**`MetaObjectV1` (V1Obj / legacy `xl.json`-derived)** — msgpack map, keys `Version`, `Format` (`"xl"`), `Stat`, `Erasure`, `Meta`, `Parts`, `VersionID` (a UUID **string**), `DataDir` (string). This is the **only** schema that stores per-part bitrot checksums in the body (`Erasure.Checksums`). Legacy timestamps use msgpack time ext (type 5 legacy / type -1). Required to read MinIO / legacy objects.

### 6.4 `ErasureInfo` and geometry on disk

`ErasureInfo` ([fileinfo.rs:117](../../crates/filemeta/src/fileinfo.rs)): `algorithm` (`"rs-vandermonde"` for RS), `data_blocks` (=`EcM`), `parity_blocks` (=`EcN`), `block_size` (=`EcBSize`), `index` (=`EcIndex`), `distribution: Vec<usize>` (=`EcDist`), `checksums: Vec<ChecksumInfo>` (empty for V2Obj). On write, `From<FileInfo>` hardcodes `ReedSolomon` + `HighwayHash` algo enums.

### 6.5 Internal metadata keys (dual prefix)

- **INVARIANT — dual prefixes.** Internal keys carry both `x-rustfs-internal-<suffix>` and `x-minio-internal-<suffix>` ([metadata_compat.rs](../../crates/utils/src/http/metadata_compat.rs)). Write path writes **both** (`insert_str` / `insert_bytes`); read path **prefers RustFS, falls back to MinIO** (`get_str` / `get_bytes`), case-insensitive on the prefix. Both prefixes must stay recognized on read and emitted on write — this is what makes MinIO-migrated keys round-trip without rewrite. See [AGENTS.md](../../AGENTS.md) Cross-Cutting Domain Invariants and the runbook table in [../operations/tier-ilm-debugging.md](../operations/tier-ilm-debugging.md).
- **INVARIANT — suffix strings** (partial list, all exact): `inline-data`, `compression`, `actual-size`, `crc`, `transition-status`, `transitioned-object`, `transitioned-versionID`, `transition-tier`, `free-version`, `purgestatus`, the replication suffixes, `tier-free-versionID`, `tier-free-marker`, `data-mov`, `healing` ([metadata_compat.rs](../../crates/utils/src/http/metadata_compat.rs)). These are the second half of on-disk keys; changing one orphans existing metadata.
- **INVARIANT — transitioned-versionID encoding.** Stored as **16 raw UUID bytes** in `meta_sys[transitioned-versionID]` (RustFS-native); read defensively as `Uuid::from_slice(...).ok().filter(!nil)` so absent / empty / nil / unparseable all mean "no tier version" (§11). MinIO stores this value as a UUID **string**; a compliant reader must accept the string form as well (or tolerate it as None) — never fail the object read on it.

### 6.6 Inline data

Small objects store their payload inline after the container CRC ([filemeta_inline.rs](../../crates/filemeta/src/filemeta_inline.rs)): 1 version byte (`INLINE_DATA_VER = 1`) then a msgpack map of `version-key → bin`. **INVARIANT — the map key** is the version-id string, `"null"` (`NULL_VERSION_ID`) for the null/None version, else the lowercase hyphenated UUID. Presence is signalled by both `meta_sys[inline-data]` and the header `InlineData` flag, which must agree. The inline threshold is `should_inline` ([storageclass.rs](../../crates/ecstore/src/config/storageclass.rs)): inline if `shard_size ≤ inline_block/8` for versioned buckets, else `≤ inline_block`; `DEFAULT_INLINE_BLOCK = 128 KiB`.

---

## 7. Write path and write quorum

`put_object` and multipart resolve the write layout (§2.2), encode (§4), write shards with bitrot (§5), then commit atomically.

- Encode-time gates: writable disks `< write_quorum` ⇒ `ErasureWriteQuorum`; committed shards `< write_quorum` after encode ⇒ error ([set_disk/ops/object.rs](../../crates/ecstore/src/set_disk/ops/object.rs)).
- **INVARIANT — atomic commit with rollback.** Commit is `rename_data` (per-disk temp → final) fanned across all disks ([core/io_primitives.rs](../../crates/ecstore/src/set_disk/core/io_primitives.rs)). If write quorum is not met (`reduce_write_quorum_errs`), every successful disk is undone (`delete_version{undo_write:true}`) and the write fails. A write that misses quorum is rolled back, never left partially committed.
- On success, `data_dir` is the value voted by ≥ write_quorum disks (`reduce_common_data_dir`); a `classify_rename_convergence` result drives post-commit heal.
- The write layout is centralized (a `WriteLayout` / `resolve_write_layout` abstraction resolves per-pool parity, storage class, and `max_parity`); the baseline computes it inline in the write path.

---

## 8. Read path and read quorum

- **INVARIANT — read quorum = `data_blocks`.** `object_quorum_from_meta` returns `(read_quorum = data_blocks, write_quorum)` ([set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs)); `parity_blocks = common_parity(...)` is the parity value held by the most disks that still reaches its own read quorum. When `default_parity_count == 0`, read = write = all shards.
- Authoritative FileInfo selection — `find_file_info_in_quorum` ([set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs)) groups valid metas by a content-identity SHA-256 (`file_info_quorum_hash`) that hashes size/flags/mod_time/transition/version_id/data_dir/parts and, for real objects, data/parity/distribution — **excluding replication-status keys** so replication noise never splits quorum. A meta counts only if its mod_time equals the common mod_time (or etag matches when mod_time is absent). The winning hash must reach quorum, else `ErasureReadQuorum`. Latest-version reads may escalate to write_quorum to avoid resurrecting a partially-overwritten version.
- **INVARIANT — decode needs ≥ `data_blocks` shards.** The stripe reader requires `available_shards ≥ data_shards`; below that the read fails closed with a read-quorum error (never silent truncation) ([set_disk/read.rs](../../crates/ecstore/src/set_disk/read.rs), [set_disk/shard_source.rs](../../crates/ecstore/src/set_disk/shard_source.rs)). Before any `block_size` / `data_shards` division, `has_valid_dimensions()` must hold (`block_size > 0 && data_shards > 0`) or the read fails instead of dividing by zero ([erasure.rs:809](../../crates/ecstore/src/erasure/coding/erasure.rs)).
- If `available ≥ data_blocks` but some shards are missing, the read is served **and** a background read-repair heal is enqueued.
- **INVARIANT — cross-stripe read verification.** When a data shard is missing and `available > data_blocks`, reconstruction regenerates parity and compares it to the surviving parity; a mismatch is `InvalidData "inconsistent read source shards"` (backlog#832), catching corruption that passed per-shard bitrot but disagrees across the stripe ([erasure.rs:711](../../crates/ecstore/src/erasure/coding/erasure.rs)).

---

## 9. Healing

Version-aware heal ([set_disk/ops/heal.rs](../../crates/ecstore/src/set_disk/ops/heal.rs)) reconstructs missing/corrupt shards and regenerates missing `xl.meta` from quorum. Key guards:

- **INVARIANT — reconstructability.** Heal refuses when `meta_to_heal_count > parity_blocks` (relaxed only if a quorum etag exists) or when any part loses more than `parity_blocks` shards.
- **INVARIANT — geometry match.** `latest_meta.erasure.distribution.len()` must equal the online-disk, outdated-disk, and parts-metadata counts, else heal refuses ("backend disks manually modified"). A real object missing `data_dir` is `FileCorrupt`.
- **Data-safety guard (backlog#920).** If data shards survive on ≥ `data_blocks` disks, regenerate the missing `xl.meta` from a valid FileInfo and re-drive heal rather than dangling-delete; torn writes (< `data_blocks`) fall through to dangling-delete handling.
- Healed shards are written to the outdated disks, each recording `erasure.index = slot + 1`, then `rename_data` to final. Heal admission / scanner budget is owned by [placement-repair-invariants.md](placement-repair-invariants.md).

---

## 10. Quorum rules (summary)

| Operation | Quorum | Source |
|-----------|--------|--------|
| Read (payload) | `data_blocks = N − parity` | [set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs) |
| Decode (shards needed) | `≥ data_blocks` | [set_disk/read.rs](../../crates/ecstore/src/set_disk/read.rs) |
| Write (payload) | `data_blocks`, `+1` iff `data == parity` | [set_disk/core/io_primitives.rs](../../crates/ecstore/src/set_disk/core/io_primitives.rs) |
| Delete marker (write + metadata vote) | `N/2 + 1` (majority) | [set_disk/ops/object.rs](../../crates/ecstore/src/set_disk/ops/object.rs), [set_disk/metadata.rs](../../crates/ecstore/src/set_disk/metadata.rs) |
| `opts.max_parity` internal writes | parity `= N/2` | [set_disk/ops/object.rs](../../crates/ecstore/src/set_disk/ops/object.rs) |

- **INVARIANT — delete markers vote by majority, not data-quorum.** A delete marker (or a version whose parity reads 0) uses `N/2 + 1`, both on the write path and in metadata voting.

---

## 11. Compatibility contract and decode tolerance

RustFS is a **read-forward-compatible consumer**: it writes the current format and reads older RustFS formats and MinIO-migrated data.

- **Version anchors — accept-older, reject-newer.** Writes emit `XL_META_VERSION = 3`. The reader accepts container `major == 1` with any `minor`, `header_ver ≤ 3`, and `meta_ver ≤ 3` (1/2/3), and rejects only newer ([codec.rs](../../crates/filemeta/src/filemeta/codec.rs)). Legacy `meta_ver = 2` objects are supported and normalized on rewrite. `XL_META_VERSION` / `BUCKET_METADATA_*` are compatibility anchors — bumping any requires a read path for the prior value plus a migration story (see [minio-file-format-compat.md](minio-file-format-compat.md)).
- **MinIO interop is fixture-proven** against a real MinIO corpus; migration is one-way (MinIO → RustFS). Reverse round-trip (a live MinIO re-reading RustFS drives) and formats MinIO SNSD never wrote (CORS / public-access-block / bucket-ACL, transitioned `xl.meta`) are explicitly out of scope — owned by [minio-file-format-compat.md](minio-file-format-compat.md).

**Decode-tolerance invariants (be liberal in what you accept on decode).** These are the contract this document newly codifies. Metadata read from disk or a peer is untrusted input, but decode must not reject shapes that legitimate older / foreign writers produce. Validation may be added, but only if it never rejects data the current or any prior RustSF/MinIO writer can legitimately produce:

- **Nil UUID ⇒ None.** `version_id`, `data_dir`, `transitioned-versionID` read as `Uuid::from_slice(...).ok().filter(!nil)` — absent, empty, and nil all mean "no value", never `Uuid::nil()` ([version.rs](../../crates/filemeta/src/filemeta/version.rs); [AGENTS.md](../../AGENTS.md) Cross-Cutting Domain Invariants).
- **Epoch mod_time ⇒ None**, and `None` is omitted on write.
- **Skip unknown msgpack fields** for forward-compat (Go `dc.Skip()` parity) at every decoder.
- **Hard-guard only length-critical arrays** (`PartNums` / `PartSizes` / `PartASizes` must match), **soft-guard recomputable ones** (`PartETags` / `PartIdx` applied only if length matches; empty ⇒ default). All-empty `PartETags` must equal absent.
- **Negative `part.actual_size` is a valid compressed-unknown sentinel** and must be tolerated on decode (the read path's `get_actual_size` already relies on it). Do **not** reject `actual_size < 0`.
- **Malformed `transitioned-versionID` ⇒ None (or recovered from the MinIO string form), never a fatal read error.** A slightly-corrupt or foreign tier id must not make an otherwise-readable object (or free-version record) unreadable.
- **Dual-key read fallback** (RustFS prefix, then MinIO prefix).
- **Container/header/meta versions**: accept `≤` current, reject only `>`.

Two related hardening exceptions that are legitimate (fail-closed is correct there): a length mismatch between required parallel part arrays is `FileCorrupt` (indexing would corrupt returned data), and a CRC / bitrot mismatch is fatal (the bytes are provably wrong). Everything else on the decode path must degrade to a tolerant default, not an error.

---

## 12. Invariants checklist (the frozen contract)

Do not change any of the following without a format-version bump, a read path for the old value, a migration story, and a real-sample compatibility test (§13):

Geometry & math
- Set size `N ∈ 2..=16`; `N = data_blocks + parity_blocks`.
- `parity ≤ N/2`; `STANDARD parity ≥ RRS parity`; parity resolved **per pool** for its own `N`.
- `read_quorum = data_blocks`; `write_quorum = data_blocks` (`+1` iff `data == parity`); delete-marker quorum `N/2 + 1`.
- Decode requires `≥ data_blocks` shards; below that, fail closed. Writes missing quorum roll back.

Algorithm
- Modern RS over GF(2⁸) (`rs-vandermonde`) for new writes; legacy GF(2¹⁶) for old files, selected by `uses_legacy_checksum`.
- `block_size = 1 MiB` (`BLOCK_SIZE_V2`), stored per version.
- Shard-size formulas: modern `div_ceil`; legacy `(div_ceil + 1) & !1`. Final block zero-padded before encode.
- Bitrot `HighwayHash256S` (legacy key variant for old files); interleaved `[hash][data]` per block; `bitrot_shard_file_size = ceil(size/shard_size)*32 + size`; verify before use.

On-disk format
- Container: `"XL2 "`, LE major/minor `1`/`3`, bin32(BE-len) meta, `0xce`+BE-u32 xxh64(seed 0) CRC, trailing inline blob.
- Header array length ⇒ version (4/5/7); v3 order `id, mtime, sig, type, flags, ec_n, ec_m`; flags `FreeVersion|UsesDataDir|InlineData`.
- `VersionType` {0,1,2,3}; wrapper keys `Type/V1Obj/V2Obj/DelObj/v`; V2Obj key set (§6.3); `EcDist` an array; nil == empty for the four collection fields; `PartIdx` omitted when empty.
- UUIDs 16 raw bytes (nil ⇄ None); mod_time unix-nanos (epoch ⇄ None); negative `actual_size` sentinel.
- Dual internal prefixes and exact suffix strings; inline map keyed by `"null"` / version-UUID.

Distribution
- `distribution` is a permutation of `1..=N` from `CRC32(bucket/object) % N` rotation; key-only. Object-to-set placement hash is separate (owned by [placement-repair-invariants.md](placement-repair-invariants.md)).

Decode tolerance
- All of §11.

---

## 13. Change procedure and guardrails

- **Risk tier.** All of the above is High-risk ([AGENTS.md](../../AGENTS.md)). Any behavior-affecting change requires the full seven-role adversarial validation.
- **Adding an on-disk field** must be additive: new msgpack key or a `minor`/`meta_ver` bump with a read path for the old value; keep decoders skipping unknown keys; write both internal-key prefixes; never repurpose or reorder existing keys or header array positions.
- **Never make a decode boundary stricter** than what §11 allows without (a) proving no legitimate older-RustFS or MinIO-migrated shape is rejected, and (b) a regression test against real on-disk and MinIO fixtures. New validation belongs at the trust boundary and must fail *open to a tolerant default*, not closed to `FileCorrupt`, for anything recoverable. (Concretely: rejecting a negative `actual_size`, or hard-failing a non-16-byte `transitioned-versionID`, breaks existing data — see §11.)
- **Codec construction.** The baseline exposes panicking `Erasure::new` / `new_with_options` (the codec's shard-count validation surfaces as an `.expect` panic), plus `has_valid_dimensions()` as the read-path preflight ([erasure.rs](../../crates/ecstore/src/erasure/coding/erasure.rs)). Production paths that build a codec from on-disk geometry must guard with `has_valid_dimensions()` first; a fallible constructor is preferred over panicking in any path reachable from untrusted metadata.
- **Guardrail scripts** (part of `make pre-commit` / `make pre-pr`):
  - [check_architecture_migration_rules.sh](../../scripts/check_architecture_migration_rules.sh) keeps the erasure engine crate-private and under its owner module, and keeps erasure-cache / `GLOBAL_IS_ERASURE*` access behind ecstore helpers.
  - [check_doc_paths.sh](../../scripts/check_doc_paths.sh) validates that every repo path this document cites exists — keep citations to real paths.
- **Tooling.** Inspect on-disk metadata with `dump_fileinfo` / `dump_versions` per [../operations/tier-ilm-debugging.md](../operations/tier-ilm-debugging.md) rather than guessing at bytes.

---

## 14. References

- Reed–Solomon codes; MDS property and GF(2⁸) byte-oriented coding — the standard basis for `rs-vandermonde` (Vandermonde generator matrix over GF(2⁸)).
- `rustfs-erasure-codec` (RustFS fork of `reed-solomon-erasure`, GF(2⁸)) and `reed-solomon-simd` (GF(2¹⁶)) — `Cargo.toml:279`–`280`.
- HighwayHash-256 — the bitrot checksum family; π-derived default key.
- MinIO `xl.meta` v1.3 format lineage — RustFS is byte-compatible for read + one-way migration; see [minio-file-format-compat.md](minio-file-format-compat.md) for the fixture-proven matrix and scope.
- Related invariants: [placement-repair-invariants.md](placement-repair-invariants.md), [ecstore-layout-boundary.md](ecstore-layout-boundary.md), [decommission-compatibility.md](decommission-compatibility.md), [../operations/tier-ilm-debugging.md](../operations/tier-ilm-debugging.md), and [AGENTS.md](../../AGENTS.md) Cross-Cutting Domain Invariants.
