// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Structured version-graph marshal -> load roundtrip property (backlog#1151
//! sec-10).
//!
//! xl.meta roundtrip coverage was a fixed example test plus byte-level no-panic
//! fuzz properties; nothing generated STRUCTURED version graphs, so a lossy
//! encoding bug in the version header, ordering, delete-marker, or
//! dual-internal-metadata-key handling would only be caught if the fixed
//! example happened to hit it. These properties generate multi-version graphs
//! (objects + delete markers, distinct and colliding mod_times, user metadata,
//! and the AGENTS.md dual `x-rustfs-internal-*` / `x-minio-internal-*` keys)
//! and assert `FileMeta::marshal_msg` -> `FileMeta::load` preserves them
//! exactly: version count, order, headers, and fully decoded per-version
//! content.

use proptest::collection::vec;
use proptest::prelude::*;
use rustfs_filemeta::{ChecksumAlgo, ErasureAlgo, FileMeta, FileMetaVersion, MetaDeleteMarker, MetaObject, VersionType};
use rustfs_utils::http::insert_bytes;
use std::collections::HashMap;
use time::OffsetDateTime;
use uuid::Uuid;

/// Internal metadata suffixes to write under BOTH internal prefixes (the
/// cross-cutting dual-key invariant). Realistic suffixes used by production
/// code paths.
const SYS_SUFFIXES: &[&str] = &["replication-status", "transition-status", "tier-name"];

#[derive(Debug, Clone)]
struct GenVersion {
    is_delete_marker: bool,
    version_id: u128,
    /// Seconds offset added to the base timestamp. Small range on purpose so
    /// some versions share a mod_time and the tie-break ordering is exercised.
    mod_offset: i64,
    size: i64,
    user_meta: Vec<(String, String)>,
    sys_suffix_idx: usize,
    sys_value: Vec<u8>,
}

fn gen_version_strategy() -> impl Strategy<Value = GenVersion> {
    (
        proptest::bool::weighted(0.3),
        1u128..u128::MAX,
        0i64..8,
        1i64..(1 << 30),
        vec(("[a-z]{1,8}", "[a-zA-Z0-9 ]{0,16}"), 0..3),
        0usize..SYS_SUFFIXES.len(),
        vec(any::<u8>(), 1..24),
    )
        .prop_map(
            |(is_delete_marker, version_id, mod_offset, size, user_meta, sys_suffix_idx, sys_value)| GenVersion {
                is_delete_marker,
                version_id,
                mod_offset,
                size,
                user_meta,
                sys_suffix_idx,
                sys_value,
            },
        )
}

const BASE_TS: i64 = 1_700_000_000;

fn build_version(g: &GenVersion) -> FileMetaVersion {
    let mod_time = OffsetDateTime::from_unix_timestamp(BASE_TS + g.mod_offset).expect("valid timestamp");
    let mut meta_sys = HashMap::new();
    // Dual internal metadata keys: insert_bytes writes BOTH the
    // x-rustfs-internal-* and x-minio-internal-* forms.
    insert_bytes(&mut meta_sys, SYS_SUFFIXES[g.sys_suffix_idx], g.sys_value.clone());

    if g.is_delete_marker {
        FileMetaVersion {
            version_type: VersionType::Delete,
            delete_marker: Some(MetaDeleteMarker {
                version_id: Some(Uuid::from_u128(g.version_id)),
                mod_time: Some(mod_time),
                meta_sys,
            }),
            write_version: 0,
            ..Default::default()
        }
    } else {
        FileMetaVersion {
            version_type: VersionType::Object,
            object: Some(MetaObject {
                version_id: Some(Uuid::from_u128(g.version_id)),
                data_dir: Some(Uuid::from_u128(g.version_id.wrapping_add(1).max(1))),
                erasure_algorithm: ErasureAlgo::ReedSolomon,
                erasure_m: 2,
                erasure_n: 2,
                erasure_block_size: 1 << 20,
                erasure_index: 1,
                erasure_dist: vec![1, 2, 3, 4],
                bitrot_checksum_algo: ChecksumAlgo::HighwayHash,
                part_numbers: vec![1],
                part_etags: vec![],
                part_sizes: vec![g.size as usize],
                part_actual_sizes: vec![g.size],
                part_indices: vec![],
                size: g.size,
                mod_time: Some(mod_time),
                meta_sys,
                meta_user: g.user_meta.iter().cloned().collect(),
            }),
            write_version: 0,
            ..Default::default()
        }
    }
}

proptest! {
    /// A generated version graph survives marshal -> load exactly: count,
    /// order, headers, and fully decoded version content (objects, delete
    /// markers, user metadata, and both internal metadata keys).
    #[test]
    fn version_graph_marshal_load_roundtrip(gens in vec(gen_version_strategy(), 1..8)) {
        let mut fm = FileMeta::new();
        for g in &gens {
            fm.add_version_filemata(build_version(g)).expect("add generated version");
        }

        let encoded = fm.marshal_msg().expect("marshal generated version graph");
        let loaded = FileMeta::load(&encoded).expect("load marshaled version graph");

        // Count and metadata version survive.
        prop_assert_eq!(loaded.versions.len(), fm.versions.len(), "version count must survive the roundtrip");
        prop_assert_eq!(loaded.meta_ver, fm.meta_ver, "meta_ver must survive the roundtrip");

        // Version ORDER survives exactly: add_version_filemata sorted the
        // in-memory graph (mod_time desc with documented tie-breaks, delete
        // markers interleaved); load must reproduce that same sequence, not
        // re-derive a different one.
        let pre: Vec<_> = fm.versions.iter().map(|v| (v.header.version_id, v.header.version_type.clone())).collect();
        let post: Vec<_> = loaded.versions.iter().map(|v| (v.header.version_id, v.header.version_type.clone())).collect();
        prop_assert_eq!(pre, post, "version (id, type) sequence must survive the roundtrip");

        for (orig, back) in fm.versions.iter().zip(loaded.versions.iter()) {
            // Full header equality (mod_time, flags, signature, ec fields).
            prop_assert_eq!(&orig.header, &back.header, "version header must survive the roundtrip");

            // Fully decoded version content equality.
            let orig_v = orig.parse_version_meta().expect("decode original version");
            let back_v = back.parse_version_meta().expect("decode roundtripped version");
            prop_assert_eq!(&orig_v, &back_v, "decoded version content must survive the roundtrip");

            // Delete-marker semantics preserved.
            if back.header.version_type == VersionType::Delete {
                prop_assert!(back_v.delete_marker.is_some(), "delete marker payload must survive");
                prop_assert!(back_v.object.is_none(), "a delete marker must not grow an object payload");
            } else {
                prop_assert!(back_v.object.is_some(), "object payload must survive");
            }

            // Dual internal metadata keys: BOTH prefixed forms survive with the
            // same value (losing either breaks MinIO interop; losing both loses
            // replication/tier state).
            let meta_sys = match (&back_v.object, &back_v.delete_marker) {
                (Some(o), _) => &o.meta_sys,
                (_, Some(d)) => &d.meta_sys,
                _ => unreachable!("version decoded above"),
            };
            let orig_sys = match (&orig_v.object, &orig_v.delete_marker) {
                (Some(o), _) => &o.meta_sys,
                (_, Some(d)) => &d.meta_sys,
                _ => unreachable!("version decoded above"),
            };
            for (k, v) in orig_sys {
                prop_assert_eq!(
                    meta_sys.get(k).map(|b| b.as_slice()),
                    Some(v.as_slice()),
                    "internal metadata key {} must survive the roundtrip with an identical value",
                    k
                );
            }
        }
    }

    /// Idempotence: a second marshal of the loaded graph is byte-identical.
    /// Catches encoders that mutate or reorder state on the way out (a lossy
    /// first pass shows up as a divergent second pass).
    #[test]
    fn version_graph_marshal_is_idempotent(gens in vec(gen_version_strategy(), 1..6)) {
        let mut fm = FileMeta::new();
        for g in &gens {
            fm.add_version_filemata(build_version(g)).expect("add generated version");
        }

        let first = fm.marshal_msg().expect("first marshal");
        let loaded = FileMeta::load(&first).expect("load first marshal");
        let second = loaded.marshal_msg().expect("second marshal");
        prop_assert_eq!(first, second, "marshal(load(marshal(x))) must be byte-identical to marshal(x)");
    }
}
