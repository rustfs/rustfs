// Copyright 2026 RustFS Team
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

use bytes::Bytes;
use rustfs_filemeta::{
    Error, FileInfo, FileInfoOpts, FileMeta, FileMetaShallowVersion, FileMetaVersion, FileMetaVersionHeader, InlineData,
    MetaDeleteMarker, VersionType, get_file_info, test_data::*,
};
use rustfs_utils::http::{SUFFIX_FREE_VERSION, insert_bytes};
use time::OffsetDateTime;
use uuid::Uuid;

fn read_options(data: bool, include_free_versions: bool, include_part_checksums: bool) -> FileInfoOpts {
    FileInfoOpts {
        data,
        include_free_versions,
        include_part_checksums,
    }
}

#[test]
fn historical_codec_preserves_signatures_and_fileinfo() {
    // Signatures were captured from the pre-optimization implementation at
    // 6681dd12e39fda867c54cc4e576ecf7f4257aec4, not from a same-code round trip.
    // MinIO fixtures pin RustFS recomputation, not MinIO's different hash algorithm.
    let fixtures = [
        (
            "minio-small",
            create_minio_small_object_xlmeta().expect("historical fixture"),
            vec![[63, 254, 115, 180]],
        ),
        (
            "minio-versioned",
            create_minio_versioned_object_xlmeta().expect("historical fixture"),
            vec![[179, 89, 130, 162], [42, 145, 112, 214], [91, 28, 96, 74]],
        ),
        (
            "minio-large",
            create_minio_large_object_xlmeta().expect("historical fixture"),
            vec![[95, 172, 233, 51]],
        ),
        (
            "legacy-v1",
            create_legacy_v1_object_xlmeta().expect("historical fixture"),
            vec![[130, 178, 104, 208]],
        ),
        (
            "issue-2288",
            create_issue_2288_legacy_xlmeta().expect("historical fixture"),
            vec![[233, 25, 148, 246]],
        ),
        (
            "issue-2265-object",
            create_issue_2265_legacy_meta_v2_object_xlmeta().expect("historical fixture"),
            vec![[64, 154, 203, 216]],
        ),
        (
            "issue-2265-config",
            create_issue_2265_legacy_meta_v2_config_xlmeta().expect("historical fixture"),
            vec![[83, 146, 54, 27]],
        ),
        (
            "issue-2434-pool",
            create_issue_2434_legacy_meta_v2_pool_xlmeta().expect("historical fixture"),
            vec![[91, 227, 222, 255]],
        ),
        (
            "pre-metadata-keys",
            create_pre_metadata_keys_xlmeta().expect("historical fixture"),
            vec![[5, 84, 29, 43]],
        ),
    ];
    for (name, raw, expected_signatures) in fixtures {
        let loaded = FileMeta::load(&raw).expect("load historical metadata");
        let signatures: Vec<_> = loaded
            .versions
            .iter()
            .map(|version| version.parse_version_meta().expect("historical version").get_signature())
            .collect();
        assert_eq!(signatures, expected_signatures, "signature changed for {name}");

        let mut selectors = vec![String::new(), Uuid::nil().to_string(), Uuid::from_u128(u128::MAX).to_string()];
        selectors.extend(
            loaded
                .versions
                .iter()
                .filter_map(|v| v.header.version_id.map(|id| id.to_string())),
        );
        for selector in selectors {
            for data in [false, true] {
                for free in [false, true] {
                    for checksums in [false, true] {
                        let direct = get_file_info(&raw, "bucket", "object", &selector, read_options(data, free, checksums));
                        let expected = if checksums {
                            loaded.into_fileinfo("bucket", "object", &selector, data, free, true)
                        } else {
                            loaded.into_fileinfo_without_part_checksums("bucket", "object", &selector, data, free)
                        };
                        assert_eq!(
                            direct, expected,
                            "{name}: selector={selector}, data={data}, free={free}, checksums={checksums}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn direct_fileinfo_reads_both_null_inline_keys() {
    let mut meta = FileMeta::new();
    let payload = Bytes::from_static(b"legacy-inline");
    let mut fi = FileInfo::new("object", 2, 1);
    fi.mod_time = Some(OffsetDateTime::from_unix_timestamp(1).expect("timestamp"));
    fi.data = Some(payload.clone());
    fi.set_inline_data();
    meta.add_version(fi).expect("add inline null version");

    for key in ["null".to_owned(), Uuid::nil().to_string()] {
        meta.data = InlineData::default();
        meta.data.replace(&key, payload.to_vec()).expect("set inline key");
        let raw = meta.marshal_msg().expect("encode null version");
        for selector in ["", "00000000-0000-0000-0000-000000000000"] {
            for data in [false, true] {
                let fi = get_file_info(&raw, "bucket", "object", selector, read_options(data, false, true))
                    .expect("read null version");
                assert_eq!(fi.version_id, None);
                assert!(fi.is_latest);
                assert_eq!(fi.num_versions, 1);
                assert_eq!(fi.data, data.then(|| payload.clone()), "key={key}, selector={selector}");
            }
        }
    }
}

#[test]
fn direct_fileinfo_preserves_free_version_selection() {
    let mut marker = MetaDeleteMarker {
        version_id: Some(Uuid::from_u128(1)),
        mod_time: Some(OffsetDateTime::from_unix_timestamp(1).expect("timestamp")),
        ..Default::default()
    };
    insert_bytes(&mut marker.meta_sys, SUFFIX_FREE_VERSION, Vec::new());
    let mut meta = FileMeta::new();
    meta.versions.push(
        FileMetaShallowVersion::try_from(FileMetaVersion {
            version_type: VersionType::Delete,
            delete_marker: Some(marker),
            ..Default::default()
        })
        .expect("free version"),
    );
    let raw = meta.marshal_msg().expect("encode free version");
    assert_eq!(
        get_file_info(&raw, "bucket", "object", "", read_options(false, false, true)),
        Err(Error::FileNotFound)
    );
    let free_info = get_file_info(&raw, "bucket", "object", "", read_options(false, true, true)).expect("include free version");
    assert!(free_info.tier_free_version());
    assert!(free_info.is_latest);
    assert_eq!(free_info.num_versions, 0);
    // Explicit addressing of the free record must retain the existing behavior.
    let explicit = get_file_info(
        &raw,
        "bucket",
        "object",
        &Uuid::from_u128(1).to_string(),
        read_options(false, false, true),
    )
    .expect("explicit free version");
    assert_eq!(explicit, free_info);

    let object = FileMeta::load(&create_minio_small_object_xlmeta().expect("object fixture")).expect("load object");
    meta.versions.extend(object.versions);
    let raw = meta.marshal_msg().expect("encode free and ordinary versions");
    for include_free in [false, true] {
        let fi = get_file_info(&raw, "bucket", "object", "", read_options(false, include_free, true))
            .expect("ordinary object remains visible");
        assert!(!fi.deleted);
        assert_eq!(fi.size, 19);
        assert_eq!(fi.num_versions, 1);
    }
}

#[test]
fn direct_fileinfo_preserves_empty_history_and_invalid_selectors() {
    let raw = create_empty_xlmeta().expect("empty history");
    for selector in [
        "",
        "00000000-0000-0000-0000-000000000000",
        "00000000-0000-0000-0000-000000000001",
    ] {
        let fi =
            get_file_info(&raw, "bucket", "object", selector, read_options(true, true, true)).expect("empty history tombstone");
        assert_eq!(
            fi.version_id,
            if selector.is_empty() {
                None
            } else {
                Some(Uuid::parse_str(selector).expect("UUID"))
            }
        );
        assert!(fi.deleted && fi.is_latest);
        assert_eq!(fi.mod_time, Some(OffsetDateTime::from_unix_timestamp(1).expect("timestamp")));
        assert_eq!(fi.num_versions, 0);
    }
    for raw in [raw.as_slice(), &[]] {
        assert!(matches!(
            get_file_info(raw, "bucket", "object", "invalid", read_options(false, false, false)),
            Err(Error::UuidParse(_))
        ));
    }
}

#[test]
fn large_version_histories_fit_initial_serialization_buffer() {
    for count in [4, 128] {
        for body_len in [255, 256, 65_535, 65_536] {
            let mut meta = FileMeta::new();
            meta.versions = vec![
                FileMetaShallowVersion {
                    // Use the widest flags and erasure fields; version_type remains a fixint.
                    header: FileMetaVersionHeader {
                        version_id: Some(Uuid::nil()),
                        mod_time: Some(OffsetDateTime::from_unix_timestamp(1).expect("timestamp")),
                        version_type: VersionType::Object,
                        flags: u8::MAX,
                        ec_n: u8::MAX,
                        ec_m: u8::MAX,
                        ..Default::default()
                    },
                    meta: vec![0; body_len],
                };
                count
            ];
            let encoded = meta.marshal_msg().expect("serialize large history");
            assert!(
                encoded.capacity() - encoded.len() <= count * 4 + 32,
                "unexpected buffer growth for {count} bodies of {body_len} bytes: length={}, capacity={}",
                encoded.len(),
                encoded.capacity()
            );
            assert_eq!(FileMeta::load(&encoded).expect("load serialized history").versions, meta.versions);
        }
    }
}
