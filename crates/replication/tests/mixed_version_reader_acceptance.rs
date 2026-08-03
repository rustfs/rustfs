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

use rustfs_replication::{
    MRF_V2_NAMESPACE, MRF_V2_VERSION, MrfOpKind, MrfV2Capabilities, MrfV2Error, MrfV2Readiness, decode_mrf_file,
};

fn mrf_header(payload: &[u8]) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + payload.len());
    data.extend_from_slice(&1_u16.to_le_bytes());
    data.extend_from_slice(&1_u16.to_le_bytes());
    data.extend_from_slice(payload);
    data
}

fn legacy_v1_delete_record() -> Vec<u8> {
    let mut payload = Vec::new();
    rmp::encode::write_array_len(&mut payload, 1).expect("legacy entry array should encode");
    rmp::encode::write_map_len(&mut payload, 4).expect("legacy entry map should encode");
    rmp::encode::write_str(&mut payload, "bucket").expect("bucket key should encode");
    rmp::encode::write_str(&mut payload, "mixed-version-bucket").expect("bucket value should encode");
    rmp::encode::write_str(&mut payload, "object").expect("object key should encode");
    rmp::encode::write_str(&mut payload, "prefix/").expect("object value should encode");
    rmp::encode::write_str(&mut payload, "retryCount").expect("retry key should encode");
    rmp::encode::write_i32(&mut payload, 0).expect("retry value should encode");
    rmp::encode::write_str(&mut payload, "size").expect("size key should encode");
    rmp::encode::write_i64(&mut payload, 0).expect("size value should encode");
    mrf_header(&payload)
}

#[test]
fn current_reader_accepts_legacy_v1_record_with_new_fields_absent() {
    let entries = decode_mrf_file(&legacy_v1_delete_record()).expect("current reader should accept legacy MRF data");

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].op, MrfOpKind::Object);
    assert_eq!(entries[0].bucket, "mixed-version-bucket");
    assert_eq!(entries[0].object, "prefix/");
    assert!(entries[0].target_arns.is_empty());
    assert!(!entries[0].force_delete);
    assert_eq!(entries[0].force_delete_id, None);
    assert!(!entries[0].force_delete_local_commit);
}

#[test]
fn dormant_v2_reader_accepts_stable_fixture_without_enabling_writer() {
    let fixture = [
        b'M', b'R', b'F', b'2', 2, 0, 2, 0, 2, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3,
    ];
    let readiness = MrfV2Readiness::dormant();
    assert!(!readiness.writer_enabled());
    let envelope = readiness
        .reader()
        .expect("dormant readiness should expose the reader")
        .read(MRF_V2_NAMESPACE, &fixture)
        .expect("v2 fixture should decode");

    assert_eq!(envelope.version(), MRF_V2_VERSION);
    assert_eq!(envelope.min_reader_version(), MRF_V2_VERSION);
    assert_eq!(envelope.capabilities(), MrfV2Capabilities::current());
    assert_eq!(envelope.payload(), &[1, 2, 3]);

    let mut rollback = fixture;
    rollback[8..10].copy_from_slice(&3_u16.to_le_bytes());
    assert_eq!(
        readiness
            .reader()
            .expect("dormant readiness should expose the reader")
            .read(MRF_V2_NAMESPACE, &rollback),
        Err(MrfV2Error::RollbackFenced {
            min_reader_version: 3,
            reader_version: 2,
        })
    );
}
