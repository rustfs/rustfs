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

use super::{ListPartsInfo, dto, list_parts_output};
use crate::app::storage_api::bucket_usecase::s3_api::bucket::{RUSTFS_OWNER_DISPLAY_NAME, RUSTFS_OWNER_ID};
use crate::storage_api::server::http::access::test_support::PartInfo;
use rustfs_gateway::{Limits, MetaView, OperationCodec, ResponseBody, TargetKind, WireRequest};
use rustfs_s3_types::S3ErrorCode;

fn page() -> ListPartsInfo {
    ListPartsInfo {
        bucket: "bucket-a".into(),
        object: "obj-a".into(),
        upload_id: "upload-a".into(),
        max_parts: 1000,
        ..Default::default()
    }
}

fn part(number: usize) -> PartInfo {
    PartInfo {
        part_num: number,
        etag: Some(format!("etag-{number}")),
        ..Default::default()
    }
}

fn encode_xml(output: dto::ListPartsOutput) -> String {
    let request = http::Request::get("http://localhost/bucket-a/obj-a?uploadId=upload-a")
        .header(http::header::HOST, "localhost")
        .body(bytes::Bytes::new())
        .unwrap();
    let wire = WireRequest::accept(request, &Limits::default()).unwrap();
    let view = MetaView::of(&wire, TargetKind::Object).unwrap().with_rustfs_response_layout();
    let encoded = dto::ListParts::encode(output, &view, 200).expect("ListParts should encode");
    assert_eq!(encoded.status, http::StatusCode::OK);
    let ResponseBody::Complete(bytes) = encoded.body else {
        panic!("ListParts must return an XML document");
    };
    String::from_utf8(bytes).expect("ListParts XML must be UTF-8")
}

#[test]
fn test_list_parts_output_maps_parts_and_owner() {
    let output = list_parts_output(ListPartsInfo {
        storage_class: "STANDARD".into(),
        part_number_marker: 1,
        next_part_number_marker: Some(2),
        is_truncated: true,
        parts: vec![PartInfo { size: 11, ..part(1) }],
        ..page()
    })
    .unwrap();

    assert_eq!(output.bucket.as_str(), "bucket-a");
    assert_eq!(output.key.as_str(), "obj-a");
    assert_eq!(output.upload_id, "upload-a");
    assert_eq!(output.storage_class.as_ref().map(|v| v.as_str()), Some("STANDARD"));
    assert_eq!(output.part_number_marker.as_deref(), Some("1"));
    assert_eq!(output.next_part_number_marker.as_deref(), Some("2"));
    assert_eq!(output.max_parts, 1000);
    assert!(output.is_truncated);
    assert_eq!(output.parts.len(), 1);
    assert_eq!(output.parts[0].part_number, 1);
    assert_eq!(output.parts[0].size, 11);
    assert!(!output.parts[0].e_tag.is_weak());
    assert_eq!(output.parts[0].e_tag.opaque_tag(), "etag-1");
    let owner = output.owner.expect("owner should be present");
    let initiator = output.initiator.expect("initiator should be present");
    assert_eq!(owner.id.as_deref(), Some(RUSTFS_OWNER_ID));
    assert_eq!(owner.display_name.as_deref(), Some(RUSTFS_OWNER_DISPLAY_NAME));
    assert_eq!(initiator.id, owner.id);
    assert_eq!(initiator.display_name, owner.display_name);
}

#[test]
fn test_list_parts_output_xml_omits_terminal_marker() {
    for (marker, parts) in [(0, vec![]), (3, vec![10]), (0, vec![1, 3])] {
        let output = list_parts_output(ListPartsInfo {
            part_number_marker: marker,
            max_parts: 2,
            parts: parts.into_iter().map(part).collect(),
            ..page()
        })
        .unwrap();
        assert!(!output.is_truncated);
        assert!(output.next_part_number_marker.is_none());
        let xml = encode_xml(output);
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(xml.contains(&format!("<PartNumberMarker>{marker}</PartNumberMarker>")));
        assert!(!xml.contains("NextPartNumberMarker"), "terminal page must omit the token: {xml}");
    }
}

#[test]
fn test_list_parts_output_xml_preserves_truncated_marker() {
    let output = list_parts_output(ListPartsInfo {
        is_truncated: true,
        next_part_number_marker: Some(3),
        max_parts: 2,
        parts: vec![part(1), part(3)],
        ..page()
    })
    .unwrap();
    assert_eq!(output.parts.len(), 2);
    let xml = encode_xml(output);
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    assert!(xml.contains("<NextPartNumberMarker>3</NextPartNumberMarker>"));
}

#[test]
fn test_list_parts_output_reports_logical_size_for_compressed_parts() {
    let output = list_parts_output(ListPartsInfo {
        parts: vec![PartInfo {
            size: 1_024,
            actual_size: 8_388_608,
            ..part(1)
        }],
        ..page()
    })
    .unwrap();
    assert_eq!(output.parts.len(), 1);
    assert_eq!(output.parts[0].size, 8_388_608);
}

#[test]
fn test_list_parts_output_reports_zero_logical_size_for_compressed_parts() {
    let mut input = page();
    rustfs_utils::http::insert_str(&mut input.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_owned());
    input.parts.push(PartInfo {
        size: 8,
        actual_size: 0,
        ..part(1)
    });
    let output = list_parts_output(input).unwrap();
    assert_eq!(output.parts[0].size, 0);
}

#[test]
fn test_list_parts_output_reports_zero_logical_size_for_encrypted_parts() {
    let output = list_parts_output(ListPartsInfo {
        user_defined: std::collections::HashMap::from([(rustfs_utils::http::AMZ_SERVER_SIDE_ENCRYPTION.into(), "AES256".into())]),
        parts: vec![
            PartInfo {
                size: 8,
                actual_size: 0,
                ..part(1)
            },
            PartInfo {
                size: 8,
                actual_size: -1,
                ..part(2)
            },
        ],
        ..page()
    })
    .unwrap();
    assert_eq!(output.parts[0].size, 0);
    assert_eq!(output.parts[1].size, 8);
}

#[test]
fn test_list_parts_output_falls_back_to_stored_size_when_actual_size_unknown() {
    let output = list_parts_output(ListPartsInfo {
        parts: vec![
            PartInfo {
                size: 1_024,
                actual_size: 0,
                ..part(1)
            },
            PartInfo {
                size: 1_024,
                actual_size: -1,
                ..part(2)
            },
        ],
        ..page()
    })
    .unwrap();
    assert_eq!(output.parts.len(), 2);
    assert_eq!(output.parts[0].size, 1024);
    assert_eq!(output.parts[1].size, 1024);
}

#[test]
fn test_list_parts_output_normalizes_legacy_storage_class_and_preserves_markers() {
    let max_marker = usize::MAX.to_string();
    for stored_class in ["STANDARD_IA", ""] {
        let output = list_parts_output(ListPartsInfo {
            storage_class: stored_class.into(),
            part_number_marker: usize::MAX,
            next_part_number_marker: Some(usize::MAX),
            is_truncated: true,
            ..page()
        })
        .unwrap();
        assert_eq!(output.storage_class.as_ref().map(|class| class.as_str()), Some("STANDARD"));
        assert_eq!(output.part_number_marker.as_deref(), Some(max_marker.as_str()));
        assert_eq!(output.next_part_number_marker.as_deref(), Some(max_marker.as_str()));
    }
}

#[test]
fn test_list_parts_output_rejects_unrepresentable_page_or_part() {
    for input in [
        ListPartsInfo {
            max_parts: usize::MAX,
            ..page()
        },
        ListPartsInfo {
            parts: vec![part(usize::MAX)],
            ..page()
        },
        ListPartsInfo {
            parts: vec![PartInfo { etag: None, ..part(1) }],
            ..page()
        },
    ] {
        assert_eq!(list_parts_output(input).unwrap_err().code(), &S3ErrorCode::InternalError);
    }
    #[cfg(target_pointer_width = "64")]
    {
        let input = ListPartsInfo {
            parts: vec![PartInfo {
                size: usize::MAX,
                ..part(1)
            }],
            ..page()
        };
        assert_eq!(list_parts_output(input).unwrap_err().code(), &S3ErrorCode::InternalError);
    }
}
