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

//! Tests of the `object_commit` legacy-edge conversions (batch E1): what crosses the gateway seam
//! for CompleteMultipartUpload, in both directions.

use super::super::object_misc::{complete_multipart_upload_answer, complete_multipart_upload_input};
use crate::storage::ecfs::*;
use crate::storage::storage_api::to_s3s_xml_etag;
use rustfs_gateway_types::dto as gateway_dto;

fn input_with_parts(parts: Vec<CompletedPart>) -> CompleteMultipartUploadInput {
    CompleteMultipartUploadInput {
        bucket: "bucket".to_owned(),
        key: "object".to_owned(),
        upload_id: "upload".to_owned(),
        multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
        ..Default::default()
    }
}

/// Preserve part numbers and entity tags across the conversion.
#[test]
fn complete_input_preserves_part_fields() {
    let parts = vec![
        CompletedPart {
            part_number: None,
            e_tag: Some(ETag::Strong(String::new())),
            ..Default::default()
        },
        CompletedPart {
            part_number: Some(2),
            e_tag: Some(ETag::Weak("abc".to_owned())),
            ..Default::default()
        },
        CompletedPart {
            part_number: Some(3),
            e_tag: None,
            ..Default::default()
        },
    ];

    let (_, converted) = complete_multipart_upload_input(input_with_parts(parts)).expect("every part crosses");
    let converted = converted.expect("multipart payload");

    let read: Vec<(i32, Option<&str>)> = converted
        .parts
        .iter()
        .map(|part| (part.part_number, part.e_tag.as_ref().map(|tag| tag.opaque_tag())))
        .collect();
    assert_eq!(read, [(0, Some("")), (2, Some("abc")), (3, None)]);
}

/// Several per-algorithm checksum members, and one of the wrong width, do not stop the request:
/// the use case reads the headers they came from.
#[test]
fn complete_input_leaves_the_checksum_members_behind() {
    let mut input = input_with_parts(Vec::new());
    input.checksum_crc32 = Some("AAAA".to_owned());
    input.checksum_sha256 = Some("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".to_owned());

    let (converted, _) = complete_multipart_upload_input(input).expect("the checksum members stay behind");

    assert!(converted.checksum_spec.is_none());
}

/// The members the legacy output cannot carry become the headers the legacy stack wrote, and the
/// entity tag reaches the XML writer escaped as main escapes a stored tag.
#[test]
fn complete_answer_writes_header_members_as_headers_and_escapes_the_tag() {
    let stored = "a&b<c";
    let output = gateway_dto::CompleteMultipartUploadOutput {
        e_tag: Some(rustfs_gateway_types::ETag::from_stored(stored)),
        checksum_header_xxhash3: Some("AAAAAAAAAAA=".to_owned()),
        sse_customer_algorithm: Some("AES256".to_owned()),
        sse_customer_key_md5: Some("SaaDheEjzuynJH8eW6AEpQ==".to_owned()),
        ..Default::default()
    };

    #[cfg(feature = "gateway-stack")]
    {
        use rustfs_gateway::{Limits, MetaView, OperationCodec, TargetKind, WireRequest};
        let request = http::Request::builder()
            .method("POST")
            .uri("http://localhost/bucket/object")
            .header("host", "localhost")
            .body(bytes::Bytes::new())
            .expect("wire request");
        let wire = WireRequest::accept(request, &Limits::default()).expect("accepted wire request");
        let view = MetaView::of(&wire, TargetKind::Object).expect("object view");
        let encoded = rustfs_gateway::dto::CompleteMultipartUpload::encode(output.clone(), &view, 200).expect("encoded response");
        assert_eq!(
            encoded.headers.get("x-amz-checksum-xxhash3").and_then(|v| v.to_str().ok()),
            Some("AAAAAAAAAAA=")
        );
    }

    let (legacy, headers) = complete_multipart_upload_answer(output).expect("the answer crosses");

    assert_eq!(legacy.e_tag, Some(to_s3s_xml_etag(stored)));
    let written: Vec<(&str, &str)> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.to_str().expect("ASCII")))
        .collect();
    assert_eq!(
        written,
        [
            ("x-amz-checksum-xxhash3", "AAAAAAAAAAA="),
            ("x-amz-server-side-encryption-customer-algorithm", "AES256"),
            ("x-amz-server-side-encryption-customer-key-md5", "SaaDheEjzuynJH8eW6AEpQ=="),
        ]
    );
}

/// A header member no header can carry is refused by name, never written or dropped.
#[test]
fn complete_answer_refuses_a_header_member_that_is_not_a_header_value() {
    let output = gateway_dto::CompleteMultipartUploadOutput {
        checksum_header_md5: Some("line\nbreak".to_owned()),
        ..Default::default()
    };

    let refused = complete_multipart_upload_answer(output).expect_err("not a header value");

    assert_eq!(refused.field, "x-amz-checksum-md5");
}
