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

//! Differential cases of the `listing_multipart` batch (see `super`): stored keys that need
//! encoding, with and without `encoding-type=url`, ListObjects' marker, ListObjectsV2 pagination,
//! and the listings' parameter refusals.

use super::super::{Fields, Recorder, none};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::EncodingType;

const BUCKET: &str = "differential-listing";
const KEYS: [&str; 3] = ["dir a/file+b%.txt", "markup &<>\"'", "~tilde"];

fn v2_fields(o: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output) -> Fields {
    let keys: Vec<_> = o.contents().iter().map(|object| object.key()).collect();
    vec![
        ("keys", format!("{keys:?}")),
        ("encoding_type", format!("{:?}", o.encoding_type())),
        ("key_count", format!("{:?}", o.key_count())),
        ("is_truncated", format!("{:?}", o.is_truncated())),
        ("has_next_token", format!("{}", o.next_continuation_token().is_some())),
    ]
}

fn v1_fields(o: &aws_sdk_s3::operation::list_objects::ListObjectsOutput) -> Fields {
    let keys: Vec<_> = o.contents().iter().map(|object| object.key()).collect();
    vec![
        ("keys", format!("{keys:?}")),
        ("marker", format!("{:?}", o.marker())),
        ("next_marker", format!("{:?}", o.next_marker())),
        ("encoding_type", format!("{:?}", o.encoding_type())),
    ]
}

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    s.step("ListObjectsV2", "setup-bucket", c.create_bucket().bucket(BUCKET).send(), none)
        .await;
    for (key, case) in KEYS.into_iter().zip(["setup-object-1", "setup-object-2", "setup-object-3"]) {
        let put = c
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send();
        s.step("ListObjectsV2", case, put, none).await;
    }

    let v2 = || c.list_objects_v2().bucket(BUCKET);
    s.step("ListObjectsV2", "raw-keys", v2().send(), v2_fields).await;
    s.step("ListObjectsV2", "url-encoded", v2().encoding_type(EncodingType::Url).send(), v2_fields)
        .await;
    let first = s
        .step("ListObjectsV2", "first-page", v2().max_keys(2).send(), v2_fields)
        .await;
    let token = first.and_then(|page| page.next_continuation_token().map(str::to_owned));
    s.step("ListObjectsV2", "next-page", v2().set_continuation_token(token).send(), v2_fields)
        .await;
    s.step("ListObjectsV2", "negative-max-keys", v2().max_keys(-1).send(), none)
        .await;
    s.step("ListObjectsV2", "undecodable-token", v2().continuation_token("%%%").send(), none)
        .await;

    let v1 = || c.list_objects().bucket(BUCKET).max_keys(1);
    s.step("ListObjects", "raw-marker", v1().send(), v1_fields).await;
    s.step(
        "ListObjects",
        "url-encoded-marker",
        v1().encoding_type(EncodingType::Url).send(),
        v1_fields,
    )
    .await;
    s.step("ListObjects", "request-marker", v1().marker(KEYS[0]).send(), v1_fields)
        .await;
    s.step("ListObjects", "negative-max-keys", v1().max_keys(-1).send(), none)
        .await;

    let create = c.create_multipart_upload().bucket(BUCKET).key(KEYS[0]).send();
    s.step("ListMultipartUploads", "setup-upload", create, none).await;
    let uploads = || c.list_multipart_uploads().bucket(BUCKET);
    s.step("ListMultipartUploads", "raw-keys", uploads().send(), upload_fields)
        .await;
    let encoded = uploads().prefix("dir a/").encoding_type(EncodingType::Url).send();
    s.step("ListMultipartUploads", "url-encoded", encoded, upload_fields).await;
    s.step("ListMultipartUploads", "zero-max-uploads", uploads().max_uploads(0).send(), none)
        .await;
    let foreign = uploads().prefix("dir").key_marker("other").send();
    s.step("ListMultipartUploads", "foreign-key-marker", foreign, none).await;
}

fn upload_fields(o: &aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput) -> Fields {
    let keys: Vec<_> = o.uploads().iter().map(|upload| upload.key()).collect();
    vec![
        ("keys", format!("{keys:?}")),
        ("prefix", format!("{:?}", o.prefix())),
        ("encoding_type", format!("{:?}", o.encoding_type())),
    ]
}
