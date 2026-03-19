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

//! Regression coverage for anonymous access on multipart control APIs.

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::primitives::ByteStream;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_anonymous_multipart_control_apis_require_auth() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_logging();

    let mut env = RustFSTestEnvironment::new().await?;
    env.start_rustfs_server(vec![]).await?;

    let bucket = "anon-multipart-auth";
    let key = "multipart-target";
    let source_key = "copy-source";

    let admin_client = env.create_s3_client();
    admin_client.create_bucket().bucket(bucket).send().await?;
    admin_client
        .put_object()
        .bucket(bucket)
        .key(source_key)
        .body(ByteStream::from_static(b"copy-source-data"))
        .send()
        .await?;

    let http = reqwest::Client::new();
    let base = format!("{}/{}/{}", env.url, bucket, key);
    let upload_id = "dummy-upload-id";

    let abort_resp = http.delete(format!("{base}?uploadId={upload_id}")).send().await?;
    assert_eq!(
        abort_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous AbortMultipartUpload should be rejected"
    );

    let list_parts_resp = http.get(format!("{base}?uploadId={upload_id}")).send().await?;
    assert_eq!(
        list_parts_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous ListParts should be rejected"
    );

    let complete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
  <Part>
    <PartNumber>1</PartNumber>
    <ETag>"dummy-etag"</ETag>
  </Part>
</CompleteMultipartUpload>"#;
    let complete_resp = http
        .post(format!("{base}?uploadId={upload_id}"))
        .header(reqwest::header::CONTENT_TYPE, "application/xml")
        .body(complete_body)
        .send()
        .await?;
    assert_eq!(
        complete_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous CompleteMultipartUpload should be rejected"
    );

    let copy_source = format!("/{bucket}/{source_key}");
    let upload_part_copy_resp = http
        .put(format!("{base}?uploadId={upload_id}&partNumber=1"))
        .header("x-amz-copy-source", copy_source)
        .send()
        .await?;
    assert_eq!(
        upload_part_copy_resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "anonymous UploadPartCopy should be rejected"
    );

    Ok(())
}
