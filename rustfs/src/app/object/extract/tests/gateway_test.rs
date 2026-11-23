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

use super::*;

#[tokio::test]
#[serial_test::serial]
async fn snowball_extract_preserves_sha256_failure_after_sniffing() {
    use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::server::TestGateway;
    use http_body_util::BodyExt as _;

    let (store, context) = real_store_test_context().await;
    let bucket = format!("extract-body-error-{}", Uuid::new_v4());
    store
        .make_bucket(&bucket, &MakeBucketOptions::default())
        .await
        .expect("create extract test bucket");
    let mut header = Header::new_gnu();
    header.set_path("member.txt").expect("valid member path");
    header.set_size(0);
    header.set_mode(0o644);
    header.set_cksum();
    let mut archive = header.as_bytes().to_vec();
    archive.extend_from_slice(&[0; 1024]);
    let archive = Bytes::from(archive);
    let slot = crate::app::runtime_sources::ServerContextSlot::new();
    assert!(slot.install(Arc::clone(&context)));
    let gateway = TestGateway::over(&context, slot);
    for valid in [false, true] {
        let digest = if valid {
            rustfs_utils::crypto::hex_sha256(&archive, str::to_owned)
        } else {
            "00".repeat(32)
        };
        // The first frame supplies the sniff prefix. The gateway compares the signed
        // payload digest when it reaches the end of the second frame.
        let body = Body::from_segments([archive.slice(..16), archive.slice(16..)]);
        let request = http::Request::put(format!("http://localhost/{bucket}/archive.tar"))
            .header(http::header::HOST, "localhost")
            .header(http::header::CONTENT_LENGTH, archive.len())
            .header("x-amz-meta-snowball-auto-extract", "true")
            .header("x-amz-content-sha256", digest)
            .body(body)
            .unwrap();
        let request = rustfs_signer::try_sign_v4(
            request,
            i64::try_from(archive.len()).unwrap(),
            TestGateway::ACCESS_KEY,
            TestGateway::SECRET_KEY,
            "",
            "us-east-1",
        )
        .expect("sign Snowball upload");
        let response = gateway.call(request).await;
        let status = response.status();
        let response = response.into_body().collect().await.unwrap().to_bytes();
        if valid {
            assert_eq!(status, http::StatusCode::OK, "{response:?}");
            assert_eq!(
                store
                    .get_object_info(&bucket, "member.txt", &ObjectOptions::default())
                    .await
                    .expect("matching digest must extract the member")
                    .size,
                0
            );
        } else {
            assert_eq!(status, http::StatusCode::BAD_REQUEST, "{response:?}");
            assert!(
                std::str::from_utf8(&response)
                    .unwrap()
                    .contains("<Code>XAmzContentSHA256Mismatch</Code>"),
                "{response:?}"
            );
            let missing = store
                .get_object_info(&bucket, "member.txt", &ObjectOptions::default())
                .await
                .expect_err("rejected archive cannot publish a member");
            assert!(is_err_object_not_found(&missing), "{missing}");
        }
        let missing = store
            .get_object_info(&bucket, "archive.tar", &ObjectOptions::default())
            .await
            .expect_err("the archive itself must not be stored");
        assert!(is_err_object_not_found(&missing), "{missing}");
    }
}
