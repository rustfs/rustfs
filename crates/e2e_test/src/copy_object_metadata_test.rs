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

//! CopyObject metadata replacement regression tests.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::MetadataDirective;
    use serial_test::serial;
    use tracing::info;

    #[tokio::test]
    #[serial]
    async fn test_self_copy_replace_metadata_preserves_readable_object() {
        init_logging();
        info!("Issue #2789: self-copy metadata replacement must preserve object data");

        let mut env = RustFSTestEnvironment::new().await.expect("Failed to create test environment");
        env.start_rustfs_server(vec![]).await.expect("Failed to start RustFS");

        let client = env.create_s3_client();
        let bucket = "self-copy-metadata-replace-test";
        let key = "assets/chunk-2F3R7JUG.js";
        let content = b"console.log('metadata replacement should keep object data readable');";

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Failed to create bucket");

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("text/javascript; charset=utf-8")
            .metadata("mtime", "1777992333")
            .metadata("stale", "must-be-removed")
            .body(ByteStream::from_static(content))
            .send()
            .await
            .expect("PUT failed");

        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Replace)
            .content_type("text/javascript; charset=utf-8")
            .metadata("mtime", "1777992348")
            .send()
            .await
            .expect("self CopyObject with metadata replacement failed");

        let head_resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed after self-copy");
        assert_eq!(head_resp.content_length(), Some(content.len() as i64));
        assert_eq!(
            head_resp.metadata().and_then(|metadata| metadata.get("mtime")),
            Some(&"1777992348".to_string()),
            "HEAD should return replaced metadata"
        );
        assert_eq!(
            head_resp.metadata().and_then(|metadata| metadata.get("stale")),
            None,
            "HEAD should not return metadata omitted by REPLACE"
        );

        let get_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET failed after self-copy");
        let body = get_resp
            .body
            .collect()
            .await
            .expect("Failed to collect GET body")
            .into_bytes();
        assert_eq!(body.as_ref(), content, "self-copy metadata replacement must not drop object data");

        client
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(format!("{bucket}/{key}"))
            .metadata_directive(MetadataDirective::Replace)
            .send()
            .await
            .expect("self CopyObject with empty metadata replacement failed");

        let empty_head_resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("HEAD failed after empty metadata replacement");
        assert_eq!(
            empty_head_resp.metadata().and_then(|metadata| metadata.get("mtime")),
            None,
            "HEAD should not return metadata omitted by empty REPLACE"
        );

        let empty_get_resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GET failed after empty metadata replacement");
        let empty_body = empty_get_resp
            .body
            .collect()
            .await
            .expect("Failed to collect GET body after empty metadata replacement")
            .into_bytes();
        assert_eq!(empty_body.as_ref(), content, "empty metadata replacement must not drop object data");

        env.stop_server();
    }
}
