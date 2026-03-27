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

//! End-to-end regression test for the objects metadata extension:
//! `GET /{bucket}?list-type=2&metadata=true`

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use http::header::HOST;
    use reqwest::StatusCode;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serial_test::serial;
    use std::error::Error;
    use tracing::info;

    async fn signed_get(
        url: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())?;

        let signed = sign_v4(request, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut request_builder = client.get(url);
        for (name, value) in signed.headers() {
            request_builder = request_builder.header(name, value);
        }

        Ok(request_builder.send().await?)
    }

    #[tokio::test]
    #[serial]
    async fn test_list_objects_v2_metadata_extension_returns_metadata_tags_and_internal()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("🧪 TEST: list-type=2&metadata=true returns object metadata extensions");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "test-list-objects-v2-metadata-ext";
        let key = "objects/metadata-object.txt";

        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .metadata("project", "alpha")
            .metadata("owner", "ops")
            .tagging("env=test&project=alpha")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"metadata extension body"))
            .send()
            .await?;

        let url = format!("{}/{}?list-type=2&metadata=true&prefix={}", env.url, bucket, urlencoding::encode(key));
        let response = signed_get(&url, &env.access_key, &env.secret_key).await?;

        assert_eq!(response.status(), StatusCode::OK, "list-type=2&metadata=true should succeed");

        let body = response.text().await?;
        info!("list-type=2&metadata=true response body: {}", body);

        assert!(body.contains("<ListBucketResult"), "expected ListBucketResult XML, got: {body}");
        assert!(body.contains("<Contents>"), "expected at least one Contents entry, got: {body}");
        assert!(body.contains("<UserMetadata>"), "expected UserMetadata extension, got: {body}");
        assert!(
            body.contains("<project>alpha</project>"),
            "expected stripped user metadata key in response, got: {body}"
        );
        assert!(
            body.contains("<owner>ops</owner>"),
            "expected second metadata key in response, got: {body}"
        );
        assert!(
            body.contains("<UserTags>env=test&amp;project=alpha</UserTags>"),
            "expected UserTags extension in response, got: {body}"
        );
        assert!(body.contains("<Internal>"), "expected Internal extension, got: {body}");
        assert!(body.contains("<K>1</K>"), "expected Internal/K in response, got: {body}");
        assert!(body.contains("<M>0</M>"), "expected Internal/M in response, got: {body}");

        Ok(())
    }
}
