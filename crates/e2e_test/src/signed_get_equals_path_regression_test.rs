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

//! Regression test for issue #2477:
//! signed GET on a non-existent object whose key ends with '=' must return
//! NoSuchKey instead of SignatureDoesNotMatch.

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
    async fn test_signed_get_nonexistent_object_with_trailing_equals_returns_nosuchkey()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("🧪 TEST: signed GET on missing object with trailing '=' returns NoSuchKey");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "signed-get-trailing-equals";
        client.create_bucket().bucket(bucket).send().await?;

        let key = "path/sitemap.xmlage=";
        let url = format!("{}/{}/{}", env.url, bucket, key);
        let response = signed_get(&url, &env.access_key, &env.secret_key).await?;

        let status = response.status();
        let body = response.text().await?;
        info!("signed GET trailing '=' status={status}, body={body}");

        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "missing object with trailing '=' should return 404 NoSuchKey, got {status}: {body}"
        );
        assert!(body.contains("<Code>NoSuchKey</Code>"), "expected NoSuchKey XML error body, got: {body}");

        Ok(())
    }
}
