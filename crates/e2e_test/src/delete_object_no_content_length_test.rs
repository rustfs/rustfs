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

//! Regression coverage for signed `DELETE Object?versionId` requests sent with
//! no body and no `Content-Length` header.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serial_test::serial;
    use std::error::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};
    use tracing::info;

    const RAW_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

    fn parse_status(raw_response: &str) -> Option<u16> {
        raw_response.lines().next()?.split_whitespace().nth(1)?.parse().ok()
    }

    fn response_body(raw_response: &str) -> &str {
        raw_response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .or_else(|| raw_response.split_once("\n\n").map(|(_, body)| body))
            .unwrap_or("")
    }

    async fn signed_delete_without_content_length(
        url: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let path_and_query = uri.path_and_query().ok_or("request URL missing path")?.as_str().to_string();

        let request = http::Request::builder()
            .method(http::Method::DELETE)
            .uri(uri)
            .header(HOST, authority.clone())
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())?;

        let signed = sign_v4(request, 0, access_key, secret_key, "", "us-east-1");

        let mut raw_request = format!("DELETE {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
        for (name, value) in signed.headers() {
            if name.as_str().eq_ignore_ascii_case("host") || name.as_str().eq_ignore_ascii_case("content-length") {
                continue;
            }
            raw_request.push_str(name.as_str());
            raw_request.push_str(": ");
            raw_request.push_str(value.to_str()?);
            raw_request.push_str("\r\n");
        }
        raw_request.push_str("\r\n");

        assert!(
            !raw_request.to_ascii_lowercase().contains("\r\ncontent-length:"),
            "raw regression request must omit Content-Length; request was:\n{raw_request}"
        );

        let mut stream = TcpStream::connect(&authority).await?;
        stream.write_all(raw_request.as_bytes()).await?;
        stream.flush().await?;

        let mut response = Vec::new();
        timeout(RAW_RESPONSE_TIMEOUT, stream.read_to_end(&mut response))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out reading raw DELETE response"))??;
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    #[tokio::test]
    #[serial]
    async fn test_delete_object_version_without_content_length_succeeds() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("🧪 TEST: signed DELETE Object?versionId succeeds without Content-Length");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "delete-version-no-content-length";
        let key = "versioned-delete-target.txt";

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"delete me after versioning is enabled"))
            .send()
            .await?;

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;

        let listed_versions = client.list_object_versions().bucket(bucket).prefix(key).send().await?;
        let version_id = listed_versions
            .versions()
            .iter()
            .find(|version| version.key() == Some(key))
            .and_then(|version| version.version_id())
            .ok_or("ListObjectVersions did not return the pre-versioning object version")?;

        let url = format!("{}/{}/{}?versionId={}", env.url, bucket, key, urlencoding::encode(version_id));
        let raw_response = signed_delete_without_content_length(&url, &env.access_key, &env.secret_key).await?;
        info!("raw DELETE response:\n{}", raw_response);

        assert_eq!(
            parse_status(&raw_response),
            Some(204),
            "DELETE Object?versionId without Content-Length should succeed, got:\n{raw_response}"
        );
        assert!(
            !raw_response.contains("MissingContentLength"),
            "DELETE Object?versionId without Content-Length regressed to MissingContentLength: {raw_response}"
        );
        assert!(
            response_body(&raw_response).trim().is_empty(),
            "successful DELETE Object?versionId should not return an error body: {raw_response}"
        );

        let get_deleted_version = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await;
        assert!(get_deleted_version.is_err(), "explicitly deleted version should no longer be readable");

        Ok(())
    }
}
