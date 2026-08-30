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

//! Regression coverage for rustfs#6830: a signed empty `PutObject` request
//! without `Content-Length` and without `Transfer-Encoding` is still a
//! zero-length object upload.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging};
    use http::header::{CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
    use rustfs_signer::sign_v4;
    use rustfs_utils::hash::EMPTY_STRING_SHA256_HASH;
    use s3s::Body;
    use std::error::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};
    use tracing::info;

    const RAW_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

    fn parse_status(raw_response: &str) -> Option<u16> {
        raw_response.lines().next()?.split_whitespace().nth(1)?.parse().ok()
    }

    async fn send_raw_signed_put(
        url: &str,
        access_key: &str,
        secret_key: &str,
        transfer_encoding: Option<&str>,
        raw_body: &[u8],
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let path_and_query = uri.path_and_query().ok_or("request URL missing path")?.as_str().to_string();

        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(uri)
            .header(HOST, authority.clone())
            .header("x-amz-content-sha256", EMPTY_STRING_SHA256_HASH);
        if let Some(value) = transfer_encoding {
            request = request.header(TRANSFER_ENCODING, value);
        }

        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let mut raw_request = format!("PUT {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
        for (name, value) in signed.headers() {
            if name == HOST || name == CONTENT_LENGTH {
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
        stream.write_all(raw_body).await?;
        stream.flush().await?;

        let mut response = Vec::new();
        timeout(RAW_RESPONSE_TIMEOUT, stream.read_to_end(&mut response))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out reading raw PUT response"))??;
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    #[tokio::test]
    async fn test_put_object_without_content_length_boundaries() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        info!("TEST: PutObject without Content-Length boundaries");

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let empty_bucket = "put-no-content-length";
        let empty_key = "empty.bin";
        let chunked_bucket = "put-chunked-no-length";
        let chunked_key = "chunked.bin";

        client.create_bucket().bucket(empty_bucket).send().await?;
        client.create_bucket().bucket(chunked_bucket).send().await?;

        let url = format!("{}/{}/{}", env.url, empty_bucket, empty_key);
        let raw_response = send_raw_signed_put(&url, &env.access_key, &env.secret_key, None, b"").await?;
        info!("raw empty PUT response:\n{}", raw_response);

        assert_eq!(
            parse_status(&raw_response),
            Some(200),
            "empty PutObject without Content-Length should succeed, got:\n{raw_response}"
        );
        assert!(
            raw_response.to_ascii_lowercase().contains("\r\netag:"),
            "successful PutObject should return an ETag header: {raw_response}"
        );

        let head = client.head_object().bucket(empty_bucket).key(empty_key).send().await?;
        assert_eq!(head.content_length(), Some(0), "stored object must be zero length");

        let url = format!("{}/{}/{}", env.url, chunked_bucket, chunked_key);
        let raw_response = send_raw_signed_put(&url, &env.access_key, &env.secret_key, Some("chunked"), b"0\r\n\r\n").await?;
        info!("raw chunked PUT response:\n{}", raw_response);

        assert_eq!(
            parse_status(&raw_response),
            Some(411),
            "unknown-length chunked PutObject must stay rejected, got:\n{raw_response}"
        );
        assert!(
            raw_response.contains("<Code>MissingContentLength</Code>"),
            "expected MissingContentLength, got:\n{raw_response}"
        );

        let missing = client
            .head_object()
            .bucket(chunked_bucket)
            .key(chunked_key)
            .send()
            .await
            .expect_err("rejected unknown-length PUT must not create an object");
        assert_eq!(
            missing.raw_response().map(|response| response.status().as_u16()),
            Some(404),
            "rejected unknown-length PUT absence probe must return HTTP 404, got {missing:?}"
        );

        Ok(())
    }
}
