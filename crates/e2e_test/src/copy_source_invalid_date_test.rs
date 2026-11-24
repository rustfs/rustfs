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

//! Regression coverage for rustfs/backlog#618 item 8: `CopyObject` with an
//! unparsable `x-amz-copy-source-if-unmodified-since` value.
//!
//! `minio-js` `CopyConditions.setUnmodified(new Date(undefined))` serialises the
//! header value as the literal string `"Invalid Date"` (via `Date.toUTCString()`)
//! and signs it. MinIO parses this header from the raw request and *ignores* it
//! when it fails to parse (`amztime.ParseHeader` error is swallowed), so the copy
//! proceeds.
//!
//! The RustFS gateway profile rejects unreadable conditional dates before the copy handler runs.
//! The signed requests below pin both the successful copy and the HTTP 400 `InvalidArgument` response.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use aws_sdk_s3::primitives::ByteStream;
    use http::header::HOST;
    use rustfs_s3_types::Body;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use std::error::Error;

    /// Signed raw `PUT` copy request with an explicit copy-source conditional
    /// header. The header is included in the SigV4 signature (as real SDKs do).
    async fn signed_copy(
        base_url: &str,
        dst_path: &str,
        copy_source: &str,
        if_unmodified_since: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<(reqwest::StatusCode, String), Box<dyn Error + Send + Sync>> {
        let url = format!("{base_url}{dst_path}");
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .header("x-amz-copy-source", copy_source)
            .header("x-amz-copy-source-if-unmodified-since", if_unmodified_since);
        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut rb = client.request(reqwest::Method::PUT, url);
        for (name, value) in signed.headers() {
            rb = rb.header(name, value);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        Ok((status, body))
    }

    #[tokio::test]
    async fn test_copy_source_if_unmodified_since_valid_and_invalid() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "copy-cond-bucket";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("src.txt")
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await?;

        let copy_source = format!("/{bucket}/src.txt");

        // A valid future HTTP-date allows the copy.
        let (status, body) = signed_copy(
            &env.url,
            &format!("/{bucket}/dst-valid.txt"),
            &copy_source,
            "Wed, 01 Jan 2031 00:00:00 GMT",
            &env.access_key,
            &env.secret_key,
        )
        .await?;
        assert_eq!(status, reqwest::StatusCode::OK, "valid copy-source date must succeed: {body}");
        assert!(body.contains("CopyObjectResult"), "expected CopyObjectResult XML: {body}");

        // The signed header reaches the RustFS profile's conditional-date check unchanged.
        let (status, body) = signed_copy(
            &env.url,
            &format!("/{bucket}/dst-invalid.txt"),
            &copy_source,
            "Invalid Date",
            &env.access_key,
            &env.secret_key,
        )
        .await?;
        assert_eq!(
            status,
            reqwest::StatusCode::BAD_REQUEST,
            "expected the signed invalid date to be rejected (backlog#618): {body}"
        );
        assert!(body.contains("InvalidArgument"), "expected InvalidArgument for invalid date: {body}");

        env.stop_server();
        Ok(())
    }
}
