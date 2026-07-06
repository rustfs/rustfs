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
//! RustFS routes S3 through the `s3s` crate, which models the copy-source
//! conditional-date headers as typed `Timestamp` fields and rejects an
//! unparsable value with `400 InvalidArgument` during request deserialization —
//! this happens *after* SigV4 verification but *before* the RustFS `CopyObject`
//! handler runs. Because the header is part of the client's `SignedHeaders`, the
//! value cannot be stripped or rewritten by a pre-`s3s` layer without breaking the
//! signature, and `s3s` exposes no hook between auth and typed deserialization.
//! Matching MinIO's tolerant behavior therefore requires a change in `s3s`
//! (lenient parsing of conditional-date headers). See the PR description for
//! details.
//!
//! This test pins the two ends of the current behavior:
//!   * a *valid* HTTP-date copy-source conditional header is accepted and the copy
//!     succeeds (the common, real-SDK path), and
//!   * an *invalid* date value is rejected with `400 InvalidArgument` at the `s3s`
//!     layer (the documented gap). When `s3s` gains lenient parsing, the second
//!     assertion should be flipped to expect a successful copy.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use aws_sdk_s3::primitives::ByteStream;
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serial_test::serial;
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
    #[serial]
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

        // A valid future HTTP-date is accepted by s3s and the copy succeeds.
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

        // An unparsable date ("Invalid Date") is currently rejected by s3s at the
        // deserialization layer with 400 InvalidArgument. This documents the s3s
        // limitation described in backlog#618 item 8. When s3s parses these headers
        // leniently (matching MinIO), this should become a successful copy.
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
            "invalid copy-source date currently rejected by s3s (backlog#618): {body}"
        );
        assert!(body.contains("InvalidArgument"), "expected InvalidArgument for invalid date: {body}");

        env.stop_server();
        Ok(())
    }
}
