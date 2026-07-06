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

//! Regression test for rustfs/backlog#601: `GET //` ListBuckets browser compat.
//!
//! The AWS S3 browser (SigV4) concatenates the endpoint with a leading slash and
//! emits `GET //` for `ListBuckets`. The `s3s` path parser rejects `//` with
//! `InvalidBucketName` (empty bucket) before routing. `DoubleSlashListBucketsCompatLayer`
//! rewrites `GET //` to `GET /` up front so it routes to `ListBuckets`, matching
//! MinIO's explicit `//` route.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use http::header::HOST;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serial_test::serial;
    use std::error::Error;

    /// Sends a SigV4-signed `GET` where the signature is computed over `sign_path`
    /// but the on-the-wire request target is `wire_path`. Returns (status, body).
    async fn signed_get(
        base_url: &str,
        sign_path: &str,
        wire_path: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<(reqwest::StatusCode, String), Box<dyn Error + Send + Sync>> {
        let sign_url = format!("{base_url}{sign_path}");
        let uri = sign_url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut rb = client.request(reqwest::Method::GET, format!("{base_url}{wire_path}"));
        for (name, value) in signed.headers() {
            rb = rb.header(name, value);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        Ok((status, body))
    }

    fn assert_list_buckets_ok(status: reqwest::StatusCode, body: &str, label: &str) {
        assert_eq!(status, reqwest::StatusCode::OK, "{label}: expected 200, body: {body}");
        assert!(body.contains("ListAllMyBucketsResult"), "{label}: expected ListBuckets XML, body: {body}");
    }

    /// `GET /` (path-style service call) returns `ListBuckets`.
    #[tokio::test]
    #[serial]
    async fn test_list_buckets_single_slash() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let (status, body) = signed_get(&env.url, "/", "/", &env.access_key, &env.secret_key).await?;
        assert_list_buckets_ok(status, &body, "GET /");

        env.stop_server();
        Ok(())
    }

    /// `GET //` returns `ListBuckets` for the S3 browser SigV4 case: the client
    /// concatenates the endpoint with a leading `/` so the request target becomes
    /// `//`, while the signature was computed over the canonical `/` path. The
    /// compat layer rewrites `//` to `/` before `s3s` parses/verifies the request,
    /// so both routing and signature verification operate on `/`.
    #[tokio::test]
    #[serial]
    async fn test_list_buckets_double_slash_browser_compat() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let (status, body) = signed_get(&env.url, "/", "//", &env.access_key, &env.secret_key).await?;
        assert_list_buckets_ok(status, &body, "GET // (signature over /)");

        env.stop_server();
        Ok(())
    }

    /// The rewrite is confined to a request target of exactly `//`. A normal
    /// path-style bucket listing (`GET /bucket`) must keep working (no routing
    /// regression), and a leading double slash before a bucket name
    /// (`GET //bucket`) must be left untouched by the compat layer — it is not a
    /// `ListBuckets` request and s3s continues to reject the empty bucket name.
    #[tokio::test]
    #[serial]
    async fn test_double_slash_rewrite_is_narrowly_scoped() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        client.create_bucket().bucket("compat-bucket").send().await?;

        // `GET /compat-bucket` still lists the bucket (routing unchanged).
        let (status, body) = signed_get(&env.url, "/compat-bucket", "/compat-bucket", &env.access_key, &env.secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::OK, "GET /compat-bucket body: {body}");
        assert!(
            body.contains("ListBucketResult") && !body.contains("ListAllMyBucketsResult"),
            "GET /compat-bucket must be a bucket listing: {body}"
        );

        // `GET //compat-bucket` is not rewritten (path is not exactly `//`); s3s
        // still rejects the empty leading bucket segment with InvalidBucketName.
        let (status, body) = signed_get(&env.url, "//compat-bucket", "//compat-bucket", &env.access_key, &env.secret_key).await?;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "GET //compat-bucket body: {body}");
        assert!(
            body.contains("InvalidBucketName"),
            "GET //compat-bucket must remain InvalidBucketName (not rewritten): {body}"
        );

        env.stop_server();
        Ok(())
    }
}
