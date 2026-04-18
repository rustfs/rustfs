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

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client, rustfs_binary_path};
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use http::header::{CONTENT_TYPE, HOST};
    use reqwest::StatusCode;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::{pre_sign_v4, sign_v4};
    use s3s::Body;
    use serial_test::serial;
    use sha2::{Digest, Sha256};
    use std::error::Error;
    use std::io::{Cursor, Write};
    use std::process::Command;
    use time::OffsetDateTime;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

    const ARCHIVE_TEST_BUCKET: &str = "archive-download-integrity";
    const MULTIPART_ARCHIVE_TEST_BUCKET: &str = "archive-multipart-integrity";
    const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;

    fn build_zip_bytes(files: &[(&str, &[u8])]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let cursor = Cursor::new(Vec::new());
        let mut zip = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);

        for (name, content) in files {
            zip.start_file(*name, options)?;
            zip.write_all(content)?;
        }

        Ok(zip.finish()?.into_inner())
    }

    fn random_bytes(size: usize) -> Vec<u8> {
        (0..size).map(|idx| (idx % 251) as u8).collect()
    }

    async fn start_rustfs_server_with_env(
        env: &mut RustFSTestEnvironment,
        extra_env: &[(&str, &str)],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let binary_path = rustfs_binary_path();
        let mut command = Command::new(&binary_path);
        command.env("RUST_LOG", "rustfs=info,rustfs_notify=debug");
        for (key, value) in extra_env {
            command.env(key, value);
        }

        let process = command
            .args([
                "--address",
                &env.address,
                "--access-key",
                &env.access_key,
                "--secret-key",
                &env.secret_key,
                &env.temp_dir,
            ])
            .spawn()?;

        env.process = Some(process);
        env.wait_for_server_ready().await?;
        Ok(())
    }

    async fn presigned_get_request_with_accept_encoding(
        url: &str,
        access_key: &str,
        secret_key: &str,
        accept_encoding: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let signed = pre_sign_v4(
            http::Request::builder()
                .method(http::Method::GET)
                .uri(uri)
                .header(HOST, authority)
                .body(Body::empty())?,
            access_key,
            secret_key,
            "",
            "us-east-1",
            600,
            OffsetDateTime::now_utc(),
        );

        let client = reqwest::Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_zstd()
            .no_deflate()
            .build()?;

        Ok(client
            .get(signed.uri().to_string())
            .header("Accept-Encoding", accept_encoding)
            .send()
            .await?)
    }

    fn find_header_terminator(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }

    async fn read_proxy_request(stream: &mut tokio::net::TcpStream) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 4096];

        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err("proxy request ended before headers were fully received".into());
            }
            buffer.extend_from_slice(&chunk[..read]);
            if find_header_terminator(&buffer).is_some() {
                return Ok(());
            }
        }
    }

    async fn spawn_reverse_proxy_to_presigned_url(
        target_url: String,
    ) -> Result<(String, tokio::task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>), Box<dyn Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let proxy_url = format!("http://{address}/");

        let handle = tokio::spawn(async move {
            let (mut downstream, _) = listener.accept().await?;
            read_proxy_request(&mut downstream).await?;

            let upstream_response: Result<reqwest::Response, reqwest::Error> = local_http_client().get(&target_url).send().await;
            let (status, body, content_type) = match upstream_response {
                Ok(response) => {
                    let status = response.status();
                    let content_type = response
                        .headers()
                        .get("content-type")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_string);
                    match response.bytes().await {
                        Ok(body) => (status, body.to_vec(), content_type),
                        Err(err) => {
                            let body = format!("upstream body read failed: {err}").into_bytes();
                            (StatusCode::BAD_GATEWAY, body, Some("text/plain".to_string()))
                        }
                    }
                }
                Err(err) => {
                    let body = format!("upstream request failed: {err}").into_bytes();
                    (StatusCode::BAD_GATEWAY, body, Some("text/plain".to_string()))
                }
            };

            let mut response_head = format!("HTTP/1.1 {}\r\ncontent-length: {}\r\nconnection: close\r\n", status, body.len());
            if let Some(content_type) = content_type {
                response_head.push_str(&format!("content-type: {content_type}\r\n"));
            }
            response_head.push_str("\r\n");

            downstream.write_all(response_head.as_bytes()).await?;
            downstream.write_all(&body).await?;
            downstream.shutdown().await?;
            Ok(())
        });

        Ok((proxy_url, handle))
    }

    async fn signed_put_request_with_headers(
        url: &str,
        access_key: &str,
        secret_key: &str,
        body: Vec<u8>,
        content_type: &str,
        content_encoding: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(uri)
            .header(HOST, authority)
            .header(CONTENT_TYPE, content_type)
            .header("content-encoding", content_encoding)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())?;
        let signed = sign_v4(request, body.len() as i64, access_key, secret_key, "", "us-east-1");

        let client = reqwest::Client::builder().no_proxy().build()?;
        let mut builder = client.put(url).body(body);
        for (name, value) in signed.headers() {
            builder = builder.header(name, value);
        }

        Ok(builder.send().await?)
    }

    async fn signed_get_request_with_headers(
        url: &str,
        access_key: &str,
        secret_key: &str,
        extra_headers: &[(&str, &str)],
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("request URL missing authority")?.to_string();
        let mut request = http::Request::builder()
            .method(http::Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD);
        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }

        let signed = sign_v4(request.body(Body::empty())?, 0, access_key, secret_key, "", "us-east-1");

        let client = local_http_client();
        let mut builder = client.get(url);
        for (name, value) in signed.headers() {
            builder = builder.header(name, value);
        }

        Ok(builder.send().await?)
    }

    async fn assert_archive_object_content_encoding(
        client: &S3Client,
        bucket: &str,
        key: &str,
        expected_content_encoding: Option<&str>,
        expected_body: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let head_resp = client.head_object().bucket(bucket).key(key).send().await?;
        assert_eq!(head_resp.content_encoding(), expected_content_encoding);

        let get_resp = client.get_object().bucket(bucket).key(key).send().await?;
        assert_eq!(get_resp.content_encoding(), expected_content_encoding);
        let body = get_resp.body.collect().await?.into_bytes();
        assert_eq!(body.as_ref(), expected_body);

        Ok(())
    }

    async fn complete_archive_multipart_upload_with_content_encoding(
        client: &S3Client,
        bucket: &str,
        key: &str,
        content_encoding: Option<&str>,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let payload = random_bytes(MULTIPART_PART_SIZE + 256 * 1024);
        let zip_bytes = build_zip_bytes(&[("payload.bin", payload.as_slice())])?;
        assert!(zip_bytes.len() > MULTIPART_PART_SIZE, "zip payload must exceed multipart threshold");

        let mut create_builder = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .content_type("application/zip");
        if let Some(content_encoding) = content_encoding {
            create_builder = create_builder.content_encoding(content_encoding);
        }
        let create_output = create_builder.send().await?;
        let upload_id = create_output.upload_id().expect("multipart upload id");

        let first_part = zip_bytes[..MULTIPART_PART_SIZE].to_vec();
        let second_part = zip_bytes[MULTIPART_PART_SIZE..].to_vec();

        let upload_part_1 = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(first_part))
            .send()
            .await?;

        let upload_part_2 = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(second_part))
            .send()
            .await?;

        let completed_upload = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(upload_part_1.e_tag().unwrap_or_default())
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .e_tag(upload_part_2.e_tag().unwrap_or_default())
                    .build(),
            )
            .build();

        client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        Ok(zip_bytes)
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_allows_content_encoding_by_default() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;
        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle.zip");
        let response =
            signed_put_request_with_headers(&object_url, &env.access_key, &env.secret_key, zip_bytes, "application/zip", "gzip")
                .await?;

        assert_eq!(response.status(), StatusCode::OK);

        let client = env.create_s3_client();
        let head_resp = client
            .head_object()
            .bucket(ARCHIVE_TEST_BUCKET)
            .key("bundle.zip")
            .send()
            .await?;
        assert_eq!(head_resp.content_encoding(), Some("gzip"));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_rejects_content_encoding_when_strict_mode_enabled() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;
        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle.zip");
        let response =
            signed_put_request_with_headers(&object_url, &env.access_key, &env.secret_key, zip_bytes, "application/zip", "gzip")
                .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.text().await?;
        assert!(
            body.contains("InvalidArgument") || body.contains("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING"),
            "unexpected error body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_with_aws_chunked_does_not_persist_content_encoding_by_default()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;

        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle-aws-chunked.zip");
        let response = signed_put_request_with_headers(
            &object_url,
            &env.access_key,
            &env.secret_key,
            zip_bytes.clone(),
            "application/zip",
            "aws-chunked",
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);

        let client = env.create_s3_client();
        assert_archive_object_content_encoding(
            &client,
            ARCHIVE_TEST_BUCKET,
            "bundle-aws-chunked.zip",
            None,
            zip_bytes.as_slice(),
        )
        .await?;

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_with_aws_chunked_and_effective_encoding_roundtrips_by_default()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;

        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle-aws-chunked-gzip.zip");
        let response = signed_put_request_with_headers(
            &object_url,
            &env.access_key,
            &env.secret_key,
            zip_bytes.clone(),
            "application/zip",
            "aws-chunked,gzip",
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);

        let client = env.create_s3_client();
        assert_archive_object_content_encoding(
            &client,
            ARCHIVE_TEST_BUCKET,
            "bundle-aws-chunked-gzip.zip",
            Some("gzip"),
            zip_bytes.as_slice(),
        )
        .await?;

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_with_aws_chunked_allowed_when_strict_mode_enabled() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;

        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle-strict-aws-chunked.zip");
        let response = signed_put_request_with_headers(
            &object_url,
            &env.access_key,
            &env.secret_key,
            zip_bytes.clone(),
            "application/zip",
            "aws-chunked",
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);

        let client = env.create_s3_client();
        assert_archive_object_content_encoding(
            &client,
            ARCHIVE_TEST_BUCKET,
            "bundle-strict-aws-chunked.zip",
            None,
            zip_bytes.as_slice(),
        )
        .await?;

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_put_with_aws_chunked_and_effective_encoding_rejects_when_strict_mode_enabled()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;

        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle-strict-aws-chunked-gzip.zip");
        let response = signed_put_request_with_headers(
            &object_url,
            &env.access_key,
            &env.secret_key,
            zip_bytes,
            "application/zip",
            "aws-chunked,gzip",
        )
        .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.text().await?;
        assert!(
            body.contains("InvalidArgument") || body.contains("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING"),
            "unexpected error body: {body}"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_download_roundtrip_with_http_compression_enabled() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(
            &mut env,
            &[
                ("RUSTFS_COMPRESS_ENABLE", "on"),
                ("RUSTFS_COMPRESS_MIME_TYPES", "text/*,application/json,application/zip"),
                ("RUSTFS_COMPRESS_MIN_SIZE", "1"),
            ],
        )
        .await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let zip_bytes = build_zip_bytes(&[
            ("docs/readme.txt", b"archive-download-integrity"),
            ("docs/notes.txt", b"response-compression-must-not-alter-zip-bytes"),
        ])?;
        let expected_sha256 = Sha256::digest(&zip_bytes);

        client
            .put_object()
            .bucket(ARCHIVE_TEST_BUCKET)
            .key("bundle.zip")
            .content_type("application/zip")
            .body(ByteStream::from(zip_bytes.clone()))
            .send()
            .await?;

        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle.zip");
        let response =
            presigned_get_request_with_accept_encoding(&object_url, &env.access_key, &env.secret_key, "gzip, br, zstd").await?;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-encoding")
                .and_then(|value| value.to_str().ok()),
            None,
            "archive download must not be HTTP-compressed"
        );

        let downloaded = response.bytes().await?;
        assert_eq!(
            downloaded.as_ref(),
            zip_bytes.as_slice(),
            "downloaded archive bytes must match uploaded bytes"
        );
        assert_eq!(
            Sha256::digest(downloaded.as_ref()).as_slice(),
            expected_sha256.as_slice(),
            "archive SHA256 mismatch"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_multipart_roundtrip_preserves_bytes() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_without_cleanup(vec![]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let payload = random_bytes(MULTIPART_PART_SIZE + 512 * 1024);
        let zip_bytes = build_zip_bytes(&[("payload.bin", payload.as_slice())])?;
        assert!(zip_bytes.len() > MULTIPART_PART_SIZE, "zip payload must exceed multipart threshold");
        let expected_sha256 = Sha256::digest(&zip_bytes);

        let create_output = client
            .create_multipart_upload()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-bundle.zip")
            .content_type("application/zip")
            .send()
            .await?;
        let upload_id = create_output.upload_id().expect("multipart upload id");

        let first_part = zip_bytes[..MULTIPART_PART_SIZE].to_vec();
        let second_part = zip_bytes[MULTIPART_PART_SIZE..].to_vec();

        let upload_part_1 = client
            .upload_part()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-bundle.zip")
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(first_part))
            .send()
            .await?;

        let upload_part_2 = client
            .upload_part()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-bundle.zip")
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(second_part))
            .send()
            .await?;

        let completed_upload = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(upload_part_1.e_tag().unwrap_or_default())
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .e_tag(upload_part_2.e_tag().unwrap_or_default())
                    .build(),
            )
            .build();

        client
            .complete_multipart_upload()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-bundle.zip")
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        let downloaded = client
            .get_object()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-bundle.zip")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();

        assert_eq!(
            downloaded.as_ref(),
            zip_bytes.as_slice(),
            "multipart archive bytes must match uploaded bytes"
        );
        assert_eq!(
            Sha256::digest(downloaded.as_ref()).as_slice(),
            expected_sha256.as_slice(),
            "multipart archive SHA256 mismatch"
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_multipart_get_ignores_empty_conditional_etag_headers() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let key = "multipart-empty-conditional-headers.zip";
        let zip_bytes =
            complete_archive_multipart_upload_with_content_encoding(&client, MULTIPART_ARCHIVE_TEST_BUCKET, key, None).await?;
        let object_url = format!("{}/{}/{}", env.url, MULTIPART_ARCHIVE_TEST_BUCKET, key);

        let response = signed_get_request_with_headers(
            &object_url,
            &env.access_key,
            &env.secret_key,
            &[("if-match", ""), ("if-none-match", "")],
        )
        .await?;
        let status = response.status();
        let body = response.bytes().await?;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected multipart GET status {status}, body: {}",
            String::from_utf8_lossy(body.as_ref())
        );
        assert_eq!(body.as_ref(), zip_bytes.as_slice());

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_multipart_with_aws_chunked_and_effective_encoding_roundtrips_by_default()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let zip_bytes = complete_archive_multipart_upload_with_content_encoding(
            &client,
            MULTIPART_ARCHIVE_TEST_BUCKET,
            "multipart-aws-chunked-gzip.zip",
            Some("aws-chunked,gzip"),
        )
        .await?;
        assert_archive_object_content_encoding(
            &client,
            MULTIPART_ARCHIVE_TEST_BUCKET,
            "multipart-aws-chunked-gzip.zip",
            Some("gzip"),
            zip_bytes.as_slice(),
        )
        .await?;

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_multipart_with_aws_chunked_allowed_when_strict_mode_enabled() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let zip_bytes = complete_archive_multipart_upload_with_content_encoding(
            &client,
            MULTIPART_ARCHIVE_TEST_BUCKET,
            "multipart-strict-aws-chunked.zip",
            Some("aws-chunked"),
        )
        .await?;
        assert_archive_object_content_encoding(
            &client,
            MULTIPART_ARCHIVE_TEST_BUCKET,
            "multipart-strict-aws-chunked.zip",
            None,
            zip_bytes.as_slice(),
        )
        .await?;

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_archive_multipart_with_aws_chunked_and_effective_encoding_rejects_when_strict_mode_enabled()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        start_rustfs_server_with_env(&mut env, &[("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING", "true")]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let create_result = client
            .create_multipart_upload()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("multipart-strict-aws-chunked-gzip.zip")
            .content_type("application/zip")
            .content_encoding("aws-chunked,gzip")
            .send()
            .await;
        let err = create_result.expect_err("strict mode should reject effective archive content encoding");
        assert_eq!(err.code(), Some("InvalidArgument"));
        assert!(
            err.message().is_some_and(|message| {
                message.contains("Content-Encoding") && message.contains("RUSTFS_REJECT_ARCHIVE_CONTENT_ENCODING")
            }),
            "unexpected error metadata: code={:?}, message={:?}",
            err.code(),
            err.message()
        );

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_presigned_get_and_reverse_proxy_preserve_multipart_bytes() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        env.create_test_bucket(MULTIPART_ARCHIVE_TEST_BUCKET).await?;

        let client = env.create_s3_client();
        let payload = random_bytes(MULTIPART_PART_SIZE + 768 * 1024);
        let zip_bytes = build_zip_bytes(&[("payload.bin", payload.as_slice())])?;
        assert!(zip_bytes.len() > MULTIPART_PART_SIZE, "zip payload must exceed multipart threshold");

        let create_output = client
            .create_multipart_upload()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("presigned-multipart-bundle.zip")
            .content_type("application/zip")
            .send()
            .await?;
        let upload_id = create_output.upload_id().expect("multipart upload id");

        let first_part = zip_bytes[..MULTIPART_PART_SIZE].to_vec();
        let second_part = zip_bytes[MULTIPART_PART_SIZE..].to_vec();

        let upload_part_1 = client
            .upload_part()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("presigned-multipart-bundle.zip")
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from(first_part))
            .send()
            .await?;

        let upload_part_2 = client
            .upload_part()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("presigned-multipart-bundle.zip")
            .upload_id(upload_id)
            .part_number(2)
            .body(ByteStream::from(second_part))
            .send()
            .await?;

        let completed_upload = CompletedMultipartUpload::builder()
            .parts(
                CompletedPart::builder()
                    .part_number(1)
                    .e_tag(upload_part_1.e_tag().unwrap_or_default())
                    .build(),
            )
            .parts(
                CompletedPart::builder()
                    .part_number(2)
                    .e_tag(upload_part_2.e_tag().unwrap_or_default())
                    .build(),
            )
            .build();

        client
            .complete_multipart_upload()
            .bucket(MULTIPART_ARCHIVE_TEST_BUCKET)
            .key("presigned-multipart-bundle.zip")
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        let object_url = format!("{}/{}/{}", env.url, MULTIPART_ARCHIVE_TEST_BUCKET, "presigned-multipart-bundle.zip");
        let direct_response =
            presigned_get_request_with_accept_encoding(&object_url, &env.access_key, &env.secret_key, "identity").await?;
        assert_eq!(direct_response.status(), StatusCode::OK);
        assert_eq!(
            direct_response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok()),
            Some(zip_bytes.len())
        );
        let direct_body = direct_response.bytes().await?;
        assert_eq!(direct_body.len(), zip_bytes.len());
        assert_eq!(direct_body.as_ref(), zip_bytes.as_slice());

        let signed = pre_sign_v4(
            http::Request::builder()
                .method(http::Method::GET)
                .uri(object_url.parse::<http::Uri>()?)
                .header(
                    HOST,
                    object_url
                        .parse::<http::Uri>()?
                        .authority()
                        .ok_or("request URL missing authority")?
                        .to_string(),
                )
                .body(Body::empty())?,
            &env.access_key,
            &env.secret_key,
            "",
            "us-east-1",
            600,
            OffsetDateTime::now_utc(),
        );
        let (proxy_url, proxy_handle) = spawn_reverse_proxy_to_presigned_url(signed.uri().to_string()).await?;
        let proxied_response: reqwest::Response = local_http_client().get(&proxy_url).send().await?;
        assert_eq!(proxied_response.status(), StatusCode::OK);
        assert_eq!(
            proxied_response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok()),
            Some(zip_bytes.len())
        );
        let proxied_body: bytes::Bytes = proxied_response.bytes().await?;
        assert_eq!(proxied_body.len(), zip_bytes.len());
        assert_eq!(proxied_body.as_ref(), zip_bytes.as_slice());

        proxy_handle.await??;
        env.stop_server();
        Ok(())
    }
}
