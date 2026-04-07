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
    use crate::common::{RustFSTestEnvironment, init_logging, rustfs_binary_path};
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

    #[tokio::test]
    #[serial]
    async fn test_archive_put_rejects_content_encoding_by_default() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server_without_cleanup(vec![]).await?;
        env.create_test_bucket(ARCHIVE_TEST_BUCKET).await?;
        let zip_bytes = build_zip_bytes(&[("alpha.txt", b"archive-body")])?;
        let object_url = format!("{}/{}/{}", env.url, ARCHIVE_TEST_BUCKET, "bundle.zip");
        let response =
            signed_put_request_with_headers(&object_url, &env.access_key, &env.secret_key, zip_bytes, "application/zip", "gzip")
                .await?;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.text().await?;
        assert!(
            body.contains("InvalidArgument") || body.contains("Content-Encoding"),
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
}
