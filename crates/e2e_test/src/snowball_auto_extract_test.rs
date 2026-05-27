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
    use crate::common::{RustFSTestEnvironment, init_logging};
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use serial_test::serial;
    use std::error::Error;
    use std::io::Cursor;

    async fn build_test_archive() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));

        for dir in ["dir/", "empty-dir/"] {
            let mut header = tokio_tar::Header::new_gnu();
            header.set_entry_type(tokio_tar::EntryType::Directory);
            header.set_size(0);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append_data(&mut header, dir, Cursor::new(Vec::new())).await?;
        }

        for (path, data) in [
            ("dir/file.txt", b"nested payload\n".as_slice()),
            ("root.txt", b"root payload\n".as_slice()),
        ] {
            let mut header = tokio_tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, path, Cursor::new(data)).await?;
        }

        Ok(builder.into_inner().await?.into_inner())
    }

    async fn build_archive_with_invalid_entry() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));

        let mut valid_header = tokio_tar::Header::new_gnu();
        valid_header.set_size(b"valid-body".len() as u64);
        valid_header.set_mode(0o644);
        valid_header.set_cksum();
        builder
            .append_data(&mut valid_header, "valid.txt", Cursor::new(b"valid-body".as_slice()))
            .await?;

        let long_name = format!("{}.txt", "a".repeat(1100));
        let mut invalid_header = tokio_tar::Header::new_gnu();
        invalid_header.set_size(b"ignored-body".len() as u64);
        invalid_header.set_mode(0o644);
        invalid_header.set_cksum();
        builder
            .append_data(&mut invalid_header, long_name, Cursor::new(b"ignored-body".as_slice()))
            .await?;

        Ok(builder.into_inner().await?.into_inner())
    }

    fn build_archive_with_parent_dir_entry(victim_bucket: &str) -> Vec<u8> {
        let path = format!("../{victim_bucket}/evil-injected.txt");
        let data = b"injected-body";
        let mut header = [0u8; 512];

        header[..path.len()].copy_from_slice(path.as_bytes());
        header[100..108].copy_from_slice(b"0000644\0");
        header[108..116].copy_from_slice(b"0000000\0");
        header[116..124].copy_from_slice(b"0000000\0");
        let size = format!("{:011o}\0", data.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0");
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");

        let checksum: u32 = header.iter().map(|byte| *byte as u32).sum();
        let checksum = format!("{:06o}\0 ", checksum);
        header[148..156].copy_from_slice(checksum.as_bytes());

        let mut archive = Vec::new();
        archive.extend_from_slice(&header);
        archive.extend_from_slice(data);
        let padding = (512 - (data.len() % 512)) % 512;
        archive.extend(std::iter::repeat(0).take(padding));
        archive.extend_from_slice(&[0u8; 1024]);
        archive
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_supports_minio_prefix_and_directory_markers() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-prefix-test";
        let archive = build_test_archive().await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "/tenant-a/")
            .body(ByteStream::from(archive))
            .send()
            .await?;

        let root = client.get_object().bucket(bucket).key("tenant-a/root.txt").send().await?;
        assert_eq!(root.body.collect().await?.into_bytes().as_ref(), b"root payload\n");

        let nested = client.get_object().bucket(bucket).key("tenant-a/dir/file.txt").send().await?;
        assert_eq!(nested.body.collect().await?.into_bytes().as_ref(), b"nested payload\n");

        let dir_marker = client.head_object().bucket(bucket).key("tenant-a/empty-dir/").send().await?;
        assert_eq!(dir_marker.content_length(), Some(0));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_supports_standard_headers_with_combined_extract_options()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-standard-options";
        let extracted_prefix = "/tenant-standard/";

        client.create_bucket().bucket(bucket).send().await?;

        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));

        let mut dir_header = tokio_tar::Header::new_gnu();
        dir_header.set_entry_type(tokio_tar::EntryType::Directory);
        dir_header.set_size(0);
        dir_header.set_mode(0o755);
        dir_header.set_cksum();
        builder
            .append_data(&mut dir_header, "ignored-dir/", Cursor::new(Vec::new()))
            .await?;

        let mut valid_header = tokio_tar::Header::new_gnu();
        valid_header.set_size(b"standard-body".len() as u64);
        valid_header.set_mode(0o644);
        valid_header.set_cksum();
        builder
            .append_data(&mut valid_header, "valid.txt", Cursor::new(b"standard-body".as_slice()))
            .await?;

        let long_name = format!("{}.txt", "a".repeat(1100));
        let mut invalid_header = tokio_tar::Header::new_gnu();
        invalid_header.set_size(b"ignored-body".len() as u64);
        invalid_header.set_mode(0o644);
        invalid_header.set_cksum();
        builder
            .append_data(&mut invalid_header, long_name, Cursor::new(b"ignored-body".as_slice()))
            .await?;

        let archive = builder.into_inner().await?.into_inner();

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .body(ByteStream::from(archive))
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
                req.headers_mut().insert("x-amz-meta-snowball-prefix", extracted_prefix);
                req.headers_mut().insert("x-amz-meta-snowball-ignore-dirs", "true");
                req.headers_mut().insert("x-amz-meta-snowball-ignore-errors", "true");
            })
            .send()
            .await?;

        let valid = client
            .get_object()
            .bucket(bucket)
            .key("tenant-standard/valid.txt")
            .send()
            .await?;
        assert_eq!(valid.body.collect().await?.into_bytes().as_ref(), b"standard-body");

        let dir_err = client
            .head_object()
            .bucket(bucket)
            .key("tenant-standard/ignored-dir/")
            .send()
            .await
            .expect_err("directory marker should be skipped when standard ignore-dirs=true");
        let dir_service_err = dir_err.into_service_error();
        assert_eq!(dir_service_err.code(), Some("NotFound"));

        let listed = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("tenant-standard/")
            .send()
            .await?;
        let keys: Vec<_> = listed.contents().iter().filter_map(|entry| entry.key()).collect();
        assert_eq!(keys, vec!["tenant-standard/valid.txt"]);

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_ignores_directories_when_requested() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-ignore-dirs-default";
        let archive = build_test_archive().await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "tenant-b")
            .metadata("Minio-Snowball-Ignore-Dirs", "true")
            .body(ByteStream::from(archive))
            .send()
            .await?;

        let err = client
            .head_object()
            .bucket(bucket)
            .key("tenant-b/empty-dir/")
            .send()
            .await
            .expect_err("directory marker should be skipped when ignore-dirs=true");
        let service_err = err.into_service_error();
        assert_eq!(service_err.code(), Some("NotFound"));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_ignores_invalid_entries_when_requested() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-ignore-errors";
        let archive = build_archive_with_invalid_entry().await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "tenant-c")
            .metadata("Minio-Snowball-Ignore-Errors", "true")
            .body(ByteStream::from(archive))
            .send()
            .await?;

        let valid = client.get_object().bucket(bucket).key("tenant-c/valid.txt").send().await?;
        assert_eq!(valid.body.collect().await?.into_bytes().as_ref(), b"valid-body");

        let listed = client.list_objects_v2().bucket(bucket).prefix("tenant-c/").send().await?;
        let keys: Vec<_> = listed.contents().iter().filter_map(|entry| entry.key()).collect();
        assert_eq!(keys, vec!["tenant-c/valid.txt"]);

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_rejects_parent_dir_entry_without_cross_bucket_write()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let attacker_bucket = "snowball-traversal-source";
        let victim_bucket = "snowball-traversal-victim";
        let archive = build_archive_with_parent_dir_entry(victim_bucket);

        client.create_bucket().bucket(attacker_bucket).send().await?;
        client.create_bucket().bucket(victim_bucket).send().await?;

        let err = client
            .put_object()
            .bucket(attacker_bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(archive))
            .send()
            .await
            .expect_err("parent directory archive entry should be rejected");
        let service_err = err.into_service_error();
        assert_eq!(service_err.code(), Some("InvalidArgument"));

        let victim_err = client
            .head_object()
            .bucket(victim_bucket)
            .key("evil-injected.txt")
            .send()
            .await
            .expect_err("rejected archive entry must not write into the victim bucket");
        let victim_service_err = victim_err.into_service_error();
        assert_eq!(victim_service_err.code(), Some("NotFound"));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn snowball_auto_extract_prefers_exact_minio_prefix_over_suffix_fallback() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-prefix-precedence";
        let archive = build_test_archive().await?;

        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .body(ByteStream::from(archive))
            .customize()
            .mutate_request(|req| {
                req.headers_mut().insert("x-amz-meta-snowball-auto-extract", "true");
                req.headers_mut()
                    .insert("x-amz-meta-acme-snowball-prefix", "/tenant-fallback/");
                req.headers_mut().insert("x-amz-meta-minio-snowball-prefix", "/tenant-exact/");
            })
            .send()
            .await?;

        let exact = client.get_object().bucket(bucket).key("tenant-exact/root.txt").send().await?;
        assert_eq!(exact.body.collect().await?.into_bytes().as_ref(), b"root payload\n");

        let fallback_err = client
            .head_object()
            .bucket(bucket)
            .key("tenant-fallback/root.txt")
            .send()
            .await
            .expect_err("fallback suffix header should not override exact MinIO prefix");
        let fallback_service_err = fallback_err.into_service_error();
        assert_eq!(fallback_service_err.code(), Some("NotFound"));

        env.stop_server();
        Ok(())
    }
}
