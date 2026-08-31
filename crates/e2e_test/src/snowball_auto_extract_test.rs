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
    use flate2::{Compression, write::GzEncoder};
    use std::error::Error;
    use std::io::{Cursor, Write};

    fn pax_record(key: &str, value: &str) -> Vec<u8> {
        let payload = format!("{key}={value}\n");
        let mut len = payload.len() + 3;
        loop {
            let record = format!("{len} {payload}");
            if record.len() == len {
                return record.into_bytes();
            }
            len = record.len();
        }
    }

    async fn append_pax_header(
        builder: &mut tokio_tar::Builder<Cursor<Vec<u8>>>,
        entry_type: tokio_tar::EntryType,
        records: &[(&str, &str)],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut payload = Vec::new();
        for (key, value) in records {
            payload.extend(pax_record(key, value));
        }
        let mut header = tokio_tar::Header::new_ustar();
        header.set_entry_type(entry_type);
        header.set_size(u64::try_from(payload.len()).expect("PAX payload length should fit in u64"));
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "PaxHeaders.X/snowball", Cursor::new(payload))
            .await?;
        Ok(())
    }

    async fn append_typed_entry(
        builder: &mut tokio_tar::Builder<Cursor<Vec<u8>>>,
        path: &str,
        entry_type: tokio_tar::EntryType,
        body: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut header = tokio_tar::Header::new_gnu();
        header.set_entry_type(entry_type);
        header.set_size(u64::try_from(body.len()).expect("TAR member length should fit in u64"));
        header.set_mode(0o644);
        header.set_cksum();
        builder.append_data(&mut header, path, Cursor::new(body)).await?;
        Ok(())
    }

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

    async fn build_archive_with_invalid_checksum() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut archive = build_test_archive().await?;
        archive[0] ^= 1;
        Ok(archive)
    }

    async fn build_archive_with_negative_gnu_mtime() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
        let mut header = tokio_tar::Header::new_gnu();
        header.set_size(b"negative-mtime-body".len() as u64);
        header.set_mode(0o644);
        header.as_old_mut().mtime.fill(0xff);
        builder
            .append_data(&mut header, "negative-mtime.txt", Cursor::new(b"negative-mtime-body".as_slice()))
            .await?;
        Ok(builder.into_inner().await?.into_inner())
    }

    fn gzip_member(payload: &[u8]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload)?;
        Ok(encoder.finish()?)
    }

    async fn build_concatenated_gzip_archive() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let archive = build_test_archive().await?;
        let split_at = archive.len() / 2;
        let mut encoded = gzip_member(&archive[..split_at])?;
        encoded.extend(gzip_member(&archive[split_at..])?);
        Ok(encoded)
    }

    async fn build_gzip_archive_with_invalid_crc() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut encoded = gzip_member(&build_test_archive().await?)?;
        let crc_offset = encoded.len().checked_sub(8).expect("gzip fixture must contain a trailer");
        encoded[crc_offset] ^= 1;
        Ok(encoded)
    }

    fn append_raw_tar_entry_with_type(archive: &mut Vec<u8>, path: &[u8], data: &[u8], entry_type: u8) {
        assert!(path.len() <= 100, "raw TAR fixture path must fit in the name field");
        let mut header = [0u8; 512];

        header[..path.len()].copy_from_slice(path);
        header[100..108].copy_from_slice(b"0000644\0");
        header[108..116].copy_from_slice(b"0000000\0");
        header[116..124].copy_from_slice(b"0000000\0");
        let size = format!("{:011o}\0", data.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0");
        header[148..156].fill(b' ');
        header[156] = entry_type;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");

        let checksum: u32 = header.iter().map(|byte| *byte as u32).sum();
        let checksum = format!("{:06o}\0 ", checksum);
        header[148..156].copy_from_slice(checksum.as_bytes());

        archive.extend_from_slice(&header);
        archive.extend_from_slice(data);
        let padding = (512 - (data.len() % 512)) % 512;
        archive.extend(std::iter::repeat_n(0, padding));
    }

    fn append_raw_tar_entry(archive: &mut Vec<u8>, path: &[u8], data: &[u8]) {
        append_raw_tar_entry_with_type(archive, path, data, b'0');
    }

    fn build_archive_with_parent_dir_entry(victim_bucket: &str) -> Vec<u8> {
        let path = format!("../{victim_bucket}/evil-injected.txt");
        let mut archive = Vec::new();
        append_raw_tar_entry(&mut archive, path.as_bytes(), b"injected-body");
        archive.extend_from_slice(&[0u8; 1024]);
        archive
    }

    async fn build_member_semantics_archive() -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
        append_pax_header(
            &mut builder,
            tokio_tar::EntryType::XGlobalHeader,
            &[
                ("minio.metadata.x-amz-meta-owner", "global"),
                ("minio.metadata.x-amz-meta-snowball-auto-extract", "true"),
            ],
        )
        .await?;
        append_pax_header(
            &mut builder,
            tokio_tar::EntryType::XHeader,
            &[("minio.metadata.x-amz-meta-owner", "local")],
        )
        .await?;
        append_typed_entry(&mut builder, "regular.txt", tokio_tar::EntryType::Regular, b"regular-body").await?;
        for (path, entry_type) in [
            ("char", tokio_tar::EntryType::Char),
            ("block", tokio_tar::EntryType::Block),
            ("fifo", tokio_tar::EntryType::Fifo),
        ] {
            append_typed_entry(&mut builder, path, entry_type, b"").await?;
        }
        let mut directory = tokio_tar::Header::new_gnu();
        directory.set_entry_type(tokio_tar::EntryType::Directory);
        directory.set_size(0);
        directory.set_mode(0o755);
        directory.set_cksum();
        builder
            .append_data(&mut directory, "directory/", Cursor::new(Vec::new()))
            .await?;
        for (path, entry_type) in [
            ("hard-link", tokio_tar::EntryType::Link),
            ("symlink", tokio_tar::EntryType::Symlink),
            ("continuous", tokio_tar::EntryType::Continuous),
            ("unknown", tokio_tar::EntryType::Other(b'9')),
        ] {
            append_typed_entry(&mut builder, path, entry_type, b"").await?;
        }
        Ok(builder.into_inner().await?.into_inner())
    }

    async fn build_versioned_member_archive(path: &str, version_id: &str) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut builder = tokio_tar::Builder::new(Cursor::new(Vec::new()));
        append_pax_header(&mut builder, tokio_tar::EntryType::XHeader, &[("minio.versionId", version_id)]).await?;
        append_typed_entry(&mut builder, path, tokio_tar::EntryType::Regular, b"versioned-body").await?;
        Ok(builder.into_inner().await?.into_inner())
    }

    fn build_archive_with_invalid_utf8_entry() -> Vec<u8> {
        let mut archive = Vec::new();
        append_raw_tar_entry(&mut archive, b"invalid-\xff.txt", b"ignored-body");
        append_raw_tar_entry(&mut archive, b"valid.txt", b"valid-body");
        archive.extend_from_slice(&[0u8; 1024]);
        archive
    }

    fn build_archive_with_invalid_utf8_symlink() -> Vec<u8> {
        let mut archive = Vec::new();
        append_raw_tar_entry_with_type(&mut archive, b"invalid-\xff-link", b"", b'2');
        append_raw_tar_entry(&mut archive, b"valid.txt", b"valid-body");
        archive.extend_from_slice(&[0u8; 1024]);
        archive
    }

    #[tokio::test]
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
    async fn snowball_auto_extract_applies_member_semantics_and_metadata_precedence() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-member-semantics";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Prefix", "members")
            .metadata("owner", "outer")
            .body(ByteStream::from(build_member_semantics_archive().await?))
            .send()
            .await?;

        let regular = client.head_object().bucket(bucket).key("members/regular.txt").send().await?;
        let regular_metadata = regular.metadata().expect("regular member should expose metadata");
        assert_eq!(regular_metadata.get("owner").map(String::as_str), Some("local"));
        assert!(!regular_metadata.contains_key("snowball-auto-extract"));
        assert!(!regular_metadata.contains_key("minio-snowball-prefix"));

        for key in ["char", "block", "fifo"] {
            let head = client
                .head_object()
                .bucket(bucket)
                .key(format!("members/{key}"))
                .send()
                .await?;
            assert_eq!(head.content_length(), Some(0), "{key} should be materialized as an empty object");
            assert_eq!(
                head.metadata().and_then(|metadata| metadata.get("owner")).map(String::as_str),
                Some("outer"),
                "{key} should not inherit global PAX metadata"
            );
        }
        let directory = client.head_object().bucket(bucket).key("members/directory/").send().await?;
        assert_eq!(directory.content_length(), Some(0));

        for key in ["hard-link", "symlink", "continuous", "unknown"] {
            let error = client
                .head_object()
                .bucket(bucket)
                .key(format!("members/{key}"))
                .send()
                .await
                .expect_err("unsupported TAR entry type must be skipped");
            assert_eq!(error.into_service_error().code(), Some("NotFound"), "{key}");
        }

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_validates_pax_version_id_against_bucket_state() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-version-semantics";
        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("null.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(build_versioned_member_archive("null.txt", "null").await?))
            .send()
            .await?;
        let null_member = client.get_object().bucket(bucket).key("null.txt").send().await?;
        assert_eq!(null_member.body.collect().await?.into_bytes().as_ref(), b"versioned-body");

        for (archive_key, member_key, version_id) in [
            ("uuid.tar", "uuid.txt", uuid::Uuid::new_v4().to_string()),
            ("uppercase-null.tar", "uppercase-null.txt", "NULL".to_string()),
        ] {
            let error = client
                .put_object()
                .bucket(bucket)
                .key(archive_key)
                .metadata("Snowball-Auto-Extract", "true")
                .body(ByteStream::from(build_versioned_member_archive(member_key, &version_id).await?))
                .send()
                .await
                .expect_err("invalid or unversioned UUID import must be rejected");
            assert_eq!(error.into_service_error().code(), Some("InvalidArgument"), "{archive_key}");
            let missing = client
                .head_object()
                .bucket(bucket)
                .key(member_key)
                .send()
                .await
                .expect_err("rejected version import must not create an object");
            assert_eq!(missing.into_service_error().code(), Some("NotFound"), "{member_key}");
        }

        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        let imported_version_id = uuid::Uuid::new_v4().to_string();
        client
            .put_object()
            .bucket(bucket)
            .key("versioned-uuid.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(
                build_versioned_member_archive("versioned-uuid.txt", &imported_version_id).await?,
            ))
            .send()
            .await?;
        let imported = client
            .get_object()
            .bucket(bucket)
            .key("versioned-uuid.txt")
            .version_id(&imported_version_id)
            .send()
            .await?;
        assert_eq!(imported.version_id(), Some(imported_version_id.as_str()));
        assert_eq!(imported.body.collect().await?.into_bytes().as_ref(), b"versioned-body");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
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
    async fn snowball_auto_extract_accepts_negative_gnu_mtime() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-negative-mtime";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(build_archive_with_negative_gnu_mtime().await?))
            .send()
            .await?;

        let object = client.get_object().bucket(bucket).key("negative-mtime.txt").send().await?;
        assert_eq!(object.body.collect().await?.into_bytes().as_ref(), b"negative-mtime-body");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_consumes_concatenated_gzip_members() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-concatenated-gzip";
        client.create_bucket().bucket(bucket).send().await?;
        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar.gz")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(build_concatenated_gzip_archive().await?))
            .send()
            .await?;

        let object = client.get_object().bucket(bucket).key("root.txt").send().await?;
        assert_eq!(object.body.collect().await?.into_bytes().as_ref(), b"root payload\n");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_rejects_gzip_crc_error_when_ignore_errors_enabled() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-gzip-crc-ignore-errors";
        client.create_bucket().bucket(bucket).send().await?;
        let err = client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar.gz")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Ignore-Errors", "true")
            .body(ByteStream::from(build_gzip_archive_with_invalid_crc().await?))
            .send()
            .await
            .expect_err("gzip integrity failures must remain fatal under ignore-errors");

        assert_eq!(err.into_service_error().code(), Some("InvalidArgument"));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_rejects_mismatched_content_md5() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-content-md5";
        client.create_bucket().bucket(bucket).send().await?;
        let err = client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .content_md5("AAAAAAAAAAAAAAAAAAAAAA==")
            .body(ByteStream::from(build_test_archive().await?))
            .send()
            .await
            .expect_err("mismatched Content-MD5 must fail after the raw body reaches EOF");

        assert_eq!(err.into_service_error().code(), Some("BadDigest"));

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
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
    async fn snowball_auto_extract_skips_non_utf8_symlink_without_ignore_errors() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-invalid-utf8-link";
        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .body(ByteStream::from(build_archive_with_invalid_utf8_symlink()))
            .send()
            .await?;

        let valid = client.get_object().bucket(bucket).key("valid.txt").send().await?;
        assert_eq!(valid.body.collect().await?.into_bytes().as_ref(), b"valid-body");
        let listed = client.list_objects_v2().bucket(bucket).send().await?;
        let keys: Vec<_> = listed.contents().iter().filter_map(|entry| entry.key()).collect();
        assert_eq!(keys, vec!["valid.txt"]);

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_skips_non_utf8_member_without_lossy_key_collision() -> Result<(), Box<dyn Error + Send + Sync>>
    {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-invalid-utf8";
        client.create_bucket().bucket(bucket).send().await?;

        client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Ignore-Errors", "true")
            .body(ByteStream::from(build_archive_with_invalid_utf8_entry()))
            .send()
            .await?;

        let valid = client.get_object().bucket(bucket).key("valid.txt").send().await?;
        assert_eq!(valid.body.collect().await?.into_bytes().as_ref(), b"valid-body");
        let listed = client.list_objects_v2().bucket(bucket).send().await?;
        let keys: Vec<_> = listed.contents().iter().filter_map(|entry| entry.key()).collect();
        assert_eq!(keys, vec!["valid.txt"]);

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_rejects_corrupt_tar_when_ignore_errors_enabled() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;

        let client = env.create_s3_client();
        let bucket = "snowball-corrupt-ignore-errors";
        let archive = build_archive_with_invalid_checksum().await?;
        client.create_bucket().bucket(bucket).send().await?;

        let err = client
            .put_object()
            .bucket(bucket)
            .key("fixture.tar")
            .metadata("Snowball-Auto-Extract", "true")
            .metadata("Minio-Snowball-Ignore-Errors", "true")
            .body(ByteStream::from(archive))
            .send()
            .await
            .expect_err("corrupt TAR structure must remain fatal under ignore-errors");
        assert_eq!(err.into_service_error().code(), Some("InvalidArgument"));

        let listed = client.list_objects_v2().bucket(bucket).send().await?;
        assert!(listed.contents().is_empty(), "corrupt archive must not produce objects");

        env.stop_server();
        Ok(())
    }

    #[tokio::test]
    async fn snowball_auto_extract_rejects_parent_dir_entry_even_when_ignore_errors_enabled()
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
            .metadata("Minio-Snowball-Ignore-Errors", "true")
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
