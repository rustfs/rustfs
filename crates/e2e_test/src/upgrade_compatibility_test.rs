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

use crate::common::{RustFSTestEnvironment, init_logging};
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, ServerSideEncryption, VersioningConfiguration,
};
use std::path::PathBuf;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const SOURCE_BINARY_ENV: &str = "RUSTFS_UPGRADE_SOURCE_BINARY";
const SSE_MASTER_KEY_ENV: &str = "RUSTFS_SSE_S3_MASTER_KEY";
const SSE_MASTER_KEY: &str = "QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI=";
const PLAIN_BUCKET: &str = "upgrade-plain-data";
const VERSIONED_BUCKET: &str = "upgrade-versioned-data";

fn source_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    let path = std::env::var_os(SOURCE_BINARY_ENV)
        .map(PathBuf::from)
        .ok_or("RUSTFS_UPGRADE_SOURCE_BINARY must point to the pinned previous release binary")?;
    if !path.is_file() {
        return Err(format!("upgrade source binary does not exist: {}", path.display()).into());
    }
    Ok(path)
}

async fn enable_versioning(client: &Client, bucket: &str) -> TestResult {
    let configuration = VersioningConfiguration::builder()
        .status(BucketVersioningStatus::Enabled)
        .build();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(configuration)
        .send()
        .await?;
    Ok(())
}

async fn read_object(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<(Option<ServerSideEncryption>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let Some(version_id) = version_id {
        request = request.version_id(version_id);
    }
    let response = request.send().await?;
    let encryption = response.server_side_encryption().cloned();
    let body = response.body.collect().await?.into_bytes().to_vec();
    Ok((encryption, body))
}

async fn write_multipart(client: &Client, bucket: &str, key: &str, parts: &[Vec<u8>]) -> TestResult {
    let created = client.create_multipart_upload().bucket(bucket).key(key).send().await?;
    let upload_id = created.upload_id().ok_or("CreateMultipartUpload omitted upload ID")?;
    let mut completed_parts = Vec::with_capacity(parts.len());

    for (index, part) in parts.iter().enumerate() {
        let part_number = i32::try_from(index + 1)?;
        let uploaded = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(part.clone()))
            .send()
            .await?;
        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_number)
                .e_tag(uploaded.e_tag().ok_or("UploadPart omitted ETag")?)
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build())
        .send()
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a pinned previous RustFS release binary"]
async fn direct_upgrade_from_rc2_preserves_object_contracts() -> TestResult {
    init_logging();
    let previous_binary = source_binary()?;
    let mut env = RustFSTestEnvironment::new().await?;
    let server_env = [(SSE_MASTER_KEY_ENV, SSE_MASTER_KEY)];
    env.start_rustfs_server_from_binary(&previous_binary, vec![], &server_env)
        .await?;

    let old_client = env.create_s3_client();
    env.create_test_bucket(PLAIN_BUCKET).await?;
    env.create_test_bucket(VERSIONED_BUCKET).await?;
    enable_versioning(&old_client, VERSIONED_BUCKET).await?;

    let plain_key = "plain-object";
    let plain_bytes = b"written by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(plain_key)
        .body(ByteStream::from_static(plain_bytes))
        .send()
        .await?;

    let encrypted_key = "sse-s3-object";
    let encrypted_bytes = b"encrypted by the previous RustFS release";
    old_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(encrypted_key)
        .server_side_encryption(ServerSideEncryption::Aes256)
        .body(ByteStream::from_static(encrypted_bytes))
        .send()
        .await?;

    let multipart_key = "multipart-object";
    let multipart_parts = vec![vec![b'a'; 5 * 1024 * 1024], b"final multipart bytes".to_vec()];
    let multipart_bytes = multipart_parts.concat();
    write_multipart(&old_client, PLAIN_BUCKET, multipart_key, &multipart_parts).await?;

    let versioned_key = "versioned-object";
    let version1_bytes = b"version one from the previous release";
    let version1 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version1_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("first versioned PUT omitted version ID")?
        .to_string();
    let version2_bytes = b"version two from the previous release";
    let version2 = old_client
        .put_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .body(ByteStream::from_static(version2_bytes))
        .send()
        .await?
        .version_id()
        .ok_or("second versioned PUT omitted version ID")?
        .to_string();
    let deleted = old_client
        .delete_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await?;
    assert_eq!(deleted.delete_marker(), Some(true));
    let delete_marker = deleted
        .version_id()
        .ok_or("versioned DELETE omitted delete marker version ID")?
        .to_string();

    env.restart_server_preserving_data(vec![], &server_env).await?;
    let current_client = env.create_s3_client();

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, plain_key, None).await?.1, plain_bytes);

    let (encryption, upgraded_encrypted_bytes) = read_object(&current_client, PLAIN_BUCKET, encrypted_key, None).await?;
    assert_eq!(encryption, Some(ServerSideEncryption::Aes256));
    assert_eq!(upgraded_encrypted_bytes, encrypted_bytes);

    assert_eq!(read_object(&current_client, PLAIN_BUCKET, multipart_key, None).await?.1, multipart_bytes);

    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version1))
            .await?
            .1,
        version1_bytes
    );
    assert_eq!(
        read_object(&current_client, VERSIONED_BUCKET, versioned_key, Some(&version2))
            .await?
            .1,
        version2_bytes
    );

    let current_read = current_client
        .get_object()
        .bucket(VERSIONED_BUCKET)
        .key(versioned_key)
        .send()
        .await
        .expect_err("the previous release's delete marker must remain current after upgrade");
    assert_eq!(current_read.raw_response().map(|response| response.status().as_u16()), Some(404));
    assert_eq!(current_read.as_service_error().and_then(ProvideErrorMetadata::code), Some("NoSuchKey"));

    let listed = current_client
        .list_object_versions()
        .bucket(VERSIONED_BUCKET)
        .prefix(versioned_key)
        .send()
        .await?;
    assert_eq!(listed.versions().len(), 2);
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version1.as_str()))
    );
    assert!(
        listed
            .versions()
            .iter()
            .any(|version| version.version_id() == Some(version2.as_str()))
    );
    assert_eq!(listed.delete_markers().len(), 1);
    assert_eq!(listed.delete_markers()[0].version_id(), Some(delete_marker.as_str()));
    assert_eq!(listed.delete_markers()[0].is_latest(), Some(true));

    let post_upgrade_key = "written-after-upgrade";
    let post_upgrade_bytes = b"written by the current RustFS build";
    current_client
        .put_object()
        .bucket(PLAIN_BUCKET)
        .key(post_upgrade_key)
        .body(ByteStream::from_static(post_upgrade_bytes))
        .send()
        .await?;
    assert_eq!(
        read_object(&current_client, PLAIN_BUCKET, post_upgrade_key, None).await?.1,
        post_upgrade_bytes
    );

    Ok(())
}
