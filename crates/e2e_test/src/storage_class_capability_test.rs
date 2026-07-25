// Copyright 2026 RustFS Team
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

//! Truthful storage-class write and discovery contract regressions.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{ObjectAttributes, StorageClass};
    use http::header::HOST;
    use reqwest::StatusCode;
    use rustfs_signer::constants::UNSIGNED_PAYLOAD;
    use rustfs_signer::sign_v4;
    use s3s::Body;
    use serde_json::Value;
    use std::error::Error;
    use std::path::Path;

    const UNSUPPORTED_AWS_CLASSES: [&str; 9] = [
        "DEEP_ARCHIVE",
        "EXPRESS_ONEZONE",
        "GLACIER",
        "GLACIER_IR",
        "INTELLIGENT_TIERING",
        "ONEZONE_IA",
        "OUTPOSTS",
        "SNOW",
        "STANDARD_IA",
    ];

    async fn assert_object_storage_class(
        client: &Client,
        bucket: &str,
        key: &str,
        expected: &str,
        body: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let head = client.head_object().bucket(bucket).key(key).send().await?;
        let expected_head = (expected != "STANDARD").then_some(expected);
        assert_eq!(
            head.storage_class().map(StorageClass::as_str),
            expected_head,
            "HeadObject must omit implicit STANDARD and report RRS"
        );

        let listed = client.list_objects_v2().bucket(bucket).prefix(key).send().await?;
        let object = listed
            .contents()
            .iter()
            .find(|object| object.key() == Some(key))
            .ok_or("object missing from ListObjectsV2")?;
        assert_eq!(object.storage_class().map(|storage_class| storage_class.as_str()), Some(expected));

        let get = client.get_object().bucket(bucket).key(key).send().await?;
        assert_eq!(
            get.storage_class().map(StorageClass::as_str),
            expected_head,
            "GetObject must report the same effective storage class as HeadObject"
        );
        let downloaded = get.body.collect().await?.into_bytes();
        assert_eq!(downloaded.as_ref(), body, "storage-class selection must not alter object bytes");
        Ok(())
    }

    async fn mutate_xl_meta(
        root: &str,
        bucket: &str,
        key: &str,
        mutate: impl FnOnce(&mut rustfs_filemeta::MetaObject),
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = Path::new(root).join(bucket).join(key).join("xl.meta");
        let bytes = tokio::fs::read(&path).await?;
        let mut file_meta = rustfs_filemeta::FileMeta::load(&bytes)?;
        let (index, mut version) = file_meta.find_version(None)?;
        let object = version.object.as_mut().ok_or("fixture version is not an object")?;
        mutate(object);
        file_meta.versions[index] = rustfs_filemeta::FileMetaShallowVersion::try_from(version)?;
        tokio::fs::write(path, file_meta.marshal_msg()?).await?;
        Ok(())
    }

    async fn signed_admin_get(
        env: &RustFSTestEnvironment,
        path: &str,
    ) -> Result<reqwest::Response, Box<dyn Error + Send + Sync>> {
        let url = format!("{}{path}", env.url);
        let uri = url.parse::<http::Uri>()?;
        let authority = uri.authority().ok_or("admin URL missing authority")?.to_string();
        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())?;
        let signed = sign_v4(request, 0, &env.access_key, &env.secret_key, "", "us-east-1");

        let mut request = local_http_client().get(&url);
        for (name, value) in signed.headers() {
            request = request.header(name, value);
        }
        Ok(request.send().await?)
    }

    #[tokio::test]
    async fn standard_and_rrs_are_supported_across_put_copy_and_multipart() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "storage-class-supported-contract";
        env.create_test_bucket(bucket).await?;

        client
            .put_object()
            .bucket(bucket)
            .key("copy-source")
            .body(ByteStream::from_static(b"copy-source-body"))
            .send()
            .await?;

        for storage_class in [StorageClass::Standard, StorageClass::ReducedRedundancy] {
            let class_name = storage_class.as_str().to_string();
            let put_key = format!("put-{class_name}");
            let put_body = format!("put-body-{class_name}").into_bytes();
            client
                .put_object()
                .bucket(bucket)
                .key(&put_key)
                .storage_class(storage_class.clone())
                .body(ByteStream::from(put_body.clone()))
                .send()
                .await?;
            assert_object_storage_class(&client, bucket, &put_key, &class_name, &put_body).await?;

            let copy_key = format!("copy-{class_name}");
            client
                .copy_object()
                .bucket(bucket)
                .key(&copy_key)
                .copy_source(format!("{bucket}/copy-source"))
                .storage_class(storage_class.clone())
                .send()
                .await?;
            assert_object_storage_class(&client, bucket, &copy_key, &class_name, b"copy-source-body").await?;

            let multipart_key = format!("multipart-{class_name}");
            let created = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&multipart_key)
                .storage_class(storage_class)
                .send()
                .await?;
            let upload_id = created.upload_id().ok_or("CreateMultipartUpload returned no upload ID")?;
            let parts = client
                .list_parts()
                .bucket(bucket)
                .key(&multipart_key)
                .upload_id(upload_id)
                .send()
                .await?;
            assert_eq!(parts.storage_class().map(StorageClass::as_str), Some(class_name.as_str()));
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(&multipart_key)
                .upload_id(upload_id)
                .send()
                .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn label_only_aws_classes_fail_before_put_copy_or_multipart_mutation() -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "storage-class-unsupported-contract";
        env.create_test_bucket(bucket).await?;

        for key in ["put-guard", "copy-source", "copy-guard"] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(format!("original-{key}").into_bytes()))
                .send()
                .await?;
        }

        for unsupported in UNSUPPORTED_AWS_CLASSES {
            let put_error = client
                .put_object()
                .bucket(bucket)
                .key("put-guard")
                .storage_class(StorageClass::from(unsupported))
                .body(ByteStream::from(format!("rejected-put-{unsupported}").into_bytes()))
                .send()
                .await
                .expect_err("label-only PUT storage class must be rejected");
            assert_eq!(
                put_error.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("InvalidStorageClass"),
                "PUT returned a different error for {unsupported}"
            );

            let copy_error = client
                .copy_object()
                .bucket(bucket)
                .key("copy-guard")
                .copy_source(format!("{bucket}/copy-source"))
                .storage_class(StorageClass::from(unsupported))
                .send()
                .await
                .expect_err("label-only CopyObject storage class must be rejected");
            assert_eq!(
                copy_error.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("InvalidStorageClass"),
                "CopyObject returned a different error for {unsupported}"
            );

            let multipart_key = format!("multipart-{unsupported}");
            let multipart_error = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(&multipart_key)
                .storage_class(StorageClass::from(unsupported))
                .send()
                .await
                .expect_err("label-only CreateMultipartUpload storage class must be rejected");
            assert_eq!(
                multipart_error.as_service_error().and_then(ProvideErrorMetadata::code),
                Some("InvalidStorageClass"),
                "CreateMultipartUpload returned a different error for {unsupported}"
            );
        }

        let put_guard = client
            .get_object()
            .bucket(bucket)
            .key("put-guard")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert_eq!(put_guard.as_ref(), b"original-put-guard");

        let copy_guard = client
            .get_object()
            .bucket(bucket)
            .key("copy-guard")
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        assert_eq!(copy_guard.as_ref(), b"original-copy-guard");

        let uploads = client.list_multipart_uploads().bucket(bucket).send().await?;
        assert!(uploads.uploads().is_empty(), "unsupported classes must not create multipart sessions");
        Ok(())
    }

    #[tokio::test]
    async fn historical_label_only_metadata_is_standard_without_hiding_a_real_transition_tier()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let client = env.create_s3_client();
        let bucket = "storage-class-historical-contract";
        let legacy_key = "legacy-label-only";
        let transitioned_key = "real-transition-tier";
        env.create_test_bucket(bucket).await?;

        for key in [legacy_key, transitioned_key] {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from_static(b"fixture-body"))
                .send()
                .await?;
        }

        env.stop_server();
        mutate_xl_meta(&env.temp_dir, bucket, legacy_key, |object| {
            object
                .meta_user
                .insert("x-amz-storage-class".to_string(), "STANDARD_IA".to_string());
        })
        .await?;
        mutate_xl_meta(&env.temp_dir, bucket, transitioned_key, |object| {
            object.set_transition(&rustfs_filemeta::FileInfo {
                transition_status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
                transition_tier: "STANDARD_IA".to_string(),
                ..Default::default()
            });
        })
        .await?;
        env.restart_server_preserving_data(Vec::new(), &[]).await?;

        assert_object_storage_class(&client, bucket, legacy_key, "STANDARD", b"fixture-body").await?;

        let legacy_attributes = client
            .get_object_attributes()
            .bucket(bucket)
            .key(legacy_key)
            .object_attributes(ObjectAttributes::StorageClass)
            .send()
            .await?;
        assert_eq!(legacy_attributes.storage_class().map(StorageClass::as_str), Some("STANDARD"));

        let versions = client.list_object_versions().bucket(bucket).prefix(legacy_key).send().await?;
        let legacy_version = versions
            .versions()
            .iter()
            .find(|version| version.key() == Some(legacy_key))
            .ok_or("legacy fixture missing from ListObjectVersions")?;
        assert_eq!(legacy_version.storage_class().map(|class| class.as_str()), Some("STANDARD"));

        let transitioned_head = client.head_object().bucket(bucket).key(transitioned_key).send().await?;
        assert_eq!(transitioned_head.storage_class().map(StorageClass::as_str), Some("STANDARD_IA"));
        let transitioned_attributes = client
            .get_object_attributes()
            .bucket(bucket)
            .key(transitioned_key)
            .object_attributes(ObjectAttributes::StorageClass)
            .send()
            .await?;
        assert_eq!(transitioned_attributes.storage_class().map(StorageClass::as_str), Some("STANDARD_IA"));
        let transitioned_list = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(transitioned_key)
            .send()
            .await?;
        assert_eq!(
            transitioned_list.contents()[0].storage_class().map(|class| class.as_str()),
            Some("STANDARD_IA")
        );
        let transitioned_versions = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(transitioned_key)
            .send()
            .await?;
        assert_eq!(
            transitioned_versions.versions()[0]
                .storage_class()
                .map(|class| class.as_str()),
            Some("STANDARD_IA")
        );
        Ok(())
    }

    #[tokio::test]
    async fn authenticated_runtime_capabilities_publish_the_versioned_storage_class_contract()
    -> Result<(), Box<dyn Error + Send + Sync>> {
        init_logging();
        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(Vec::new()).await?;
        let path = "/rustfs/admin/v4/runtime/capabilities";

        let unsigned = local_http_client().get(format!("{}{path}", env.url)).send().await?;
        assert_eq!(unsigned.status(), StatusCode::FORBIDDEN);
        let unsigned_body = unsigned.text().await?;
        assert!(
            !unsigned_body.contains("supported_write_classes"),
            "the capability contract must not bypass admin authentication"
        );

        let response = signed_admin_get(&env, path).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = response.json().await?;
        assert_eq!(body["storage_classes"]["contract_version"], 1);
        assert_eq!(
            body["storage_classes"]["supported_write_classes"],
            serde_json::json!(["STANDARD", "REDUCED_REDUNDANCY"])
        );
        assert_eq!(body["storage_classes"]["unsupported_write_error"], "InvalidStorageClass");
        assert_eq!(body["storage_classes"]["legacy_label_behavior"], "normalized_to_effective_class");
        Ok(())
    }
}
