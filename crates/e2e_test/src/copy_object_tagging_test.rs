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

//! CopyObject tagging directive regression tests.

#[cfg(test)]
mod tests {
    use crate::common::{RustFSTestEnvironment, init_logging, local_http_client};
    use aws_sdk_s3::Client;
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, MetadataDirective, TaggingDirective, VersioningConfiguration};
    use serial_test::serial;
    use std::collections::BTreeMap;

    async fn object_tags(client: &Client, bucket: &str, key: &str) -> BTreeMap<String, String> {
        client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("GetObjectTagging should succeed")
            .tag_set()
            .iter()
            .map(|tag| (tag.key().to_string(), tag.value().to_string()))
            .collect()
    }

    #[tokio::test]
    #[serial]
    async fn copy_object_applies_copy_replace_and_empty_tagging_directives() {
        init_logging();
        let mut env = RustFSTestEnvironment::new()
            .await
            .expect("test environment should initialize");
        env.start_rustfs_server(vec![]).await.expect("RustFS should start");

        let client = env.create_s3_client();
        let bucket = "copy-object-tagging-directive";
        let source = "source.txt";

        client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("bucket creation should succeed");
        client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .expect("versioning should be enabled");

        let first_version = client
            .put_object()
            .bucket(bucket)
            .key(source)
            .tagging("project=rustfs&stage=first")
            .body(ByteStream::from_static(b"first"))
            .send()
            .await
            .expect("first source version should be written")
            .version_id()
            .expect("versioned PUT should return a version ID")
            .to_string();

        client
            .put_object()
            .bucket(bucket)
            .key(source)
            .tagging("project=rustfs&stage=current")
            .body(ByteStream::from_static(b"current"))
            .send()
            .await
            .expect("current source version should be written");

        client
            .copy_object()
            .bucket(bucket)
            .key("default-copy.txt")
            .copy_source(format!("{bucket}/{source}"))
            .send()
            .await
            .expect("default CopyObject should preserve current source tags");
        assert_eq!(
            object_tags(&client, bucket, "default-copy.txt").await,
            BTreeMap::from([
                ("project".to_string(), "rustfs".to_string()),
                ("stage".to_string(), "current".to_string()),
            ])
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("explicit-copy.txt")
            .copy_source(format!("{bucket}/{source}?versionId={first_version}"))
            .tagging_directive(TaggingDirective::Copy)
            .send()
            .await
            .expect("COPY should preserve the selected historical version's tags");
        assert_eq!(
            object_tags(&client, bucket, "explicit-copy.txt").await,
            BTreeMap::from([
                ("project".to_string(), "rustfs".to_string()),
                ("stage".to_string(), "first".to_string()),
            ])
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("replace.txt")
            .copy_source(format!("{bucket}/{source}"))
            .tagging_directive(TaggingDirective::Replace)
            .tagging("project=cli&label=copy%20test")
            .send()
            .await
            .expect("REPLACE should atomically apply requested tags");
        assert_eq!(
            object_tags(&client, bucket, "replace.txt").await,
            BTreeMap::from([
                ("label".to_string(), "copy test".to_string()),
                ("project".to_string(), "cli".to_string()),
            ])
        );
        let replace_head = client
            .head_object()
            .bucket(bucket)
            .key("replace.txt")
            .send()
            .await
            .expect("HEAD should succeed after tag replacement");
        assert_eq!(replace_head.tag_count(), Some(2));

        client
            .copy_object()
            .bucket(bucket)
            .key("empty-replace.txt")
            .copy_source(format!("{bucket}/{source}"))
            .tagging_directive(TaggingDirective::Replace)
            .send()
            .await
            .expect("REPLACE without Tagging should clear the destination tag set");
        assert!(object_tags(&client, bucket, "empty-replace.txt").await.is_empty());
        let empty_head = client
            .head_object()
            .bucket(bucket)
            .key("empty-replace.txt")
            .send()
            .await
            .expect("HEAD should succeed after empty tag replacement");
        assert_eq!(empty_head.tag_count(), None);

        client
            .copy_object()
            .bucket(bucket)
            .key("metadata-replace-tag-copy.txt")
            .copy_source(format!("{bucket}/{source}"))
            .metadata_directive(MetadataDirective::Replace)
            .metadata("updated", "true")
            .send()
            .await
            .expect("metadata REPLACE must preserve tags under the default COPY directive");
        assert_eq!(
            object_tags(&client, bucket, "metadata-replace-tag-copy.txt").await,
            BTreeMap::from([
                ("project".to_string(), "rustfs".to_string()),
                ("stage".to_string(), "current".to_string()),
            ])
        );

        client
            .copy_object()
            .bucket(bucket)
            .key("combined-replace.txt")
            .copy_source(format!("{bucket}/{source}"))
            .metadata_directive(MetadataDirective::Replace)
            .metadata("updated", "true")
            .tagging_directive(TaggingDirective::Replace)
            .tagging("project=combined")
            .send()
            .await
            .expect("metadata and tagging REPLACE directives must be independent");
        assert_eq!(
            object_tags(&client, bucket, "combined-replace.txt").await,
            BTreeMap::from([("project".to_string(), "combined".to_string())])
        );

        client
            .copy_object()
            .bucket(bucket)
            .key(source)
            .copy_source(format!("{bucket}/{source}"))
            .tagging_directive(TaggingDirective::Replace)
            .tagging("project=self-copy")
            .send()
            .await
            .expect("self-copy with tag replacement should update tags atomically");
        assert_eq!(
            object_tags(&client, bucket, source).await,
            BTreeMap::from([("project".to_string(), "self-copy".to_string())])
        );
        let self_copy_body = client
            .get_object()
            .bucket(bucket)
            .key(source)
            .send()
            .await
            .expect("self-copy destination should remain readable")
            .body
            .collect()
            .await
            .expect("self-copy body should be complete")
            .into_bytes();
        assert_eq!(self_copy_body.as_ref(), b"current", "tag-only self-copy must preserve the object body");

        client
            .put_object()
            .bucket(bucket)
            .key("malformed.txt")
            .tagging("state=original")
            .body(ByteStream::from_static(b"original destination"))
            .send()
            .await
            .expect("preexisting malformed-test destination should be written");

        let malformed = client
            .copy_object()
            .bucket(bucket)
            .key("malformed.txt")
            .copy_source(format!("{bucket}/{source}"))
            .tagging_directive(TaggingDirective::Replace)
            .tagging("project=rustfs%ZZ")
            .send()
            .await
            .expect_err("malformed tags must fail CopyObject");
        assert_eq!(malformed.as_service_error().and_then(ProvideErrorMetadata::code), Some("InvalidTag"));
        assert_eq!(
            object_tags(&client, bucket, "malformed.txt").await,
            BTreeMap::from([("state".to_string(), "original".to_string())])
        );
        let preserved_body = client
            .get_object()
            .bucket(bucket)
            .key("malformed.txt")
            .send()
            .await
            .expect("malformed tags must not replace an existing destination")
            .body
            .collect()
            .await
            .expect("preserved destination body should be readable")
            .into_bytes();
        assert_eq!(
            preserved_body.as_ref(),
            b"original destination",
            "malformed tags must leave destination data unchanged"
        );

        let discarded = client
            .copy_object()
            .bucket(bucket)
            .key("discarded.txt")
            .copy_source(format!("{bucket}/{source}"))
            .tagging("project=must-not-be-discarded")
            .send()
            .await
            .expect_err("Tagging without REPLACE must fail instead of discarding requested tags");
        assert_eq!(discarded.as_service_error().and_then(ProvideErrorMetadata::code), Some("InvalidRequest"));

        let invalid_directive = client
            .copy_object()
            .bucket(bucket)
            .key("invalid-directive.txt")
            .copy_source(format!("{bucket}/{source}"))
            .tagging_directive(TaggingDirective::from("UNKNOWN"))
            .send()
            .await
            .expect_err("an unknown TaggingDirective must fail");
        assert_eq!(
            invalid_directive.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("InvalidArgument")
        );

        env.stop_server();
    }

    #[tokio::test]
    #[serial]
    async fn copy_object_tag_replacement_honors_request_tag_policy_denial() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        init_logging();
        let source_bucket = "copy-tags-policy-source";
        let destination_bucket = "copy-tags-policy-destination";

        let mut env = RustFSTestEnvironment::new().await?;
        env.start_rustfs_server(vec![]).await?;
        let admin = env.create_s3_client();
        admin.create_bucket().bucket(source_bucket).send().await?;
        admin.create_bucket().bucket(destination_bucket).send().await?;
        admin
            .put_object()
            .bucket(source_bucket)
            .key("source.txt")
            .tagging("source=allowed")
            .body(ByteStream::from_static(b"source"))
            .send()
            .await?;
        admin
            .put_object()
            .bucket(source_bucket)
            .key("conditioned.txt")
            .body(ByteStream::from_static(b"conditioned source"))
            .send()
            .await?;

        let source_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [format!("arn:aws:s3:::{source_bucket}/source.txt")]
                },
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [format!("arn:aws:s3:::{source_bucket}/conditioned.txt")],
                    "Condition": {
                        "StringEquals": {
                            "s3:RequestObjectTag/classification": "public"
                        }
                    }
                }
            ]
        })
        .to_string();
        admin
            .put_bucket_policy()
            .bucket(source_bucket)
            .policy(source_policy)
            .send()
            .await?;

        let destination_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:PutObject"],
                    "Resource": [format!("arn:aws:s3:::{destination_bucket}/*")]
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["s3:PutObject"],
                    "Resource": [format!("arn:aws:s3:::{destination_bucket}/*")],
                    "Condition": {
                        "StringEquals": {
                            "s3:RequestObjectTag/classification": "restricted"
                        }
                    }
                }
            ]
        })
        .to_string();
        admin
            .put_bucket_policy()
            .bucket(destination_bucket)
            .policy(destination_policy)
            .send()
            .await?;

        let copy_source = format!("/{source_bucket}/source.txt");
        let allowed = local_http_client()
            .put(format!("{}/{destination_bucket}/allowed.txt", env.url))
            .header("x-amz-copy-source", &copy_source)
            .header("x-amz-tagging-directive", "REPLACE")
            .header("x-amz-tagging", "classification=public")
            .send()
            .await?;
        assert_eq!(
            allowed.status(),
            reqwest::StatusCode::OK,
            "a tag set allowed by the request-tag policy should copy successfully"
        );
        assert_eq!(
            object_tags(&admin, destination_bucket, "allowed.txt").await,
            BTreeMap::from([("classification".to_string(), "public".to_string())])
        );

        let denied = local_http_client()
            .put(format!("{}/{destination_bucket}/denied.txt", env.url))
            .header("x-amz-copy-source", copy_source)
            .header("x-amz-tagging-directive", "REPLACE")
            .header("x-amz-tagging", "classification=restricted")
            .send()
            .await?;
        assert_eq!(
            denied.status(),
            reqwest::StatusCode::FORBIDDEN,
            "CopyObject must honor a request-tag policy Deny"
        );

        let source_condition_bypass = local_http_client()
            .put(format!("{}/{destination_bucket}/source-condition.txt", env.url))
            .header("x-amz-copy-source", format!("/{source_bucket}/conditioned.txt"))
            .header("x-amz-tagging-directive", "REPLACE")
            .header("x-amz-tagging", "classification=public")
            .send()
            .await?;
        assert_eq!(
            source_condition_bypass.status(),
            reqwest::StatusCode::FORBIDDEN,
            "destination request tags must not satisfy source GetObject policy conditions"
        );

        let missing_destination = admin
            .head_object()
            .bucket(destination_bucket)
            .key("denied.txt")
            .send()
            .await
            .expect_err("an access-denied copy must not create a destination object");
        assert_eq!(
            missing_destination.as_service_error().and_then(ProvideErrorMetadata::code),
            Some("NotFound")
        );
        let missing_bypass_destination = admin
            .head_object()
            .bucket(destination_bucket)
            .key("source-condition.txt")
            .send()
            .await
            .expect_err("a source authorization denial must not create a destination object");
        assert_eq!(
            missing_bypass_destination
                .as_service_error()
                .and_then(ProvideErrorMetadata::code),
            Some("NotFound")
        );

        env.stop_server();
        Ok(())
    }
}
