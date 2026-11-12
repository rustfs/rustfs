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

//! Byte-level goldens for the 13 persisted bucket-configuration XML families
//! (rustfs/backlog#2744).
//!
//! Responsible for: pinning the exact bytes that `deserialize` followed by
//! `serialize` produces for every family in its three source shapes (AWS
//! documentation shape, MinIO-written shape, RustFS-written shape), and the
//! exact bytes RustFS writes for a typed value. Only bytes are compared; a
//! structural comparison would let a codec replacement change the persisted
//! encoding without failing here.
//!
//! Not responsible for: validating configurations, touching the bucket
//! metadata store, or choosing a canonical shape.
//!
//! Upstream: the bucket-config `deserialize`/`serialize` pair, reached through the
//! integration-test facade `tests/storage_api.rs`, and the configuration types they are
//! generic over (`BucketConfigXml`: the gateway persistence shapes for the families that
//! have moved, the s3s DTOs for the rest). Downstream: the DTO/codec
//! replacement (T1.4) and the persisted-bytes comparison (T3.7), which must
//! keep every golden green without regenerating it. Regeneration is reserved
//! to `scripts/gen_bucket_config_goldens.sh`, which sets
//! `BUCKET_CONFIG_GOLDENS_WRITE`: in that mode every test writes its files
//! and then fails on purpose, so a write run never prints a green line.

mod storage_api;

use rustfs_gateway_types::cors_tagging::{
    PersistedCorsConfiguration as CORSConfiguration, PersistedCorsRule as CORSRule, PersistedTag as Tag,
    PersistedTagging as Tagging,
};
use rustfs_gateway_types::dto::{Status, StorageClass};
use rustfs_gateway_types::persistence::{
    PersistedAbortIncompleteMultipartUpload, PersistedAccelerateConfiguration as AccelerateConfiguration,
    PersistedBucketEncryptionConfiguration as ServerSideEncryptionConfiguration,
    PersistedBucketEncryptionRule as ServerSideEncryptionRule, PersistedBucketLoggingStatus as BucketLoggingStatus,
    PersistedDefaultRetention as DefaultRetention, PersistedDelMarkerExpiration,
    PersistedEncryptionByDefault as ServerSideEncryptionByDefault, PersistedErrorDocument as ErrorDocument,
    PersistedFilterRule as FilterRule, PersistedIndexDocument as IndexDocument, PersistedLifecycleAnd,
    PersistedLifecycleConfiguration, PersistedLifecycleExpiration, PersistedLifecycleFilter, PersistedLifecycleRule,
    PersistedLifecycleTag, PersistedLoggingEnabled as LoggingEnabled, PersistedNoncurrentVersionExpiration,
    PersistedNotificationConfiguration as NotificationConfiguration,
    PersistedNotificationConfigurationFilter as NotificationConfigurationFilter,
    PersistedObjectLockConfiguration as ObjectLockConfiguration, PersistedObjectLockRule as ObjectLockRule,
    PersistedOptionalReplicationStatus, PersistedPublicAccessBlockConfiguration as PublicAccessBlockConfiguration,
    PersistedQueueConfiguration as QueueConfiguration, PersistedRedirect as Redirect, PersistedReplicationAnd,
    PersistedReplicationConfiguration, PersistedReplicationDestination, PersistedReplicationFilter, PersistedReplicationRule,
    PersistedReplicationStatus, PersistedReplicationTag, PersistedRequestPaymentConfiguration as RequestPaymentConfiguration,
    PersistedRoutingRule as RoutingRule, PersistedRoutingRuleCondition as Condition, PersistedS3KeyFilter as S3KeyFilter,
    PersistedSourceSelectionCriteria, PersistedTransition, PersistedVersioningConfiguration as VersioningConfiguration,
    PersistedWebsiteConfiguration as WebsiteConfiguration,
};
use std::path::{Path, PathBuf};
use storage_api::bucket_config_codec::{BucketConfigXml, deserialize, serialize};

/// Set only by `scripts/gen_bucket_config_goldens.sh`. Every pair test then
/// writes its `.out.xml` (and, for the `rustfs` shape, its `.in.xml`) and
/// fails on purpose, so a write run can never be mistaken for a verification.
const WRITE_ENV: &str = "BUCKET_CONFIG_GOLDENS_WRITE";

fn goldens_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("bucket-config-goldens")
}

fn pair_path(family: &str, shape: &str, side: &str) -> PathBuf {
    goldens_dir().join(family).join(format!("{shape}.{side}.xml"))
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

fn round_trip<T>(input: &[u8], label: &str) -> Vec<u8>
where
    T: BucketConfigXml,
{
    let parsed: T = deserialize(input).unwrap_or_else(|err| panic!("{label}: deserialize failed: {err}"));
    serialize(&parsed).unwrap_or_else(|err| panic!("{label}: serialize failed: {err}"))
}

/// Byte comparison with a readable report. Parsed values are never compared.
fn assert_bytes_eq(actual: &[u8], expected: &[u8], context: &str) {
    if actual != expected {
        panic!(
            "{context}\n--- expected ({} bytes) ---\n{}\n--- actual ({} bytes) ---\n{}\n",
            expected.len(),
            String::from_utf8_lossy(expected),
            actual.len(),
            String::from_utf8_lossy(actual),
        );
    }
}

/// Checks one `<family>/<shape>` pair. `typed` is the value RustFS writes for
/// the `rustfs` shape; the `aws` and `minio` shapes pass `None`.
fn check_pair<T>(family: &str, shape: &str, typed: Option<&T>)
where
    T: BucketConfigXml,
{
    let in_path = pair_path(family, shape, "in");
    let out_path = pair_path(family, shape, "out");
    let label = format!("{family}/{shape}");

    if std::env::var_os(WRITE_ENV).is_some() {
        if let Some(value) = typed {
            let written = serialize(value).unwrap_or_else(|err| panic!("{label}: typed value failed to serialize: {err}"));
            write(&in_path, &written);
        }
        let input = read(&in_path);
        write(&out_path, &round_trip::<T>(&input, &label));
        panic!(
            "{WRITE_ENV} is set: wrote {} without verifying anything; rerun without {WRITE_ENV}",
            out_path.display()
        );
    }

    let input = read(&in_path);
    let expected = read(&out_path);
    if let Some(value) = typed {
        let written = serialize(value).unwrap_or_else(|err| panic!("{label}: typed value failed to serialize: {err}"));
        assert_bytes_eq(
            &written,
            &input,
            &format!(
                "{label}: RustFS no longer writes the pinned bytes for the typed value ({})",
                in_path.display()
            ),
        );
    }
    let actual = round_trip::<T>(&input, &label);
    assert_bytes_eq(
        &actual,
        &expected,
        &format!("{label}: deserialize->serialize bytes drifted from {}", out_path.display()),
    );
    let again = round_trip::<T>(&expected, &label);
    assert_bytes_eq(
        &again,
        &expected,
        &format!("{label}: the golden is not a fixed point of deserialize->serialize"),
    );
}

/// The typed values behind every `rustfs.in.xml`: what RustFS writes today
/// through `serialize` when it persists a configuration it built itself.
/// MinIO-only members (`DelMarkerExpiration`, `ExcludeFolders`,
/// `ExcludedPrefixes`, `DeleteReplication`) are included where RustFS accepts
/// them, so the `rustfs` shape is the one that pins their encoding.
mod rustfs_values {
    use super::*;

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: Some(key.to_owned()),
            value: Some(value.to_owned()),
        }
    }

    pub(super) fn notification() -> NotificationConfiguration {
        NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                events: vec!["s3:ObjectCreated:*".to_owned(), "s3:ObjectRemoved:*".to_owned()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some("prefix".to_owned()),
                            value: Some("uploads/".to_owned()),
                        }]),
                    }),
                }),
                id: Some("rustfs-webhook".to_owned()),
                queue_arn: "arn:rustfs:sqs::primary:webhook".to_owned(),
            }]),
            ..Default::default()
        }
    }

    pub(super) fn lifecycle() -> PersistedLifecycleConfiguration {
        PersistedLifecycleConfiguration {
            rules: vec![
                PersistedLifecycleRule {
                    abort_incomplete_multipart_upload: Some(PersistedAbortIncompleteMultipartUpload {
                        days_after_initiation: Some(3),
                    }),
                    del_marker_expiration: Some(PersistedDelMarkerExpiration { days: Some(7) }),
                    expiration: Some(PersistedLifecycleExpiration {
                        days: Some(30),
                        ..Default::default()
                    }),
                    filter: Some(PersistedLifecycleFilter {
                        prefix: Some("tmp/".to_owned()),
                        ..Default::default()
                    }),
                    id: Some("expire-tmp".to_owned()),
                    noncurrent_version_expiration: Some(PersistedNoncurrentVersionExpiration {
                        newer_noncurrent_versions: Some(2),
                        noncurrent_days: Some(7),
                    }),
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: "Enabled".to_owned(),
                    transitions: None,
                },
                PersistedLifecycleRule {
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    expiration: None,
                    filter: Some(PersistedLifecycleFilter {
                        and: Some(PersistedLifecycleAnd {
                            prefix: Some("archive/".to_owned()),
                            tags: Some(vec![PersistedLifecycleTag {
                                key: Some("tier".to_owned()),
                                value: Some("cold".to_owned()),
                            }]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    id: Some("tier-cold".to_owned()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: "Enabled".to_owned(),
                    transitions: Some(vec![PersistedTransition {
                        days: Some(90),
                        storage_class: Some("COLDTIER".to_owned()),
                        ..Default::default()
                    }]),
                },
            ],
            ..Default::default()
        }
    }

    pub(super) fn object_lock() -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some("Enabled".to_owned()),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: Some(30),
                    mode: Some("GOVERNANCE".to_owned()),
                    years: None,
                }),
            }),
        }
    }

    pub(super) fn versioning() -> VersioningConfiguration {
        VersioningConfiguration {
            exclude_folders: Some(true),
            excluded_prefixes: Some(vec![Some("tmp/".to_owned()), Some("cache/".to_owned())]),
            mfa_delete: None,
            status: Some("Enabled".to_owned()),
        }
    }

    pub(super) fn encryption() -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    kms_master_key_id: Some("rustfs-bucket-key".to_owned()),
                    sse_algorithm: "aws:kms".to_owned(),
                }),
                blocked_encryption_types: None,
                bucket_key_enabled: Some(false),
            }],
        }
    }

    pub(super) fn tagging() -> Tagging {
        Tagging {
            tag_set: vec![tag("team", "storage"), tag("env", "staging")],
        }
    }

    pub(super) fn replication() -> PersistedReplicationConfiguration {
        PersistedReplicationConfiguration {
            role: String::new(),
            rules: vec![PersistedReplicationRule {
                delete_marker_replication: Some(PersistedOptionalReplicationStatus {
                    status: Some(Status::ENABLED.to_string()),
                }),
                delete_replication: Some(PersistedReplicationStatus {
                    status: Status::ENABLED.to_string(),
                }),
                destination: PersistedReplicationDestination {
                    bucket: "arn:rustfs:replication::0b6f3c2a-5e6a-4d2b-9c1e-7a8f9d0e1b2c:interop-dr".to_owned(),
                    storage_class: Some(StorageClass::STANDARD.to_string()),
                    ..Default::default()
                },
                existing_object_replication: Some(PersistedReplicationStatus {
                    status: Status::ENABLED.to_string(),
                }),
                filter: Some(PersistedReplicationFilter {
                    and: Some(PersistedReplicationAnd {
                        prefix: Some("docs/".to_owned()),
                        tags: Some(vec![PersistedReplicationTag {
                            key: Some("replicate".to_owned()),
                            value: Some("yes".to_owned()),
                        }]),
                    }),
                    ..Default::default()
                }),
                id: Some("rustfs-dr".to_owned()),
                prefix: None,
                priority: Some(1),
                source_selection_criteria: Some(PersistedSourceSelectionCriteria {
                    replica_modifications: Some(PersistedReplicationStatus {
                        status: Status::ENABLED.to_string(),
                    }),
                    sse_kms_encrypted_objects: None,
                }),
                status: Status::ENABLED.to_string(),
            }],
        }
    }

    pub(super) fn cors() -> CORSConfiguration {
        CORSConfiguration {
            cors_rules: vec![CORSRule {
                allowed_headers: Some(vec!["Authorization".to_owned(), "Content-Type".to_owned()]),
                allowed_methods: vec!["GET".to_owned(), "PUT".to_owned()],
                allowed_origins: vec!["https://app.example.internal".to_owned()],
                expose_headers: Some(vec!["ETag".to_owned(), "x-amz-request-id".to_owned()]),
                id: Some("rustfs-app".to_owned()),
                max_age_seconds: Some(3600),
            }],
        }
    }

    pub(super) fn logging() -> BucketLoggingStatus {
        BucketLoggingStatus {
            logging_enabled: Some(LoggingEnabled {
                target_bucket: "rustfs-access-logs".to_owned(),
                target_grants: None,
                target_object_key_format: None,
                target_prefix: "buckets/interop/".to_owned(),
            }),
        }
    }

    pub(super) fn website() -> WebsiteConfiguration {
        WebsiteConfiguration {
            error_document: Some(ErrorDocument {
                key: "404.html".to_owned(),
            }),
            index_document: Some(IndexDocument {
                suffix: "index.html".to_owned(),
            }),
            redirect_all_requests_to: None,
            routing_rules: Some(vec![RoutingRule {
                condition: Some(Condition {
                    http_error_code_returned_equals: None,
                    key_prefix_equals: Some("legacy/".to_owned()),
                }),
                redirect: Redirect {
                    host_name: None,
                    http_redirect_code: Some("301".to_owned()),
                    protocol: None,
                    replace_key_prefix_with: Some("current/".to_owned()),
                    replace_key_with: None,
                },
            }]),
        }
    }

    pub(super) fn accelerate() -> AccelerateConfiguration {
        AccelerateConfiguration {
            status: Some("Suspended".to_owned()),
        }
    }

    pub(super) fn request_payment() -> RequestPaymentConfiguration {
        RequestPaymentConfiguration {
            payer: "BucketOwner".to_owned(),
        }
    }

    pub(super) fn public_access_block() -> PublicAccessBlockConfiguration {
        PublicAccessBlockConfiguration {
            block_public_acls: Some(true),
            block_public_policy: Some(false),
            ignore_public_acls: Some(true),
            restrict_public_buckets: Some(false),
        }
    }
}

/// One test per `<family>/<shape>` pair: 13 families x 3 shapes = 39.
mod bucket_config_goldens {
    use super::*;

    macro_rules! golden {
        ($name:ident, $family:literal, $shape:literal, $ty:ty) => {
            #[test]
            fn $name() {
                check_pair::<$ty>($family, $shape, None);
            }
        };
        ($name:ident, $family:literal, rustfs = $value:expr) => {
            #[test]
            fn $name() {
                check_pair($family, "rustfs", Some(&$value));
            }
        };
    }

    golden!(notification_aws, "notification", "aws", NotificationConfiguration);
    golden!(notification_minio, "notification", "minio", NotificationConfiguration);
    golden!(notification_rustfs, "notification", rustfs = rustfs_values::notification());

    golden!(lifecycle_aws, "lifecycle", "aws", PersistedLifecycleConfiguration);
    golden!(lifecycle_minio, "lifecycle", "minio", PersistedLifecycleConfiguration);
    golden!(lifecycle_rustfs, "lifecycle", rustfs = rustfs_values::lifecycle());

    golden!(object_lock_aws, "object_lock", "aws", ObjectLockConfiguration);
    golden!(object_lock_minio, "object_lock", "minio", ObjectLockConfiguration);
    golden!(object_lock_rustfs, "object_lock", rustfs = rustfs_values::object_lock());

    golden!(versioning_aws, "versioning", "aws", VersioningConfiguration);
    golden!(versioning_minio, "versioning", "minio", VersioningConfiguration);
    golden!(versioning_rustfs, "versioning", rustfs = rustfs_values::versioning());

    golden!(encryption_aws, "encryption", "aws", ServerSideEncryptionConfiguration);
    golden!(encryption_minio, "encryption", "minio", ServerSideEncryptionConfiguration);
    golden!(encryption_rustfs, "encryption", rustfs = rustfs_values::encryption());

    golden!(tagging_aws, "tagging", "aws", Tagging);
    golden!(tagging_minio, "tagging", "minio", Tagging);
    golden!(tagging_rustfs, "tagging", rustfs = rustfs_values::tagging());

    /// `replication/aws.in.xml` is the unknown-element sample from
    /// `crates/replication/src/config.rs`
    /// (`s3_xml_parser_discards_unknown_replication_elements_before_validation`).
    /// Beyond the byte golden, the pair proves the root-level `FutureTopLevel`
    /// element is skipped on read and never re-emitted on write.
    #[test]
    fn replication_aws() {
        check_pair::<PersistedReplicationConfiguration>("replication", "aws", None);
        let input = read(&pair_path("replication", "aws", "in"));
        let output = read(&pair_path("replication", "aws", "out"));
        assert!(
            contains(&input, b"<FutureTopLevel>future</FutureTopLevel>"),
            "the sample must still carry the unknown element"
        );
        assert!(
            !contains(&output, b"FutureTopLevel"),
            "an unknown element must not be re-emitted: {}",
            String::from_utf8_lossy(&output)
        );
    }
    golden!(replication_minio, "replication", "minio", PersistedReplicationConfiguration);
    golden!(replication_rustfs, "replication", rustfs = rustfs_values::replication());

    golden!(cors_aws, "cors", "aws", CORSConfiguration);
    golden!(cors_minio, "cors", "minio", CORSConfiguration);
    golden!(cors_rustfs, "cors", rustfs = rustfs_values::cors());

    golden!(logging_aws, "logging", "aws", BucketLoggingStatus);
    golden!(logging_minio, "logging", "minio", BucketLoggingStatus);
    golden!(logging_rustfs, "logging", rustfs = rustfs_values::logging());

    golden!(website_aws, "website", "aws", WebsiteConfiguration);
    golden!(website_minio, "website", "minio", WebsiteConfiguration);
    golden!(website_rustfs, "website", rustfs = rustfs_values::website());

    golden!(accelerate_aws, "accelerate", "aws", AccelerateConfiguration);
    golden!(accelerate_minio, "accelerate", "minio", AccelerateConfiguration);
    golden!(accelerate_rustfs, "accelerate", rustfs = rustfs_values::accelerate());

    golden!(request_payment_aws, "request_payment", "aws", RequestPaymentConfiguration);
    golden!(request_payment_minio, "request_payment", "minio", RequestPaymentConfiguration);
    golden!(request_payment_rustfs, "request_payment", rustfs = rustfs_values::request_payment());

    golden!(public_access_block_aws, "public_access_block", "aws", PublicAccessBlockConfiguration);
    golden!(public_access_block_minio, "public_access_block", "minio", PublicAccessBlockConfiguration);
    golden!(
        public_access_block_rustfs,
        "public_access_block",
        rustfs = rustfs_values::public_access_block()
    );
}

mod bucket_config_lifecycle_date_compat {
    use super::*;
    use rustfs_lifecycle::core::format_lifecycle_date;
    use time::OffsetDateTime;

    const DATES: [&str; 3] = [
        "2023-11-14T22:13:20.123456789Z",
        "2023-11-14T22:13:20.123456789+05:30",
        "2025-01-02T00:00:00+02:00",
    ];

    fn document(date: &str) -> Vec<u8> {
        format!(
            "<LifecycleConfiguration><ExpiryUpdatedAt>{date}</ExpiryUpdatedAt><Rule><ID>date-compat</ID><Status>Enabled</Status><Expiration><Date>{date}</Date></Expiration><Transition><Date>{date}</Date><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>"
        )
        .into_bytes()
    }

    fn legacy_read(input: &[u8]) -> s3s::dto::BucketLifecycleConfiguration {
        let mut reader = s3s::xml::Deserializer::new(input);
        let value = s3s::xml::Deserialize::deserialize(&mut reader).expect("the actual old lifecycle reader accepts the date");
        reader.expect_eof().expect("the old reader consumes the entire date fixture");
        value
    }

    #[test]
    fn lifecycle_xml_reader_preserves_legacy_date_precision_and_offset() {
        for date in DATES {
            let input = document(date);
            let old = legacy_read(&input);
            let new: PersistedLifecycleConfiguration = deserialize(&input).expect("read persisted lifecycle date");
            let old_date = |value: &s3s::dto::Timestamp| {
                format_lifecycle_date(OffsetDateTime::from(value.clone())).expect("format the original old-reader date")
            };
            assert_eq!(
                [
                    new.expiry_updated_at.clone(),
                    new.rules[0].expiration.as_ref().and_then(|value| value.date.clone()),
                    new.rules[0].transitions.as_ref().unwrap()[0].date.clone(),
                ],
                [
                    Some(old_date(old.expiry_updated_at.as_ref().unwrap())),
                    Some(old_date(old.rules[0].expiration.as_ref().unwrap().date.as_ref().unwrap())),
                    Some(old_date(old.rules[0].transitions.as_ref().unwrap()[0].date.as_ref().unwrap())),
                ],
                "the lifecycle reader must retain the old date representation for {date}"
            );
        }
    }

    #[test]
    fn lifecycle_xml_writer_preserves_legacy_date_bytes() {
        for date in DATES {
            let old = legacy_read(&document(date));
            let mut old_xml = Vec::new();
            s3s::xml::Serialize::serialize(&old, &mut s3s::xml::Serializer::new(&mut old_xml))
                .expect("the actual old lifecycle writer emits the date");
            let new = PersistedLifecycleConfiguration {
                expiry_updated_at: Some(date.to_owned()),
                rules: vec![PersistedLifecycleRule {
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    expiration: Some(PersistedLifecycleExpiration {
                        date: Some(date.to_owned()),
                        ..Default::default()
                    }),
                    filter: None,
                    id: Some("date-compat".to_owned()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: "Enabled".to_owned(),
                    transitions: Some(vec![PersistedTransition {
                        date: Some(date.to_owned()),
                        days: None,
                        storage_class: Some("GLACIER".to_owned()),
                    }]),
                }],
            };
            assert_eq!(
                serialize(&new).expect("write persisted lifecycle date"),
                old_xml,
                "legacy date bytes for {date}"
            );
        }
    }
}
