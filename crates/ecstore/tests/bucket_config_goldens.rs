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

use rustfs_gateway_types::dto::{Status, StorageClass};
use rustfs_gateway_types::persistence::{
    PersistedOptionalReplicationStatus, PersistedReplicationAnd, PersistedReplicationConfiguration,
    PersistedReplicationDestination, PersistedReplicationFilter, PersistedReplicationRule, PersistedReplicationStatus,
    PersistedReplicationTag, PersistedSourceSelectionCriteria,
};
use s3s::dto::{
    AbortIncompleteMultipartUpload, AccelerateConfiguration, BucketAccelerateStatus, BucketLifecycleConfiguration,
    BucketLoggingStatus, BucketVersioningStatus, CORSConfiguration, CORSRule, Condition, DefaultRetention, DelMarkerExpiration,
    ErrorDocument, Event, ExcludedPrefix, ExpirationStatus, FilterRule, FilterRuleName, IndexDocument, LifecycleExpiration,
    LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter, LoggingEnabled, NoncurrentVersionExpiration,
    NotificationConfiguration, NotificationConfigurationFilter, ObjectLockConfiguration, ObjectLockEnabled,
    ObjectLockRetentionMode, ObjectLockRule, Payer, PublicAccessBlockConfiguration, QueueConfiguration, Redirect,
    RequestPaymentConfiguration, RoutingRule, S3KeyFilter, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, Tag, Tagging, Transition, TransitionStorageClass,
    VersioningConfiguration, WebsiteConfiguration,
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
                events: vec![
                    Event::from("s3:ObjectCreated:*".to_owned()),
                    Event::from("s3:ObjectRemoved:*".to_owned()),
                ],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
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

    pub(super) fn lifecycle() -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration {
            rules: vec![
                LifecycleRule {
                    abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                        days_after_initiation: Some(3),
                    }),
                    del_marker_expiration: Some(DelMarkerExpiration { days: Some(7) }),
                    expiration: Some(LifecycleExpiration {
                        days: Some(30),
                        ..Default::default()
                    }),
                    filter: Some(LifecycleRuleFilter {
                        prefix: Some("tmp/".to_owned()),
                        ..Default::default()
                    }),
                    id: Some("expire-tmp".to_owned()),
                    noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                        newer_noncurrent_versions: Some(2),
                        noncurrent_days: Some(7),
                    }),
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    transitions: None,
                },
                LifecycleRule {
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    expiration: None,
                    filter: Some(LifecycleRuleFilter {
                        and: Some(LifecycleRuleAndOperator {
                            prefix: Some("archive/".to_owned()),
                            tags: Some(vec![tag("tier", "cold")]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    id: Some("tier-cold".to_owned()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    transitions: Some(vec![Transition {
                        days: Some(90),
                        storage_class: Some(TransitionStorageClass::from_static("COLDTIER")),
                        ..Default::default()
                    }]),
                },
            ],
            ..Default::default()
        }
    }

    pub(super) fn object_lock() -> ObjectLockConfiguration {
        ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule: Some(ObjectLockRule {
                default_retention: Some(DefaultRetention {
                    days: Some(30),
                    mode: Some(ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE)),
                    years: None,
                }),
            }),
        }
    }

    pub(super) fn versioning() -> VersioningConfiguration {
        VersioningConfiguration {
            exclude_folders: Some(true),
            excluded_prefixes: Some(vec![
                ExcludedPrefix {
                    prefix: Some("tmp/".to_owned()),
                },
                ExcludedPrefix {
                    prefix: Some("cache/".to_owned()),
                },
            ]),
            mfa_delete: None,
            status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        }
    }

    pub(super) fn encryption() -> ServerSideEncryptionConfiguration {
        ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: Some(ServerSideEncryptionByDefault {
                    kms_master_key_id: Some("rustfs-bucket-key".to_owned()),
                    sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
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
            status: Some(BucketAccelerateStatus::from_static(BucketAccelerateStatus::SUSPENDED)),
        }
    }

    pub(super) fn request_payment() -> RequestPaymentConfiguration {
        RequestPaymentConfiguration {
            payer: Payer::from_static(Payer::BUCKET_OWNER),
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

    golden!(lifecycle_aws, "lifecycle", "aws", BucketLifecycleConfiguration);
    golden!(lifecycle_minio, "lifecycle", "minio", BucketLifecycleConfiguration);
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

mod bucket_config_goldens_carrier_compat {
    use super::storage_api::replication_compat::{BucketTargets, ReplicationConfig};
    use rustfs_gateway_types::dto::Status;
    use rustfs_gateway_types::persistence::{
        PersistedReplicationConfiguration, PersistedReplicationDestination, PersistedReplicationFilter, PersistedReplicationRule,
    };
    use serde::{Deserialize, Serialize};

    fn replication_rule() -> PersistedReplicationRule {
        PersistedReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: PersistedReplicationDestination {
                bucket: "arn:aws:s3:::target-bucket".to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule".to_string()),
            prefix: Some(String::new()),
            priority: Some(1),
            source_selection_criteria: None,
            status: Status::ENABLED.to_string(),
        }
    }

    #[test]
    fn replication_cache_matches_the_legacy_reader_for_nested_tag_cache_shapes() {
        #[derive(Serialize, Deserialize)]
        struct LegacyReplicationConfig {
            config: Option<s3s::dto::ReplicationConfiguration>,
            remotes: Option<BucketTargets>,
        }

        fn encode(value: &impl Serialize, named: bool) -> Vec<u8> {
            if named {
                rmp_serde::to_vec_named(value).expect("encode the actual cache carrier as a named map")
            } else {
                rmp_serde::to_vec(value).expect("encode the actual cache carrier as an array")
            }
        }

        let mut rule = replication_rule();
        rule.prefix = None;
        rule.filter = Some(PersistedReplicationFilter {
            prefix: Some("cache-marker/".to_string()),
            ..Default::default()
        });
        let expected = PersistedReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/replication".to_string(),
            rules: vec![rule],
        };
        let legacy = LegacyReplicationConfig {
            config: Some(s3s::dto::ReplicationConfiguration {
                role: expected.role.clone(),
                rules: vec![s3s::dto::ReplicationRule {
                    delete_marker_replication: None,
                    delete_replication: None,
                    destination: s3s::dto::Destination {
                        bucket: "arn:aws:s3:::target-bucket".to_string(),
                        ..Default::default()
                    },
                    existing_object_replication: None,
                    filter: Some(s3s::dto::ReplicationRuleFilter {
                        prefix: Some("cache-marker/".to_string()),
                        ..Default::default()
                    }),
                    id: Some("rule".to_string()),
                    prefix: None,
                    priority: Some(1),
                    source_selection_criteria: None,
                    status: s3s::dto::ReplicationRuleStatus::from_static(s3s::dto::ReplicationRuleStatus::ENABLED),
                }],
            }),
            remotes: None,
        };
        let mut mismatches = Vec::new();
        for named in [false, true] {
            let bytes = encode(&legacy, named);
            let marker = if named {
                let mut marker = rmp_serde::to_vec("cached_tags").expect("encode the legacy cache field name");
                marker.push(0x80);
                marker
            } else {
                let mut marker = vec![0x80];
                marker.extend(rmp_serde::to_vec("cache-marker/").expect("encode the unique filter prefix"));
                marker
            };
            let position = bytes
                .windows(marker.len())
                .position(|window| window == marker)
                .expect("the real legacy serializer must write the nested empty tag cache");
            let cache_position = position + if named { marker.len() - 1 } else { 0 };
            for (name, cache, valid) in [
                ("empty-map", &[0x80][..], true),
                ("string-map", &[0x81, 0xa1, b'k', 0xa1, b'v'][..], true),
                ("nil", &[0xc0][..], false),
                ("scalar", &[0x01][..], false),
                ("array", &[0x90][..], false),
                ("nonstring-key", &[0x81, 0x01, 0xa1, b'v'][..], false),
                ("nonstring-value", &[0x81, 0xa1, b'k', 0x01][..], false),
            ] {
                let mut input = bytes.clone();
                input.splice(cache_position..=cache_position, cache.iter().copied());
                let old = rmp_serde::from_slice::<LegacyReplicationConfig>(&input);
                let new = rmp_serde::from_slice::<ReplicationConfig>(&input);
                assert_eq!(old.is_ok(), valid, "the real old reader's contract for {name}, named={named}");
                if new.is_ok() != old.is_ok() {
                    mismatches.push(format!("{name}, named={named}: old={:?}, new={}", old.as_ref().err(), new.is_ok()));
                }
                if let (Ok(old), Ok(new)) = (old, new) {
                    assert_eq!(old.config, legacy.config, "the old reader must discard only cached tags");
                    assert_eq!(
                        new.config.as_ref(),
                        Some(&expected),
                        "the new reader must preserve every configuration field"
                    );
                    let reread = rmp_serde::from_slice::<LegacyReplicationConfig>(&encode(&new, named))
                        .expect("the real old reader must read the newly serialized carrier");
                    assert_eq!(reread.config, legacy.config, "the new writer must preserve the old configuration");
                    assert!(reread.remotes.is_none(), "the absent remote configuration must remain absent");
                }
            }
            let absent = LegacyReplicationConfig {
                config: None,
                remotes: None,
            };
            let input = encode(&absent, named);
            let new = rmp_serde::from_slice::<ReplicationConfig>(&input).expect("read an absent optional configuration");
            assert!(new.config.is_none());
            assert_eq!(
                encode(&new, named),
                input,
                "an absent configuration must keep its actual carrier encoding"
            );
        }
        let missing =
            rmp_serde::to_vec_named(&serde_json::json!({"remotes": null})).expect("encode the named map without config");
        let old = rmp_serde::from_slice::<LegacyReplicationConfig>(&missing).expect("the old reader permits an omitted config");
        assert!(old.config.is_none());
        let new = rmp_serde::from_slice::<ReplicationConfig>(&missing);
        if !matches!(&new, Ok(value) if value.config.is_none()) {
            mismatches.push(format!("omitted config, named=true: old=None, new={new:?}"));
        }
        assert!(mismatches.is_empty(), "cache acceptance must match the real old reader: {mismatches:?}");
    }
}
