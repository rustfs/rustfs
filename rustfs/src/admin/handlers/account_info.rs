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

use crate::admin::auth::authenticate_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_action_credentials, object_store_from_req};
use crate::admin::service::caller_identity::oidc_profile_fields;
use crate::admin::storage_api::bucket::versioning_sys::BucketVersioningSys;
use crate::admin::storage_api::contract::admin::StorageAdminApi;
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::data_usage::{load_admin_data_usage_with_memory_overlay, load_bucket_scan_progress_for_admin};
use crate::admin::storage_api::metadata_sys;
use crate::auth::get_condition_values;
use crate::server::ADMIN_PREFIX;
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_data_usage::{BucketUsageInfo, DataUsageInfo};
use rustfs_policy::policy::BucketPolicy;
use rustfs_policy::policy::default::DEFAULT_POLICIES;
use rustfs_policy::policy::{Args, action::Action, action::S3Action};
use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled, ReplicationConfiguration, ReplicationRuleStatus};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

const DATA_USAGE_LOAD_ERROR_MESSAGE: &str = "failed to load data usage";

fn map_data_usage_result<E>(result: Result<DataUsageInfo, E>) -> S3Result<DataUsageInfo> {
    result.map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, DATA_USAGE_LOAD_ERROR_MESSAGE))
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: rustfs_madmin::BackendInfo,
    pub policy: BucketPolicy,
}

pub struct AccountInfoHandler {}

#[derive(Debug, Serialize)]
struct AccountInfoResponse {
    #[serde(flatten)]
    account: rustfs_madmin::AccountInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
}

pub fn register_account_info_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/accountinfo").as_str(),
        AdminOperation(&AccountInfoHandler {}),
    )?;

    Ok(())
}

fn resolve_bucket_access(can_list_bucket: bool, can_get_bucket_location: bool, can_put_object: bool) -> (bool, bool) {
    (can_list_bucket || can_get_bucket_location, can_put_object)
}

fn admin_scan_progress(progress: rustfs_data_usage::BucketScanProgress) -> rustfs_madmin::BucketScanProgress {
    use rustfs_data_usage::BucketScanStage;
    let rustfs_data_usage::BucketScanProgress {
        stage,
        updated_at,
        objects,
        size,
        sources_observed,
        sources_total,
    } = progress;

    rustfs_madmin::BucketScanProgress {
        stage: match stage {
            BucketScanStage::Enumerating => rustfs_madmin::BucketScanStage::Enumerating,
            BucketScanStage::Scanning => rustfs_madmin::BucketScanStage::Scanning,
            BucketScanStage::AwaitingSources => rustfs_madmin::BucketScanStage::AwaitingSources,
            BucketScanStage::Completed => rustfs_madmin::BucketScanStage::Completed,
        },
        updated_at,
        objects,
        size,
        sources_observed,
        sources_total,
    }
}

fn apply_usage_to_bucket_access_info(bucket_info: &mut rustfs_madmin::BucketAccessInfo, usage: Option<&BucketUsageInfo>) {
    // No snapshot coverage for this bucket: leave the stats absent so clients
    // render "unknown" instead of confirmed zeros (rustfs/backlog#1306).
    let Some(usage) = usage else {
        return;
    };

    bucket_info.size = Some(usage.size);
    bucket_info.objects = Some(usage.objects_count);
    bucket_info.usage_value_state = usage.usage_value_state.map(|state| {
        use rustfs_data_usage::BucketUsageValueState;
        match state {
            BucketUsageValueState::Certified => rustfs_madmin::BucketUsageValueState::Certified,
            BucketUsageValueState::Carried => rustfs_madmin::BucketUsageValueState::Carried,
            BucketUsageValueState::Observed => rustfs_madmin::BucketUsageValueState::Observed,
            BucketUsageValueState::Unknown => rustfs_madmin::BucketUsageValueState::Unknown,
        }
    });
    bucket_info.usage_updated_at = usage.usage_certification.and_then(|certificate| {
        u64::try_from(
            certificate
                .certified_at
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .ok()?
                .as_millis(),
        )
        .ok()
    });
    bucket_info.object_sizes_histogram = Some(usage.object_size_histogram.clone());
    bucket_info.object_versions_histogram = Some(usage.object_versions_histogram.clone());
}

fn object_lock_config_enabled(config: &ObjectLockConfiguration) -> bool {
    config
        .object_lock_enabled
        .as_ref()
        .is_some_and(|enabled| enabled.as_str() == ObjectLockEnabled::ENABLED)
}

fn replication_config_enabled(config: &ReplicationConfiguration) -> bool {
    config
        .rules
        .iter()
        .any(|rule| rule.status.as_str() == ReplicationRuleStatus::ENABLED)
}

async fn bucket_locking_enabled(bucket: &str) -> bool {
    metadata_sys::get_object_lock_config(bucket)
        .await
        .ok()
        .is_some_and(|(config, _)| object_lock_config_enabled(&config))
}

async fn bucket_replication_enabled(bucket: &str) -> bool {
    metadata_sys::get_replication_config(bucket)
        .await
        .ok()
        .is_some_and(|(config, _)| replication_config_enabled(&config))
}

async fn bucket_quota_limit(bucket: &str) -> Option<u64> {
    metadata_sys::get_quota_config(bucket)
        .await
        .ok()
        .and_then(|(config, _)| config.get_quota_limit())
}

#[async_trait::async_trait]
impl Operation for AccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(store) = object_store_from_req(&req) else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) = authenticate_request(&req.headers, &req.uri, &input_cred).await?;

        let Ok(iam_store) = crate::admin::runtime_sources::current_ready_iam_handle() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);

        let cred_clone = cred.clone();
        let remote_addr = crate::admin::auth::policy_source_addr(&req.extensions);
        let conditions = get_condition_values(&req.headers, &cred_clone, None, None, remote_addr);
        let cred_clone = Arc::new(cred_clone);
        let conditions = Arc::new(conditions);

        let is_allow = Box::new({
            let iam_clone = Arc::clone(&iam_store);
            let cred_clone = Arc::clone(&cred_clone);
            let conditions = Arc::clone(&conditions);
            move |name: String| {
                let iam_clone = Arc::clone(&iam_clone);
                let cred_clone = Arc::clone(&cred_clone);
                let conditions = Arc::clone(&conditions);
                async move {
                    let can_list_bucket = iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::ListBucketAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await;

                    let can_get_bucket_location = iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::GetBucketLocationAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await;

                    let can_put_object = iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::PutObjectAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await;

                    resolve_bucket_access(can_list_bucket, can_get_bucket_location, can_put_object)
                }
            }
        });

        let account_name = if cred.is_temp() || cred.is_service_account() {
            cred.parent_user.clone()
        } else {
            cred.access_key.clone()
        };

        let Some(admin_cred) = current_action_credentials() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "action credentials are not initialized".to_string(),
            ));
        };

        let mut effective_policy: rustfs_policy::policy::Policy = Default::default();

        if account_name == admin_cred.access_key {
            for (name, p) in DEFAULT_POLICIES.iter() {
                if *name == "consoleAdmin" {
                    effective_policy = p.clone();
                    break;
                }
            }
        } else {
            // Reuse the canonical IAM preparation path so accountinfo policy view
            // stays in sync with real authorization semantics (STS/group fallback included).
            let empty_conditions = HashMap::new();
            let auth_args = Args {
                account: &cred.access_key,
                groups: &cred.groups,
                action: Action::None,
                bucket: "",
                conditions: &empty_conditions,
                is_owner: owner,
                object: "",
                claims,
                deny_only: false,
            };
            let prepared = iam_store.prepare_auth(&auth_args).await;
            if let Some(policy) = prepared.combined_policy_for_view() {
                effective_policy = policy.clone();
            }
        };

        let policy_str = serde_json::to_string(&effective_policy)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse policy failed"))?;

        let (username, email) = oidc_profile_fields(&cred);
        let mut account_info = rustfs_madmin::AccountInfo {
            account_name,
            server: StorageAdminApi::backend_info(store.as_ref()).await,
            policy: serde_json::Value::String(policy_str),
            ..Default::default()
        };

        // TODO: bucket policy
        let buckets = store
            .list_bucket(&BucketOptions {
                cached: true,
                ..Default::default()
            })
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        // Serve the last persisted scanner snapshot plus an authoritative
        // single-process overlay. Distributed nodes must not promote their
        // process-local absolute counters to cluster-wide bucket totals.
        // This path never triggers a live full-version listing
        // (rustfs/backlog#1306); freshness is owned by the scanner.
        let data_usage_info = map_data_usage_result(load_admin_data_usage_with_memory_overlay(store.clone()).await)?;
        account_info.usage_snapshot_converged = data_usage_info.usage_snapshot_converged;

        for bucket in buckets.iter() {
            let (rd, wr) = is_allow(bucket.name.clone()).await;
            if rd || wr {
                let mut bucket_info = rustfs_madmin::BucketAccessInfo {
                    name: bucket.name.clone(),
                    details: Some(rustfs_madmin::BucketDetails {
                        versioning: BucketVersioningSys::enabled(bucket.name.as_str()).await,
                        versioning_suspended: BucketVersioningSys::suspended(bucket.name.as_str()).await,
                        locking: bucket_locking_enabled(bucket.name.as_str()).await,
                        replication: bucket_replication_enabled(bucket.name.as_str()).await,
                        quota: bucket_quota_limit(bucket.name.as_str()).await,
                    }),
                    created: bucket.created,
                    access: rustfs_madmin::AccountAccess { read: rd, write: wr },
                    ..Default::default()
                };
                apply_usage_to_bucket_access_info(&mut bucket_info, data_usage_info.buckets_usage.get(&bucket.name));
                account_info.buckets.push(bucket_info);
            }
        }

        let visible_buckets = account_info
            .buckets
            .iter()
            .map(|bucket| bucket.name.clone())
            .collect::<Vec<_>>();
        let mut progress = load_bucket_scan_progress_for_admin(store, &visible_buckets, &data_usage_info).await;
        for bucket in &mut account_info.buckets {
            bucket.scan_progress = progress.remove(&bucket.name).map(admin_scan_progress);
        }

        let data = serde_json::to_vec(&AccountInfoResponse {
            account: account_info,
            username,
            email,
        })
        .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_madmin::BackendInfo;
    use rustfs_policy::policy::BucketPolicy;
    use s3s::dto::{Destination, ReplicationRule};

    #[test]
    fn accountinfo_response_adds_optional_oidc_display_fields() {
        let mut response = AccountInfoResponse {
            account: rustfs_madmin::AccountInfo::default(),
            username: Some("oidc-user".to_string()),
            email: Some("oidc-user@example.test".to_string()),
        };

        let value = serde_json::to_value(&response).expect("serialize accountinfo response");
        assert_eq!(value["username"], "oidc-user");
        assert_eq!(value["email"], "oidc-user@example.test");

        response.username = None;
        response.email = None;
        let legacy_shape = serde_json::to_value(&response).expect("serialize accountinfo response without OIDC fields");
        assert!(!legacy_shape.as_object().unwrap().contains_key("username"));
        assert!(!legacy_shape.as_object().unwrap().contains_key("email"));
    }

    #[test]
    fn test_account_info_structure() {
        // Test AccountInfo struct creation and serialization
        let account_info = AccountInfo {
            account_name: "test-account".to_string(),
            server: BackendInfo::default(),
            policy: BucketPolicy::default(),
        };

        assert_eq!(account_info.account_name, "test-account");

        // Test JSON serialization (PascalCase rename)
        let json_str = serde_json::to_string(&account_info).unwrap();
        assert!(json_str.contains("AccountName"));
    }

    #[test]
    fn test_account_info_default() {
        // Test that AccountInfo can be created with default values
        let default_info = AccountInfo::default();

        assert!(default_info.account_name.is_empty());
    }

    #[test]
    fn test_resolve_bucket_access() {
        assert_eq!(resolve_bucket_access(false, false, false), (false, false));
        assert_eq!(resolve_bucket_access(true, false, false), (true, false));
        assert_eq!(resolve_bucket_access(false, true, false), (true, false));
        assert_eq!(resolve_bucket_access(false, false, true), (false, true));
    }

    #[test]
    fn accountinfo_bucket_access_info_uses_data_usage_stats() {
        let mut bucket_info = rustfs_madmin::BucketAccessInfo {
            name: "agent".to_string(),
            ..Default::default()
        };
        let usage = BucketUsageInfo {
            size: 222 * 1024 * 1024,
            objects_count: 109,
            object_size_histogram: HashMap::from([("1MiB-10MiB".to_string(), 109)]),
            object_versions_histogram: HashMap::from([("SINGLE_VERSION".to_string(), 109)]),
            ..Default::default()
        };

        apply_usage_to_bucket_access_info(&mut bucket_info, Some(&usage));

        assert_eq!(bucket_info.size, Some(usage.size));
        assert_eq!(bucket_info.objects, Some(usage.objects_count));
        assert_eq!(bucket_info.object_sizes_histogram.as_ref(), Some(&usage.object_size_histogram));
        assert_eq!(bucket_info.object_versions_histogram.as_ref(), Some(&usage.object_versions_histogram));
    }

    /// Buckets without snapshot coverage keep their stats absent so the wire
    /// omits the fields instead of reporting confirmed zeros
    /// (rustfs/backlog#1306 contract decision, option A).
    #[test]
    fn accountinfo_bucket_access_info_leaves_stats_absent_without_usage() {
        let mut bucket_info = rustfs_madmin::BucketAccessInfo {
            name: "agent".to_string(),
            ..Default::default()
        };

        apply_usage_to_bucket_access_info(&mut bucket_info, None);

        assert_eq!(bucket_info.size, None);
        assert_eq!(bucket_info.objects, None);
        assert_eq!(bucket_info.object_sizes_histogram, None);
        assert_eq!(bucket_info.object_versions_histogram, None);
    }

    #[test]
    fn accountinfo_scanner_fields_preserve_the_wire_format() {
        use rustfs_data_usage::{BucketScanProgress, BucketScanStage::*, BucketUsageValueState::*};
        for (stage, state, counts) in [
            (Enumerating, Unknown, None),
            (Scanning, Observed, Some((7, 71))),
            (AwaitingSources, Carried, None),
            (Completed, Certified, Some((0, 0))),
        ] {
            let progress = BucketScanProgress {
                stage,
                updated_at: 1000,
                objects: counts.map(|(objects, _)| objects),
                size: counts.map(|(_, size)| size),
                sources_observed: 1,
                sources_total: 2,
            };
            let expected = serde_json::to_value(&progress).unwrap();
            let mut bucket = rustfs_madmin::BucketAccessInfo {
                scan_progress: Some(admin_scan_progress(progress)),
                ..Default::default()
            };
            let usage = BucketUsageInfo {
                usage_value_state: Some(state),
                ..Default::default()
            };
            apply_usage_to_bucket_access_info(&mut bucket, Some(&usage));
            let wire = serde_json::to_value(&bucket).unwrap();
            assert_eq!(wire["scan_progress"], expected);
            assert_eq!(wire["usage_value_state"], serde_json::to_value(state).unwrap());
            let decoded: rustfs_madmin::BucketAccessInfo = serde_json::from_value(wire).unwrap();
            assert_eq!(decoded.scan_progress, bucket.scan_progress);
            assert_eq!(decoded.usage_value_state, bucket.usage_value_state);
        }
    }

    #[test]
    fn accountinfo_partial_progress_never_fills_total_usage_fields() {
        let mut bucket_info = rustfs_madmin::BucketAccessInfo {
            name: "scanning".into(),
            scan_progress: Some(admin_scan_progress(rustfs_data_usage::BucketScanProgress {
                stage: rustfs_data_usage::BucketScanStage::Scanning,
                updated_at: 1000,
                objects: Some(7),
                size: Some(71),
                sources_observed: 1,
                sources_total: 2,
            })),
            ..Default::default()
        };
        apply_usage_to_bucket_access_info(&mut bucket_info, None);
        let value = serde_json::to_value(&bucket_info).expect("serialize partial-only bucket");
        assert!(value.get("objects").is_none());
        assert!(value.get("size").is_none());
        assert_eq!(value["scan_progress"]["objects"], 7);
        assert_eq!(value["scan_progress"]["size"], 71);
        let mut usage = BucketUsageInfo {
            size: 900,
            objects_count: 90,
            ..Default::default()
        };
        usage.certify(rustfs_data_usage::BucketUsageCertification {
            epoch: 1,
            cycle: 2,
            certified_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(500),
        });
        apply_usage_to_bucket_access_info(&mut bucket_info, Some(&usage));
        assert_eq!((bucket_info.objects, bucket_info.size), (Some(90), Some(900)));
        assert_eq!(bucket_info.usage_updated_at, Some(500));
        assert_eq!(bucket_info.scan_progress.expect("separate partial view").objects, Some(7));
    }

    #[test]
    fn accountinfo_data_usage_error_message_is_generic() {
        let err = map_data_usage_result::<&str>(Err("sensitive disk path")).expect_err("load failure must reach the response");

        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(err.message(), Some("failed to load data usage"));
    }

    #[test]
    fn accountinfo_bucket_details_marks_object_lock_enabled() {
        let enabled = ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            ..Default::default()
        };
        let disabled = ObjectLockConfiguration::default();

        assert!(object_lock_config_enabled(&enabled));
        assert!(!object_lock_config_enabled(&disabled));
    }

    #[test]
    fn accountinfo_bucket_details_marks_replication_enabled_rules() {
        let enabled = replication_config_with_status(ReplicationRuleStatus::ENABLED);
        let disabled = replication_config_with_status(ReplicationRuleStatus::DISABLED);

        assert!(replication_config_enabled(&enabled));
        assert!(!replication_config_enabled(&disabled));
    }

    fn replication_config_with_status(status: &'static str) -> ReplicationConfiguration {
        ReplicationConfiguration {
            role: "arn:aws:iam::123456789012:role/replication".to_string(),
            rules: vec![ReplicationRule {
                delete_marker_replication: None,
                delete_replication: None,
                destination: Destination {
                    bucket: "arn:aws:s3:::target".to_string(),
                    ..Default::default()
                },
                existing_object_replication: None,
                filter: None,
                id: None,
                prefix: None,
                priority: None,
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(status),
            }],
        }
    }
}
