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
use crate::admin::runtime_sources::{current_action_credentials, current_object_store_handle};
use crate::admin::storage_api::bucket::versioning_sys::BucketVersioningSys;
use crate::admin::storage_api::contract::admin::StorageAdminApi;
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions};
use crate::admin::storage_api::data_usage::{
    apply_bucket_usage_memory_overlay, load_data_usage_from_backend, refresh_bucket_usage_from_object_layer,
    refresh_versioned_bucket_usage_from_object_layer, replace_bucket_usage_memory_from_info,
};
use crate::admin::storage_api::metadata_sys;
use crate::auth::get_condition_values;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_data_usage::BucketUsageInfo;
use rustfs_policy::policy::BucketPolicy;
use rustfs_policy::policy::default::DEFAULT_POLICIES;
use rustfs_policy::policy::{Args, action::Action, action::S3Action};
use s3s::dto::{ObjectLockConfiguration, ObjectLockEnabled, ReplicationConfiguration, ReplicationRuleStatus};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

#[allow(dead_code)]
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: rustfs_madmin::BackendInfo,
    pub policy: BucketPolicy,
}

pub struct AccountInfoHandler {}

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

fn apply_usage_to_bucket_access_info(bucket_info: &mut rustfs_madmin::BucketAccessInfo, usage: Option<&BucketUsageInfo>) {
    let Some(usage) = usage else {
        return;
    };

    bucket_info.size = usage.size;
    bucket_info.objects = usage.objects_count;
    bucket_info.object_sizes_histogram = usage.object_size_histogram.clone();
    bucket_info.object_versions_histogram = usage.object_versions_histogram.clone();
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
        let Some(store) = current_object_store_handle() else {
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
        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
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

        let mut data_usage_info = load_data_usage_from_backend(store.clone())
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;
        refresh_versioned_bucket_usage_from_object_layer(store.clone(), &mut data_usage_info).await;
        replace_bucket_usage_memory_from_info(&data_usage_info).await;
        apply_bucket_usage_memory_overlay(&mut data_usage_info).await;

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
                // AccountInfo backs Console bucket stats, so prefer object-layer usage over potentially cold scanner snapshots.
                if let Err(err) = refresh_bucket_usage_from_object_layer(store.clone(), &mut data_usage_info, &bucket.name).await
                {
                    debug!(
                        bucket = %bucket.name,
                        error = %err,
                        "failed to refresh account info bucket usage from object layer"
                    );
                }
                apply_usage_to_bucket_access_info(&mut bucket_info, data_usage_info.buckets_usage.get(&bucket.name));
                account_info.buckets.push(bucket_info);
            }
        }

        let data = serde_json::to_vec(&account_info)
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

        assert_eq!(bucket_info.size, usage.size);
        assert_eq!(bucket_info.objects, usage.objects_count);
        assert_eq!(bucket_info.object_sizes_histogram, usage.object_size_histogram);
        assert_eq!(bucket_info.object_versions_histogram, usage.object_versions_histogram);
    }

    #[test]
    fn accountinfo_bucket_access_info_ignores_missing_usage() {
        let mut bucket_info = rustfs_madmin::BucketAccessInfo {
            name: "agent".to_string(),
            size: 5,
            objects: 2,
            ..Default::default()
        };

        apply_usage_to_bucket_access_info(&mut bucket_info, None);

        assert_eq!(bucket_info.size, 5);
        assert_eq!(bucket_info.objects, 2);
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
