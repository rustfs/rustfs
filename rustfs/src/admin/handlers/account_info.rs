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

use crate::admin::router::Operation;
use crate::auth::{check_key_valid, get_condition_values, get_session_token};
use crate::server::RemoteAddr;
use http::{HeaderMap, HeaderValue};
use hyper::StatusCode;
use matchit::Params;
use rustfs_credentials::get_global_action_cred;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOptions, StorageAPI};
use rustfs_iam::store::MappedPolicy;
use rustfs_policy::policy::BucketPolicy;
use rustfs_policy::policy::default::DEFAULT_POLICIES;
use rustfs_policy::policy::{Args, action::Action, action::S3Action};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: rustfs_madmin::BackendInfo,
    pub policy: BucketPolicy,
}

pub struct AccountInfoHandler {}

fn resolve_bucket_access(can_list_bucket: bool, can_get_bucket_location: bool, can_put_object: bool) -> (bool, bool) {
    (can_list_bucket || can_get_bucket_location, can_put_object)
}

#[async_trait::async_trait]
impl Operation for AccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Ok(iam_store) = rustfs_iam::get() else {
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

        let claims_args = Args {
            account: "",
            groups: &None,
            action: Action::None,
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims,
            deny_only: false,
        };

        let role_arn = claims_args.get_role_arn();

        // TODO: get_policies_from_claims(claims);

        let Some(admin_cred) = get_global_action_cred() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "get_global_action_cred failed".to_string(),
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
        } else if let Some(arn) = role_arn {
            let (_, policy_name) = iam_store
                .get_role_policy(arn)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

            let policies = MappedPolicy::new(&policy_name).to_slice();
            effective_policy = iam_store.get_combined_policy(&policies).await;
        } else {
            let policies = iam_store
                .policy_db_get(&account_name, &cred.groups)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get policy failed: {e}")))?;

            effective_policy = iam_store.get_combined_policy(&policies).await;
        };

        let policy_str = serde_json::to_string(&effective_policy)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse policy failed"))?;

        let mut account_info = rustfs_madmin::AccountInfo {
            account_name,
            server: store.backend_info().await,
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

        for bucket in buckets.iter() {
            let (rd, wr) = is_allow(bucket.name.clone()).await;
            if rd || wr {
                // TODO: BucketQuotaSys
                // TODO: other attributes
                account_info.buckets.push(rustfs_madmin::BucketAccessInfo {
                    name: bucket.name.clone(),
                    details: Some(rustfs_madmin::BucketDetails {
                        versioning: BucketVersioningSys::enabled(bucket.name.as_str()).await,
                        versioning_suspended: BucketVersioningSys::suspended(bucket.name.as_str()).await,
                        ..Default::default()
                    }),
                    created: bucket.created,
                    access: rustfs_madmin::AccountAccess { read: rd, write: wr },
                    ..Default::default()
                });
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
}
