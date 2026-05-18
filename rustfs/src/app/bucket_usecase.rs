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

//! Bucket application use-case contracts.

use crate::admin::handlers::site_replication::{
    site_replication_bucket_meta_hook, site_replication_delete_bucket_hook, site_replication_make_bucket_hook,
};
use crate::app::context::{AppContext, default_notify_interface, get_global_app_context};
use crate::auth::get_condition_values_with_client_info;
use crate::error::ApiError;
use crate::server::RemoteAddr;
use crate::storage::access::{ReqInfo, authorize_request, req_info_ref};
use crate::storage::helper::{OperationHelper, spawn_background_with_context};
use crate::storage::s3_api::bucket::{
    ListObjectVersionsParams, ListObjectsV2Params, build_list_buckets_output, build_list_object_versions_output,
    build_list_objects_output, build_list_objects_v2_output, parse_list_object_versions_params, parse_list_objects_v2_params,
};
use crate::storage::s3_api::common::rustfs_owner;
use crate::storage::*;
use futures::StreamExt;
use http::StatusCode;
use metrics::counter;
use rustfs_config::RUSTFS_REGION;
use rustfs_ecstore::bucket::{
    bucket_target_sys::BucketTargetSys,
    lifecycle::bucket_lifecycle_ops::{
        enqueue_expiry_for_existing_objects, enqueue_transition_for_existing_objects, validate_transition_tier,
    },
    metadata::{
        BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
        BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG,
        BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
    },
    metadata_sys,
    object_lock::ObjectLockApi,
    policy_sys::PolicySys,
    target::{BucketTargetType, BucketTargets},
    utils::serialize,
    versioning::VersioningApi,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::notification_sys::get_global_notification_sys;
use rustfs_ecstore::store_api::{
    BucketOperations, BucketOptions, DeleteBucketOptions, ListObjectVersionsInfo, ListObjectsV2Info, ListOperations,
    MakeBucketOptions, ObjectInfo,
};
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_policy::policy::{
    action::{Action, S3Action},
    {BucketPolicy, BucketPolicyArgs, Effect, Validator},
};
use rustfs_s3_common::S3Operation;
use rustfs_targets::{
    EventName,
    arn::{ARN, TargetIDError},
};
use rustfs_trusted_proxies::ClientInfo;
use rustfs_utils::http::{SUFFIX_FORCE_DELETE, get_header};
use rustfs_utils::obj::extract_user_defined_metadata;
use rustfs_utils::string::parse_bool;
use s3s::dto::*;
use s3s::region::Region;
use s3s::xml;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};
use tracing::{debug, error, info, instrument, warn};
use urlencoding::encode;

fn serialize_config<T: xml::Serialize>(value: &T) -> S3Result<Vec<u8>> {
    serialize(value).map_err(to_internal_error)
}

fn to_internal_error(err: impl Display) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{err}"))
}

fn is_valid_notification_filter_value(value: &str) -> bool {
    if value.len() > 1024 || value.contains('\\') {
        return false;
    }
    !value.split('/').any(|segment| segment == "." || segment == "..")
}

fn invalid_filter_value_message(cfg_scope: &str, value: &str) -> String {
    format!("invalid notification filter value (len={}) ({cfg_scope})", value.len())
}

fn invalid_filter_name_message(cfg_scope: &str, name: &str) -> String {
    format!(
        "invalid notification filter name (len={}) (only 'prefix'/'suffix' are supported) ({cfg_scope})",
        name.len()
    )
}

fn validate_notification_filter_rules(
    filter: Option<&NotificationConfigurationFilter>,
    cfg_kind: &str,
    cfg_id: Option<&str>,
) -> S3Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    let Some(s3key_filter) = filter.key.as_ref() else {
        return Ok(());
    };
    let Some(rules) = s3key_filter.filter_rules.as_ref() else {
        return Ok(());
    };

    let mut has_prefix = false;
    let mut has_suffix = false;
    let cfg_scope = cfg_id.map_or_else(|| cfg_kind.to_string(), |id| format!("{cfg_kind} id={id}"));

    for rule in rules {
        let Some(name) = rule.name.as_ref() else {
            return Err(s3_error!(InvalidArgument, "invalid notification filter rule: missing Name ({cfg_scope})"));
        };
        let Some(value) = rule.value.as_ref() else {
            return Err(s3_error!(
                InvalidArgument,
                "invalid notification filter rule: missing Value ({cfg_scope})"
            ));
        };

        if !is_valid_notification_filter_value(value) {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_value_message(&cfg_scope, value)));
        }

        if name.as_str().eq_ignore_ascii_case("prefix") {
            if has_prefix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'prefix' ({cfg_scope})"));
            }
            has_prefix = true;
        } else if name.as_str().eq_ignore_ascii_case("suffix") {
            if has_suffix {
                return Err(s3_error!(InvalidArgument, "duplicate notification filter name 'suffix' ({cfg_scope})"));
            }
            has_suffix = true;
        } else {
            return Err(s3_error!(InvalidArgument, "{}", invalid_filter_name_message(&cfg_scope, name.as_str())));
        }
    }

    Ok(())
}

fn validate_notification_configuration_filters(notification_configuration: &NotificationConfiguration) -> S3Result<()> {
    if let Some(queue_configs) = notification_configuration.queue_configurations.as_ref() {
        for cfg in queue_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "QueueConfiguration", cfg.id.as_deref())?;
        }
    }
    if let Some(topic_configs) = notification_configuration.topic_configurations.as_ref() {
        for cfg in topic_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "TopicConfiguration", cfg.id.as_deref())?;
        }
    }
    if let Some(lambda_configs) = notification_configuration.lambda_function_configurations.as_ref() {
        for cfg in lambda_configs {
            validate_notification_filter_rules(cfg.filter.as_ref(), "LambdaFunctionConfiguration", cfg.id.as_deref())?;
        }
    }
    Ok(())
}

fn sr_bucket_meta_item(bucket: String, item_type: &str) -> SRBucketMeta {
    SRBucketMeta {
        bucket,
        r#type: item_type.to_string(),
        updated_at: Some(time::OffsetDateTime::now_utc()),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

fn notify_bucket_metadata_reload(
    bucket: String,
    operation: &'static str,
    request_context: Option<request_context::RequestContext>,
) {
    spawn_background_with_context(request_context, async move {
        if let Some(notification_sys) = get_global_notification_sys()
            && let Err(err) = notification_sys.load_bucket_metadata(&bucket).await
        {
            warn!(bucket = %bucket, error = %err, "failed to notify peers after {operation}");
        }
    });
}

fn replication_target_arns(config: &ReplicationConfiguration) -> HashSet<String> {
    let mut arns = HashSet::new();

    if !config.role.trim().is_empty() {
        arns.insert(config.role.clone());
        return arns;
    }

    for rule in &config.rules {
        let arn = rule.destination.bucket.trim();
        if !arn.is_empty() {
            arns.insert(arn.to_string());
        }
    }

    arns
}

fn validate_replication_config_targets(targets: &BucketTargets, config: &ReplicationConfiguration) -> S3Result<()> {
    let configured_arns = targets
        .targets
        .iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .map(|target| target.arn.as_str())
        .collect::<HashSet<_>>();

    for rule in &config.rules {
        if rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED) {
            continue;
        }

        let configured_arn = if config.role.trim().is_empty() {
            rule.destination.bucket.trim()
        } else {
            config.role.trim()
        };

        if !configured_arn.is_empty() && configured_arns.contains(configured_arn) {
            continue;
        }

        return Err(s3_error!(
            InvalidRequest,
            "replication config with rule ID {} has a stale target",
            rule.id.clone().unwrap_or_default()
        ));
    }

    Ok(())
}

async fn validate_bucket_replication_update(bucket: &str, config: &ReplicationConfiguration) -> S3Result<()> {
    if !BucketVersioningSys::enabled(bucket).await {
        return Err(s3_error!(
            InvalidRequest,
            "bucket versioning must be enabled before replication can be configured"
        ));
    }

    let targets = metadata_sys::get_bucket_targets_config(bucket)
        .await
        .map_err(|err| match err {
            StorageError::ConfigNotFound => {
                S3Error::with_message(S3ErrorCode::InvalidRequest, "replication target configuration not found".to_string())
            }
            other => ApiError::from(other).into(),
        })?;

    validate_replication_config_targets(&targets, config)
}

async fn remove_replication_targets_for_config(bucket: &str, config: &ReplicationConfiguration) -> S3Result<()> {
    let target_arns = replication_target_arns(config);
    if target_arns.is_empty() {
        return Ok(());
    }

    let mut targets = match metadata_sys::get_bucket_targets_config(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => {
            BucketTargetSys::get().update_all_targets(bucket, None).await;
            return Ok(());
        }
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let original_len = targets.targets.len();
    targets.targets.retain(|target| {
        target.target_type != BucketTargetType::ReplicationService || !target_arns.contains(target.arn.as_str())
    });

    if targets.targets.len() == original_len {
        return Ok(());
    }

    let removed = original_len - targets.targets.len();
    let json_targets = serde_json::to_vec(&targets).map_err(to_internal_error)?;
    metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
        .await
        .map_err(ApiError::from)?;
    BucketTargetSys::get().update_all_targets(bucket, Some(&targets)).await;
    info!(bucket = %bucket, removed, "removed replication remote targets referenced by deleted bucket replication config");

    Ok(())
}

fn versioning_configuration_has_object_lock_incompatible_settings(config: &VersioningConfiguration) -> bool {
    config.suspended()
        || config.exclude_folders.unwrap_or(false)
        || config
            .excluded_prefixes
            .as_ref()
            .is_some_and(|excluded_prefixes| !excluded_prefixes.is_empty())
}

async fn validate_bucket_versioning_update(bucket: &str, config: &VersioningConfiguration) -> S3Result<()> {
    match metadata_sys::get_object_lock_config(bucket).await {
        Ok((object_lock_config, _)) => {
            if object_lock_config.enabled() && versioning_configuration_has_object_lock_incompatible_settings(config) {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidBucketState,
                    "An Object Lock configuration is present on this bucket, versioning cannot be suspended.".to_string(),
                ));
            }
        }
        Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ObjectMetadataPermissions {
    metadata_allowed: bool,
    tags_allowed: bool,
}

fn encode_list_versions_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type.is_some_and(|encoding| encoding.as_str() == EncodingType::URL) {
        encode(value).into_owned()
    } else {
        value.to_string()
    }
}

fn encode_list_objects_v2_value(value: &str, encoding_type: Option<&EncodingType>) -> String {
    if encoding_type.is_some_and(|encoding| encoding.as_str() == EncodingType::URL) {
        value
            .split('/')
            .map(|part| encode(part).into_owned())
            .collect::<Vec<_>>()
            .join("/")
    } else {
        value.to_string()
    }
}

fn build_metadata_extension_user_metadata(user_defined: &HashMap<String, String>) -> Option<UserMetadataCollection> {
    let mut items = extract_user_defined_metadata(user_defined)
        .into_iter()
        .filter(|(key, _)| !key.is_empty())
        .map(|(key, value)| UserMetadataEntry { key, value })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.key.cmp(&right.key));

    if items.is_empty() {
        None
    } else {
        Some(UserMetadataCollection { items })
    }
}

async fn is_list_objects_metadata_action_allowed<T>(
    req: &S3Request<T>,
    bucket: &str,
    object: &str,
    action: S3Action,
) -> S3Result<bool> {
    let mut auth_req = S3Request {
        input: (),
        method: req.method.clone(),
        uri: req.uri.clone(),
        headers: req.headers.clone(),
        extensions: req.extensions.clone(),
        credentials: req.credentials.clone(),
        region: req.region.clone(),
        service: req.service.clone(),
        trailing_headers: req.trailing_headers.clone(),
    };

    let mut req_info = req_info_ref(req)?.clone();
    req_info.bucket = Some(bucket.to_string());
    req_info.object = Some(object.to_string());
    req_info.version_id = None;
    auth_req.extensions.insert(req_info);

    match authorize_request(&mut auth_req, Action::S3Action(action)).await {
        Ok(()) => Ok(true),
        Err(err) if err.code() == &S3ErrorCode::AccessDenied => Ok(false),
        Err(err) => Err(err),
    }
}

async fn collect_list_objects_metadata_permissions<T>(
    req: &S3Request<T>,
    bucket: &str,
    objects: &[ObjectInfo],
) -> S3Result<HashMap<String, ObjectMetadataPermissions>> {
    let mut permissions = HashMap::new();

    for object in objects {
        if object.name.is_empty() || permissions.contains_key(&object.name) {
            continue;
        }

        let metadata_allowed =
            is_list_objects_metadata_action_allowed(req, bucket, &object.name, S3Action::GetObjectAction).await?;
        let tags_allowed =
            is_list_objects_metadata_action_allowed(req, bucket, &object.name, S3Action::GetObjectTaggingAction).await?;

        permissions.insert(
            object.name.clone(),
            ObjectMetadataPermissions {
                metadata_allowed,
                tags_allowed,
            },
        );
    }

    Ok(permissions)
}

fn build_list_object_versions_m_output(
    object_infos: ListObjectVersionsInfo,
    bucket: &str,
    params: &ListObjectVersionsParams,
    encoding_type: Option<&EncodingType>,
    permissions: &HashMap<String, ObjectMetadataPermissions>,
) -> ListObjectVersionsMOutput {
    let owner = rustfs_owner();
    let common_prefixes = object_infos
        .prefixes
        .into_iter()
        .map(|prefix_value| CommonPrefix {
            prefix: Some(encode_list_versions_value(&prefix_value, encoding_type)),
        })
        .collect::<Vec<_>>();

    let entries = object_infos
        .objects
        .into_iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            let object_name = encode_list_versions_value(&object.name, encoding_type);
            let version_id = object
                .version_id
                .map(|version| version.to_string())
                .unwrap_or_else(|| "null".to_string());
            let permission = permissions.get(&object.name).copied().unwrap_or_default();
            let user_metadata = if permission.metadata_allowed {
                build_metadata_extension_user_metadata(&object.user_defined)
            } else {
                None
            };
            let user_tags = if permission.tags_allowed && !object.user_tags.is_empty() {
                Some(object.user_tags.clone())
            } else {
                None
            };
            let internal = if permission.metadata_allowed && (object.data_blocks > 0 || object.parity_blocks > 0) {
                Some(ObjectInternalInfo {
                    k: object.data_blocks as i32,
                    m: object.parity_blocks as i32,
                })
            } else {
                None
            };

            if object.delete_marker {
                ListObjectVersionMEntry::DeleteMarker(DeleteMarkerM {
                    key: Some(object_name),
                    last_modified: object.mod_time.map(Timestamp::from),
                    owner: Some(owner.clone()),
                    version_id: Some(version_id),
                    is_latest: Some(object.is_latest),
                    user_metadata,
                    user_tags,
                    internal,
                })
            } else {
                ListObjectVersionMEntry::Version(ObjectVersionM {
                    key: Some(object_name),
                    last_modified: object.mod_time.map(Timestamp::from),
                    size: Some(object.size),
                    version_id: Some(version_id),
                    is_latest: Some(object.is_latest),
                    e_tag: object.etag.clone().map(|etag| to_s3s_etag(&etag)),
                    storage_class: Some(ObjectVersionStorageClass::from(
                        object
                            .storage_class
                            .unwrap_or_else(|| ObjectVersionStorageClass::STANDARD.to_string()),
                    )),
                    owner: Some(owner.clone()),
                    user_metadata,
                    user_tags,
                    internal,
                })
            }
        })
        .collect::<Vec<_>>();

    let next_key_marker = object_infos
        .next_marker
        .filter(|marker| !marker.is_empty())
        .map(|marker| encode_list_versions_value(&marker, encoding_type));
    let next_version_id_marker = object_infos.next_version_idmarker.filter(|marker| !marker.is_empty());

    ListObjectVersionsMOutput {
        common_prefixes: Some(common_prefixes),
        delimiter: params
            .delimiter
            .clone()
            .map(|value| encode_list_versions_value(&value, encoding_type)),
        encoding_type: encoding_type.cloned(),
        is_truncated: Some(object_infos.is_truncated),
        key_marker: Some(encode_list_versions_value(
            params.key_marker.as_deref().unwrap_or_default(),
            encoding_type,
        )),
        max_keys: Some(params.max_keys),
        name: Some(bucket.to_owned()),
        next_key_marker,
        next_version_id_marker,
        prefix: Some(encode_list_versions_value(&params.prefix, encoding_type)),
        request_charged: None,
        version_id_marker: Some(params.version_id_marker.clone().unwrap_or_default()),
        entries,
    }
}

fn build_list_objects_v2m_output(
    object_infos: ListObjectsV2Info,
    bucket: &str,
    params: &ListObjectsV2Params,
    encoding_type: Option<&EncodingType>,
    fetch_owner: bool,
    permissions: &HashMap<String, ObjectMetadataPermissions>,
) -> ListObjectsV2MOutput {
    let owner = rustfs_owner();

    let contents = object_infos
        .objects
        .iter()
        .filter(|object| !object.name.is_empty())
        .map(|object| {
            let permission = permissions.get(&object.name).copied().unwrap_or_default();
            let user_metadata = if permission.metadata_allowed {
                build_metadata_extension_user_metadata(&object.user_defined)
            } else {
                None
            };
            let user_tags = if permission.tags_allowed && !object.user_tags.is_empty() {
                Some(object.user_tags.clone())
            } else {
                None
            };
            let internal = if permission.metadata_allowed && (object.data_blocks > 0 || object.parity_blocks > 0) {
                Some(ObjectInternalInfo {
                    k: object.data_blocks as i32,
                    m: object.parity_blocks as i32,
                })
            } else {
                None
            };

            ObjectM {
                key: Some(encode_list_objects_v2_value(&object.name, encoding_type)),
                last_modified: object.mod_time.map(Timestamp::from),
                size: Some(object.get_actual_size().unwrap_or_default()),
                e_tag: object.etag.clone().map(|etag| to_s3s_etag(&etag)),
                storage_class: Some(ObjectStorageClass::from(
                    object
                        .storage_class
                        .clone()
                        .unwrap_or_else(|| ObjectStorageClass::STANDARD.to_string()),
                )),
                owner: fetch_owner.then_some(owner.clone()),
                user_metadata,
                user_tags,
                internal,
            }
        })
        .collect::<Vec<_>>();

    let common_prefixes = object_infos
        .prefixes
        .into_iter()
        .map(|prefix| CommonPrefix {
            prefix: Some(encode_list_objects_v2_value(&prefix, encoding_type)),
        })
        .collect::<Vec<_>>();

    let key_count = (contents.len() + common_prefixes.len()) as i32;
    let next_continuation_token = object_infos
        .next_continuation_token
        .map(|token| base64_simd::STANDARD.encode_to_string(token.as_bytes()));

    ListObjectsV2MOutput {
        name: Some(bucket.to_owned()),
        prefix: Some(params.prefix.clone()),
        max_keys: Some(params.max_keys),
        key_count: Some(key_count),
        continuation_token: params.response_continuation_token.clone(),
        is_truncated: Some(object_infos.is_truncated),
        next_continuation_token,
        contents: Some(contents),
        common_prefixes: Some(common_prefixes),
        delimiter: params.delimiter.clone(),
        encoding_type: encoding_type.cloned(),
        start_after: params.response_start_after.clone(),
        ..Default::default()
    }
}

fn create_bucket_exists_response(is_owner: bool) -> S3Result<S3Response<CreateBucketOutput>> {
    if is_owner {
        return Ok(S3Response::new(CreateBucketOutput::default()));
    }

    Err(s3_error!(
        BucketAlreadyExists,
        "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again."
    ))
}

fn resolve_notification_region(global_region: Option<Region>, request_region: Option<Region>) -> String {
    global_region
        .or(request_region)
        .map(|region| region.to_string())
        .unwrap_or_else(|| RUSTFS_REGION.to_string())
}

const ERR_LIFECYCLE_RULE_STATUS: &str = "Rule status must be either Enabled or Disabled";

fn assign_lifecycle_rule_ids(rules: &mut [LifecycleRule]) {
    let mut rule_ids: HashSet<String> = HashSet::new();

    for rule in rules.iter() {
        if let Some(id) = rule.id.as_ref() {
            rule_ids.insert(id.to_string());
        }
    }

    for (idx, rule) in rules.iter_mut().enumerate() {
        if rule.id.is_none() {
            let mut suffix = 0usize;
            let mut generated_id = format!("rule-{}", idx);

            while rule_ids.contains(&generated_id) {
                suffix += 1;
                generated_id = format!("rule-{idx}-{suffix}");
            }

            rule_ids.insert(generated_id.clone());
            rule.id = Some(generated_id);
        }
    }
}

fn validate_lifecycle_rule_status(rules: &[LifecycleRule]) -> Result<(), &'static str> {
    for rule in rules {
        if rule.status != ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            && rule.status != ExpirationStatus::from_static(ExpirationStatus::DISABLED)
        {
            return Err(ERR_LIFECYCLE_RULE_STATUS);
        }
    }

    Ok(())
}

fn lifecycle_has_transition_rules(config: &BucketLifecycleConfiguration) -> bool {
    config.rules.iter().any(|rule| {
        rule.status == ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            && (rule.transitions.as_ref().is_some_and(|transitions| {
                transitions.iter().any(|transition| {
                    transition
                        .storage_class
                        .as_ref()
                        .is_some_and(|storage_class| !storage_class.as_str().is_empty())
                })
            }) || rule.noncurrent_version_transitions.as_ref().is_some_and(|transitions| {
                transitions.iter().any(|transition| {
                    transition
                        .storage_class
                        .as_ref()
                        .is_some_and(|storage_class| !storage_class.as_str().is_empty())
                })
            }))
    })
}

fn lifecycle_has_expiry_rules(config: &BucketLifecycleConfiguration) -> bool {
    config.rules.iter().any(|rule| {
        rule.status == ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            && (rule.expiration.is_some() || rule.del_marker_expiration.is_some() || rule.noncurrent_version_expiration.is_some())
    })
}

#[derive(Clone, Default)]
pub struct DefaultBucketUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultBucketUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    fn global_region(&self) -> Option<Region> {
        self.context.as_ref().and_then(|context| context.region().get())
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketCreated, S3Operation::CreateBucket);
        let requester_is_owner = match req_info_ref(&req) {
            Ok(r) => r.is_owner,
            Err(_) => {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Missing request info".to_string()));
            }
        };
        let CreateBucketInput {
            bucket,
            object_lock_enabled_for_bucket,
            ..
        } = req.input;
        let lock_enabled = object_lock_enabled_for_bucket.is_some_and(|v| v);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let make_result = store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    force_create: false,
                    lock_enabled,
                    ..Default::default()
                },
            )
            .await;

        match make_result {
            Ok(()) => {}
            Err(StorageError::BucketExists(_)) => {
                // Per S3 spec: bucket namespace is global. Owner recreating returns 200 OK;
                // non-owner gets 409 BucketAlreadyExists.
                let result = create_bucket_exists_response(requester_is_owner);
                let _ = helper.complete(&result);
                return result;
            }
            Err(e) => return Err(ApiError::from(e).into()),
        }

        if let Err(err) = site_replication_make_bucket_hook(&bucket, lock_enabled).await {
            warn!(bucket = %bucket, error = ?err, "site replication make bucket hook failed");
        }

        let output = CreateBucketOutput::default();
        counter!("rustfs_create_bucket_total").increment(1);
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_bucket(&self, mut req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketRemoved, S3Operation::DeleteBucket);
        let input = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let force_str = get_header(&req.headers, SUFFIX_FORCE_DELETE)
            .map(|v| v.into_owned())
            .unwrap_or_default();

        let force = parse_bool(&force_str).unwrap_or_default();

        if force {
            authorize_request(&mut req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }

        store
            .delete_bucket(
                &input.bucket,
                &DeleteBucketOptions {
                    force,
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        if let Err(err) = site_replication_delete_bucket_hook(&input.bucket, force).await {
            warn!(bucket = %input.bucket, error = ?err, "site replication delete bucket hook failed");
        }

        let result = Ok(S3Response::new(DeleteBucketOutput {}));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(region) = self.global_region() {
            return Ok(S3Response::new(GetBucketLocationOutput {
                location_constraint: Some(BucketLocationConstraint::from(region.to_string())),
            }));
        }

        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut req = req;

        if req.credentials.as_ref().is_none_or(|cred| cred.access_key.is_empty()) {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
        }

        let bucket_infos = if let Err(e) = authorize_request(&mut req, Action::S3Action(S3Action::ListAllMyBucketsAction)).await {
            if e.code() != &S3ErrorCode::AccessDenied {
                return Err(e);
            }

            let mut list_bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

            list_bucket_infos = futures::stream::iter(list_bucket_infos)
                .filter_map(|info| async {
                    let mut req_clone = req.clone();
                    let Some(req_info) = req_clone.extensions.get_mut::<ReqInfo>() else {
                        debug!(bucket = %info.name, "ReqInfo missing in extensions, skipping bucket authorization");
                        return None;
                    };
                    req_info.bucket = Some(info.name.clone());

                    if authorize_request(&mut req_clone, Action::S3Action(S3Action::ListBucketAction))
                        .await
                        .is_ok()
                        || authorize_request(&mut req_clone, Action::S3Action(S3Action::GetBucketLocationAction))
                            .await
                            .is_ok()
                    {
                        Some(info)
                    } else {
                        None
                    }
                })
                .collect()
                .await;

            if list_bucket_infos.is_empty() {
                return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Access Denied"));
            }
            list_bucket_infos
        } else {
            store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?
        };

        Ok(S3Response::new(build_list_buckets_output(&bucket_infos)))
    }

    pub async fn execute_delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let DeleteBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_SSECONFIG)
            .await
            .map_err(ApiError::from)?;

        let item = sr_bucket_meta_item(bucket.clone(), "sse-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket encryption delete hook failed");
        }

        Ok(S3Response::with_status(DeleteBucketEncryptionOutput::default(), StatusCode::NO_CONTENT))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let DeleteBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_CORS_CONFIG)
            .await
            .map_err(ApiError::from)?;

        let item = sr_bucket_meta_item(bucket.clone(), "cors-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket cors delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketCorsOutput {}))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketLifecycleInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_LIFECYCLE_CONFIG)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket lifecycle", request_context);

        let item = sr_bucket_meta_item(bucket.clone(), "lc-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket lifecycle delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    pub async fn execute_delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeleteBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_POLICY_CONFIG)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete bucket policy", request_context);

        let item = sr_bucket_meta_item(bucket.clone(), "policy");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket policy delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketPolicyOutput {}))
    }

    pub async fn execute_delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let DeleteBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        let replication_config = match metadata_sys::get_replication_config(&bucket).await {
            Ok((config, _)) => Some(config),
            Err(StorageError::ConfigNotFound) => None,
            Err(err) => return Err(ApiError::from(err).into()),
        };

        metadata_sys::delete(&bucket, BUCKET_REPLICATION_CONFIG)
            .await
            .map_err(ApiError::from)?;
        if let Some(config) = replication_config.as_ref()
            && let Err(err) = remove_replication_targets_for_config(&bucket, config).await
        {
            warn!(bucket = %bucket, error = ?err, "failed to remove replication targets referenced by deleted bucket replication config");
        }

        let item = sr_bucket_meta_item(bucket.clone(), "replication-config");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config delete hook failed");
        }

        info!(bucket = %bucket, "deleted bucket replication config");

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let DeleteBucketTaggingInput { bucket, .. } = req.input;

        metadata_sys::delete(&bucket, BUCKET_TAGGING_CONFIG)
            .await
            .map_err(ApiError::from)?;

        let item = sr_bucket_meta_item(bucket.clone(), "tags");
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket tagging delete hook failed");
        }

        Ok(S3Response::new(DeleteBucketTaggingOutput {}))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let DeletePublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "delete public access block", request_context);

        Ok(S3Response::with_status(DeletePublicAccessBlockOutput::default(), StatusCode::NO_CONTENT))
    }

    pub async fn execute_get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let GetBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let server_side_encryption_configuration = match metadata_sys::get_sse_config(&bucket).await {
            Ok((cfg, _)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(s3_error!(ServerSideEncryptionConfigurationNotFoundError));
                }
                warn!("get_sse_config err {:?}", err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration,
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let GetBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cors_configuration = match metadata_sys::get_cors_config(&bucket).await {
            Ok((config, _)) => config,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::NoSuchCORSConfiguration,
                        "The CORS configuration does not exist".to_string(),
                    ));
                }
                warn!("get_cors_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetBucketCorsOutput {
            cors_rules: Some(cors_configuration.cors_rules),
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let GetBucketLifecycleConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let rules = match metadata_sys::get_lifecycle_config(&bucket).await {
            Ok((cfg, _)) => cfg.rules,
            Err(_) => {
                return Err(s3_error!(NoSuchLifecycleConfiguration));
            }
        };

        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules: Some(rules),
            ..Default::default()
        }))
    }

    pub async fn execute_get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        let GetBucketNotificationConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let has_notification_config = metadata_sys::get_notification_config(&bucket).await.unwrap_or_else(|err| {
            warn!("get_notification_config err {:?}", err);
            None
        });

        if let Some(NotificationConfiguration {
            event_bridge_configuration,
            lambda_function_configurations,
            queue_configurations,
            topic_configurations,
        }) = has_notification_config
        {
            Ok(S3Response::new(GetBucketNotificationConfigurationOutput {
                event_bridge_configuration,
                lambda_function_configurations,
                queue_configurations,
                topic_configurations,
            }))
        } else {
            Ok(S3Response::new(GetBucketNotificationConfigurationOutput::default()))
        }
    }

    pub async fn execute_get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let GetBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let (policy_str, _) = match metadata_sys::get_bucket_policy_raw(&bucket).await {
            Ok(res) => res,
            Err(err) => {
                if StorageError::ConfigNotFound == err {
                    return Err(s3_error!(NoSuchBucketPolicy));
                }
                return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
            }
        };

        Ok(S3Response::new(GetBucketPolicyOutput {
            policy: Some(policy_str),
        }))
    }

    pub async fn execute_get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        let GetBucketPolicyStatusInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        let client_info = req.extensions.get::<ClientInfo>();
        let conditions = get_condition_values_with_client_info(
            &req.headers,
            &rustfs_credentials::Credentials::default(),
            None,
            None,
            remote_addr,
            client_info,
        );

        let read_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::ListBucketAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let write_allowed = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::PutObjectAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let mut is_public = read_allowed || write_allowed;
        let ignore_public_acls = match metadata_sys::get_public_access_block_config(&bucket).await {
            Ok((config, _)) => config.ignore_public_acls.unwrap_or(false),
            Err(_) => false,
        };

        let _ = ignore_public_acls;

        let policy_public = match metadata_sys::get_bucket_policy(&bucket).await {
            Ok((cfg, _)) => cfg.statements.iter().any(|statement| {
                matches!(statement.effect, Effect::Allow)
                    && statement.principal.is_match("*")
                    && statement.conditions.is_empty()
                    && statement.actions.is_match(&Action::S3Action(S3Action::ListBucketAction))
            }),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    false
                } else {
                    return Err(ApiError::from(err).into());
                }
            }
        };

        if policy_public {
            is_public = true;
        }

        let output = GetBucketPolicyStatusOutput {
            policy_status: Some(PolicyStatus {
                is_public: Some(is_public),
            }),
        };

        Ok(S3Response::new(output))
    }

    pub async fn execute_get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let GetBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let replication_configuration = match metadata_sys::get_replication_config(&bucket).await {
            Ok((cfg, _created)) => cfg,
            Err(err) => {
                error!("get_replication_config err {:?}", err);
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ReplicationConfigurationNotFoundError,
                        "replication not found".to_string(),
                    ));
                }
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration: Some(replication_configuration),
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let GetBucketTaggingInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let Tagging { tag_set } = match metadata_sys::get_tagging_config(&bucket).await {
            Ok((tags, _)) => tags,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(S3ErrorCode::NoSuchTagSet, "The TagSet does not exist".to_string()));
                }
                warn!("get_tagging_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetBucketTaggingOutput { tag_set }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        let GetPublicAccessBlockInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let config = match metadata_sys::get_public_access_block_config(&bucket).await {
            Ok((config, _)) => config,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::Custom("NoSuchPublicAccessBlockConfiguration".into()),
                        "Public access block configuration does not exist".to_string(),
                    ));
                }
                return Err(ApiError::from(err).into());
            }
        };

        Ok(S3Response::new(GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(config),
        }))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let GetBucketVersioningInput { bucket, .. } = req.input;
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let VersioningConfiguration { status, .. } = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;

        Ok(S3Response::new(GetBucketVersioningOutput {
            status,
            ..Default::default()
        }))
    }

    pub async fn execute_put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let PutBucketEncryptionInput {
            bucket,
            server_side_encryption_configuration,
            ..
        } = req.input;

        info!("sse_config {:?}", &server_side_encryption_configuration);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&server_side_encryption_configuration)?;
        metadata_sys::update(&bucket, BUCKET_SSECONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let mut item = sr_bucket_meta_item(bucket.clone(), "sse-config");
        item.sse_config = Some(
            serialize_config(&server_side_encryption_configuration)
                .and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket encryption hook failed");
        }
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;

        let Some(mut input_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };
        assign_lifecycle_rule_ids(&mut input_cfg.rules);
        if let Err(err) = validate_lifecycle_rule_status(&input_cfg.rules) {
            return Err(S3Error::with_message(S3ErrorCode::MalformedXML, format!("Malformed XML: {err}")));
        }

        let rcfg = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _)) => cfg,
            Err(StorageError::ConfigNotFound) => ObjectLockConfiguration::default(),
            Err(err) => {
                warn!("get_object_lock_config err {:?}", err);
                return Err(ApiError::from(err).into());
            }
        };

        if let Err(err) = rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle::validate(&input_cfg, &rcfg).await {
            return Err(s3_error!(InvalidArgument, "{err}"));
        }

        if let Err(err) = validate_transition_tier(&input_cfg).await {
            return Err(s3_error!(InvalidArgument, "{err}"));
        }

        input_cfg.expiry_updated_at = Some(Timestamp::from(time::OffsetDateTime::now_utc()));
        let data = serialize_config(&input_cfg)?;
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket lifecycle", request_context);

        let mut item = sr_bucket_meta_item(bucket.clone(), "lc-config");
        item.expiry_lc_config =
            Some(serialize_config(&input_cfg).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        item.expiry_updated_at = item.updated_at;
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket lifecycle hook failed");
        }

        if lifecycle_has_transition_rules(&input_cfg)
            && let Some(store) = new_object_layer_fn()
        {
            let bucket_name = bucket.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
            spawn_background_with_context(request_context, async move {
                if let Err(err) = enqueue_transition_for_existing_objects(store, &bucket_name).await {
                    warn!(bucket = %bucket_name, error = ?err, "failed to enqueue transition for existing objects");
                }
            });
        }

        if lifecycle_has_expiry_rules(&input_cfg)
            && let Some(store) = new_object_layer_fn()
        {
            let bucket_name = bucket.clone();
            let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
            spawn_background_with_context(request_context, async move {
                if let Err(err) = enqueue_expiry_for_existing_objects(store, &bucket_name).await {
                    warn!(bucket = %bucket_name, error = ?err, "failed to enqueue expiry for existing objects");
                }
            });
        }

        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }

    pub async fn execute_put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let request_region = req.region.clone();

        let PutBucketNotificationConfigurationInput {
            bucket,
            notification_configuration,
            ..
        } = req.input;

        validate_notification_configuration_filters(&notification_configuration)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&notification_configuration)?;
        metadata_sys::update(&bucket, BUCKET_NOTIFICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let region = resolve_notification_region(self.global_region(), request_region);
        let notify = self
            .context
            .as_ref()
            .map(|context| context.notify())
            .unwrap_or_else(default_notify_interface);
        let clear_rules = notify.clear_bucket_notification_rules(&bucket);
        let parse_rules = async {
            let mut event_rules = Vec::new();

            process_queue_configurations(&mut event_rules, notification_configuration.queue_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_topic_configurations(&mut event_rules, notification_configuration.topic_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_lambda_configurations(
                &mut event_rules,
                notification_configuration.lambda_function_configurations.clone(),
                |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                },
            )?;

            Ok::<_, TargetIDError>(event_rules)
        };

        let (clear_result, event_rules_result) = tokio::join!(clear_rules, parse_rules);

        clear_result.map_err(|e| s3_error!(InternalError, "Failed to clear rules: {e}"))?;
        let event_rules =
            event_rules_result.map_err(|e| s3_error!(InvalidArgument, "Invalid ARN in notification configuration: {e}"))?;
        warn!("notify event rules: {:?}", &event_rules);
        notify
            .add_event_specific_rules(&bucket, region.as_str(), &event_rules)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

        Ok(S3Response::new(PutBucketNotificationConfigurationOutput {}))
    }

    pub async fn execute_put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cfg: BucketPolicy =
            serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?;

        if let Err(err) = cfg.is_valid() {
            warn!("put_bucket_policy err input {:?}, {:?}", &policy, err);
            return Err(s3_error!(MalformedPolicy));
        }

        let is_public_policy = cfg
            .statements
            .iter()
            .any(|statement| matches!(statement.effect, Effect::Allow) && statement.principal.is_match("*"));

        if is_public_policy {
            match metadata_sys::get_public_access_block_config(&bucket).await {
                Ok((config, _)) => {
                    if config.block_public_policy.unwrap_or(false) {
                        return Err(s3_error!(AccessDenied, "Access Denied"));
                    }
                }
                Err(err) => {
                    if err != StorageError::ConfigNotFound {
                        warn!("get_public_access_block_config err {:?}", &err);
                        return Err(ApiError::from(err).into());
                    }
                }
            }
        }

        let data = policy.as_bytes().to_vec();

        metadata_sys::update(&bucket, BUCKET_POLICY_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put bucket policy", request_context);

        let mut item = sr_bucket_meta_item(bucket.clone(), "policy");
        item.policy = Some(serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket policy hook failed");
        }

        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let PutBucketCorsInput {
            bucket,
            cors_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&cors_configuration)?;
        metadata_sys::update(&bucket, BUCKET_CORS_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let mut item = sr_bucket_meta_item(bucket.clone(), "cors-config");
        item.cors =
            Some(serialize_config(&cors_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket cors hook failed");
        }

        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    pub async fn execute_put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let PutBucketReplicationInput {
            bucket,
            replication_configuration,
            ..
        } = req.input;
        info!(bucket = %bucket, "updating bucket replication config");

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_bucket_replication_update(&bucket, &replication_configuration).await?;
        let data = serialize_config(&replication_configuration)?;
        metadata_sys::update(&bucket, BUCKET_REPLICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let mut item = sr_bucket_meta_item(bucket.clone(), "replication-config");
        item.replication_config = Some(
            serialize_config(&replication_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket replication-config hook failed");
        }

        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let request_context = req.extensions.get::<request_context::RequestContext>().cloned();
        let PutPublicAccessBlockInput {
            bucket,
            public_access_block_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&public_access_block_configuration)?;
        metadata_sys::update(&bucket, BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        notify_bucket_metadata_reload(bucket.clone(), "put public access block", request_context);

        Ok(S3Response::new(PutPublicAccessBlockOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_tagging(
        &self,
        req: S3Request<PutBucketTaggingInput>,
    ) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let PutBucketTaggingInput { bucket, tagging, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = serialize_config(&tagging)?;

        metadata_sys::update(&bucket, BUCKET_TAGGING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let mut item = sr_bucket_meta_item(bucket.clone(), "tags");
        item.tags = Some(serialize_config(&tagging).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?);
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket tagging hook failed");
        }

        Ok(S3Response::new(PutBucketTaggingOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let PutBucketVersioningInput {
            bucket,
            versioning_configuration,
            ..
        } = req.input;

        validate_bucket_versioning_update(&bucket, &versioning_configuration).await?;

        let data = serialize_config(&versioning_configuration)?;

        metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        let mut item = sr_bucket_meta_item(bucket.clone(), "version-config");
        item.versioning = Some(
            serialize_config(&versioning_configuration).and_then(|bytes| String::from_utf8(bytes).map_err(to_internal_error))?,
        );
        if let Err(err) = site_replication_bucket_meta_hook(item).await {
            warn!(bucket = %bucket, error = ?err, "site replication bucket versioning hook failed");
        }

        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        // warn!("list_objects_v2 req {:?}", &req.input);
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = req.input;

        let params = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;

        validate_list_object_unordered_with_delimiter(params.delimiter.as_ref(), req.uri.query())?;

        let store = get_validated_store(&bucket).await?;

        let incl_deleted = get_header(&req.headers, rustfs_utils::http::SUFFIX_INCLUDE_DELETED)
            .map(|v| v.as_ref() == "true")
            .unwrap_or_default();

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &params.prefix,
                params.decoded_continuation_token.clone(),
                params.delimiter.clone(),
                params.max_keys,
                fetch_owner.unwrap_or_default(),
                params.start_after_for_query.clone(),
                incl_deleted,
            )
            .await
            .map_err(ApiError::from)?;

        let output = build_list_objects_v2_output(
            object_infos,
            fetch_owner.unwrap_or_default(),
            params.max_keys,
            bucket,
            params.prefix,
            params.delimiter,
            encoding_type,
            params.response_continuation_token,
            params.response_start_after,
        );

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_objects_v2m(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2MOutput>> {
        let input = req.input.clone();
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = input;

        let params = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;

        validate_list_object_unordered_with_delimiter(params.delimiter.as_ref(), req.uri.query())?;

        let store = get_validated_store(&bucket).await?;
        let incl_deleted = get_header(&req.headers, rustfs_utils::http::SUFFIX_INCLUDE_DELETED)
            .map(|value| value.as_ref() == "true")
            .unwrap_or_default();

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &params.prefix,
                params.decoded_continuation_token.clone(),
                params.delimiter.clone(),
                params.max_keys,
                fetch_owner.unwrap_or_default(),
                params.start_after_for_query.clone(),
                incl_deleted,
            )
            .await
            .map_err(ApiError::from)?;

        let permissions = collect_list_objects_metadata_permissions(&req, &bucket, &object_infos.objects).await?;
        let output = build_list_objects_v2m_output(
            object_infos,
            &bucket,
            &params,
            encoding_type.as_ref(),
            fetch_owner.unwrap_or_default(),
            &permissions,
        );

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = req.input;

        let ListObjectVersionsParams {
            prefix,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
        } = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;

        let object_infos = store
            .list_object_versions(&bucket, &prefix, key_marker, version_id_marker, delimiter.clone(), max_keys)
            .await
            .map_err(ApiError::from)?;

        let output = build_list_object_versions_output(object_infos, bucket, prefix, delimiter, max_keys);

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_object_versions_m(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsMOutput>> {
        let input = req.input.clone();
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            encoding_type,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = input;

        let params = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;
        let object_infos = store
            .list_object_versions(
                &bucket,
                &params.prefix,
                params.key_marker.clone(),
                params.version_id_marker.clone(),
                params.delimiter.clone(),
                params.max_keys,
            )
            .await
            .map_err(ApiError::from)?;

        let permissions = collect_list_objects_metadata_permissions(&req, &bucket, &object_infos.objects).await?;
        let output = build_list_object_versions_m_output(object_infos, &bucket, &params, encoding_type.as_ref(), &permissions);

        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let request_marker = req.input.marker.clone();
        let v2_resp = self.execute_list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| build_list_objects_output(v2, request_marker)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, Method, Uri};

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn build_request_with_req_info<T>(input: T, method: Method, req_info: ReqInfo) -> S3Request<T> {
        let mut req = build_request(input, method);
        req.extensions.insert(req_info);
        req
    }

    fn usecase_method_source<'a>(source: &'a str, method: &str) -> &'a str {
        let start_marker = format!("pub async fn {method}");
        let start = source.find(&start_marker).expect("method should exist");
        let rest = &source[start + start_marker.len()..];
        let end = rest.find("\n    pub async fn ").unwrap_or(rest.len());
        &rest[..end]
    }

    #[test]
    fn bucket_policy_and_public_access_block_changes_notify_peer_metadata_reload() {
        let source = include_str!("bucket_usecase.rs");
        for (method, operation) in [
            ("execute_delete_bucket_policy", "delete bucket policy"),
            ("execute_put_bucket_policy", "put bucket policy"),
            ("execute_delete_public_access_block", "delete public access block"),
            ("execute_put_public_access_block", "put public access block"),
        ] {
            let body = usecase_method_source(source, method);
            assert!(
                body.contains("notify_bucket_metadata_reload("),
                "{method} should notify peers to reload cached bucket metadata"
            );
            assert!(
                body.contains(operation),
                "{method} should identify the bucket metadata operation in reload logs"
            );
        }
    }

    fn replication_rule_for_target(arn: &str) -> ReplicationRule {
        ReplicationRule {
            delete_marker_replication: None,
            delete_replication: None,
            destination: Destination {
                bucket: arn.to_string(),
                ..Default::default()
            },
            existing_object_replication: None,
            filter: None,
            id: Some("rule-1".to_string()),
            prefix: None,
            priority: Some(1),
            source_selection_criteria: None,
            status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
        }
    }

    #[test]
    fn replication_target_arns_use_role_when_present() {
        let role = "arn:rustfs:replication:us-east-1:source:bucket";
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: role.to_string(),
            rules: vec![replication_rule_for_target(destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(role));
        assert!(!arns.contains(destination));
    }

    #[test]
    fn replication_target_arns_use_rule_destinations_without_role() {
        let destination = "arn:rustfs:replication:us-east-1:target:bucket";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(destination)],
        };

        let arns = replication_target_arns(&config);

        assert!(arns.contains(destination));
    }

    fn replication_targets_with_arn(arns: &[&str]) -> BucketTargets {
        BucketTargets {
            targets: arns
                .iter()
                .map(|arn| rustfs_ecstore::bucket::target::BucketTarget {
                    arn: (*arn).to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[test]
    fn validate_replication_config_targets_accepts_matching_destination_arns() {
        let arn = "arn:rustfs:replication:us-east-1:target:bucket";
        let targets = replication_targets_with_arn(&[arn]);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(arn)],
        };

        validate_replication_config_targets(&targets, &config).expect("matching target should pass validation");
    }

    #[test]
    fn validate_replication_config_targets_rejects_stale_destination_arns() {
        let targets = replication_targets_with_arn(&["arn:rustfs:replication:us-east-1:target:bucket-a"]);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![replication_rule_for_target(
                "arn:rustfs:replication:us-east-1:target:bucket-b",
            )],
        };

        let err = validate_replication_config_targets(&targets, &config).expect_err("stale target should fail validation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn validate_replication_config_targets_accepts_matching_role_arn() {
        let arn = "arn:rustfs:replication:us-east-1:role-target:bucket";
        let targets = replication_targets_with_arn(&[arn]);
        let config = ReplicationConfiguration {
            role: arn.to_string(),
            rules: vec![replication_rule_for_target("arn:rustfs:replication:us-east-1:ignored:bucket")],
        };

        validate_replication_config_targets(&targets, &config).expect("matching role ARN should pass validation");
    }

    #[test]
    fn validate_replication_config_targets_ignores_disabled_rules() {
        let targets = replication_targets_with_arn(&[]);
        let mut rule = replication_rule_for_target("arn:rustfs:replication:us-east-1:stale:bucket");
        rule.status = ReplicationRuleStatus::from_static(ReplicationRuleStatus::DISABLED);
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![rule],
        };

        validate_replication_config_targets(&targets, &config).expect("disabled rules should not require live targets");
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_suspended() {
        let config = VersioningConfiguration {
            status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_exclude_folders() {
        let config = VersioningConfiguration {
            exclude_folders: Some(true),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[test]
    fn versioning_configuration_has_object_lock_incompatible_settings_rejects_excluded_prefixes() {
        let config = VersioningConfiguration {
            excluded_prefixes: Some(vec![ExcludedPrefix {
                prefix: Some("archive/".to_string()),
            }]),
            ..Default::default()
        };

        assert!(versioning_configuration_has_object_lock_incompatible_settings(&config));
    }

    #[test]
    fn resolve_notification_region_prefers_global_region() {
        let binding = resolve_notification_region(Some("us-east-1".parse().unwrap()), Some("ap-southeast-1".parse().unwrap()));
        assert_eq!(binding, "us-east-1");
    }

    #[test]
    fn resolve_notification_region_falls_back_to_request_region() {
        let binding = resolve_notification_region(None, Some("ap-southeast-1".parse().unwrap()));
        assert_eq!(binding, "ap-southeast-1");
    }

    #[test]
    fn resolve_notification_region_defaults_value() {
        let binding = resolve_notification_region(None, None);
        assert_eq!(binding, RUSTFS_REGION);
    }

    #[test]
    fn create_bucket_exists_response_returns_ok_for_owner() {
        let response = create_bucket_exists_response(true).expect("owner recreate should succeed");
        assert_eq!(response.output.location, None);
    }

    #[test]
    fn create_bucket_exists_response_returns_bucket_already_exists_for_non_owner() {
        let err = create_bucket_exists_response(false).expect_err("non-owner recreate should fail");
        assert_eq!(err.code(), &S3ErrorCode::BucketAlreadyExists);
    }

    #[test]
    fn build_request_with_req_info_preserves_owner_state() {
        let input = CreateBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();
        let req = build_request_with_req_info(
            input,
            Method::PUT,
            ReqInfo {
                is_owner: true,
                ..Default::default()
            },
        );

        assert!(req_info_ref(&req).expect("req info should be present").is_owner);
    }

    #[tokio::test]
    async fn execute_create_bucket_returns_internal_error_when_store_uninitialized() {
        let input = CreateBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_create_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_bucket_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = DeleteBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_head_bucket_returns_internal_error_when_store_uninitialized() {
        let input = HeadBucketInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::HEAD);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_head_bucket(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_location_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketLocationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_location(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = DeletePublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_delete_public_access_block(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_encryption_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketEncryptionInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_encryption(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = GetPublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_public_access_block(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_tagging_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_bucket_versioning_returns_internal_error_when_store_uninitialized() {
        let input = GetBucketVersioningInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_get_bucket_versioning(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn normalize_lifecycle_rules_generate_rule_ids_for_missing_values() {
        let mut rules = vec![
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(30),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(60),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("rule-1".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
            LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: Some(LifecycleExpiration {
                    days: Some(90),
                    ..Default::default()
                }),
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: None,
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: None,
            },
        ];

        assign_lifecycle_rule_ids(&mut rules);

        assert_eq!(rules[0].id.as_deref(), Some("rule-0"));
        assert_eq!(rules[1].id.as_deref(), Some("rule-1"));
        assert_eq!(rules[2].id.as_deref(), Some("rule-2"));
    }

    #[test]
    fn validate_lifecycle_rule_status_rejects_invalid_status() {
        let rules = vec![LifecycleRule {
            status: ExpirationStatus::from_static("enabled"),
            expiration: Some(LifecycleExpiration {
                days: Some(30),
                ..Default::default()
            }),
            abort_incomplete_multipart_upload: None,
            del_marker_expiration: None,
            filter: None,
            id: None,
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: None,
            prefix: None,
            transitions: None,
        }];

        assert_eq!(validate_lifecycle_rule_status(&rules).unwrap_err(), ERR_LIFECYCLE_RULE_STATUS);
    }

    #[test]
    fn lifecycle_has_transition_rules_ignores_disabled_rules() {
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::DISABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("disabled-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
            }],
        };

        assert!(!lifecycle_has_transition_rules(&config));
    }

    #[test]
    fn lifecycle_has_transition_rules_accepts_enabled_noncurrent_transitions() {
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("enabled-noncurrent-transition".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![NoncurrentVersionTransition {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                    storage_class: Some(TransitionStorageClass::from_static("WARM")),
                }]),
                prefix: None,
                transitions: None,
            }],
        };

        assert!(lifecycle_has_transition_rules(&config));
    }

    #[tokio::test]
    async fn execute_list_buckets_returns_internal_error_when_store_uninitialized() {
        let input = ListBucketsInput::builder().build().unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_buckets(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_object_versions_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions_m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn build_list_object_versions_m_output_maps_metadata_and_preserves_entry_order() {
        use time::macros::datetime;
        use uuid::Uuid;

        let object_infos = ListObjectVersionsInfo {
            is_truncated: true,
            next_marker: Some("obj-z".to_string()),
            next_version_idmarker: Some("null".to_string()),
            prefixes: vec!["logs/".to_string()],
            objects: vec![
                ObjectInfo {
                    bucket: "demo-bucket".to_string(),
                    name: "obj-a".to_string(),
                    mod_time: Some(datetime!(2025-01-01 00:00 UTC)),
                    size: 11,
                    user_defined: HashMap::from([("project".to_string(), "alpha".to_string())]),
                    parity_blocks: 2,
                    data_blocks: 4,
                    version_id: Some(Uuid::nil()),
                    user_tags: "env=prod".to_string(),
                    is_latest: true,
                    etag: Some("0123456789abcdef0123456789abcdef".to_string()),
                    ..Default::default()
                },
                ObjectInfo {
                    bucket: "demo-bucket".to_string(),
                    name: "obj-b".to_string(),
                    mod_time: Some(datetime!(2025-01-02 00:00 UTC)),
                    delete_marker: true,
                    user_defined: HashMap::from([("marker".to_string(), "true".to_string())]),
                    version_id: None,
                    ..Default::default()
                },
            ],
        };

        let permissions = HashMap::from([
            (
                "obj-a".to_string(),
                ObjectMetadataPermissions {
                    metadata_allowed: true,
                    tags_allowed: true,
                },
            ),
            (
                "obj-b".to_string(),
                ObjectMetadataPermissions {
                    metadata_allowed: true,
                    tags_allowed: false,
                },
            ),
        ]);

        let params = ListObjectVersionsParams {
            prefix: "pre".to_string(),
            delimiter: Some("/".to_string()),
            key_marker: Some("start marker".to_string()),
            version_id_marker: Some("vid-1".to_string()),
            max_keys: 1000,
        };
        let output = build_list_object_versions_m_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            &permissions,
        );

        assert_eq!(output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.prefix.as_deref(), Some("pre"));
        assert_eq!(output.key_marker.as_deref(), Some("start%20marker"));
        assert_eq!(output.next_key_marker.as_deref(), Some("obj-z"));
        assert_eq!(output.next_version_id_marker.as_deref(), Some("null"));
        assert_eq!(output.entries.len(), 2);

        match &output.entries[0] {
            ListObjectVersionMEntry::Version(version) => {
                assert_eq!(version.key.as_deref(), Some("obj-a"));
                assert_eq!(version.version_id.as_deref(), Some(Uuid::nil().to_string().as_str()));
                assert_eq!(version.user_tags.as_deref(), Some("env=prod"));
                assert_eq!(version.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
                assert_eq!(
                    version.user_metadata.as_ref().map(|metadata| metadata.items.clone()),
                    Some(vec![UserMetadataEntry {
                        key: "project".to_string(),
                        value: "alpha".to_string(),
                    }])
                );
            }
            other => panic!("expected version entry, got {other:?}"),
        }

        match &output.entries[1] {
            ListObjectVersionMEntry::DeleteMarker(marker) => {
                assert_eq!(marker.key.as_deref(), Some("obj-b"));
                assert_eq!(marker.version_id.as_deref(), Some("null"));
                assert!(marker.user_tags.is_none());
                assert_eq!(
                    marker.user_metadata.as_ref().map(|metadata| metadata.items.clone()),
                    Some(vec![UserMetadataEntry {
                        key: "marker".to_string(),
                        value: "true".to_string(),
                    }])
                );
            }
            other => panic!("expected delete marker entry, got {other:?}"),
        }
    }

    #[test]
    fn build_list_object_versions_m_output_uses_params_and_hides_metadata_without_permissions() {
        use time::macros::datetime;

        let object_infos = ListObjectVersionsInfo {
            is_truncated: false,
            next_marker: Some(String::new()),
            next_version_idmarker: Some(String::new()),
            prefixes: vec!["logs and more/".to_string()],
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs and more/object one.txt".to_string(),
                mod_time: Some(datetime!(2025-01-04 00:00 UTC)),
                size: 7,
                user_defined: HashMap::from([("secret".to_string(), "value".to_string())]),
                user_tags: "env=prod".to_string(),
                parity_blocks: 1,
                data_blocks: 2,
                ..Default::default()
            }],
        };

        let params = ListObjectVersionsParams {
            prefix: "logs and more/".to_string(),
            delimiter: Some(" ".to_string()),
            key_marker: Some("marker value".to_string()),
            version_id_marker: None,
            max_keys: 25,
        };

        let output = build_list_object_versions_m_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            &HashMap::new(),
        );

        assert_eq!(output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.prefix.as_deref(), Some("logs%20and%20more%2F"));
        assert_eq!(output.delimiter.as_deref(), Some("%20"));
        assert_eq!(output.key_marker.as_deref(), Some("marker%20value"));
        assert_eq!(output.version_id_marker.as_deref(), Some(""));
        assert_eq!(output.next_key_marker, None);
        assert_eq!(output.next_version_id_marker, None);

        match &output.entries[0] {
            ListObjectVersionMEntry::Version(version) => {
                assert_eq!(version.key.as_deref(), Some("logs%20and%20more%2Fobject%20one.txt"));
                assert!(version.user_metadata.is_none());
                assert!(version.user_tags.is_none());
                assert!(version.internal.is_none());
            }
            other => panic!("expected version entry, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_list_objects_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectsInput::builder().bucket("test-bucket".to_string()).build().unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_returns_internal_error_when_store_uninitialized() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn build_list_objects_v2m_output_maps_metadata_and_key_count() {
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: true,
            next_continuation_token: Some("next-token".to_string()),
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs/obj a.txt".to_string(),
                mod_time: Some(datetime!(2025-01-03 00:00 UTC)),
                size: 11,
                user_defined: HashMap::from([("project".to_string(), "alpha".to_string())]),
                parity_blocks: 2,
                data_blocks: 4,
                user_tags: "env=prod".to_string(),
                etag: Some("0123456789abcdef0123456789abcdef".to_string()),
                ..Default::default()
            }],
            prefixes: vec!["logs/archive/".to_string()],
            ..Default::default()
        };

        let permissions = HashMap::from([(
            "logs/obj a.txt".to_string(),
            ObjectMetadataPermissions {
                metadata_allowed: true,
                tags_allowed: true,
            },
        )]);

        let params = ListObjectsV2Params {
            prefix: "logs/".to_string(),
            max_keys: 1000,
            delimiter: Some("/".to_string()),
            response_start_after: Some("logs/start after".to_string()),
            start_after_for_query: None,
            response_continuation_token: Some("start token".to_string()),
            decoded_continuation_token: None,
        };

        let output = build_list_objects_v2m_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            true,
            &permissions,
        );

        assert_eq!(output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.prefix.as_deref(), Some("logs/"));
        assert_eq!(output.continuation_token.as_deref(), Some("start token"));
        assert_eq!(output.start_after.as_deref(), Some("logs/start after"));
        assert_eq!(output.next_continuation_token.as_deref(), Some("bmV4dC10b2tlbg=="));
        assert_eq!(output.key_count, Some(2));
        assert_eq!(output.contents.as_ref().map(Vec::len), Some(1));
        assert_eq!(output.common_prefixes.as_ref().map(Vec::len), Some(1));

        let object = output.contents.as_ref().unwrap().first().unwrap();
        assert_eq!(object.key.as_deref(), Some("logs/obj%20a.txt"));
        assert_eq!(object.user_tags.as_deref(), Some("env=prod"));
        assert_eq!(object.internal, Some(ObjectInternalInfo { k: 4, m: 2 }));
        assert!(object.owner.is_some());
        assert_eq!(
            object.user_metadata.as_ref().map(|metadata| metadata.items.clone()),
            Some(vec![UserMetadataEntry {
                key: "project".to_string(),
                value: "alpha".to_string(),
            }])
        );

        let prefix = output.common_prefixes.as_ref().unwrap().first().unwrap();
        assert_eq!(prefix.prefix.as_deref(), Some("logs/archive/"));
    }

    #[test]
    fn build_list_objects_v2m_output_uses_params_and_hides_owner_without_fetch_owner() {
        use time::macros::datetime;

        let object_infos = ListObjectsV2Info {
            is_truncated: false,
            next_continuation_token: None,
            objects: vec![ObjectInfo {
                bucket: "demo-bucket".to_string(),
                name: "logs and more/object one.txt".to_string(),
                mod_time: Some(datetime!(2025-01-05 00:00 UTC)),
                size: 13,
                user_defined: HashMap::from([("secret".to_string(), "value".to_string())]),
                user_tags: "env=prod".to_string(),
                parity_blocks: 1,
                data_blocks: 2,
                ..Default::default()
            }],
            prefixes: vec!["logs and more/archive/".to_string()],
            ..Default::default()
        };

        let params = ListObjectsV2Params {
            prefix: "logs and more/".to_string(),
            max_keys: 25,
            delimiter: Some("/".to_string()),
            response_start_after: Some("logs and more/start after".to_string()),
            start_after_for_query: Some("decoded start after".to_string()),
            response_continuation_token: Some("opaque token".to_string()),
            decoded_continuation_token: Some("decoded token".to_string()),
        };

        let output = build_list_objects_v2m_output(
            object_infos,
            "demo-bucket",
            &params,
            Some(&EncodingType::from_static(EncodingType::URL)),
            false,
            &HashMap::new(),
        );

        assert_eq!(output.name.as_deref(), Some("demo-bucket"));
        assert_eq!(output.prefix.as_deref(), Some("logs and more/"));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.continuation_token.as_deref(), Some("opaque token"));
        assert_eq!(output.start_after.as_deref(), Some("logs and more/start after"));
        assert_eq!(output.key_count, Some(2));
        assert_eq!(output.encoding_type.as_ref().map(EncodingType::as_str), Some(EncodingType::URL));

        let object = output.contents.as_ref().unwrap().first().unwrap();
        assert_eq!(object.key.as_deref(), Some("logs%20and%20more/object%20one.txt"));
        assert!(object.owner.is_none());
        assert!(object.user_metadata.is_none());
        assert!(object.user_tags.is_none());
        assert!(object.internal.is_none());

        let prefix = output.common_prefixes.as_ref().unwrap().first().unwrap();
        assert_eq!(prefix.prefix.as_deref(), Some("logs%20and%20more/archive/"));
    }

    #[tokio::test]
    async fn execute_put_bucket_lifecycle_configuration_rejects_missing_configuration() {
        let input = PutBucketLifecycleConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_lifecycle_configuration(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_name() {
        let raw_name = "unsupported".repeat(100);
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from(raw_name.clone())),
                            value: Some("uploads/".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains(&raw_name), "error message should not echo full raw filter name");
    }

    #[test]
    fn validate_notification_configuration_filters_accepts_case_insensitive_filter_names() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from("Prefix".to_string())),
                                value: Some("uploads/".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from("Suffix".to_string())),
                                value: Some(".csv".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        validate_notification_configuration_filters(&cfg).expect("capitalized filter names should be accepted");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_prefix_rules() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                                value: Some("uploads/".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                                value: Some("images/".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_invalid_filter_value() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                            value: Some("../secret".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        let msg = err.message().unwrap_or_default();
        assert!(msg.contains("len="), "error message should include summarized length");
        assert!(!msg.contains("../secret"), "error message should not echo full raw filter value");
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_name() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: None,
                            value: Some("uploads/".to_string()),
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_missing_filter_value() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![FilterRule {
                            name: Some(FilterRuleName::from_static(FilterRuleName::PREFIX)),
                            value: None,
                        }]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn validate_notification_configuration_filters_rejects_duplicate_suffix_rules() {
        let cfg = NotificationConfiguration {
            queue_configurations: Some(vec![QueueConfiguration {
                id: Some("q1".to_string()),
                queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                events: vec!["s3:ObjectCreated:*".to_string().into()],
                filter: Some(NotificationConfigurationFilter {
                    key: Some(S3KeyFilter {
                        filter_rules: Some(vec![
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                                value: Some(".csv".to_string()),
                            },
                            FilterRule {
                                name: Some(FilterRuleName::from_static(FilterRuleName::SUFFIX)),
                                value: Some(".log".to_string()),
                            },
                        ]),
                    }),
                }),
            }]),
            ..Default::default()
        };

        let err = validate_notification_configuration_filters(&cfg).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_put_bucket_policy_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketPolicyInput::builder()
            .bucket("test-bucket".to_string())
            .policy("{}".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_policy(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_notification_configuration_rejects_invalid_filter_before_store_lookup() {
        let input = PutBucketNotificationConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .notification_configuration(NotificationConfiguration {
                queue_configurations: Some(vec![QueueConfiguration {
                    id: Some("q1".to_string()),
                    queue_arn: "arn:rustfs:sqs:us-east-1:1:webhook".to_string(),
                    events: vec!["s3:ObjectCreated:*".to_string().into()],
                    filter: Some(NotificationConfigurationFilter {
                        key: Some(S3KeyFilter {
                            filter_rules: Some(vec![FilterRule {
                                name: Some(FilterRuleName::from("unsupported".to_string())),
                                value: Some("uploads/".to_string()),
                            }]),
                        }),
                    }),
                }]),
                ..Default::default()
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_notification_configuration(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_put_bucket_cors_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketCorsInput::builder()
            .bucket("test-bucket".to_string())
            .cors_configuration(CORSConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_cors(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_replication_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketReplicationInput::builder()
            .bucket("test-bucket".to_string())
            .replication_configuration(ReplicationConfiguration {
                role: "arn:aws:iam::123456789012:role/test".to_string(),
                rules: vec![],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_replication(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_encryption_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketEncryptionInput::builder()
            .bucket("test-bucket".to_string())
            .server_side_encryption_configuration(ServerSideEncryptionConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_encryption(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_bucket_tagging_returns_internal_error_when_store_uninitialized() {
        let input = PutBucketTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .tagging(Tagging {
                tag_set: vec![Tag {
                    key: Some("env".to_string()),
                    value: Some("prod".to_string()),
                }],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_bucket_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_public_access_block_returns_internal_error_when_store_uninitialized() {
        let input = PutPublicAccessBlockInput::builder()
            .bucket("test-bucket".to_string())
            .public_access_block_configuration(PublicAccessBlockConfiguration::default())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_put_public_access_block(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_objects_v2_rejects_negative_max_keys() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_objects_v2_rejects_invalid_continuation_token_before_store_lookup() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .continuation_token(Some("%%%".to_string()))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_negative_max_keys() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_objects_v2m_rejects_invalid_continuation_token_before_store_lookup() {
        let input = ListObjectsV2Input::builder()
            .bucket("test-bucket".to_string())
            .continuation_token(Some("%%%".to_string()))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_objects_v2m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid continuation token"));
    }

    #[tokio::test]
    async fn execute_list_object_versions_rejects_negative_max_keys_before_store_lookup() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_list_object_versions_m_rejects_negative_max_keys_before_store_lookup() {
        let input = ListObjectVersionsInput::builder()
            .bucket("test-bucket".to_string())
            .max_keys(Some(-1))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultBucketUsecase::without_context();

        let err = usecase.execute_list_object_versions_m(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
