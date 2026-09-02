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

//! Per-bucket On-Demand Migration admin handlers (ODM-07, rustfs/backlog#2154).
//!
//! A bucket can name an external S3-compatible source whose objects are
//! pulled on first access. This module is the management plane only:
//! `PUT`/`GET`/`DELETE /v3/on-demand-migration/{bucket}` configure, read and
//! clear the source, `?dry-run=true` validates and probes without saving, and
//! `GET .../status` reports the switch state plus this node's runtime
//! snapshot of the bucket (breaker, counters, last source error). The data
//! plane and backfill live in other ODM tasks.
//!
//! Credentials in the request body are never echoed: every response carries
//! the `redacted()` config, probe failures name only the error class, and no
//! log line carries the config.

use crate::admin::auth::authorize_admin_request;
use crate::admin::handlers::admin_json_response;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    AppContext, app_context_from_req, current_deployment_id, current_endpoints_handle, current_notification_system_for_context,
    object_store_from_req,
};
use crate::admin::storage_api::bucket::metadata::BUCKET_ON_DEMAND_MIGRATION_CONFIG;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::on_demand_migration::source_client::{
    SourceClient, SourceClientSpec, SourceError, SourceProbe, SourceProvider, SourceTimeouts,
};
use crate::admin::storage_api::bucket::on_demand_migration::{
    OdmBucketSnapshot, OnDemandMigrationConfig, OnDemandMigrationConfigError, OnDemandMigrationSys, PathStyle, ValidationContext,
};
use crate::admin::storage_api::bucket::remote_s3_client::{PathStyle as RemotePathStyle, RemoteCredentials, RemoteS3ClientError};
use crate::admin::storage_api::contract::bucket::{BucketOperations as _, BucketOptions};
use crate::admin::storage_api::error::StorageError;
use crate::admin::storage_api::s3::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, error as admin_s3_error};
use crate::admin::utils::{extract_query_params, read_compatible_admin_body};
use crate::error::ApiError;
use crate::license::license_check;
use crate::server::ADMIN_PREFIX;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_credentials::Credentials;
use rustfs_policy::policy::action::{Action, AdminAction};
use serde::Serialize;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::{info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_ON_DEMAND_MIGRATION: &str = "bucket_on_demand_migration";
const EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG: &str = "admin_bucket_on_demand_migration_config";

const ROUTE_PATH: &str = "/v3/on-demand-migration/{bucket}";
const STATUS_ROUTE_PATH: &str = "/v3/on-demand-migration/{bucket}/status";
const DRY_RUN_QUERY: &str = "dry-run";

/// Error code returned when the module switch is off and a write is attempted.
pub(crate) const ERR_CODE_MODULE_DISABLED: &str = "OnDemandMigrationDisabled";
/// Error code returned when the source bucket did not answer the probe.
pub(crate) const ERR_CODE_SOURCE_UNREACHABLE: &str = "OnDemandMigrationSourceUnreachable";
/// Error code returned by `GET` when the bucket has no configuration.
pub(crate) const ERR_CODE_NO_SUCH_CONFIGURATION: &str = "NoSuchConfiguration";

/// The published switch is `RUSTFS_ON_DEMAND_MIGRATION_ENABLED`, owned by
/// ODM-05 in `module_switches.rs`. This is the only read of it in the admin
/// plane so the orchestrator can swap the call for the published predicate.
const ENV_ON_DEMAND_MIGRATION_ENABLED: &str = "RUSTFS_ON_DEMAND_MIGRATION_ENABLED";

fn module_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_ON_DEMAND_MIGRATION_ENABLED, false)
}

/// What the source answered during `PUT` validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProbeSummary {
    /// `HeadBucket` succeeded.
    pub reachable: bool,
    /// A one-key `ListObjectsV2` succeeded.
    pub listable: bool,
    /// The first key the listing returned, if the source bucket is not empty.
    pub sample_key: Option<String>,
}

impl From<SourceProbe> for ProbeSummary {
    fn from(probe: SourceProbe) -> Self {
        Self {
            reachable: true,
            listable: true,
            sample_key: probe.sample_object.map(|object| object.key),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SetBucketOnDemandMigrationResponse {
    pub bucket: String,
    pub dry_run: bool,
    pub config: OnDemandMigrationConfig,
    /// `null` for a dry run: nothing was saved.
    pub updated_at: Option<String>,
    pub probe: ProbeSummary,
}

#[derive(Debug, Serialize)]
pub(crate) struct GetBucketOnDemandMigrationResponse {
    pub bucket: String,
    pub config: OnDemandMigrationConfig,
    pub updated_at: String,
}

/// `GET .../status` body. Field order and `null` handling are pinned by the
/// `rustfs-madmin` fixture. Runtime fields are `null` while the bucket has no
/// live state on this node; `provider` and `endpoint_host` then fall back to
/// the saved config so a disabled module still shows what is configured.
#[derive(Debug, Serialize)]
pub(crate) struct BucketOnDemandMigrationStatus {
    pub configured: bool,
    pub enabled: bool,
    pub module_enabled: bool,
    pub provider: Option<String>,
    pub endpoint_host: Option<String>,
    pub breaker: Option<BreakerStatus>,
    pub counters: Option<RuntimeCounters>,
    pub last_source_error: Option<LastSourceErrorStatus>,
    pub inflight_pulls: u64,
    pub queue_depth: u64,
    /// `source_hit / (source_hit + local GETs)`. The API request metrics
    /// count per operation, not per bucket, and the runtime only sees
    /// misses, so there is no per-bucket GET total to divide by: this stays
    /// `null` rather than reporting a made-up 0.
    pub served_by_source_ratio: Option<f64>,
    /// RFC 3339 save time of the config; `null` when not configured.
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BreakerStatus {
    pub state: &'static str,
    /// The runtime snapshot does not carry the breaker's open instant yet
    /// (it is a monotonic clock reading inside ecstore), so this is `null`.
    pub opened_at: Option<String>,
}

/// Lifetime counters of the bucket's runtime on this node, keyed by the
/// fixed label values of the Prometheus series with the same names.
#[derive(Debug, Serialize)]
pub(crate) struct RuntimeCounters {
    pub requests_total: BTreeMap<String, BTreeMap<String, u64>>,
    pub pulled_bytes_total: u64,
    pub pulled_objects_total: BTreeMap<String, u64>,
    pub pull_failures_total: BTreeMap<String, u64>,
    pub source_latency: SourceLatencyStatus,
}

#[derive(Debug, Serialize)]
pub(crate) struct SourceLatencyStatus {
    pub buckets: Vec<LatencyBucketStatus>,
    pub count: u64,
    pub sum_ms: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct LatencyBucketStatus {
    pub le_ms: u64,
    pub count: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct LastSourceErrorStatus {
    pub class: String,
    pub at: String,
}

/// Host of the configured source endpoint, matching the runtime's
/// `endpoint_host` so the status reads the same with or without live state.
fn config_endpoint_host(config: &OnDemandMigrationConfig) -> Option<String> {
    url::Url::parse(&config.source.effective_endpoint())
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
}

fn bucket_status(
    config: Option<(&OnDemandMigrationConfig, OffsetDateTime)>,
    runtime: Option<OdmBucketSnapshot>,
    module_enabled: bool,
) -> S3Result<BucketOnDemandMigrationStatus> {
    let updated_at = config.map(|(_, updated_at)| format_updated_at(updated_at)).transpose()?;
    let mut status = BucketOnDemandMigrationStatus {
        configured: config.is_some(),
        enabled: config.is_some_and(|(config, _)| config.enabled),
        module_enabled,
        provider: config.map(|(config, _)| config.source.provider.as_str().to_string()),
        endpoint_host: config.and_then(|(config, _)| config_endpoint_host(config)),
        breaker: None,
        counters: None,
        last_source_error: None,
        inflight_pulls: 0,
        queue_depth: 0,
        served_by_source_ratio: None,
        updated_at,
    };
    let Some(runtime) = runtime else {
        return Ok(status);
    };
    let stats = runtime.stats;
    status.provider = Some(runtime.provider);
    status.endpoint_host = Some(runtime.endpoint_host);
    status.breaker = Some(BreakerStatus {
        state: stats.breaker_state.as_str(),
        opened_at: None,
    });
    status.counters = Some(RuntimeCounters {
        requests_total: stats.requests_total,
        pulled_bytes_total: stats.pulled_bytes_total,
        pulled_objects_total: stats.pulled_objects_total,
        pull_failures_total: stats.pull_failures_total,
        source_latency: SourceLatencyStatus {
            buckets: stats
                .source_latency
                .buckets
                .into_iter()
                .map(|bucket| LatencyBucketStatus {
                    le_ms: bucket.le_ms,
                    count: bucket.count,
                })
                .collect(),
            count: stats.source_latency.count,
            sum_ms: stats.source_latency.sum_ms,
        },
    });
    status.last_source_error = stats
        .last_source_error
        .map(|error| {
            Ok::<_, S3Error>(LastSourceErrorStatus {
                class: error.class,
                at: format_updated_at(error.at)?,
            })
        })
        .transpose()?;
    status.inflight_pulls = stats.inflight_pulls;
    status.queue_depth = stats.queue_depth;
    Ok(status)
}

pub struct SetBucketOnDemandMigrationHandler;
pub struct GetBucketOnDemandMigrationHandler;
pub struct DeleteBucketOnDemandMigrationHandler;
pub struct GetBucketOnDemandMigrationStatusHandler;

pub fn register_on_demand_migration_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::PUT,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&SetBucketOnDemandMigrationHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&GetBucketOnDemandMigrationHandler {}),
    )?;
    r.insert(
        Method::DELETE,
        format!("{ADMIN_PREFIX}{ROUTE_PATH}").as_str(),
        AdminOperation(&DeleteBucketOnDemandMigrationHandler {}),
    )?;
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}{STATUS_ROUTE_PATH}").as_str(),
        AdminOperation(&GetBucketOnDemandMigrationStatusHandler {}),
    )?;
    Ok(())
}

fn custom_error(code: &'static str, status: StatusCode, message: String) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom(code.into()), message);
    err.set_status_code(status);
    err
}

fn bucket_from_params(params: &Params<'_, '_>) -> S3Result<String> {
    let bucket = params.get("bucket").unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(admin_s3_error(S3ErrorCode::InvalidRequest, "bucket name is required"));
    }
    Ok(bucket)
}

/// Authorize with `action` and confirm the bucket exists (404 otherwise).
async fn authorize_for_bucket(req: &S3Request<Body>, action: AdminAction, bucket: &str) -> S3Result<Credentials> {
    let cred = authorize_admin_request(req, vec![Action::AdminAction(action)]).await?;

    let Some(store) = object_store_from_req(req) else {
        return Err(admin_s3_error(S3ErrorCode::InternalError, "object store is not initialized"));
    };
    store
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    Ok(cred)
}

/// Same mapping as the object zip download: a denied entitlement is
/// `AccessDenied`, anything else is an internal failure whose detail stays in
/// the log.
fn license_gate() -> S3Result<()> {
    license_check().map_err(|err| match err.kind() {
        std::io::ErrorKind::PermissionDenied => admin_s3_error(S3ErrorCode::AccessDenied, format!("{err}")),
        _ => {
            tracing::error!(
                event = EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                error = %err,
                "license check failed"
            );
            admin_s3_error(S3ErrorCode::InternalError, "License validation failed")
        }
    })
}

fn parse_config(body: &[u8]) -> S3Result<OnDemandMigrationConfig> {
    if body.is_empty() {
        return Err(admin_s3_error(
            S3ErrorCode::InvalidRequest,
            "request body is required: an on-demand migration config JSON",
        ));
    }
    OnDemandMigrationConfig::from_json(body).map_err(|err| match err {
        OnDemandMigrationConfigError::Malformed(reason) => {
            admin_s3_error(S3ErrorCode::InvalidArgument, format!("invalid on-demand migration config: {reason}"))
        }
        other => admin_s3_error(S3ErrorCode::InvalidArgument, format!("{other}")),
    })
}

fn is_dry_run(req: &S3Request<Body>) -> bool {
    extract_query_params(&req.uri)
        .get(DRY_RUN_QUERY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Every endpoint of this deployment, as `scheme://host:port`, so a source
/// naming one of them with the same bucket is rejected as a self-reference.
/// Single-node local-disk layouts carry no host and contribute nothing; the
/// outbound endpoint policy still refuses loopback sources for them.
fn local_endpoints() -> Vec<String> {
    let mut endpoints: Vec<String> = current_endpoints_handle()
        .map(|pools| {
            pools
                .as_ref()
                .iter()
                .flat_map(|pool| pool.endpoints.as_ref().iter())
                .map(|endpoint| endpoint.grid_host())
                .filter(|host| !host.is_empty())
                .collect()
        })
        .unwrap_or_default();
    endpoints.sort();
    endpoints.dedup();
    endpoints
}

/// `(endpoint URL, target bucket)` of every replication target of `bucket`;
/// a bucket that never had targets configured has none.
async fn replication_target_endpoints(bucket: &str) -> S3Result<Vec<(String, String)>> {
    let targets = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => return Ok(Vec::new()),
        Err(err) => {
            return Err(admin_s3_error(
                S3ErrorCode::InternalError,
                format!("failed to read replication targets: {err}"),
            ));
        }
    };
    Ok(targets
        .targets
        .iter()
        .filter_map(|target| target.url().ok().map(|url| (url.to_string(), target.target_bucket.clone())))
        .collect())
}

async fn validate_config(bucket: &str, config: &OnDemandMigrationConfig) -> S3Result<()> {
    let deployment_id = current_deployment_id().unwrap_or_default();
    let local_endpoints = local_endpoints();
    let replication_targets = replication_target_endpoints(bucket).await?;
    config
        .validate(ValidationContext {
            local_bucket: bucket,
            local_deployment_id: &deployment_id,
            local_endpoints: &local_endpoints,
            replication_target_endpoints: &replication_targets,
        })
        .map_err(|err| admin_s3_error(S3ErrorCode::InvalidArgument, format!("{err}")))
}

fn source_provider(config: &OnDemandMigrationConfig) -> SourceProvider {
    use crate::admin::storage_api::bucket::on_demand_migration::Provider;
    match config.source.provider {
        Provider::S3 => SourceProvider::S3,
        Provider::Aws => SourceProvider::Aws,
        Provider::Minio => SourceProvider::Minio,
        Provider::Rustfs => SourceProvider::Rustfs,
        Provider::R2 => SourceProvider::R2,
        Provider::Gcs => SourceProvider::Gcs,
    }
}

/// Map the persisted config onto the client builder's spec. `first_byte_ms`
/// bounds the read timeout: the probe only issues HEAD and a one-key list.
pub(crate) fn source_client_spec(config: &OnDemandMigrationConfig) -> SourceClientSpec {
    let source = &config.source;
    let timeout = &config.policy.source_timeout;
    SourceClientSpec {
        endpoint: source.effective_endpoint(),
        region: source.effective_region().to_string(),
        bucket: source.bucket.clone(),
        source_prefix: config.filter.source_prefix.clone(),
        provider: source_provider(config),
        path_style: match source.path_style {
            PathStyle::Auto => RemotePathStyle::Auto,
            PathStyle::Path => RemotePathStyle::Path,
            PathStyle::Virtual => RemotePathStyle::VirtualHost,
        },
        credentials: source.credentials.as_ref().map(|credentials| RemoteCredentials {
            access_key: credentials.access_key.clone(),
            secret_key: credentials.secret_key.clone(),
            session_token: credentials.session_token.clone(),
            expiration: None,
            account_id: String::new(),
        }),
        skip_tls_verify: source.tls.skip_verify,
        ca_cert_pem: source.tls.ca_cert_pem.clone(),
        timeouts: SourceTimeouts {
            connect: Duration::from_millis(timeout.connect_ms),
            read: Duration::from_millis(timeout.first_byte_ms),
        },
        bandwidth_limit: config.policy.bandwidth_limit_bytes_per_sec.and_then(NonZeroU64::new),
    }
}

/// Builder failures are input errors: the endpoint policy, the CA PEM or the
/// credentials the operator supplied. Anonymous sources are not wired yet
/// (ODM-05 adds the credential-less path), so `MissingCredentials` is a 400
/// naming the field instead of an opaque internal error.
fn client_build_error(err: RemoteS3ClientError) -> S3Error {
    match err {
        RemoteS3ClientError::MissingCredentials => admin_s3_error(
            S3ErrorCode::InvalidArgument,
            "source.credentials is required: anonymous sources are not supported yet",
        ),
        other => admin_s3_error(S3ErrorCode::InvalidArgument, format!("source client cannot be built: {other}")),
    }
}

/// Only the error class crosses the boundary: SDK messages can carry the
/// signed request, including the endpoint host and query.
pub(crate) fn probe_error(err: &SourceError) -> S3Error {
    custom_error(
        ERR_CODE_SOURCE_UNREACHABLE,
        StatusCode::BAD_REQUEST,
        format!("source bucket probe failed: {}", err.class_label()),
    )
}

async fn probe_source(bucket: &str, config: &OnDemandMigrationConfig) -> S3Result<ProbeSummary> {
    let spec = source_client_spec(config);
    let client = SourceClient::new(&spec).await.map_err(client_build_error)?;
    match client.probe().await {
        Ok(probe) => Ok(ProbeSummary::from(probe)),
        Err(err) => {
            warn!(
                event = EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                bucket = %bucket,
                probe_error_class = err.class_label(),
                "on-demand migration source probe failed"
            );
            Err(probe_error(&err))
        }
    }
}

fn format_updated_at(updated_at: OffsetDateTime) -> S3Result<String> {
    updated_at
        .format(&Rfc3339)
        .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to format timestamp: {err}")))
}

/// Ask every peer to reload the bucket metadata so the new source takes
/// effect cluster-wide before the periodic refresh. A peer that does not
/// answer is a warning: the refresh loop converges it.
pub(crate) async fn reload_peers(context: Option<&AppContext>, bucket: &str) -> Result<(), String> {
    let Some(notification_sys) = current_notification_system_for_context(context) else {
        return Ok(());
    };
    notification_sys
        .load_bucket_metadata(bucket)
        .await
        .map_err(|err| err.to_string())
}

fn notify_peers_reload(context: Option<Arc<AppContext>>, bucket: String, operation: &'static str) {
    tokio::spawn(async move {
        if let Err(error) = reload_peers(context.as_deref(), &bucket).await {
            warn!(
                event = EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                bucket = %bucket,
                error = %error,
                "failed to notify peers after {operation}"
            );
        }
    });
}

#[async_trait::async_trait]
impl Operation for SetBucketOnDemandMigrationHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::SetBucketOnDemandMigrationAction, &bucket).await?;

        if !module_enabled() {
            return Err(custom_error(
                ERR_CODE_MODULE_DISABLED,
                StatusCode::BAD_REQUEST,
                format!("on-demand migration is disabled: set {ENV_ON_DEMAND_MIGRATION_ENABLED}=true"),
            ));
        }
        license_gate()?;

        let dry_run = is_dry_run(&req);
        let path = req.uri.path().to_string();
        let context = app_context_from_req(&req);

        // Taken before the (possibly slow) probe: a bucket recreated meanwhile
        // must not receive this config.
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to capture bucket incarnation: {err}")))?;

        let body = read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, &path, &cred.secret_key).await?;
        let config = parse_config(&body)?;
        validate_config(&bucket, &config).await?;
        let probe = probe_source(&bucket, &config).await?;

        let updated_at = if dry_run {
            None
        } else {
            let json = config
                .to_json()
                .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to encode config: {err}")))?;
            let updated_at =
                metadata_sys::update_if_incarnation(&bucket, BUCKET_ON_DEMAND_MIGRATION_CONFIG, json, expected_incarnation_id)
                    .await
                    .map_err(|err| {
                        admin_s3_error(S3ErrorCode::InternalError, format!("failed to save on-demand migration config: {err}"))
                    })?;
            info!(
                event = EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
                bucket = %bucket,
                enabled = config.enabled,
                "on-demand migration config set"
            );
            notify_peers_reload(context, bucket.clone(), "set on-demand migration config");
            Some(format_updated_at(updated_at)?)
        };

        let response = SetBucketOnDemandMigrationResponse {
            bucket,
            dry_run,
            config: config.redacted(),
            updated_at,
            probe,
        };
        admin_json_response(&path, &cred.secret_key, StatusCode::OK, &response)
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketOnDemandMigrationHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::GetBucketOnDemandMigrationAction, &bucket).await?;

        let Some((config, updated_at)) = metadata_sys::get_on_demand_migration_config(&bucket).await.map_err(|err| {
            admin_s3_error(S3ErrorCode::InternalError, format!("failed to read on-demand migration config: {err}"))
        })?
        else {
            return Err(custom_error(
                ERR_CODE_NO_SUCH_CONFIGURATION,
                StatusCode::NOT_FOUND,
                format!("on-demand migration is not configured for bucket {bucket}"),
            ));
        };

        let response = GetBucketOnDemandMigrationResponse {
            bucket,
            config: config.redacted(),
            updated_at: format_updated_at(updated_at)?,
        };
        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &response)
    }
}

#[async_trait::async_trait]
impl Operation for DeleteBucketOnDemandMigrationHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        authorize_for_bucket(&req, AdminAction::SetBucketOnDemandMigrationAction, &bucket).await?;

        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
            .await
            .map_err(|err| admin_s3_error(S3ErrorCode::InternalError, format!("failed to capture bucket incarnation: {err}")))?;

        // Idempotent: clearing an absent config still rewrites the metadata
        // (fresh timestamp) and answers 204. Pulled objects stay in place.
        metadata_sys::delete_if_incarnation(&bucket, BUCKET_ON_DEMAND_MIGRATION_CONFIG, expected_incarnation_id)
            .await
            .map_err(|err| {
                admin_s3_error(S3ErrorCode::InternalError, format!("failed to clear on-demand migration config: {err}"))
            })?;

        info!(
            event = EVENT_ADMIN_ON_DEMAND_MIGRATION_CONFIG,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_ON_DEMAND_MIGRATION,
            bucket = %bucket,
            "on-demand migration config cleared"
        );
        notify_peers_reload(app_context_from_req(&req), bucket, "clear on-demand migration config");

        Ok(S3Response::new((StatusCode::NO_CONTENT, Body::empty())))
    }
}

#[async_trait::async_trait]
impl Operation for GetBucketOnDemandMigrationStatusHandler {
    #[tracing::instrument(skip_all)]
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bucket = bucket_from_params(&params)?;
        let cred = authorize_for_bucket(&req, AdminAction::GetBucketOnDemandMigrationAction, &bucket).await?;

        let config = metadata_sys::get_on_demand_migration_config(&bucket).await.map_err(|err| {
            admin_s3_error(S3ErrorCode::InternalError, format!("failed to read on-demand migration config: {err}"))
        })?;
        let runtime = OnDemandMigrationSys::get().bucket_snapshot(&bucket);

        let status = bucket_status(
            config.as_ref().map(|(config, updated_at)| (config, *updated_at)),
            runtime,
            module_enabled(),
        )?;
        admin_json_response(req.uri.path(), &cred.secret_key, StatusCode::OK, &status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Uri;

    /// `matchit::Params` cannot be built by hand; route a sample path the way
    /// the admin router does.
    pub(super) fn bucket_router() -> matchit::Router<()> {
        let mut router = matchit::Router::new();
        router.insert("/{bucket}", ()).expect("bucket route");
        router
    }

    /// Wire fixtures shared with the `rustfs-madmin` client tests: both sides
    /// must reproduce these bytes exactly.
    const SET_REQUEST_FIXTURE: &str = include_str!("../../../../crates/madmin/fixtures/on_demand_migration/set_request.json");
    const SET_RESPONSE_FIXTURE: &str = include_str!("../../../../crates/madmin/fixtures/on_demand_migration/set_response.json");
    const GET_RESPONSE_FIXTURE: &str = include_str!("../../../../crates/madmin/fixtures/on_demand_migration/get_response.json");
    const STATUS_FIXTURE: &str = include_str!("../../../../crates/madmin/fixtures/on_demand_migration/status.json");
    const FIXTURE_UPDATED_AT: &str = "2026-09-02T10:00:00Z";

    fn fixture_config() -> OnDemandMigrationConfig {
        OnDemandMigrationConfig::from_json(SET_REQUEST_FIXTURE.trim().as_bytes()).expect("fixture config parses")
    }

    #[test]
    fn parse_config_rejects_empty_body_unknown_fields_and_malformed_json() {
        assert_eq!(parse_config(b"").unwrap_err().code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(parse_config(b"{").unwrap_err().code(), &S3ErrorCode::InvalidArgument);

        let unknown =
            br#"{"source":{"provider":"s3","endpoint":"https://s.example","region":"us-east-1","bucket":"b"},"bogus":1}"#;
        let err = parse_config(unknown).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert!(
            err.message().unwrap_or_default().contains("bogus"),
            "message must name the offending field"
        );

        assert!(parse_config(SET_REQUEST_FIXTURE.trim().as_bytes()).is_ok());
    }

    #[test]
    fn set_request_fixture_is_the_plaintext_wire_shape() {
        let config = fixture_config();
        assert_eq!(
            config.source.credentials.as_ref().map(|c| c.secret_key.as_str()),
            Some("sourceSecretKey123")
        );
        // A client sends the exact bytes the server re-serializes: the fixture is canonical.
        let reserialized = String::from_utf8(config.to_json().expect("serialize")).expect("utf-8");
        assert_eq!(reserialized, SET_REQUEST_FIXTURE.trim());
    }

    #[test]
    fn set_response_matches_madmin_golden_fixture() {
        let response = SetBucketOnDemandMigrationResponse {
            bucket: "photos".to_string(),
            dry_run: false,
            config: fixture_config().redacted(),
            updated_at: Some(FIXTURE_UPDATED_AT.to_string()),
            probe: ProbeSummary {
                reachable: true,
                listable: true,
                sample_key: Some("photos/2024/01.jpg".to_string()),
            },
        };
        let json = serde_json::to_string(&response).expect("serialize");
        assert_eq!(json, SET_RESPONSE_FIXTURE.trim());
        assert!(!json.contains("sourceSecretKey123"), "responses must never echo the secret key");
        assert!(json.contains(r#""secret_key":"REDACTED""#));
    }

    #[test]
    fn get_response_matches_madmin_golden_fixture() {
        let response = GetBucketOnDemandMigrationResponse {
            bucket: "photos".to_string(),
            config: fixture_config().redacted(),
            updated_at: FIXTURE_UPDATED_AT.to_string(),
        };
        assert_eq!(serde_json::to_string(&response).expect("serialize"), GET_RESPONSE_FIXTURE.trim());
    }

    /// The runtime snapshot the ecstore golden test (`snapshot_matches_golden_json`)
    /// produces, as this node would hand it to the status route.
    fn fixture_runtime_snapshot() -> OdmBucketSnapshot {
        let fixture: serde_json::Value = serde_json::from_str(STATUS_FIXTURE.trim()).expect("status fixture parses");
        let counters = &fixture["counters"];
        let snapshot = serde_json::json!({
            "bucket": "photos",
            "provider": fixture["provider"],
            "endpoint_host": fixture["endpoint_host"],
            "applied_at": FIXTURE_UPDATED_AT,
            "client_error": null,
            "negative_cache_entries": 0,
            "inflight_keys": 1,
            "max_concurrent_pulls": 8,
            "stats": {
                "requests_total": counters["requests_total"],
                "pulled_bytes_total": counters["pulled_bytes_total"],
                "pulled_objects_total": counters["pulled_objects_total"],
                "pull_failures_total": counters["pull_failures_total"],
                "inflight_pulls": fixture["inflight_pulls"],
                "queue_depth": fixture["queue_depth"],
                "source_latency": counters["source_latency"],
                "last_source_error": fixture["last_source_error"],
                "breaker_state": fixture["breaker"]["state"],
            }
        });
        serde_json::from_value(snapshot).expect("runtime snapshot decodes")
    }

    #[test]
    fn status_matches_madmin_golden_fixture() {
        let config = fixture_config();
        let updated_at = OffsetDateTime::from_unix_timestamp(1_788_343_200).expect("timestamp");
        let status = bucket_status(Some((&config, updated_at)), Some(fixture_runtime_snapshot()), true).expect("status");
        let json = serde_json::to_string(&status).expect("serialize");
        assert_eq!(json, STATUS_FIXTURE.trim());
        assert!(json.contains(r#""served_by_source_ratio":null"#), "the ratio field is present as null");
    }

    #[test]
    fn status_without_runtime_state_describes_the_config_and_nulls_the_runtime() {
        let config = fixture_config();
        let updated_at = OffsetDateTime::from_unix_timestamp(1_788_343_200).expect("timestamp");
        let status = bucket_status(Some((&config, updated_at)), None, false).expect("status");
        assert_eq!(
            serde_json::to_value(&status).expect("serialize"),
            serde_json::json!({
                "configured": true,
                "enabled": true,
                "module_enabled": false,
                "provider": "minio",
                "endpoint_host": "source.example.com",
                "breaker": null,
                "counters": null,
                "last_source_error": null,
                "inflight_pulls": 0,
                "queue_depth": 0,
                "served_by_source_ratio": null,
                "updated_at": FIXTURE_UPDATED_AT,
            })
        );

        let status = bucket_status(None, None, true).expect("status");
        assert_eq!(
            serde_json::to_value(&status).expect("serialize"),
            serde_json::json!({
                "configured": false,
                "enabled": false,
                "module_enabled": true,
                "provider": null,
                "endpoint_host": null,
                "breaker": null,
                "counters": null,
                "last_source_error": null,
                "inflight_pulls": 0,
                "queue_depth": 0,
                "served_by_source_ratio": null,
                "updated_at": null,
            })
        );
    }

    #[test]
    fn config_endpoint_host_matches_the_runtime_host_rule() {
        let mut config = fixture_config();
        assert_eq!(config_endpoint_host(&config).as_deref(), Some("source.example.com"));
        config.source.endpoint = Some("https://Bucket.S3.Example:9000/base".to_string());
        assert_eq!(config_endpoint_host(&config).as_deref(), Some("bucket.s3.example"));
    }

    #[test]
    fn updated_at_uses_rfc3339_utc() {
        let ts = OffsetDateTime::from_unix_timestamp(1_788_343_200).expect("timestamp");
        assert_eq!(format_updated_at(ts).expect("format"), FIXTURE_UPDATED_AT);
    }

    #[test]
    fn source_client_spec_maps_provider_path_style_credentials_and_timeouts() {
        let config = fixture_config();
        let spec = source_client_spec(&config);

        assert_eq!(spec.endpoint, "https://source.example.com:9000");
        assert_eq!(spec.region, "us-east-1");
        assert_eq!(spec.bucket, "legacy-photos");
        assert_eq!(spec.source_prefix.as_deref(), Some("photos/"));
        assert_eq!(spec.provider, SourceProvider::Minio);
        assert_eq!(spec.path_style, RemotePathStyle::Auto);
        let credentials = spec.credentials.as_ref().expect("credentials mapped");
        assert_eq!(credentials.access_key, "AKIASOURCE");
        assert_eq!(credentials.secret_key, "sourceSecretKey123");
        assert_eq!(credentials.session_token, None);
        assert_eq!(spec.timeouts.connect, Duration::from_millis(5000));
        assert_eq!(spec.timeouts.read, Duration::from_millis(15_000));
        assert_eq!(spec.bandwidth_limit, None);

        let mut virtual_host = config.clone();
        virtual_host.source.path_style = PathStyle::Virtual;
        virtual_host.policy.bandwidth_limit_bytes_per_sec = Some(1 << 20);
        let spec = source_client_spec(&virtual_host);
        assert_eq!(spec.path_style, RemotePathStyle::VirtualHost);
        assert_eq!(spec.bandwidth_limit, NonZeroU64::new(1 << 20));

        let mut anonymous = config;
        anonymous.source.credentials = None;
        assert!(source_client_spec(&anonymous).credentials.is_none());
    }

    #[test]
    fn probe_error_names_only_the_error_class() {
        let err = probe_error(&SourceError::Connect(
            "dispatch failure: GET https://AKIASOURCE:sourceSecretKey123@source.example.com:9000/legacy-photos".to_string(),
        ));
        assert_eq!(err.code(), &S3ErrorCode::Custom(ERR_CODE_SOURCE_UNREACHABLE.into()));
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
        let message = err.message().unwrap_or_default();
        assert_eq!(message, "source bucket probe failed: connect");
        assert!(!message.contains("sourceSecretKey123"));
        assert!(!message.contains("source.example.com"));

        let denied = probe_error(&SourceError::AccessDenied);
        assert_eq!(denied.message(), Some("source bucket probe failed: access_denied"));
    }

    #[test]
    fn missing_credentials_is_a_400_naming_the_field() {
        let err = client_build_error(RemoteS3ClientError::MissingCredentials);
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert!(err.message().unwrap_or_default().contains("source.credentials"));
    }

    #[test]
    fn module_switch_defaults_off_and_reads_the_env() {
        temp_env::with_var(ENV_ON_DEMAND_MIGRATION_ENABLED, None::<&str>, || assert!(!module_enabled()));
        temp_env::with_var(ENV_ON_DEMAND_MIGRATION_ENABLED, Some("true"), || assert!(module_enabled()));
        temp_env::with_var(ENV_ON_DEMAND_MIGRATION_ENABLED, Some("false"), || assert!(!module_enabled()));
    }

    #[test]
    fn dry_run_query_requires_a_literal_true() {
        let request = |uri: &'static str| S3Request {
            input: Body::empty(),
            method: Method::PUT,
            uri: Uri::from_static(uri),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        assert!(is_dry_run(&request("/rustfs/admin/v3/on-demand-migration/b?dry-run=true")));
        assert!(is_dry_run(&request("/rustfs/admin/v3/on-demand-migration/b?dry-run=TRUE")));
        assert!(!is_dry_run(&request("/rustfs/admin/v3/on-demand-migration/b?dry-run=1")));
        assert!(!is_dry_run(&request("/rustfs/admin/v3/on-demand-migration/b")));
    }

    #[cfg(not(feature = "license"))]
    #[test]
    fn license_gate_always_passes_without_the_license_feature() {
        assert!(license_gate().is_ok());
    }

    /// Strict builds refuse writes until a license is installed; the mapping
    /// is the one the object zip download uses.
    #[cfg(feature = "license")]
    #[test]
    #[serial_test::serial]
    fn license_gate_rejects_writes_without_a_license() {
        let err = license_gate().expect_err("strict build without a license must refuse");
        assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn handlers_reject_requests_without_credentials_before_touching_storage() {
        let request = |method: Method, uri: &'static str| S3Request {
            input: Body::empty(),
            method,
            uri: Uri::from_static(uri),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let router = bucket_router();
        let params = router.at("/photos").expect("route matches").params;
        let handlers: [(&dyn Operation, Method, &'static str); 4] = [
            (
                &SetBucketOnDemandMigrationHandler {},
                Method::PUT,
                "/rustfs/admin/v3/on-demand-migration/photos",
            ),
            (
                &GetBucketOnDemandMigrationHandler {},
                Method::GET,
                "/rustfs/admin/v3/on-demand-migration/photos",
            ),
            (
                &DeleteBucketOnDemandMigrationHandler {},
                Method::DELETE,
                "/rustfs/admin/v3/on-demand-migration/photos",
            ),
            (
                &GetBucketOnDemandMigrationStatusHandler {},
                Method::GET,
                "/rustfs/admin/v3/on-demand-migration/photos/status",
            ),
        ];
        for (handler, method, uri) in handlers {
            let err = handler
                .call(request(method, uri), params.clone())
                .await
                .expect_err("a request without credentials must be rejected");
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
            assert_eq!(err.message(), Some("get cred failed"));
        }
    }

    #[tokio::test]
    async fn handlers_reject_an_empty_bucket_path_param() {
        let request = S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: Uri::from_static("/rustfs/admin/v3/on-demand-migration/"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let err = GetBucketOnDemandMigrationHandler {}
            .call(request, Params::new())
            .await
            .expect_err("an empty bucket must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("bucket name is required"));
    }
}

/// Store-backed coverage: one `TestECStoreEnv` per test binary is the rule
/// (ambient globals), so every scenario runs from a single test body.
#[cfg(all(test, not(feature = "license")))]
mod store_tests {
    use super::*;
    use crate::admin::runtime_sources::{NotificationSystemInterface, publish_test_app_context};
    use crate::admin::storage_api::NotificationSys;
    use crate::admin::storage_api::runtime_sources::ECStore;
    use crate::admin::storage_api::s3::auth as s3_auth;
    use http::{Extensions, HeaderMap, Uri};
    use http_body_util::BodyExt as _;
    use rustfs_iam::store::{Store as _, UserType};
    use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq};
    use rustfs_policy::policy::Policy;
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const ROOT_ACCESS_KEY: &str = "ODMADMINROOT";
    const ROOT_SECRET_KEY: &str = "odmAdminRootSecret123";
    const READER_ACCESS_KEY: &str = "odmreader";
    const READER_SECRET_KEY: &str = "odmReaderSecret123";
    const BUCKET: &str = "photos";
    const SOURCE_BUCKET: &str = "legacy-photos";

    fn admin_request(method: Method, uri: String, body: Vec<u8>, access_key: &str, secret_key: &str) -> S3Request<Body> {
        S3Request {
            input: Body::from(body),
            method,
            uri: Uri::try_from(uri).expect("valid uri"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: Some(s3_auth::Credentials {
                access_key: access_key.to_string(),
                secret_key: s3_auth::SecretKey::from(secret_key.to_string()),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn root_request(method: Method, uri: String, body: Vec<u8>) -> S3Request<Body> {
        admin_request(method, uri, body, ROOT_ACCESS_KEY, ROOT_SECRET_KEY)
    }

    fn bucket_router() -> matchit::Router<()> {
        super::tests::bucket_router()
    }

    fn bucket_params(router: &matchit::Router<()>) -> Params<'_, 'static> {
        router.at(concat!("/", "photos")).expect("route matches").params
    }

    fn config_uri(query: &str) -> String {
        format!("/rustfs/admin/v3/on-demand-migration/{BUCKET}{query}")
    }

    fn config_json(endpoint: &str) -> Vec<u8> {
        serde_json::json!({
            "source": {
                "provider": "minio",
                "endpoint": endpoint,
                "region": "us-east-1",
                "bucket": SOURCE_BUCKET,
                "credentials": {"access_key": "AKIASOURCE", "secret_key": "sourceSecretKey123"}
            },
            "policy": {"source_timeout": {"connect_ms": 500, "first_byte_ms": 2000, "idle_ms": 2000}}
        })
        .to_string()
        .into_bytes()
    }

    async fn response_json(response: S3Response<(StatusCode, Body)>) -> (StatusCode, Value) {
        let (status, body) = response.output;
        let bytes = body.collect().await.expect("collect body").to_bytes();
        let value = serde_json::from_slice(&bytes).expect("json body");
        (status, value)
    }

    async fn get_config() -> Result<(StatusCode, Value), S3Error> {
        let router = bucket_router();
        let response = GetBucketOnDemandMigrationHandler {}
            .call(root_request(Method::GET, config_uri(""), Vec::new()), bucket_params(&router))
            .await?;
        Ok(response_json(response).await)
    }

    fn assert_status_switches(status: &Value, configured: bool, enabled: bool, module_enabled: bool) {
        assert_eq!(status["configured"], Value::Bool(configured), "{status}");
        assert_eq!(status["enabled"], Value::Bool(enabled), "{status}");
        assert_eq!(status["module_enabled"], Value::Bool(module_enabled), "{status}");
    }

    async fn status() -> Value {
        let router = bucket_router();
        let response = GetBucketOnDemandMigrationStatusHandler {}
            .call(root_request(Method::GET, config_uri("/status"), Vec::new()), bucket_params(&router))
            .await
            .expect("status is readable");
        response_json(response).await.1
    }

    /// A minimal S3 source: `HEAD /{bucket}` answers 200 and a one-key
    /// `ListObjectsV2` returns `sample_key`; everything else is 404.
    async fn spawn_fake_source(sample_key: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake source");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            for _ in 0..32 {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buffer = Vec::with_capacity(4096);
                let mut chunk = [0u8; 4096];
                loop {
                    if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buffer.extend_from_slice(&chunk[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&buffer);
                let request_line = head.lines().next().unwrap_or_default().to_string();
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or_default();
                let target = parts.next().unwrap_or_default();
                let (path, query) = target.split_once('?').unwrap_or((target, ""));
                let bucket_path = format!("/{SOURCE_BUCKET}");
                let is_bucket = path.trim_end_matches('/') == bucket_path;
                let response = if method == "HEAD" && is_bucket {
                    "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                } else if method == "GET" && is_bucket && query.contains("list-type=2") {
                    let body = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{SOURCE_BUCKET}</Name><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>{sample_key}</Key><Size>1</Size></Contents></ListBucketResult>"
                    );
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    )
                } else {
                    eprintln!("fake source: unmatched request line {request_line:?}");
                    "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                };
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    /// A loopback port nothing listens on: the probe must fail with `connect`.
    async fn closed_endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind probe port");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        format!("http://{addr}")
    }

    struct TestNotificationSystemInterface(Arc<NotificationSys>);

    impl NotificationSystemInterface for TestNotificationSystemInterface {
        fn handle(&self) -> Option<Arc<NotificationSys>> {
            Some(self.0.clone())
        }
    }

    async fn seed_reader(iam: &rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>) {
        let policy = Policy::parse_config(
            br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["admin:GetBucketOnDemandMigration"],"Resource":["arn:aws:s3:::*"]}]}"#,
        )
        .expect("reader policy parses");
        iam.set_policy("odm-reader", policy).await.expect("store reader policy");
        iam.create_user(
            READER_ACCESS_KEY,
            &AddOrUpdateUserReq {
                secret_key: READER_SECRET_KEY.to_string(),
                policy: None,
                status: AccountStatus::Enabled,
            },
        )
        .await
        .expect("create reader");
        iam.policy_db_set(READER_ACCESS_KEY, UserType::Reg, false, "odm-reader")
            .await
            .expect("attach reader policy");
    }

    async fn build_env(temp: &std::path::Path) -> Arc<ECStore> {
        let _ = rustfs_credentials::init_global_action_credentials(
            Some(ROOT_ACCESS_KEY.to_string()),
            Some(ROOT_SECRET_KEY.to_string()),
        );
        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .base_dir(temp)
            .disk_count(1)
            .build()
            .await;
        env.make_bucket(BUCKET, false).await;
        rustfs_iam::store::object::ObjectStore::new(Arc::clone(&env.ecstore))
            .save_iam_config(
                serde_json::json!({"version": 1}),
                format!("{}/format.json", *rustfs_iam::store::object::IAM_CONFIG_PREFIX),
            )
            .await
            .expect("seed IAM format");
        let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
            .await
            .expect("build test IAM");
        seed_reader(&iam).await;

        // One unreachable peer: `reload_peers` must report it, proving the
        // notification fan-out ran after a successful write.
        let mut notification_system = NotificationSys::new(Default::default()).await;
        notification_system.peer_clients.push(None);
        let context =
            AppContext::with_default_interfaces(Arc::clone(&env.ecstore), iam, Arc::new(rustfs_kms::KmsServiceManager::new()))
                .with_test_notification_system_interface(Arc::new(TestNotificationSystemInterface(Arc::new(
                    notification_system,
                ))));
        publish_test_app_context(Arc::new(context));
        Arc::clone(&env.ecstore)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial]
    async fn admin_api_covers_switch_validation_probe_persistence_and_authorization() {
        let temp = tempfile::tempdir().expect("odm admin test root");
        let _store = build_env(temp.path()).await;
        let router = bucket_router();

        // Module switch off: reads work, writes are refused with the dedicated code.
        temp_env::async_with_vars([(ENV_ON_DEMAND_MIGRATION_ENABLED, Some("false"))], async {
            let err = SetBucketOnDemandMigrationHandler {}
                .call(
                    root_request(Method::PUT, config_uri(""), config_json("https://source.example.com")),
                    bucket_params(&router),
                )
                .await
                .expect_err("PUT must be refused while the module is disabled");
            assert_eq!(err.code(), &S3ErrorCode::Custom(ERR_CODE_MODULE_DISABLED.into()));
            assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));

            let err = get_config().await.expect_err("nothing is configured yet");
            assert_eq!(err.code(), &S3ErrorCode::Custom(ERR_CODE_NO_SUCH_CONFIGURATION.into()));
            assert_eq!(err.status_code(), Some(StatusCode::NOT_FOUND));
            let body = status().await;
            assert_status_switches(&body, false, false, false);
            assert_eq!(body["provider"], Value::Null);
            assert_eq!(body["counters"], Value::Null);
            assert_eq!(body["updated_at"], Value::Null);
        })
        .await;

        temp_env::async_with_vars(
            [
                (ENV_ON_DEMAND_MIGRATION_ENABLED, Some("true")),
                ("RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET", Some("true")),
            ],
            async {
                // Unknown bucket: 404 before any body is read.
                let missing = router.at("/no-such-bucket").expect("route matches").params;
                let err = SetBucketOnDemandMigrationHandler {}
                    .call(
                        root_request(
                            Method::PUT,
                            "/rustfs/admin/v3/on-demand-migration/no-such-bucket".to_string(),
                            config_json("https://source.example.com"),
                        ),
                        missing,
                    )
                    .await
                    .expect_err("unknown bucket must be rejected");
                assert_eq!(err.code(), &S3ErrorCode::NoSuchBucket);

                // Validation failure (empty region) does not persist anything.
                let mut invalid: Value = serde_json::from_slice(&config_json("https://source.example.com")).expect("json");
                invalid["source"]["region"] = Value::String(String::new());
                let err = SetBucketOnDemandMigrationHandler {}
                    .call(
                        root_request(Method::PUT, config_uri(""), invalid.to_string().into_bytes()),
                        bucket_params(&router),
                    )
                    .await
                    .expect_err("invalid config must be rejected");
                assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
                assert!(err.message().unwrap_or_default().contains("region"));
                assert!(get_config().await.is_err(), "a rejected PUT must not persist");

                // Probe failure: 400 with the dedicated code, no secret in the message, nothing persisted.
                let closed = closed_endpoint().await;
                let err = SetBucketOnDemandMigrationHandler {}
                    .call(root_request(Method::PUT, config_uri(""), config_json(&closed)), bucket_params(&router))
                    .await
                    .expect_err("unreachable source must be rejected");
                assert_eq!(err.code(), &S3ErrorCode::Custom(ERR_CODE_SOURCE_UNREACHABLE.into()));
                assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
                let rendered = format!("{err:?}");
                assert!(!rendered.contains("sourceSecretKey123"), "probe errors must not carry the secret");
                assert!(get_config().await.is_err(), "a failed probe must not persist");

                // Dry run: probe succeeds, response is redacted, still nothing persisted.
                let source = spawn_fake_source("photos/2024/01.jpg").await;
                let response = SetBucketOnDemandMigrationHandler {}
                    .call(
                        root_request(Method::PUT, config_uri("?dry-run=true"), config_json(&source)),
                        bucket_params(&router),
                    )
                    .await
                    .expect("dry run succeeds against the fake source");
                let (status_code, body) = response_json(response).await;
                assert_eq!(status_code, StatusCode::OK);
                assert_eq!(body["dry_run"], Value::Bool(true));
                assert_eq!(body["updated_at"], Value::Null);
                assert_eq!(
                    body["probe"],
                    serde_json::json!({"reachable": true, "listable": true, "sample_key": "photos/2024/01.jpg"})
                );
                assert_eq!(body["config"]["source"]["credentials"]["secret_key"], Value::String("REDACTED".into()));
                assert!(get_config().await.is_err(), "a dry run must not persist");

                // Real PUT persists; GET returns the redacted config and the same timestamp.
                let response = SetBucketOnDemandMigrationHandler {}
                    .call(root_request(Method::PUT, config_uri(""), config_json(&source)), bucket_params(&router))
                    .await
                    .expect("PUT succeeds against the fake source");
                let (status_code, body) = response_json(response).await;
                assert_eq!(status_code, StatusCode::OK);
                assert_eq!(body["dry_run"], Value::Bool(false));
                let first_updated_at = body["updated_at"].as_str().expect("updated_at is set").to_string();

                let (status_code, body) = get_config().await.expect("config is readable after PUT");
                assert_eq!(status_code, StatusCode::OK);
                assert_eq!(body["bucket"], Value::String(BUCKET.into()));
                assert_eq!(body["config"]["source"]["credentials"]["secret_key"], Value::String("REDACTED".into()));
                assert_eq!(body["config"]["source"]["credentials"]["access_key"], Value::String("AKIASOURCE".into()));
                assert_eq!(body["updated_at"], Value::String(first_updated_at.clone()));
                let body = status().await;
                assert_status_switches(&body, true, true, true);
                assert_eq!(body["provider"], Value::String("minio".into()));
                assert_eq!(body["endpoint_host"], Value::String("127.0.0.1".into()));
                assert_eq!(body["updated_at"], Value::String(first_updated_at.clone()));
                assert_eq!(body["served_by_source_ratio"], Value::Null, "no per-bucket GET total exists");
                assert_eq!(body["inflight_pulls"], Value::from(0));
                assert_eq!(body["queue_depth"], Value::from(0));

                // The peer fan-out ran: the single unreachable peer is reported.
                let context = crate::admin::runtime_sources::current_app_context();
                let err = reload_peers(context.as_deref(), BUCKET)
                    .await
                    .expect_err("an unreachable peer must surface from the reload");
                assert!(err.contains("load_bucket_metadata"), "unexpected reload error: {err}");

                // A second PUT moves the timestamp forward.
                tokio::time::sleep(Duration::from_millis(1100)).await;
                let response = SetBucketOnDemandMigrationHandler {}
                    .call(root_request(Method::PUT, config_uri(""), config_json(&source)), bucket_params(&router))
                    .await
                    .expect("second PUT succeeds");
                let (_, body) = response_json(response).await;
                let second_updated_at = body["updated_at"].as_str().expect("updated_at is set").to_string();
                assert!(second_updated_at > first_updated_at, "{second_updated_at} must follow {first_updated_at}");

                // Read-only principal: GET and status answer, PUT and DELETE are 403.
                let reader_get = GetBucketOnDemandMigrationHandler {}
                    .call(
                        admin_request(Method::GET, config_uri(""), Vec::new(), READER_ACCESS_KEY, READER_SECRET_KEY),
                        bucket_params(&router),
                    )
                    .await
                    .expect("reader may GET");
                assert_eq!(reader_get.output.0, StatusCode::OK);
                let reader_status = GetBucketOnDemandMigrationStatusHandler {}
                    .call(
                        admin_request(Method::GET, config_uri("/status"), Vec::new(), READER_ACCESS_KEY, READER_SECRET_KEY),
                        bucket_params(&router),
                    )
                    .await
                    .expect("reader may read status");
                assert_eq!(reader_status.output.0, StatusCode::OK);
                let err = SetBucketOnDemandMigrationHandler {}
                    .call(
                        admin_request(Method::PUT, config_uri(""), config_json(&source), READER_ACCESS_KEY, READER_SECRET_KEY),
                        bucket_params(&router),
                    )
                    .await
                    .expect_err("reader must not PUT");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
                let err = DeleteBucketOnDemandMigrationHandler {}
                    .call(
                        admin_request(Method::DELETE, config_uri(""), Vec::new(), READER_ACCESS_KEY, READER_SECRET_KEY),
                        bucket_params(&router),
                    )
                    .await
                    .expect_err("reader must not DELETE");
                assert_eq!(err.code(), &S3ErrorCode::AccessDenied);
                assert!(get_config().await.is_ok(), "denied writes must not change the config");

                // DELETE clears, answers 204, and is idempotent.
                let response = DeleteBucketOnDemandMigrationHandler {}
                    .call(root_request(Method::DELETE, config_uri(""), Vec::new()), bucket_params(&router))
                    .await
                    .expect("DELETE succeeds");
                assert_eq!(response.output.0, StatusCode::NO_CONTENT);
                let err = get_config().await.expect_err("config is gone after DELETE");
                assert_eq!(err.code(), &S3ErrorCode::Custom(ERR_CODE_NO_SUCH_CONFIGURATION.into()));
                let metadata = metadata_sys::get(BUCKET).await.expect("bucket metadata");
                assert!(metadata.on_demand_migration_config_json.is_empty());
                assert!(metadata.on_demand_migration_config_updated_at > OffsetDateTime::UNIX_EPOCH);
                let body = status().await;
                assert_status_switches(&body, false, false, true);
                assert_eq!(body["provider"], Value::Null);
                assert_eq!(body["breaker"], Value::Null);
                assert_eq!(body["updated_at"], Value::Null);

                let response = DeleteBucketOnDemandMigrationHandler {}
                    .call(root_request(Method::DELETE, config_uri(""), Vec::new()), bucket_params(&router))
                    .await
                    .expect("a second DELETE is idempotent");
                assert_eq!(response.output.0, StatusCode::NO_CONTENT);
            },
        )
        .await;
    }
}
