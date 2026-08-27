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

use crate::admin::handlers::site_replication::site_replication_bucket_meta_hook;
use crate::admin::runtime_sources::object_store_from_extensions;
use crate::admin::storage_api::bucket::utils::{deserialize, serialize};
use crate::admin::storage_api::bucket::{
    metadata::{
        BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE,
        BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
        BucketMetadata, OBJECT_LOCK_CONFIG,
    },
    metadata_sys,
    quota::BucketQuota,
    target::BucketTargets,
};
use crate::admin::storage_api::contract::bucket::{BucketOperations, BucketOptions, MakeBucketOptions};
use crate::admin::storage_api::error::StorageError;
use crate::storage::storage_api::lock_bucket_targets_metadata;
use crate::{
    admin::{
        auth::authorize_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    server::ADMIN_PREFIX,
};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_BUCKET_METADATA_IMPORT_SIZE;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_policy::policy::{
    BucketPolicy,
    action::{Action, AdminAction},
};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use s3s::{
    Body, S3Request, S3Response, S3Result,
    dto::{
        BucketLifecycleConfiguration, ObjectLockConfiguration, ReplicationConfiguration, ServerSideEncryptionConfiguration,
        Tagging, VersioningConfiguration,
    },
    header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use std::{
    collections::HashMap,
    io::{Cursor, Read as _, Write as _},
};
use time::OffsetDateTime;
use tracing::warn;
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_BUCKET_META: &str = "bucket_meta";
const EVENT_ADMIN_BUCKET_META_STATE: &str = "admin_bucket_meta_state";

#[derive(Debug, Default, serde::Deserialize)]
pub struct ExportBucketMetadataQuery {
    pub bucket: String,
}

pub struct ExportBucketMetadata {}

pub fn register_bucket_meta_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/export-bucket-metadata").as_str(),
        AdminOperation(&ExportBucketMetadata {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/export-bucket-metadata").as_str(),
        AdminOperation(&ExportBucketMetadata {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/import-bucket-metadata").as_str(),
        AdminOperation(&ImportBucketMetadata {}),
    )?;

    r.insert(
        Method::PUT,
        format!("{}{}", ADMIN_PREFIX, "/v3/import-bucket-metadata").as_str(),
        AdminOperation(&ImportBucketMetadata {}),
    )?;

    Ok(())
}

#[async_trait::async_trait]
impl Operation for ExportBucketMetadata {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ExportBucketMetadataQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                ExportBucketMetadataQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::ExportBucketMetadataAction)]).await?;

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let buckets = if query.bucket.is_empty() {
            store
                .list_bucket(&BucketOptions::default())
                .await
                .map_err(|e| s3_error!(InternalError, "failed to list buckets: {e}"))?
        } else {
            let bucket = store
                .get_bucket_info(&query.bucket, &BucketOptions::default())
                .await
                .map_err(|e| s3_error!(InternalError, "failed to load bucket: {e}"))?;
            vec![bucket]
        };

        let mut zip_writer = ZipWriter::new(Cursor::new(Vec::new()));

        let confs = [
            BUCKET_POLICY_CONFIG,
            BUCKET_NOTIFICATION_CONFIG,
            BUCKET_LIFECYCLE_CONFIG,
            BUCKET_SSECONFIG,
            BUCKET_TAGGING_CONFIG,
            BUCKET_QUOTA_CONFIG_FILE,
            OBJECT_LOCK_CONFIG,
            BUCKET_VERSIONING_CONFIG,
            BUCKET_REPLICATION_CONFIG,
            BUCKET_TARGETS_FILE,
        ];

        for bucket in buckets {
            for &conf in confs.iter() {
                let conf_path = path_join_buf(&[bucket.name.as_str(), conf]);
                match conf {
                    BUCKET_POLICY_CONFIG => {
                        let config: BucketPolicy = match metadata_sys::get_bucket_policy(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "failed to load bucket metadata: {e}"));
                            }
                        };
                        let config_json = serde_json::to_vec(&config)
                            .map_err(|e| s3_error!(InternalError, "failed to serialize config: {e}"))?;
                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "failed to start archive entry: {e}"))?;
                        zip_writer
                            .write_all(&config_json)
                            .map_err(|e| s3_error!(InternalError, "failed to write archive entry: {e}"))?;
                    }
                    BUCKET_NOTIFICATION_CONFIG => {
                        let config: s3s::dto::NotificationConfiguration =
                            match metadata_sys::get_notification_config(&bucket.name).await {
                                Ok(Some(res)) => res,
                                Err(e) => {
                                    if e == StorageError::ConfigNotFound {
                                        continue;
                                    }
                                    return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                                }
                                Ok(None) => continue,
                            };

                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    BUCKET_LIFECYCLE_CONFIG => {
                        let config: BucketLifecycleConfiguration = match metadata_sys::get_lifecycle_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "failed to load bucket metadata: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "failed to serialize config: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "failed to start archive entry: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "failed to write archive entry: {e}"))?;
                    }
                    BUCKET_TAGGING_CONFIG => {
                        let config: Tagging = match metadata_sys::get_tagging_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "failed to load bucket metadata: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "failed to serialize config: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "failed to start archive entry: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "failed to write archive entry: {e}"))?;
                    }
                    BUCKET_QUOTA_CONFIG_FILE => {
                        let config: BucketQuota = match metadata_sys::get_quota_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };
                        let config_json =
                            serde_json::to_vec(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_json)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    OBJECT_LOCK_CONFIG => {
                        let config = match metadata_sys::get_object_lock_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    BUCKET_SSECONFIG => {
                        let config = match metadata_sys::get_sse_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    BUCKET_VERSIONING_CONFIG => {
                        let config = match metadata_sys::get_versioning_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    BUCKET_REPLICATION_CONFIG => {
                        let config = match metadata_sys::get_replication_config(&bucket.name).await {
                            Ok((res, _)) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };
                        let config_xml =
                            serialize(&config).map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_xml)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    BUCKET_TARGETS_FILE => {
                        let config: BucketTargets = match metadata_sys::get_bucket_targets_config(&bucket.name).await {
                            Ok(res) => res,
                            Err(e) => {
                                if e == StorageError::ConfigNotFound {
                                    continue;
                                }
                                return Err(s3_error!(InternalError, "get bucket metadata failed: {e}"));
                            }
                        };

                        let config_json = serde_json::to_vec(&config.redacted_credentials())
                            .map_err(|e| s3_error!(InternalError, "serialize config failed: {e}"))?;

                        zip_writer
                            .start_file(conf_path, SimpleFileOptions::default())
                            .map_err(|e| s3_error!(InternalError, "start file failed: {e}"))?;
                        zip_writer
                            .write_all(&config_json)
                            .map_err(|e| s3_error!(InternalError, "write file failed: {e}"))?;
                    }
                    _ => {}
                }
            }
        }

        let zip_bytes = zip_writer
            .finish()
            .map_err(|e| s3_error!(InternalError, "failed to finalize export archive: {e}"))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/zip".parse().expect("valid header value"));
        header.insert(
            CONTENT_DISPOSITION,
            "attachment; filename=bucket-meta.zip".parse().expect("valid header value"),
        );
        header.insert(CONTENT_LENGTH, zip_bytes.get_ref().len().to_string().parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::from(zip_bytes.into_inner())), header))
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ImportBucketMetadataQuery {
    pub bucket: String,
}

pub struct ImportBucketMetadata {}

#[async_trait::async_trait]
impl Operation for ImportBucketMetadata {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let _query = {
            if let Some(query) = req.uri.query() {
                let input: ImportBucketMetadataQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "failed to decode query"))?;
                input
            } else {
                ImportBucketMetadataQuery::default()
            }
        };

        if req.credentials.is_none() {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        }

        authorize_admin_request(&req, vec![Action::AdminAction(AdminAction::ImportBucketMetadataAction)]).await?;

        let mut input = req.input;
        let body = match input.store_all_limited(MAX_BUCKET_METADATA_IMPORT_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    event = EVENT_ADMIN_BUCKET_META_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_BUCKET_META,
                    action = "import_bucket_metadata",
                    result = "body_read_failed",
                    error = ?e,
                    "admin bucket meta state"
                );
                return Err(s3_error!(InvalidRequest, "bucket metadata import body too large or failed to read"));
            }
        };

        let mut zip_reader =
            ZipArchive::new(Cursor::new(body)).map_err(|e| s3_error!(InternalError, "failed to read import archive: {e}"))?;

        // First pass: read all file contents into memory
        let mut file_contents = Vec::new();
        for i in 0..zip_reader.len() {
            let mut file = zip_reader
                .by_index(i)
                .map_err(|e| s3_error!(InternalError, "failed to read archive entry: {e}"))?;
            let file_path = file.name().to_string();

            let mut content = Vec::new();
            file.read_to_end(&mut content)
                .map_err(|e| s3_error!(InternalError, "failed to read archive entry content: {e}"))?;

            file_contents.push((file_path, content));
        }

        let durable_quota_import = imported_quota_requires_fleet_proof(&file_contents)?;
        let quota_fleet_proof =
            if durable_quota_import {
                Some(crate::admin::storage_api::acquire_cross_pool_fence_fleet_proof().ok_or_else(|| {
                    s3_error!(ServiceUnavailable, "durable quota capability is not confirmed across the cluster")
                })?)
            } else {
                None
            };

        // Extract bucket names
        let mut bucket_names = Vec::new();
        for (file_path, _) in &file_contents {
            let file_path_split = file_path.split(SLASH_SEPARATOR).collect::<Vec<&str>>();

            if file_path_split.len() < 2 {
                warn!(
                    event = EVENT_ADMIN_BUCKET_META_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_BUCKET_META,
                    action = "import_bucket_metadata",
                    result = "invalid_file_path",
                    file_path = %file_path,
                    "admin bucket meta state"
                );
                continue;
            }

            let bucket_name = file_path_split[0].to_string();
            if !bucket_names.contains(&bucket_name) {
                bucket_names.push(bucket_name);
            }
        }

        // Get existing bucket metadata
        let mut bucket_metadatas: HashMap<String, BucketMetadata> = HashMap::new();
        for bucket_name in bucket_names {
            match metadata_sys::get_config_from_disk(&bucket_name).await {
                Ok(res) => {
                    bucket_metadatas.insert(bucket_name, res);
                }
                Err(e) => {
                    if e == StorageError::ConfigNotFound {
                        warn!(
                            event = EVENT_ADMIN_BUCKET_META_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_BUCKET_META,
                            action = "import_bucket_metadata",
                            result = "bucket_metadata_missing",
                            error = %e,
                            "admin bucket meta state"
                        );
                        continue;
                    }
                    warn!(
                        event = EVENT_ADMIN_BUCKET_META_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_BUCKET_META,
                        action = "import_bucket_metadata",
                        result = "bucket_metadata_load_failed",
                        error = %e,
                        "admin bucket meta state"
                    );
                    continue;
                }
            };
        }

        let Some(store) = object_store_from_extensions(&req.extensions) else {
            return Err(s3_error!(InternalError, "object store is not initialized"));
        };

        let update_at = OffsetDateTime::now_utc();

        // Second pass: process file contents
        for (file_path, content) in file_contents {
            let file_path_split = file_path.split(SLASH_SEPARATOR).collect::<Vec<&str>>();

            if file_path_split.len() < 2 {
                warn!(
                    event = EVENT_ADMIN_BUCKET_META_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_BUCKET_META,
                    action = "import_bucket_metadata",
                    result = "invalid_file_path",
                    file_path = %file_path,
                    "admin bucket meta state"
                );
                continue;
            }

            let bucket_name = file_path_split[0];
            let conf_name = file_path_split[1];

            // create bucket if not exists
            if !bucket_metadatas.contains_key(bucket_name) {
                if let Err(e) = store
                    .make_bucket(
                        bucket_name,
                        &MakeBucketOptions {
                            force_create: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    warn!(
                        event = EVENT_ADMIN_BUCKET_META_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_BUCKET_META,
                        action = "import_bucket_metadata",
                        result = "bucket_create_failed",
                        bucket = %bucket_name,
                        error = %e,
                        "admin bucket meta state"
                    );
                    continue;
                }

                let metadata = metadata_sys::get(bucket_name).await.unwrap_or_default();

                bucket_metadatas.insert(bucket_name.to_string(), (*metadata).clone());
            }

            match conf_name {
                BUCKET_POLICY_CONFIG => {
                    let config: BucketPolicy = match serde_json::from_slice(&content) {
                        Ok(config) => config,
                        Err(e) => {
                            warn!(
                                event = EVENT_ADMIN_BUCKET_META_STATE,
                                component = LOG_COMPONENT_ADMIN,
                                subsystem = LOG_SUBSYSTEM_BUCKET_META,
                                action = "import_bucket_metadata",
                                result = "config_deserialize_failed",
                                bucket = %bucket_name,
                                config_name = %conf_name,
                                error = %e,
                                "admin bucket meta state"
                            );
                            continue;
                        }
                    };

                    if config.version.is_empty() {
                        continue;
                    }

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.policy_config_json = content;
                    metadata.policy_config_updated_at = update_at;
                }
                BUCKET_NOTIFICATION_CONFIG
                | BUCKET_LIFECYCLE_CONFIG
                | BUCKET_SSECONFIG
                | BUCKET_TAGGING_CONFIG
                | OBJECT_LOCK_CONFIG
                | BUCKET_VERSIONING_CONFIG
                | BUCKET_REPLICATION_CONFIG
                | BUCKET_TARGETS_FILE => {
                    if let Err(e) =
                        apply_imported_bucket_config(&mut bucket_metadatas, bucket_name, conf_name, content, update_at)
                    {
                        warn!(
                            event = EVENT_ADMIN_BUCKET_META_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_BUCKET_META,
                            action = "import_bucket_metadata",
                            result = "config_deserialize_failed",
                            bucket = %bucket_name,
                            config_name = %conf_name,
                            error = %e,
                            "admin bucket meta state"
                        );
                        continue;
                    }
                }

                BUCKET_QUOTA_CONFIG_FILE => {
                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.quota_config_json = content;
                    metadata.quota_config_updated_at = update_at;
                }

                _ => {}
            }
        }

        for (bucket_name, metadata) in &bucket_metadatas {
            for (config_file, data) in imported_configs_to_persist(metadata) {
                let site_replication_item = imported_config_to_site_replication_item(bucket_name, metadata, config_file, &data)?;
                let targets_guard = if matches!(config_file, BUCKET_REPLICATION_CONFIG | BUCKET_TARGETS_FILE) {
                    Some(lock_bucket_targets_metadata(bucket_name).await)
                } else {
                    None
                };
                let persist_result = if config_file == BUCKET_QUOTA_CONFIG_FILE {
                    let quota: BucketQuota =
                        serde_json::from_slice(&data).map_err(|e| s3_error!(InvalidRequest, "invalid bucket quota: {e}"))?;
                    if quota.uses_durable_reservations() {
                        let proof = quota_fleet_proof.as_ref().ok_or_else(|| {
                            s3_error!(ServiceUnavailable, "durable quota capability is not confirmed across the cluster")
                        })?;
                        metadata_sys::update_quota_if_incarnation(bucket_name, data, metadata.bucket_incarnation_id, proof).await
                    } else {
                        metadata_sys::update_if_incarnation(bucket_name, config_file, data, metadata.bucket_incarnation_id).await
                    }
                } else {
                    metadata_sys::update_if_incarnation(bucket_name, config_file, data, metadata.bucket_incarnation_id).await
                };
                if let Err(e) = persist_result {
                    warn!(
                        event = EVENT_ADMIN_BUCKET_META_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_BUCKET_META,
                        action = "import_bucket_metadata",
                        result = "config_persist_failed",
                        bucket = %bucket_name,
                        config_name = %config_file,
                        error = %e,
                        "admin bucket meta state"
                    );
                    return Err(s3_error!(
                        InternalError,
                        "failed to persist imported bucket metadata for {bucket_name}/{config_file}: {e}"
                    ));
                }
                drop(targets_guard);
                if let Some(item) = site_replication_item
                    && let Err(err) = site_replication_bucket_meta_hook(item).await
                {
                    warn!(
                        event = EVENT_ADMIN_BUCKET_META_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_BUCKET_META,
                        action = "import_bucket_metadata",
                        result = "site_replication_notify_failed",
                        bucket = %bucket_name,
                        config_name = %config_file,
                        error = %err,
                        "admin bucket meta state"
                    );
                }
            }
        }

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}

fn imported_quota_requires_fleet_proof(file_contents: &[(String, Vec<u8>)]) -> S3Result<bool> {
    let mut durable = false;
    for (file_path, content) in file_contents {
        let mut parts = file_path.split(SLASH_SEPARATOR);
        let Some(_bucket) = parts.next() else {
            continue;
        };
        if parts.next() != Some(BUCKET_QUOTA_CONFIG_FILE) {
            continue;
        }
        let quota: BucketQuota =
            serde_json::from_slice(content).map_err(|e| s3_error!(InvalidRequest, "invalid bucket quota: {e}"))?;
        if quota.has_unsupported_reservation_protocol() {
            return Err(s3_error!(InvalidRequest, "unsupported bucket quota reservation protocol"));
        }
        durable |= quota.uses_durable_reservations();
    }
    Ok(durable)
}

/// Store one imported bucket config that follows the shared validate-then-store shape: the seven
/// validated XML configs plus the JSON bucket-targets file.
///
/// A single `conf_name` match owns both the type a payload must parse as and the [`BucketMetadata`]
/// field it lands in, so a config file cannot be validated as one type but stored into another
/// config's field. Validation runs before the metadata lookup, so an unparsable payload is rejected
/// whether or not `bucket_name` has in-memory metadata.
///
/// `Err` carries the parse error's display form and leaves every bucket untouched. `Ok(false)` means
/// nothing was stored because `conf_name` is not one of these configs, or because `bucket_name` has
/// no in-memory metadata.
fn apply_imported_bucket_config(
    bucket_metadatas: &mut HashMap<String, BucketMetadata>,
    bucket_name: &str,
    conf_name: &str,
    content: Vec<u8>,
    update_at: OffsetDateTime,
) -> Result<bool, String> {
    macro_rules! validated_config {
        ($validate:expr, $payload_field:ident, $updated_at_field:ident) => {{
            $validate(&content).map_err(|e| e.to_string())?;
            |metadata: &mut BucketMetadata, payload: Vec<u8>, updated_at: OffsetDateTime| {
                metadata.$payload_field = payload;
                metadata.$updated_at_field = updated_at;
            }
        }};
    }

    let store: fn(&mut BucketMetadata, Vec<u8>, OffsetDateTime) = match conf_name {
        BUCKET_NOTIFICATION_CONFIG => validated_config!(
            deserialize::<s3s::dto::NotificationConfiguration>,
            notification_config_xml,
            notification_config_updated_at
        ),
        BUCKET_LIFECYCLE_CONFIG => {
            validated_config!(
                deserialize::<BucketLifecycleConfiguration>,
                lifecycle_config_xml,
                lifecycle_config_updated_at
            )
        }
        BUCKET_SSECONFIG => validated_config!(
            deserialize::<ServerSideEncryptionConfiguration>,
            encryption_config_xml,
            encryption_config_updated_at
        ),
        BUCKET_TAGGING_CONFIG => validated_config!(deserialize::<Tagging>, tagging_config_xml, tagging_config_updated_at),
        OBJECT_LOCK_CONFIG => {
            validated_config!(
                deserialize::<ObjectLockConfiguration>,
                object_lock_config_xml,
                object_lock_config_updated_at
            )
        }
        BUCKET_VERSIONING_CONFIG => {
            validated_config!(
                deserialize::<VersioningConfiguration>,
                versioning_config_xml,
                versioning_config_updated_at
            )
        }
        BUCKET_REPLICATION_CONFIG => {
            validated_config!(
                deserialize::<ReplicationConfiguration>,
                replication_config_xml,
                replication_config_updated_at
            )
        }
        BUCKET_TARGETS_FILE => validated_config!(
            serde_json::from_slice::<BucketTargets>,
            bucket_targets_config_json,
            bucket_targets_config_updated_at
        ),
        _ => return Ok(false),
    };

    let Some(metadata) = bucket_metadatas.get_mut(bucket_name) else {
        return Ok(false);
    };
    store(metadata, content, update_at);
    Ok(true)
}

/// The `(config_file, data)` pairs to persist for an imported bucket's metadata: every non-empty
/// config field keyed by its on-disk config-file name, as owned data ready for
/// `metadata_sys::update`. Empty fields are skipped so an import never overwrites an existing
/// on-disk config with an empty payload. Shared by [`import_bucket_metadata`] and its tests so both
/// exercise the same mapping.
fn imported_configs_to_persist(metadata: &BucketMetadata) -> Vec<(&'static str, Vec<u8>)> {
    let configs: [(&'static str, &Vec<u8>); 10] = [
        (BUCKET_POLICY_CONFIG, &metadata.policy_config_json),
        (BUCKET_NOTIFICATION_CONFIG, &metadata.notification_config_xml),
        (BUCKET_LIFECYCLE_CONFIG, &metadata.lifecycle_config_xml),
        (BUCKET_SSECONFIG, &metadata.encryption_config_xml),
        (BUCKET_TAGGING_CONFIG, &metadata.tagging_config_xml),
        (BUCKET_QUOTA_CONFIG_FILE, &metadata.quota_config_json),
        (OBJECT_LOCK_CONFIG, &metadata.object_lock_config_xml),
        (BUCKET_VERSIONING_CONFIG, &metadata.versioning_config_xml),
        (BUCKET_REPLICATION_CONFIG, &metadata.replication_config_xml),
        (BUCKET_TARGETS_FILE, &metadata.bucket_targets_config_json),
    ];
    configs
        .into_iter()
        .filter(|(_, d)| !d.is_empty())
        .map(|(name, d)| (name, d.clone()))
        .collect()
}

fn imported_config_to_site_replication_item(
    bucket_name: &str,
    metadata: &BucketMetadata,
    config_file: &str,
    data: &[u8],
) -> S3Result<Option<SRBucketMeta>> {
    let mut item = match config_file {
        BUCKET_POLICY_CONFIG => site_replication_bucket_meta_item(bucket_name, "policy", metadata.policy_config_updated_at),
        BUCKET_LIFECYCLE_CONFIG => {
            site_replication_bucket_meta_item(bucket_name, "lc-config", metadata.lifecycle_config_updated_at)
        }
        BUCKET_SSECONFIG => site_replication_bucket_meta_item(bucket_name, "sse-config", metadata.encryption_config_updated_at),
        BUCKET_TAGGING_CONFIG => site_replication_bucket_meta_item(bucket_name, "tags", metadata.tagging_config_updated_at),
        BUCKET_QUOTA_CONFIG_FILE => {
            site_replication_bucket_meta_item(bucket_name, "quota-config", metadata.quota_config_updated_at)
        }
        OBJECT_LOCK_CONFIG => {
            site_replication_bucket_meta_item(bucket_name, "object-lock-config", metadata.object_lock_config_updated_at)
        }
        BUCKET_VERSIONING_CONFIG => {
            site_replication_bucket_meta_item(bucket_name, "version-config", metadata.versioning_config_updated_at)
        }
        BUCKET_REPLICATION_CONFIG => {
            site_replication_bucket_meta_item(bucket_name, "replication-config", metadata.replication_config_updated_at)
        }
        _ => return Ok(None),
    };

    match config_file {
        BUCKET_POLICY_CONFIG => {
            item.policy =
                Some(serde_json::from_slice(data).map_err(|e| {
                    s3_error!(InternalError, "failed to encode imported bucket policy for site replication: {e}")
                })?);
        }
        BUCKET_LIFECYCLE_CONFIG => {
            set_imported_config_string(&mut item.expiry_lc_config, config_file, data)?;
            item.expiry_updated_at = item.updated_at;
        }
        BUCKET_SSECONFIG => set_imported_config_string(&mut item.sse_config, config_file, data)?,
        BUCKET_TAGGING_CONFIG => set_imported_config_string(&mut item.tags, config_file, data)?,
        BUCKET_QUOTA_CONFIG_FILE => {
            item.quota = Some(
                serde_json::from_slice(data)
                    .map_err(|e| s3_error!(InternalError, "failed to encode imported bucket quota for site replication: {e}"))?,
            );
        }
        OBJECT_LOCK_CONFIG => set_imported_config_string(&mut item.object_lock_config, config_file, data)?,
        BUCKET_VERSIONING_CONFIG => set_imported_config_string(&mut item.versioning, config_file, data)?,
        BUCKET_REPLICATION_CONFIG => set_imported_config_string(&mut item.replication_config, config_file, data)?,
        _ => unreachable!(),
    }

    Ok(Some(item))
}

fn site_replication_bucket_meta_item(bucket_name: &str, item_type: &str, updated_at: OffsetDateTime) -> SRBucketMeta {
    SRBucketMeta {
        bucket: bucket_name.to_string(),
        r#type: item_type.to_string(),
        updated_at: Some(updated_at),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

fn set_imported_config_string(target: &mut Option<String>, config_file: &str, data: &[u8]) -> S3Result<()> {
    *target = Some(
        String::from_utf8(data.to_vec())
            .map_err(|e| s3_error!(InternalError, "imported bucket metadata {config_file} is not valid UTF-8: {e}"))?,
    );
    Ok(())
}

#[cfg(test)]
mod imported_config_apply_tests {
    use super::*;

    const BUCKET: &str = "restored-bucket";

    /// Distinct from every `BucketMetadata::new` default, so a written timestamp is visible.
    fn imported_at() -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1_755_000_000)
    }

    /// One imported config file, the [`BucketMetadata`] field pair it owns, and payloads its type
    /// accepts and rejects.
    struct ImportCase {
        conf_name: &'static str,
        valid: &'static [u8],
        invalid: &'static [u8],
        payload: fn(&BucketMetadata) -> &Vec<u8>,
        updated_at: fn(&BucketMetadata) -> OffsetDateTime,
    }

    /// The `conf_name` -> (validated type, metadata field) mapping the import handler must honour.
    /// Storing a config file's payload into another config's field is the regression this table
    /// pins down.
    fn import_cases() -> Vec<ImportCase> {
        vec![
            ImportCase {
                conf_name: BUCKET_NOTIFICATION_CONFIG,
                valid: b"<NotificationConfiguration></NotificationConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.notification_config_xml,
                updated_at: |m| m.notification_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_LIFECYCLE_CONFIG,
                valid: b"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.lifecycle_config_xml,
                updated_at: |m| m.lifecycle_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_SSECONFIG,
                valid: b"<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.encryption_config_xml,
                updated_at: |m| m.encryption_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_TAGGING_CONFIG,
                valid: b"<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>",
                invalid: b"not xml",
                payload: |m| &m.tagging_config_xml,
                updated_at: |m| m.tagging_config_updated_at,
            },
            ImportCase {
                conf_name: OBJECT_LOCK_CONFIG,
                valid: b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.object_lock_config_xml,
                updated_at: |m| m.object_lock_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_VERSIONING_CONFIG,
                valid: b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.versioning_config_xml,
                updated_at: |m| m.versioning_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_REPLICATION_CONFIG,
                valid: b"<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/replication</Role><Rule><Status>Enabled</Status><Priority>1</Priority><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><Filter><Prefix></Prefix></Filter><Destination><Bucket>arn:aws:s3:::backup</Bucket></Destination></Rule></ReplicationConfiguration>",
                invalid: b"not xml",
                payload: |m| &m.replication_config_xml,
                updated_at: |m| m.replication_config_updated_at,
            },
            ImportCase {
                conf_name: BUCKET_TARGETS_FILE,
                valid: br#"{"targets":[]}"#,
                invalid: b"[]",
                payload: |m| &m.bucket_targets_config_json,
                updated_at: |m| m.bucket_targets_config_updated_at,
            },
        ]
    }

    fn imported_bucket() -> HashMap<String, BucketMetadata> {
        HashMap::from([(BUCKET.to_string(), BucketMetadata::new(BUCKET))])
    }

    #[test]
    fn a_valid_payload_lands_only_in_the_field_its_config_file_owns() {
        for case in import_cases() {
            let mut metadatas = imported_bucket();
            let stored = apply_imported_bucket_config(&mut metadatas, BUCKET, case.conf_name, case.valid.to_vec(), imported_at())
                .unwrap_or_else(|e| panic!("{} payload must validate: {e}", case.conf_name));
            assert!(stored, "{} must be stored", case.conf_name);

            let metadata = &metadatas[BUCKET];
            assert_eq!((case.payload)(metadata), case.valid, "{} landed in the wrong field", case.conf_name);
            assert_eq!((case.updated_at)(metadata), imported_at(), "{} timestamp was not written", case.conf_name);
            for other in import_cases().iter().filter(|other| other.conf_name != case.conf_name) {
                assert!(
                    (other.payload)(metadata).is_empty(),
                    "{} payload leaked into the {} field",
                    case.conf_name,
                    other.conf_name
                );
            }
        }
    }

    #[test]
    fn a_rejected_payload_leaves_the_field_untouched() {
        for case in import_cases() {
            let mut metadatas = imported_bucket();
            let before = metadatas[BUCKET].clone();
            let error = match apply_imported_bucket_config(
                &mut metadatas,
                BUCKET,
                case.conf_name,
                case.invalid.to_vec(),
                imported_at(),
            ) {
                Ok(_) => panic!("{} must reject an unparsable payload", case.conf_name),
                Err(e) => e,
            };
            assert!(!error.is_empty(), "{} must report why the payload was rejected", case.conf_name);

            let metadata = &metadatas[BUCKET];
            assert_eq!((case.payload)(metadata), (case.payload)(&before), "{} field was mutated", case.conf_name);
            assert_eq!(
                (case.updated_at)(metadata),
                (case.updated_at)(&before),
                "{} timestamp was mutated",
                case.conf_name
            );
        }
    }

    #[test]
    fn a_rejected_payload_does_not_stop_the_remaining_configs() {
        // The handler warns and moves to the next archive entry, so a rejected config must not
        // keep the entries after it from being imported.
        let mut metadatas = imported_bucket();
        for case in import_cases() {
            assert!(
                apply_imported_bucket_config(&mut metadatas, BUCKET, case.conf_name, case.invalid.to_vec(), imported_at())
                    .is_err()
            );
        }
        assert!(imported_configs_to_persist(&metadatas[BUCKET]).is_empty());

        for case in import_cases() {
            assert!(
                apply_imported_bucket_config(&mut metadatas, BUCKET, case.conf_name, case.valid.to_vec(), imported_at())
                    .unwrap_or_else(|e| panic!("{} payload must validate: {e}", case.conf_name))
            );
        }
        assert_eq!(imported_configs_to_persist(&metadatas[BUCKET]).len(), import_cases().len());
    }

    #[test]
    fn an_absent_bucket_is_skipped_without_masking_a_parse_error() {
        // Validation runs before the metadata lookup, so an unparsable payload is still reported
        // for a bucket that has no in-memory metadata.
        for case in import_cases() {
            let mut metadatas = HashMap::new();
            assert!(
                !apply_imported_bucket_config(&mut metadatas, BUCKET, case.conf_name, case.valid.to_vec(), imported_at())
                    .unwrap_or_else(|e| panic!("{} payload must validate: {e}", case.conf_name))
            );
            assert!(
                apply_imported_bucket_config(&mut metadatas, BUCKET, case.conf_name, case.invalid.to_vec(), imported_at())
                    .is_err()
            );
        }
    }

    #[test]
    fn policy_and_quota_keep_their_own_handling() {
        // Both are imported by their own match arms; this helper must not claim them.
        for conf_name in [BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE] {
            let mut metadatas = imported_bucket();
            assert!(
                !apply_imported_bucket_config(&mut metadatas, BUCKET, conf_name, br#"{"quota":1024}"#.to_vec(), imported_at())
                    .expect("configs outside the shared shape are not validated here")
            );
            assert!(imported_configs_to_persist(&metadatas[BUCKET]).is_empty());
        }
    }
}

#[cfg(test)]
mod import_persist_tests {
    use super::*;

    #[test]
    fn imported_versioning_and_policy_are_scheduled_for_persistence() {
        // State the second pass builds in memory after importing a versioning + policy config.
        let mut metadata = BucketMetadata::new("restored-bucket");
        metadata.versioning_config_xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
        metadata.policy_config_json = br#"{"Version":"2012-10-17","Statement":[]}"#.to_vec();

        let plan = imported_configs_to_persist(&metadata);

        // The bug: the old handler produced zero persistence calls (mutated memory, returned 200).
        assert_eq!(plan.len(), 2, "both imported configs must be persisted, got {plan:?}");
        assert!(
            plan.iter()
                .any(|(n, d)| *n == BUCKET_VERSIONING_CONFIG && d == &metadata.versioning_config_xml)
        );
        assert!(
            plan.iter()
                .any(|(n, d)| *n == BUCKET_POLICY_CONFIG && d == &metadata.policy_config_json)
        );
    }

    #[test]
    fn empty_configs_are_not_persisted() {
        // A freshly-created metadata with no imported configs must schedule nothing, so import
        // never overwrites existing on-disk configs with empty payloads.
        let metadata = BucketMetadata::new("untouched-bucket");
        assert!(imported_configs_to_persist(&metadata).is_empty());
    }

    #[test]
    fn imported_configs_are_scheduled_for_site_replication() {
        let updated_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(42);
        let mut metadata = BucketMetadata::new("restored-bucket");
        metadata.policy_config_json = br#"{"Version":"2012-10-17","Statement":[]}"#.to_vec();
        metadata.policy_config_updated_at = updated_at;
        metadata.versioning_config_xml = b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec();
        metadata.versioning_config_updated_at = updated_at;
        metadata.lifecycle_config_xml = b"<LifecycleConfiguration></LifecycleConfiguration>".to_vec();
        metadata.lifecycle_config_updated_at = updated_at;
        metadata.quota_config_json = br#"{"quota":1024}"#.to_vec();
        metadata.quota_config_updated_at = updated_at;

        let mut items = imported_configs_to_persist(&metadata)
            .into_iter()
            .filter_map(|(config_file, data)| {
                imported_config_to_site_replication_item("restored-bucket", &metadata, config_file, &data)
                    .expect("site replication item should encode")
            })
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.r#type.cmp(&right.r#type));

        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|item| item.bucket == "restored-bucket"));
        assert!(items.iter().all(|item| item.updated_at == Some(updated_at)));
        assert!(
            items
                .iter()
                .all(|item| item.api_version.as_deref() == Some(SITE_REPL_API_VERSION))
        );
        assert!(items.iter().any(|item| item.r#type == "policy" && item.policy.is_some()));
        assert!(
            items
                .iter()
                .any(|item| item.r#type == "version-config" && item.versioning.is_some())
        );
        assert!(items.iter().any(|item| {
            item.r#type == "lc-config" && item.expiry_lc_config.is_some() && item.expiry_updated_at == Some(updated_at)
        }));
        assert!(items.iter().any(|item| item.r#type == "quota-config" && item.quota.is_some()));
    }

    #[test]
    fn unsupported_imported_configs_are_not_site_replicated() {
        let mut metadata = BucketMetadata::new("restored-bucket");
        metadata.notification_config_xml = b"<NotificationConfiguration></NotificationConfiguration>".to_vec();
        metadata.bucket_targets_config_json = b"[]".to_vec();

        let has_site_replication_item = imported_configs_to_persist(&metadata)
            .into_iter()
            .filter_map(|(config_file, data)| {
                imported_config_to_site_replication_item("restored-bucket", &metadata, config_file, &data)
                    .expect("unsupported configs should be skipped")
            })
            .next()
            .is_some();

        assert!(!has_site_replication_item);
    }

    #[test]
    fn quota_import_preflight_rejects_invalid_and_unknown_protocols() {
        let missing_limit = vec![(
            format!("bucket/{BUCKET_QUOTA_CONFIG_FILE}"),
            br#"{"quota":0,"reservation_protocol":1}"#.to_vec(),
        )];
        assert!(imported_quota_requires_fleet_proof(&missing_limit).is_err());

        let unknown_protocol = vec![(
            format!("bucket/{BUCKET_QUOTA_CONFIG_FILE}"),
            br#"{"quota":0,"reservation_protocol":2,"reservation_quota":1024}"#.to_vec(),
        )];
        assert!(imported_quota_requires_fleet_proof(&unknown_protocol).is_err());
    }

    #[test]
    fn quota_import_preflight_requires_proof_only_for_durable_quota() {
        let legacy = vec![(format!("bucket/{BUCKET_QUOTA_CONFIG_FILE}"), br#"{"quota":1024}"#.to_vec())];
        assert!(!imported_quota_requires_fleet_proof(&legacy).expect("legacy quota should remain compatible"));

        let durable = vec![(
            format!("bucket/{BUCKET_QUOTA_CONFIG_FILE}"),
            serde_json::to_vec(&BucketQuota::new(Some(1024))).expect("durable quota should encode"),
        )];
        assert!(imported_quota_requires_fleet_proof(&durable).expect("durable quota should pass preflight"));
    }
}

#[cfg(test)]
mod shared_gate_tests {
    use super::*;
    use http::Uri;
    use s3s::S3ErrorCode;

    fn credential_less_request(method: Method, uri: &'static str) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method,
            uri: Uri::from_static(uri),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn assert_missing_credentials(operation: &dyn Operation, method: Method, uri: &'static str) {
        let err = operation
            .call(credential_less_request(method, uri), Params::new())
            .await
            .expect_err("a bucket metadata admin request without credentials must fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(err.message(), Some("authentication required"));
    }

    /// The shared gate reports "get cred failed"; the per-handler pre-check keeps
    /// the message each endpoint has always returned (rustfs/backlog#1829).
    #[tokio::test]
    async fn bucket_metadata_handlers_keep_their_missing_credentials_response() {
        assert_missing_credentials(&ExportBucketMetadata {}, Method::GET, "/rustfs/admin/v3/export-bucket-metadata").await;
        assert_missing_credentials(&ImportBucketMetadata {}, Method::PUT, "/rustfs/admin/v3/import-bucket-metadata").await;
    }

    fn source_block<'a>(production: &'a str, marker: &str) -> &'a str {
        let block = production
            .split_once(marker)
            .unwrap_or_else(|| panic!("{marker} should exist"))
            .1;
        let end = ["\npub struct ", "\nfn ", "\n#[derive(", "\n#[cfg(test)]"]
            .into_iter()
            .filter_map(|boundary| block.find(boundary))
            .min()
            .unwrap_or(block.len());
        &block[..end]
    }

    fn assert_shared_gate_wiring(block: &str, item: &str, actions: &[&str], binds_credentials: bool) {
        assert_eq!(
            block.matches("authorize_admin_request(").count(),
            1,
            "{item} must use exactly one shared gate"
        );
        assert_eq!(
            block.matches("Action::AdminAction(").count(),
            actions.len(),
            "{item} must preserve its exact action-vector length"
        );
        for action in actions {
            assert!(block.contains(&format!("AdminAction::{action}")), "{item} must authorize with {action}");
        }
        assert_eq!(
            block.contains("let cred = authorize_admin_request("),
            binds_credentials,
            "{item} credential binding must match its payload-processing contract"
        );
    }

    #[test]
    fn bucket_metadata_handlers_use_the_shared_admin_gate_with_their_actions() {
        let production = include_str!("bucket_meta.rs")
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("production source must precede tests");

        for (handler, action) in [
            ("ExportBucketMetadata", "ExportBucketMetadataAction"),
            ("ImportBucketMetadata", "ImportBucketMetadataAction"),
        ] {
            let block = source_block(production, &format!("impl Operation for {handler}"));
            assert_shared_gate_wiring(block, handler, &[action], false);
        }

        assert!(!production.contains("check_key_valid(get_session_token"));
    }
}
