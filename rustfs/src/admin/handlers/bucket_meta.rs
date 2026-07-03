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
use crate::{
    admin::runtime_sources::current_object_store_handle,
    admin::{
        auth::validate_admin_request,
        router::{AdminOperation, Operation, S3Router},
    },
    auth::{check_key_valid, get_session_token},
    server::{ADMIN_PREFIX, RemoteAddr},
};
use http::{HeaderMap, StatusCode};
use hyper::Method;
use matchit::Params;
use rustfs_config::MAX_BUCKET_METADATA_IMPORT_SIZE;
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

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ExportBucketMetadataAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

        let Some(store) = current_object_store_handle() else {
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
    #[allow(dead_code)]
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

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        validate_admin_request(
            &req.headers,
            &cred,
            owner,
            false,
            vec![Action::AdminAction(AdminAction::ImportBucketMetadataAction)],
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
        )
        .await?;

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

        let Some(store) = current_object_store_handle() else {
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
                BUCKET_NOTIFICATION_CONFIG => {
                    if let Err(e) = deserialize::<s3s::dto::NotificationConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.notification_config_xml = content;
                    metadata.notification_config_updated_at = update_at;
                }

                BUCKET_LIFECYCLE_CONFIG => {
                    if let Err(e) = deserialize::<BucketLifecycleConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.lifecycle_config_xml = content;
                    metadata.lifecycle_config_updated_at = update_at;
                }

                BUCKET_SSECONFIG => {
                    if let Err(e) = deserialize::<ServerSideEncryptionConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.encryption_config_xml = content;
                    metadata.encryption_config_updated_at = update_at;
                }

                BUCKET_TAGGING_CONFIG => {
                    if let Err(e) = deserialize::<Tagging>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.tagging_config_xml = content;
                    metadata.tagging_config_updated_at = update_at;
                }

                BUCKET_QUOTA_CONFIG_FILE => {
                    if let Err(e) = serde_json::from_slice::<BucketQuota>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.quota_config_json = content;
                    metadata.quota_config_updated_at = update_at;
                }

                OBJECT_LOCK_CONFIG => {
                    if let Err(e) = deserialize::<ObjectLockConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.object_lock_config_xml = content;
                    metadata.object_lock_config_updated_at = update_at;
                }

                BUCKET_VERSIONING_CONFIG => {
                    if let Err(e) = deserialize::<VersioningConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.versioning_config_xml = content;
                    metadata.versioning_config_updated_at = update_at;
                }

                BUCKET_REPLICATION_CONFIG => {
                    if let Err(e) = deserialize::<ReplicationConfiguration>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.replication_config_xml = content;
                    metadata.replication_config_updated_at = update_at;
                }

                BUCKET_TARGETS_FILE => {
                    if let Err(e) = serde_json::from_slice::<BucketTargets>(&content) {
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

                    let metadata = match bucket_metadatas.get_mut(bucket_name) {
                        Some(m) => m,
                        None => continue,
                    };
                    metadata.bucket_targets_config_json = content;
                    metadata.bucket_targets_config_updated_at = update_at;
                }

                _ => {}
            }
        }

        // Persist the assembled metadata to disk. Prior to this, the import only mutated the
        // in-memory `bucket_metadatas` map and returned 200, silently dropping every imported
        // config. `metadata_sys::update` loads the on-disk metadata, overwrites the given config
        // field and saves it, preserving any configs not present in the import archive.
        for (bucket_name, metadata) in &bucket_metadatas {
            for (config_file, data) in imported_configs_to_persist(metadata) {
                if let Err(e) = metadata_sys::update(bucket_name, config_file, data).await {
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
            }
        }

        // TODO: site replication notify

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().expect("valid header value"));
        header.insert(CONTENT_LENGTH, "0".parse().expect("valid header value"));
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
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
}
