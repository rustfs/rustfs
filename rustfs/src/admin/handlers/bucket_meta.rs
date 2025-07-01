use std::io::{Cursor, Write as _};

use crate::{
    admin::router::Operation,
    auth::{check_key_valid, get_session_token},
};
use ecstore::bucket::utils::serialize;
use ecstore::{
    StorageAPI,
    bucket::{
        metadata::{
            BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE,
            BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG,
            OBJECT_LOCK_CONFIG,
        },
        metadata_sys,
    },
    error::StorageError,
    new_object_layer_fn,
    store_api::BucketOptions,
};
use http::{HeaderMap, StatusCode};
use matchit::Params;
use rustfs_utils::path::path_join_buf;
use s3s::{
    Body, S3Request, S3Response, S3Result,
    header::{CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE},
    s3_error,
};
use serde::Deserialize;
use serde_urlencoded::from_bytes;
use zip::{ZipArchive, ZipWriter, result::ZipError, write::SimpleFileOptions};

#[derive(Debug, Default, serde::Deserialize)]
pub struct ExportBucketMetadataQuery {
    pub bucket: String,
}

pub struct ExportBucketMetadata {}

#[async_trait::async_trait]
impl Operation for ExportBucketMetadata {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let query = {
            if let Some(query) = req.uri.query() {
                let input: ExportBucketMetadataQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                ExportBucketMetadataQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InvalidRequest, "object store not init"));
        };

        let buckets = if query.bucket.is_empty() {
            store
                .list_bucket(&BucketOptions::default())
                .await
                .map_err(|e| s3_error!(InternalError, "list buckets failed: {e}"))?
        } else {
            let bucket = store
                .get_bucket_info(&query.bucket, &BucketOptions::default())
                .await
                .map_err(|e| s3_error!(InternalError, "get bucket failed: {e}"))?;
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
                        let config = match metadata_sys::get_bucket_policy(&bucket.name).await {
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
                    BUCKET_NOTIFICATION_CONFIG => {
                        let config = match metadata_sys::get_notification_config(&bucket.name).await {
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
                        let config = match metadata_sys::get_lifecycle_config(&bucket.name).await {
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
                    BUCKET_TAGGING_CONFIG => {
                        let config = match metadata_sys::get_tagging_config(&bucket.name).await {
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
                    BUCKET_QUOTA_CONFIG_FILE => {
                        let config = match metadata_sys::get_quota_config(&bucket.name).await {
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
                        let config = match metadata_sys::get_bucket_targets_config(&bucket.name).await {
                            Ok(res) => res,
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
                    _ => {}
                }
            }
        }

        let zip_bytes = zip_writer
            .finish()
            .map_err(|e| s3_error!(InternalError, "finish zip failed: {e}"))?;
        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/zip".parse().unwrap());
        header.insert(CONTENT_DISPOSITION, "attachment; filename=bucket-meta.zip".parse().unwrap());
        header.insert(CONTENT_LENGTH, zip_bytes.get_ref().len().to_string().parse().unwrap());
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
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get query failed"))?;
                input
            } else {
                ImportBucketMetadataQuery::default()
            }
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (_cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        Ok(S3Response::with_headers((StatusCode::OK, Body::empty()), header))
    }
}
