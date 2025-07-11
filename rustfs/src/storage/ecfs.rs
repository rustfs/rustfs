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

use super::access::authorize_request;
use super::options::del_opts;
use super::options::extract_metadata;
use super::options::put_opts;
use crate::auth::get_condition_values;
use crate::error::ApiError;
use crate::storage::access::ReqInfo;
use crate::storage::options::copy_dst_opts;
use crate::storage::options::copy_src_opts;
use crate::storage::options::{extract_metadata_from_mime, get_opts};
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use datafusion::arrow::csv::WriterBuilder as CsvWriterBuilder;
use datafusion::arrow::json::WriterBuilder as JsonWriterBuilder;
use datafusion::arrow::json::writer::JsonArray;
use rustfs_s3select_api::object_store::bytes_stream;
use rustfs_s3select_api::query::Context;
use rustfs_s3select_api::query::Query;
use rustfs_s3select_api::server::dbms::DatabaseManagerSystem;

// use rustfs_ecstore::store_api::RESERVED_METADATA_PREFIX;
use futures::StreamExt;
use http::HeaderMap;
use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::validate_transition_tier;
use rustfs_ecstore::bucket::lifecycle::lifecycle::Lifecycle;
use rustfs_ecstore::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_POLICY_CONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_REPLICATION_CONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_SSECONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_TAGGING_CONFIG;
use rustfs_ecstore::bucket::metadata::BUCKET_VERSIONING_CONFIG;
use rustfs_ecstore::bucket::metadata::OBJECT_LOCK_CONFIG;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::policy_sys::PolicySys;
use rustfs_ecstore::bucket::tagging::decode_tags;
use rustfs_ecstore::bucket::tagging::encode_tags;
use rustfs_ecstore::bucket::utils::serialize;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::cmd::bucket_replication::ReplicationStatusType;
use rustfs_ecstore::cmd::bucket_replication::ReplicationType;
use rustfs_ecstore::cmd::bucket_replication::get_must_replicate_options;
use rustfs_ecstore::cmd::bucket_replication::must_replicate;
use rustfs_ecstore::cmd::bucket_replication::schedule_replication;
use rustfs_ecstore::compress::MIN_COMPRESSIBLE_SIZE;
use rustfs_ecstore::compress::is_compressible;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::DEFAULT_READ_BUFFER_SIZE;
use rustfs_ecstore::store_api::BucketOptions;
use rustfs_ecstore::store_api::CompletePart;
use rustfs_ecstore::store_api::DeleteBucketOptions;
use rustfs_ecstore::store_api::HTTPRangeSpec;
use rustfs_ecstore::store_api::MakeBucketOptions;
use rustfs_ecstore::store_api::MultipartUploadResult;
use rustfs_ecstore::store_api::ObjectIO;
use rustfs_ecstore::store_api::ObjectOptions;
use rustfs_ecstore::store_api::ObjectToDelete;
use rustfs_ecstore::store_api::PutObjReader;
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_filemeta::headers::RESERVED_METADATA_PREFIX_LOWER;
use rustfs_filemeta::headers::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING};
use rustfs_notify::EventName;
use rustfs_policy::auth;
use rustfs_policy::policy::action::Action;
use rustfs_policy::policy::action::S3Action;
use rustfs_policy::policy::{BucketPolicy, BucketPolicyArgs, Validator};
use rustfs_rio::CompressReader;
use rustfs_rio::EtagReader;
use rustfs_rio::HashReader;
use rustfs_rio::Reader;
use rustfs_rio::WarpReader;
use rustfs_s3select_query::instance::make_rustfsms;
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::path::path_join_buf;
use rustfs_zip::CompressionFormat;
use s3s::S3;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::dto::*;
use s3s::s3_error;
use s3s::{S3Request, S3Response};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tar::Archive;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)));
            }
        }
    };
}

static RUSTFS_OWNER: LazyLock<Owner> = LazyLock::new(|| Owner {
    display_name: Some("rustfs".to_owned()),
    id: Some("c19050dbcee97fda828689dda99097a6321af2248fa760517237346e5d9c8a66".to_owned()),
});

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

impl FS {
    pub fn new() -> Self {
        // let store: ECStore = ECStore::new(address, endpoint_pools).await?;
        Self {}
    }

    async fn put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            ..
        } = req.input;
        let event_version_id = version_id;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let body = StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string()))));

        // let etag_stream = EtagReader::new(body);

        let Some(ext) = Path::new(&key).extension().and_then(|s| s.to_str()) else {
            return Err(s3_error!(InvalidArgument, "key extension not found"));
        };

        let ext = ext.to_owned();

        // TODO: support zip
        let decoder = CompressionFormat::from_extension(&ext).get_decoder(body).map_err(|e| {
            error!("get_decoder err {:?}", e);
            s3_error!(InvalidArgument, "get_decoder err")
        })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!("get entries err {:?}", e);
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let prefix = req
            .headers
            .get("X-Amz-Meta-Rustfs-Snowball-Prefix")
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or_default();
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        while let Some(entry) = entries.next().await {
            let f = match entry {
                Ok(f) => f,
                Err(e) => {
                    println!("Error reading entry: {e}");
                    return Err(s3_error!(InvalidArgument, "Error reading entry {:?}", e));
                }
            };

            if f.header().entry_type().is_dir() {
                continue;
            }

            if let Ok(fpath) = f.path() {
                let mut fpath = fpath.to_string_lossy().to_string();

                if !prefix.is_empty() {
                    fpath = format!("{prefix}/{fpath}");
                }

                let mut size = f.header().size().unwrap_or_default() as i64;

                println!("Extracted: {fpath}, size {size}");

                let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(f));

                let mut metadata = HashMap::new();

                let actual_size = size;

                if is_compressible(&HeaderMap::new(), &fpath) && size > MIN_COMPRESSIBLE_SIZE as i64 {
                    metadata.insert(
                        format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                        CompressionAlgorithm::default().to_string(),
                    );
                    metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

                    let hrd = HashReader::new(reader, size, actual_size, None, false).map_err(ApiError::from)?;

                    reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
                    size = -1;
                }

                let hrd = HashReader::new(reader, size, actual_size, None, false).map_err(ApiError::from)?;
                let mut reader = PutObjReader::new(hrd);

                let _obj_info = store
                    .put_object(&bucket, &fpath, &mut reader, &ObjectOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                let e_tag = _obj_info.clone().etag;

                // // store.put_object(bucket, object, data, opts);

                let output = PutObjectOutput {
                    e_tag,
                    ..Default::default()
                };

                let event_args = rustfs_notify::event::EventArgs {
                    event_name: EventName::ObjectCreatedPut,
                    bucket_name: bucket.clone(),
                    object: _obj_info.clone(),
                    req_params: rustfs_utils::extract_req_params_header(&req.headers),
                    resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
                    version_id: version_id.clone(),
                    host: rustfs_utils::get_request_host(&req.headers),
                    user_agent: rustfs_utils::get_request_user_agent(&req.headers),
                };

                // Asynchronous call will not block the response of the current request
                tokio::spawn(async move {
                    rustfs_notify::global::notifier_instance().notify(event_args).await;
                });
            }
        }

        // match decompress(
        //     body,
        //     CompressionFormat::from_extension(&ext),
        //     |entry: tokio_tar::Entry<tokio_tar::Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>| async move {
        //         let path = entry.path().unwrap();
        //         println!("Extracted: {}", path.display());
        //         Ok(())
        //     },
        // )
        // .await
        // {
        //     Ok(_) => println!("Decompression successful!"),
        //     Err(e) => println!("Decompression failed: {}", e),
        // }

        // TODO: etag
        let output = PutObjectOutput {
            // e_tag: Some(etag_stream.etag().await),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}
#[async_trait::async_trait]
impl S3 for FS {
    #[tracing::instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let CreateBucketInput {
            bucket,
            object_lock_enabled_for_bucket,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    force_create: true,
                    lock_enabled: object_lock_enabled_for_bucket.is_some_and(|v| v),
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let output = CreateBucketOutput::default();

        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::BucketCreated,
            bucket_name: bucket.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo { ..Default::default() },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id: String::new(),
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    /// Copy an object from one location to another
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let CopyObjectInput {
            copy_source,
            bucket,
            key,
            ..
        } = req.input.clone();
        let (src_bucket, src_key, version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string())),
        };

        // warn!("copy_object {}/{}, to {}/{}", &src_bucket, &src_key, &bucket, &key);

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;

        src_opts.version_id = version_id.clone();

        let mut get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let dst_opts = copy_dst_opts(&bucket, &key, version_id, &req.headers, HashMap::new())
            .await
            .map_err(ApiError::from)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);

        if cp_src_dst_same {
            get_opts.no_lock = true;
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let h = HeaderMap::new();

        let gr = store
            .get_object_reader(&src_bucket, &src_key, None, h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        let mut src_info = gr.object_info.clone();

        if cp_src_dst_same {
            src_info.metadata_only = true;
        }

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(gr.stream));

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let mut length = actual_size;

        let mut compress_metadata = HashMap::new();

        if is_compressible(&req.headers, &key) && actual_size > MIN_COMPRESSIBLE_SIZE as i64 {
            compress_metadata.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                CompressionAlgorithm::default().to_string(),
            );
            compress_metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), actual_size.to_string());

            let hrd = EtagReader::new(reader, None);

            // let hrd = HashReader::new(reader, length, actual_size, None, false).map_err(ApiError::from)?;

            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            length = -1;
        } else {
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression-size"));
        }

        let hrd = HashReader::new(reader, length, actual_size, None, false).map_err(ApiError::from)?;

        src_info.put_object_reader = Some(PutObjReader::new(hrd));

        // check quota
        // TODO: src metadada

        for (k, v) in compress_metadata {
            src_info.user_defined.insert(k, v);
        }

        // TODO: src tags

        let oi = store
            .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag,
            last_modified: oi.mod_time.map(Timestamp::from),
            ..Default::default()
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            ..Default::default()
        };

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };

        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedCopy,
            bucket_name: bucket.clone(),
            object: object_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    /// Delete a bucket
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let input = req.input;
        // TODO: DeleteBucketInput doesn't have force parameter?
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .delete_bucket(
                &input.bucket,
                &DeleteBucketOptions {
                    force: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::BucketRemoved,
            bucket_name: input.bucket,
            object: rustfs_ecstore::store_api::ObjectInfo { ..Default::default() },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(DeleteBucketOutput {})),
            version_id: String::new(),
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    /// Delete an object
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let DeleteObjectInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let metadata = extract_metadata(&req.headers);

        let opts: ObjectOptions = del_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

        let version_id = opts.version_id.as_ref().map(|v| Uuid::parse_str(v).ok()).unwrap_or_default();
        let dobj = ObjectToDelete {
            object_name: key.clone(),
            version_id,
        };

        let objects: Vec<ObjectToDelete> = vec![dobj];

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let (dobjs, _errs) = store.delete_objects(&bucket, objects, opts).await.map_err(ApiError::from)?;

        // TODO: let errors;

        let (delete_marker, version_id) = {
            if let Some((a, b)) = dobjs
                .iter()
                .map(|v| {
                    let delete_marker = { if v.delete_marker { Some(true) } else { None } };

                    let version_id = v.version_id.clone();

                    (delete_marker, version_id)
                })
                .next()
            {
                (a, b)
            } else {
                (None, None)
            }
        };
        let del_version_id = version_id.as_ref().map(|v| v.to_string()).unwrap_or_default();
        let output = DeleteObjectOutput {
            delete_marker,
            version_id,
            ..Default::default()
        };

        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectRemovedDelete,
            bucket_name: bucket.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo {
                name: key,
                bucket,
                ..Default::default()
            },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(DeleteBucketOutput {})),
            version_id: del_version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    /// Delete multiple objects
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // info!("delete_objects args {:?}", req.input);

        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        let objects: Vec<ObjectToDelete> = delete
            .objects
            .iter()
            .map(|v| {
                let version_id = v.version_id.as_ref().map(|v| Uuid::parse_str(v).ok()).unwrap_or_default();
                ObjectToDelete {
                    object_name: v.key.clone(),
                    version_id,
                }
            })
            .collect();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let metadata = extract_metadata(&req.headers);

        let opts: ObjectOptions = del_opts(&bucket, "", None, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

        let (dobjs, errs) = store.delete_objects(&bucket, objects, opts).await.map_err(ApiError::from)?;

        let deleted = dobjs
            .iter()
            .map(|v| DeletedObject {
                delete_marker: { if v.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: v.delete_marker_version_id.clone(),
                key: Some(v.object_name.clone()),
                version_id: v.version_id.clone(),
            })
            .collect();

        // TODO: let errors;
        for err in errs.iter().flatten() {
            warn!("delete_objects err  {:?}", err);
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            // errors,
            ..Default::default()
        };
        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            for dobj in dobjs {
                let version_id = match dobj.version_id {
                    None => String::new(),
                    Some(v) => v.to_string(),
                };
                let mut event_name = EventName::ObjectRemovedDelete;
                if dobj.delete_marker {
                    event_name = EventName::ObjectRemovedDeleteMarkerCreated;
                }

                let event_args = rustfs_notify::event::EventArgs {
                    event_name,
                    bucket_name: bucket.clone(),
                    object: rustfs_ecstore::store_api::ObjectInfo {
                        name: dobj.object_name,
                        bucket: bucket.clone(),
                        ..Default::default()
                    },
                    req_params: rustfs_utils::extract_req_params_header(&req.headers),
                    resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(DeleteObjectsOutput {
                        ..Default::default()
                    })),
                    version_id,
                    host: rustfs_utils::get_request_host(&req.headers),
                    user_agent: rustfs_utils::get_request_user_agent(&req.headers),
                };
                rustfs_notify::global::notifier_instance().notify(event_args).await;
            }
        });

        Ok(S3Response::new(output))
    }

    /// Get bucket location
    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // mc get  1
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(region) = rustfs_ecstore::global::get_global_region() {
            return Ok(S3Response::new(GetBucketLocationOutput {
                location_constraint: Some(BucketLocationConstraint::from(region)),
            }));
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    /// Get bucket notification
    #[tracing::instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        // mc get 3

        let GetObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            ..
        } = req.input.clone();

        // TODO: getObjectInArchiveFileHandler object = xxx.zip/xxx/xxx.xxx

        // let range = HTTPRangeSpec::nil();

        let h = HeaderMap::new();

        let part_number = part_number.map(|v| v as usize);

        if let Some(part_num) = part_number {
            if part_num == 0 {
                return Err(s3_error!(InvalidArgument, "part_numer invalid"));
            }
        }

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let reader = store
            .get_object_reader(bucket.as_str(), key.as_str(), rs, h, &opts)
            .await
            .map_err(ApiError::from)?;

        let info = reader.object_info;
        let event_info = info.clone();
        let content_type = {
            if let Some(content_type) = info.content_type {
                match ContentType::from_str(&content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("parse content-type err {} {:?}", &content_type, err);
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        let body = Some(StreamingBlob::wrap(bytes_stream(
            ReaderStream::with_capacity(reader.stream, DEFAULT_READ_BUFFER_SIZE),
            info.size as usize,
        )));

        let output = GetObjectOutput {
            body,
            content_length: Some(info.size as i64),
            last_modified,
            content_type,
            ..Default::default()
        };

        let version_id = match req.input.version_id {
            None => String::new(),
            Some(v) => v.to_string(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectAccessedGet,
            bucket_name: bucket.clone(),
            object: event_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(GetObjectOutput { ..Default::default() })),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        // mc cp step 2 GetBucketInfo

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        // mc get 2
        let HeadObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            ..
        } = req.input.clone();

        let part_number = part_number.map(|v| v as usize);

        if let Some(part_num) = part_number {
            if part_num == 0 {
                return Err(s3_error!(InvalidArgument, "part_numer invalid"));
            }
        }

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let info = store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        // warn!("head_object info {:?}", &info);
        let event_info = info.clone();
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("parse content-type err {} {:?}", &content_type, err);
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        // TODO: range download

        let content_length = info.get_actual_size().map_err(ApiError::from)?;

        let metadata = info.user_defined;

        let output = HeadObjectOutput {
            content_length: Some(content_length),
            content_type,
            last_modified,
            e_tag: info.etag,
            metadata: Some(metadata),
            version_id: info.version_id.map(|v| v.to_string()),
            // metadata: object_metadata,
            ..Default::default()
        };

        let version_id = match req.input.version_id {
            None => String::new(),
            Some(v) => v.to_string(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectAccessedGet,
            bucket_name: bucket,
            object: event_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // mc ls

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

        let mut req = req;

        if authorize_request(&mut req, Action::S3Action(S3Action::ListAllMyBucketsAction))
            .await
            .is_err()
        {
            bucket_infos.retain(|info| {
                let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");

                req_info.bucket = Some(info.name.clone());

                futures::executor::block_on(async {
                    authorize_request(&mut req, Action::S3Action(S3Action::ListBucketAction))
                        .await
                        .is_ok()
                        || authorize_request(&mut req, Action::S3Action(S3Action::GetBucketLocationAction))
                            .await
                            .is_ok()
                })
            });
        }

        let buckets: Vec<Bucket> = bucket_infos
            .iter()
            .map(|v| Bucket {
                creation_date: v.created.map(Timestamp::from),
                name: Some(v.name.clone()),
                ..Default::default()
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(RUSTFS_OWNER.to_owned()),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        // warn!("list_objects_v2 req {:?}", &req.input);
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = req.input;

        let prefix = prefix.unwrap_or_default();
        let max_keys = max_keys.unwrap_or(1000);

        let delimiter = delimiter.filter(|v| !v.is_empty());
        let continuation_token = continuation_token.filter(|v| !v.is_empty());
        let start_after = start_after.filter(|v| !v.is_empty());

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &prefix,
                continuation_token,
                delimiter.clone(),
                max_keys,
                fetch_owner.unwrap_or_default(),
                start_after,
            )
            .await
            .map_err(ApiError::from)?;

        // warn!("object_infos objects {:?}", object_infos.objects);

        let objects: Vec<Object> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty())
            .map(|v| {
                let mut obj = Object {
                    key: Some(v.name.to_owned()),
                    last_modified: v.mod_time.map(Timestamp::from),
                    size: Some(v.get_actual_size().unwrap_or_default()),
                    e_tag: v.etag.clone(),
                    ..Default::default()
                };

                if fetch_owner.is_some_and(|v| v) {
                    obj.owner = Some(Owner {
                        display_name: Some("rustfs".to_owned()),
                        id: Some("v0.1".to_owned()),
                    });
                }
                obj
            })
            .collect();

        let key_count = objects.len() as i32;

        let common_prefixes = object_infos
            .prefixes
            .into_iter()
            .map(|v| CommonPrefix { prefix: Some(v) })
            .collect();

        let output = ListObjectsV2Output {
            is_truncated: Some(object_infos.is_truncated),
            continuation_token: object_infos.continuation_token,
            next_continuation_token: object_infos.next_continuation_token,
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            delimiter,
            name: Some(bucket),
            prefix: Some(prefix),
            common_prefixes: Some(common_prefixes),
            ..Default::default()
        };

        // let output = ListObjectsV2Output { ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn list_object_versions(
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

        let prefix = prefix.unwrap_or_default();
        let max_keys = max_keys.unwrap_or(1000);

        let key_marker = key_marker.filter(|v| !v.is_empty());
        let version_id_marker = version_id_marker.filter(|v| !v.is_empty());
        let delimiter = delimiter.filter(|v| !v.is_empty());

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let object_infos = store
            .list_object_versions(&bucket, &prefix, key_marker, version_id_marker, delimiter.clone(), max_keys)
            .await
            .map_err(ApiError::from)?;

        let objects: Vec<ObjectVersion> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty())
            .map(|v| {
                ObjectVersion {
                    key: Some(v.name.to_owned()),
                    last_modified: v.mod_time.map(Timestamp::from),
                    size: Some(v.size),
                    version_id: v.version_id.map(|v| v.to_string()),
                    is_latest: Some(v.is_latest),
                    e_tag: v.etag.clone(),
                    ..Default::default() // TODO: another fields
                }
            })
            .collect();

        let key_count = objects.len() as i32;

        let common_prefixes = object_infos
            .prefixes
            .into_iter()
            .map(|v| CommonPrefix { prefix: Some(v) })
            .collect();

        let output = ListObjectVersionsOutput {
            // is_truncated: Some(object_infos.is_truncated),
            max_keys: Some(key_count),
            delimiter,
            name: Some(bucket),
            prefix: Some(prefix),
            common_prefixes: Some(common_prefixes),
            versions: Some(objects),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    // #[tracing::instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        if req
            .headers
            .get("X-Amz-Meta-Snowball-Auto-Extract")
            .is_some_and(|v| v.to_str().unwrap_or_default() == "true")
        {
            return self.put_object_extract(req).await;
        }

        let input = req.input;

        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }
        let event_version_id = input.version_id.as_ref().map(|v| v.to_string()).unwrap_or_default();
        let PutObjectInput {
            body,
            bucket,
            key,
            content_length,
            tagging,
            metadata,
            version_id,
            ..
        } = input;

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let mut size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        let body = StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string()))));

        // let body = Box::new(StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))));

        // let mut reader = PutObjReader::new(body, content_length as usize);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut metadata = metadata.unwrap_or_default();

        extract_metadata_from_mime(&req.headers, &mut metadata);

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let actual_size = size;

        if is_compressible(&req.headers, &key) && size > MIN_COMPRESSIBLE_SIZE as i64 {
            metadata.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                CompressionAlgorithm::default().to_string(),
            );
            metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

            let hrd = HashReader::new(reader, size as i64, size as i64, None, false).map_err(ApiError::from)?;

            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            size = -1;
        }

        // TODO: md5 check
        let reader = HashReader::new(reader, size, actual_size, None, false).map_err(ApiError::from)?;

        let mut reader = PutObjReader::new(reader);

        let mt = metadata.clone();
        let mt2 = metadata.clone();

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id, &req.headers, mt)
            .await
            .map_err(ApiError::from)?;

        let repoptions =
            get_must_replicate_options(&mt2, "", ReplicationStatusType::Unknown, ReplicationType::ObjectReplicationType, &opts);

        let dsc = must_replicate(&bucket, &key, &repoptions).await;
        // warn!("dsc {}", &dsc.replicate_any().clone());
        if dsc.replicate_any() {
            let k = format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp");
            let now: DateTime<Utc> = Utc::now();
            let formatted_time = now.to_rfc3339();
            opts.user_defined.insert(k, formatted_time);
            let k = format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-status");
            opts.user_defined.insert(k, dsc.pending_status());
        }

        let obj_info = store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;
        let event_info = obj_info.clone();
        let e_tag = obj_info.etag.clone();

        let repoptions =
            get_must_replicate_options(&mt2, "", ReplicationStatusType::Unknown, ReplicationType::ObjectReplicationType, &opts);

        let dsc = must_replicate(&bucket, &key, &repoptions).await;

        if dsc.replicate_any() {
            let objectlayer = new_object_layer_fn();
            schedule_replication(obj_info, objectlayer.unwrap(), dsc, 1).await;
        }

        let output = PutObjectOutput {
            e_tag,
            ..Default::default()
        };

        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedPut,
            bucket_name: bucket,
            object: event_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id: event_version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput {
            bucket,
            key,
            tagging,
            version_id,
            ..
        } = req.input.clone();

        // mc cp step 3

        // debug!("create_multipart_upload meta {:?}", &metadata);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut metadata = extract_metadata(&req.headers);

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        if is_compressible(&req.headers, &key) {
            metadata.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                CompressionAlgorithm::default().to_string(),
            );
        }

        let opts: ObjectOptions = put_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

        let MultipartUploadResult { upload_id, .. } = store
            .new_multipart_upload(&bucket, &key, &opts)
            .await
            .map_err(ApiError::from)?;
        let object_name = key.clone();
        let bucket_name = bucket.clone();
        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        };

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedCompleteMultipartUpload,
            bucket_name: bucket_name.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo {
                name: object_name,
                bucket: bucket_name,
                ..Default::default()
            },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            bucket,
            key,
            upload_id,
            part_number,
            content_length,
            // content_md5,
            ..
        } = req.input;

        let part_id = part_number as usize;

        // let upload_id =

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let mut size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        let body = StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string()))));

        // mc cp step 4

        let opts = ObjectOptions::default();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let fi = store
            .get_multipart_info(&bucket, &key, &upload_id, &opts)
            .await
            .map_err(ApiError::from)?;

        let is_compressible = fi
            .user_defined
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}compression").as_str());

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let actual_size = size;

        if is_compressible {
            let hrd = HashReader::new(reader, size, actual_size, None, false).map_err(ApiError::from)?;

            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            size = -1;
        }

        // TODO: md5 check
        let reader = HashReader::new(reader, size, actual_size, None, false).map_err(ApiError::from)?;

        let mut reader = PutObjReader::new(reader);

        let info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;

        let output = UploadPartOutput {
            e_tag: info.etag,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let _input = req.input;

        let _output = UploadPartCopyOutput { ..Default::default() };

        unimplemented!("upload_part_copy");
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket, key, upload_id, ..
        } = req.input;

        let output = ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            ..
        } = req.input;

        // error!("complete_multipart_upload {:?}", multipart_upload);
        // mc cp step 5

        let Some(multipart_upload) = multipart_upload else { return Err(s3_error!(InvalidPart)) };

        let opts = &ObjectOptions::default();

        let mut uploaded_parts = Vec::new();

        for part in multipart_upload.parts.into_iter().flatten() {
            uploaded_parts.push(CompletePart::from(part));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let obj_info = store
            .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, opts)
            .await
            .map_err(ApiError::from)?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone(),
            location: Some("us-east-1".to_string()),
            ..Default::default()
        };

        let mt2 = HashMap::new();
        let repoptions =
            get_must_replicate_options(&mt2, "", ReplicationStatusType::Unknown, ReplicationType::ObjectReplicationType, opts);

        let dsc = must_replicate(&bucket, &key, &repoptions).await;

        if dsc.replicate_any() {
            warn!("need multipart replication");
            let objectlayer = new_object_layer_fn();
            schedule_replication(obj_info, objectlayer.unwrap(), dsc, 1).await;
        }
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket, key, upload_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts = &ObjectOptions::default();

        store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), opts)
            .await
            .map_err(ApiError::from)?;
        Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let bucket = req.input.bucket.clone();
        // check bucket exists.
        let _bucket = self
            .head_bucket(req.map_input(|input| HeadBucketInput {
                bucket: input.bucket,
                expected_bucket_owner: None,
            }))
            .await?;

        let Tagging { tag_set } = match metadata_sys::get_tagging_config(&bucket).await {
            Ok((tags, _)) => tags,
            Err(err) => {
                warn!("get_tagging_config err {:?}", &err);
                // TODO: check not found
                Tagging::default()
            }
        };

        Ok(S3Response::new(GetBucketTaggingOutput { tag_set }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let PutBucketTaggingInput { bucket, tagging, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = try_!(serialize(&tagging));

        metadata_sys::update(&bucket, BUCKET_TAGGING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(Default::default()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let DeleteBucketTaggingInput { bucket, .. } = req.input;

        metadata_sys::delete(&bucket, BUCKET_TAGGING_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(DeleteBucketTaggingOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let tags = encode_tags(tagging.tag_set);

        // TODO: getOpts
        // TODO: Replicate

        store
            .put_object_tags(&bucket, &object, &tags, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedPutTagging,
            bucket_name: bucket.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo {
                name: object.clone(),
                bucket,
                ..Default::default()
            },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(PutObjectTaggingOutput { version_id: None })),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(PutObjectTaggingOutput { version_id: None }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let GetObjectTaggingInput { bucket, key: object, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // TODO: version
        let tags = store
            .get_object_tags(&bucket, &object, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let tag_set = decode_tags(tags.as_str());

        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: None,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let DeleteObjectTaggingInput { bucket, key: object, .. } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // TODO: Replicate
        // TODO: version
        store
            .delete_object_tags(&bucket, &object, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => Uuid::new_v4().to_string(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedDeleteTagging,
            bucket_name: bucket.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo {
                name: object.clone(),
                bucket,
                ..Default::default()
            },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(DeleteObjectTaggingOutput { version_id: None })),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(DeleteObjectTaggingOutput { version_id: None }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
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

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let PutBucketVersioningInput {
            bucket,
            versioning_configuration,
            ..
        } = req.input;

        // TODO: check other sys
        // check site replication enable
        // check bucket object lock enable
        // check replication suspended

        let data = try_!(serialize(&versioning_configuration));

        metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // TODO: globalSiteReplicationSys.BucketMetaHook

        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }

    async fn get_bucket_policy_status(
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

        let conditions = get_condition_values(&req.headers, &auth::Credentials::default());

        let read_olny = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::ListBucketAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let write_olny = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::PutObjectAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let is_public = read_olny && write_olny;

        let output = GetBucketPolicyStatusOutput {
            policy_status: Some(PolicyStatus {
                is_public: Some(is_public),
            }),
        };

        Ok(S3Response::new(output))
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let GetBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let cfg = match PolicySys::get(&bucket).await {
            Ok(res) => res,
            Err(err) => {
                if StorageError::ConfigNotFound == err {
                    return Err(s3_error!(NoSuchBucketPolicy));
                }
                return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
            }
        };

        let policys = try_!(serde_json::to_string(&cfg));

        Ok(S3Response::new(GetBucketPolicyOutput { policy: Some(policys) }))
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // warn!("input policy {}", &policy);

        let cfg: BucketPolicy =
            serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?;

        if let Err(err) = cfg.is_valid() {
            warn!("put_bucket_policy err input {:?}, {:?}", &policy, err);
            return Err(s3_error!(InvalidPolicyDocument));
        }

        let data = serde_json::to_vec(&cfg).map_err(|e| s3_error!(InternalError, "parse policy failed {:?}", e))?;

        metadata_sys::update(&bucket, BUCKET_POLICY_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
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

        Ok(S3Response::new(DeleteBucketPolicyOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
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
            Ok((cfg, _)) => Some(cfg.rules),
            Err(_err) => {
                // if BucketMetadataError::BucketLifecycleNotFound.is(&err) {
                //     return Err(s3_error!(NoSuchLifecycleConfiguration));
                // }
                // warn!("get_lifecycle_config err {:?}", err);
                None
            }
        };

        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules,
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;

        let lr_retention = false;
        /*let rcfg = metadata_sys::get_object_lock_config(&bucket).await;
        if let Ok(rcfg) = rcfg {
            if let Some(rule) = rcfg.0.rule {
                if let Some(retention) = rule.default_retention {
                    if let Some(mode) = retention.mode {
                        //if mode == ObjectLockRetentionMode::from_static(ObjectLockRetentionMode::GOVERNANCE) {
                            lr_retention = true;
                        //}
                    }
                }
            }
        }*/

        //info!("lifecycle_configuration: {:?}", &lifecycle_configuration);

        let Some(input_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };

        if let Err(err) = input_cfg.validate(lr_retention).await {
            //return Err(S3Error::with_message(S3ErrorCode::Custom("BucketLockValidateFailed".into()), "bucket lock validate failed."));
            return Err(S3Error::with_message(S3ErrorCode::Custom("ValidateFailed".into()), err.to_string()));
        }

        if let Err(err) = validate_transition_tier(&input_cfg).await {
            //warn!("lifecycle_configuration add failed, err: {:?}", err);
            return Err(S3Error::with_message(S3ErrorCode::Custom("CustomError".into()), err.to_string()));
        }

        let data = try_!(serialize(&input_cfg));
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
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

        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    async fn get_bucket_encryption(
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
                // if BucketMetadataError::BucketLifecycleNotFound.is(&err) {
                //     return Err(s3_error!(ErrNoSuchBucketSSEConfig));
                // }
                warn!("get_sse_config err {:?}", err);
                None
            }
        };

        Ok(S3Response::new(GetBucketEncryptionOutput {
            server_side_encryption_configuration,
        }))
    }

    async fn put_bucket_encryption(
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

        // TODO: check kms

        let data = try_!(serialize(&server_side_encryption_configuration));
        metadata_sys::update(&bucket, BUCKET_SSECONFIG, data)
            .await
            .map_err(ApiError::from)?;
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
    }

    async fn delete_bucket_encryption(
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

        Ok(S3Response::new(DeleteBucketEncryptionOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        let GetObjectLockConfigurationInput { bucket, .. } = req.input;

        let object_lock_configuration = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                debug!("get_object_lock_config err {:?}", err);
                None
            }
        };

        // warn!("object_lock_configuration {:?}", &object_lock_configuration);

        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration,
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = try_!(serialize(&input_cfg));

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    async fn get_bucket_replication(
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

        let rcfg = match metadata_sys::get_replication_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
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

        if rcfg.is_none() {
            return Err(S3Error::with_message(
                S3ErrorCode::ReplicationConfigurationNotFoundError,
                "replication not found".to_string(),
            ));
        }

        // Ok(S3Response::new(GetBucketReplicationOutput {
        //     replication_configuration: rcfg,
        // }))

        if rcfg.is_some() {
            Ok(S3Response::new(GetBucketReplicationOutput {
                replication_configuration: rcfg,
            }))
        } else {
            let rep = ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            };
            Ok(S3Response::new(GetBucketReplicationOutput {
                replication_configuration: Some(rep),
            }))
        }
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let PutBucketReplicationInput {
            bucket,
            replication_configuration,
            ..
        } = req.input;
        warn!("put bucket replication");

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // TODO: check enable, versioning enable
        let data = try_!(serialize(&replication_configuration));

        metadata_sys::update(&bucket, BUCKET_REPLICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    async fn delete_bucket_replication(
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
        metadata_sys::delete(&bucket, BUCKET_REPLICATION_CONFIG)
            .await
            .map_err(ApiError::from)?;

        // TODO: remove targets
        error!("delete bucket");

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    async fn get_bucket_notification_configuration(
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

        let has_notification_config = match metadata_sys::get_notification_config(&bucket).await {
            Ok(cfg) => cfg,
            Err(err) => {
                warn!("get_notification_config err {:?}", err);
                None
            }
        };

        // TODO: valid target list

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

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let PutBucketNotificationConfigurationInput {
            bucket,
            notification_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = try_!(serialize(&notification_configuration));

        metadata_sys::update(&bucket, BUCKET_NOTIFICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // TODO: event notice add rule

        Ok(S3Response::new(PutBucketNotificationConfigurationOutput::default()))
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let grants = vec![Grant {
            grantee: Some(Grantee {
                type_: Type::from_static(Type::CANONICAL_USER),
                display_name: None,
                email_address: None,
                id: None,
                uri: None,
            }),
            permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
        }];

        Ok(S3Response::new(GetBucketAclOutput {
            grants: Some(grants),
            owner: Some(RUSTFS_OWNER.to_owned()),
        }))
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let PutBucketAclInput {
            bucket,
            acl,
            access_control_policy,
            ..
        } = req.input;

        // TODO:checkRequestAuthType

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(canned_acl) = acl {
            if canned_acl.as_str() != BucketCannedACL::PRIVATE {
                return Err(s3_error!(NotImplemented));
            }
        } else {
            let is_full_control = access_control_policy.is_some_and(|v| {
                v.grants.is_some_and(|gs| {
                    //
                    !gs.is_empty()
                        && gs.first().is_some_and(|g| {
                            g.to_owned()
                                .permission
                                .is_some_and(|p| p.as_str() == Permission::FULL_CONTROL)
                        })
                })
            });

            if !is_full_control {
                return Err(s3_error!(NotImplemented));
            }
        }
        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        let GetObjectAclInput { bucket, key, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        let grants = vec![Grant {
            grantee: Some(Grantee {
                type_: Type::from_static(Type::CANONICAL_USER),
                display_name: None,
                email_address: None,
                id: None,
                uri: None,
            }),
            permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
        }];

        Ok(S3Response::new(GetObjectAclOutput {
            grants: Some(grants),
            owner: Some(RUSTFS_OWNER.to_owned()),
            ..Default::default()
        }))
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let GetObjectAttributesInput { bucket, key, .. } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        if let Err(e) = store
            .get_object_reader(&bucket, &key, None, HeaderMap::new(), &ObjectOptions::default())
            .await
        {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        let output = GetObjectAttributesOutput {
            delete_marker: None,
            object_parts: None,
            ..Default::default()
        };
        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectAccessedAttributes,
            bucket_name: bucket.clone(),
            object: rustfs_ecstore::store_api::ObjectInfo {
                name: key.clone(),
                bucket,
                ..Default::default()
            },
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        let PutObjectAclInput {
            bucket,
            key,
            acl,
            access_control_policy,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        if let Some(canned_acl) = acl {
            if canned_acl.as_str() != BucketCannedACL::PRIVATE {
                return Err(s3_error!(NotImplemented));
            }
        } else {
            let is_full_control = access_control_policy.is_some_and(|v| {
                v.grants.is_some_and(|gs| {
                    //
                    !gs.is_empty()
                        && gs.first().is_some_and(|g| {
                            g.to_owned()
                                .permission
                                .is_some_and(|p| p.as_str() == Permission::FULL_CONTROL)
                        })
                })
            });

            if !is_full_control {
                return Err(s3_error!(NotImplemented));
            }
        }
        Ok(S3Response::new(PutObjectAclOutput::default()))
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        info!("handle select_object_content");

        let input = Arc::new(req.input);
        info!("{:?}", input);

        let db = make_rustfsms(input.clone(), false).await.map_err(|e| {
            error!("make db failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;
        let query = Query::new(Context { input: input.clone() }, input.request.expression.clone());
        let result = db
            .execute(&query)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?;

        let results = result.result().chunk_result().await.unwrap().to_vec();

        let mut buffer = Vec::new();
        if input.request.output_serialization.csv.is_some() {
            let mut csv_writer = CsvWriterBuilder::new().with_header(false).build(&mut buffer);
            for batch in results {
                csv_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "cann't encode output to csv. e: {}", e.to_string()))?;
            }
        } else if input.request.output_serialization.json.is_some() {
            let mut json_writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut buffer);
            for batch in results {
                json_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "cann't encode output to json. e: {}", e.to_string()))?;
            }
            json_writer
                .finish()
                .map_err(|e| s3_error!(InternalError, "writer output into json error, e: {}", e.to_string()))?;
        } else {
            return Err(s3_error!(InvalidArgument, "unknow output format"));
        }

        let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(2);
        let stream = ReceiverStream::new(rx);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
                .await;
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Records(RecordsEvent {
                    payload: Some(Bytes::from(buffer)),
                })))
                .await;
            let _ = tx.send(Ok(SelectObjectContentEvent::End(EndEvent::default()))).await;

            drop(tx);
        });

        Ok(S3Response::new(SelectObjectContentOutput {
            payload: Some(SelectObjectContentEventStream::new(stream)),
        }))
    }
    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // check object lock
        let _ = metadata_sys::get_object_lock_config(&bucket).await.map_err(ApiError::from)?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let legal_hold = object_info
            .user_defined
            .get("x-amz-object-lock-legal-hold")
            .map(|v| v.as_str().to_string());

        let status = if let Some(v) = legal_hold {
            v
        } else {
            ObjectLockLegalHoldStatus::OFF.to_string()
        };

        let output = GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from(status)),
            }),
        };

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => Uuid::new_v4().to_string(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectAccessedGetLegalHold,
            bucket_name: bucket.clone(),
            object: object_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let PutObjectLegalHoldInput {
            bucket,
            key,
            legal_hold,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // check object lock
        let _ = metadata_sys::get_object_lock_config(&bucket).await.map_err(ApiError::from)?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let mut eval_metadata = HashMap::new();
        let legal_hold = legal_hold
            .map(|v| v.status.map(|v| v.as_str().to_string()))
            .unwrap_or_default()
            .unwrap_or("OFF".to_string());

        let now = OffsetDateTime::now_utc();
        eval_metadata.insert("x-amz-object-lock-legal-hold".to_string(), legal_hold);
        eval_metadata.insert(
            format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-legalhold-timestamp"),
            format!("{}.{:09}Z", now.format(&Rfc3339).unwrap(), now.nanosecond()),
        );

        let popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        let info = store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };
        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedPutLegalHold,
            bucket_name: bucket.clone(),
            object: info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // check object lock
        let _ = metadata_sys::get_object_lock_config(&bucket).await.map_err(ApiError::from)?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let mode = object_info
            .user_defined
            .get("x-amz-object-lock-mode")
            .map(|v| ObjectLockRetentionMode::from(v.as_str().to_string()));

        let retain_until_date = object_info
            .user_defined
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| OffsetDateTime::parse(v.as_str(), &Rfc3339).ok())
            .map(Timestamp::from);

        let output = GetObjectRetentionOutput {
            retention: Some(ObjectLockRetention { mode, retain_until_date }),
        };
        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectAccessedGetRetention,
            bucket_name: bucket.clone(),
            object: object_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let PutObjectRetentionInput {
            bucket,
            key,
            retention,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // check object lock
        let _ = metadata_sys::get_object_lock_config(&bucket).await.map_err(ApiError::from)?;

        // TODO: check allow

        let mut eval_metadata = HashMap::new();

        if let Some(v) = retention {
            let mode = v.mode.map(|v| v.as_str().to_string()).unwrap_or_default();
            let retain_until_date = v
                .retain_until_date
                .map(|v| OffsetDateTime::from(v).format(&Rfc3339).unwrap())
                .unwrap_or_default();
            let now = OffsetDateTime::now_utc();
            eval_metadata.insert("x-amz-object-lock-mode".to_string(), mode);
            eval_metadata.insert("x-amz-object-lock-retain-until-date".to_string(), retain_until_date);
            eval_metadata.insert(
                format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "objectlock-retention-timestamp"),
                format!("{}.{:09}Z", now.format(&Rfc3339).unwrap(), now.nanosecond()),
            );
        }

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.eval_metadata = Some(eval_metadata);

        let object_info = store.put_object_metadata(&bucket, &key, &opts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };

        let version_id = match req.input.version_id {
            Some(v) => v.to_string(),
            None => Uuid::new_v4().to_string(),
        };
        let event_args = rustfs_notify::event::EventArgs {
            event_name: EventName::ObjectCreatedPutRetention,
            bucket_name: bucket.clone(),
            object: object_info,
            req_params: rustfs_utils::extract_req_params_header(&req.headers),
            resp_elements: rustfs_utils::extract_resp_elements(&S3Response::new(output.clone())),
            version_id,
            host: rustfs_utils::get_request_host(&req.headers),
            user_agent: rustfs_utils::get_request_user_agent(&req.headers),
        };

        // Asynchronous call will not block the response of the current request
        tokio::spawn(async move {
            rustfs_notify::global::notifier_instance().notify(event_args).await;
        });

        Ok(S3Response::new(output))
    }
}
