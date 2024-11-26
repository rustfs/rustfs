use bytes::Bytes;
use common::error::Result;
use ecstore::bucket::error::BucketMetadataError;
use ecstore::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
use ecstore::bucket::metadata::BUCKET_NOTIFICATION_CONFIG;
use ecstore::bucket::metadata::BUCKET_POLICY_CONFIG;
use ecstore::bucket::metadata::BUCKET_REPLICATION_CONFIG;
use ecstore::bucket::metadata::BUCKET_SSECONFIG;
use ecstore::bucket::metadata::BUCKET_TAGGING_CONFIG;
use ecstore::bucket::metadata::BUCKET_VERSIONING_CONFIG;
use ecstore::bucket::metadata::OBJECT_LOCK_CONFIG;
use ecstore::bucket::metadata_sys;
use ecstore::bucket::policy::bucket_policy::BucketPolicy;
use ecstore::bucket::policy_sys::PolicySys;
use ecstore::bucket::tagging::decode_tags;
use ecstore::bucket::tagging::encode_tags;
use ecstore::bucket::versioning_sys::BucketVersioningSys;
use ecstore::new_object_layer_fn;
use ecstore::options::extract_metadata;
use ecstore::options::put_opts;
use ecstore::store_api::BucketOptions;
use ecstore::store_api::CompletePart;
use ecstore::store_api::DeleteBucketOptions;
use ecstore::store_api::HTTPRangeSpec;
use ecstore::store_api::MakeBucketOptions;
use ecstore::store_api::MultipartUploadResult;
use ecstore::store_api::ObjectIO;
use ecstore::store_api::ObjectOptions;
use ecstore::store_api::ObjectToDelete;
use ecstore::store_api::PutObjReader;
use ecstore::store_api::StorageAPI;
use ecstore::utils::xml;
use ecstore::xhttp;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use http::HeaderMap;
use lazy_static::lazy_static;
use log::warn;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use std::fmt::Debug;
use std::str::FromStr;
use tracing::error;
use tracing::info;
use transform_stream::AsyncTryStream;
use uuid::Uuid;

use crate::storage::error::to_s3_error;

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

lazy_static! {
    static ref RUSTFS_OWNER: Owner = Owner {
        display_name: Some("rustfs".to_owned()),
        id: Some("c19050dbcee97fda828689dda99097a6321af2248fa760517237346e5d9c8a66".to_owned()),
    };
}

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

impl FS {
    pub fn new() -> Self {
        // let store: ECStore = ECStore::new(address, endpoint_pools).await?;
        Self {}
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
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
            .map_err(to_s3_error)?;

        let output = CreateBucketOutput::default();
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (_bucket, _key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { ref bucket, ref key, .. } => (bucket, key),
        };

        let output = CopyObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let input = req.input;
        // TODO: DeleteBucketInput 没有force参数？
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
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
            .map_err(to_s3_error)?;

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let DeleteObjectInput {
            bucket, key, version_id, ..
        } = req.input;

        let version_id = version_id
            .as_ref()
            .map(|v| match Uuid::parse_str(v) {
                Ok(id) => Some(id),
                Err(_) => None,
            })
            .unwrap_or_default();
        let dobj = ObjectToDelete {
            object_name: key,
            version_id,
        };

        let objects: Vec<ObjectToDelete> = vec![dobj];

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };
        let (dobjs, _errs) = store
            .delete_objects(&bucket, objects, ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;

        // TODO: let errors;

        let (delete_marker, version_id) = {
            if let Some((a, b)) = dobjs
                .iter()
                .map(|v| {
                    let delete_marker = {
                        if v.delete_marker {
                            Some(true)
                        } else {
                            None
                        }
                    };

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

        let output = DeleteObjectOutput {
            delete_marker,
            version_id,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // info!("delete_objects args {:?}", req.input);

        let DeleteObjectsInput { bucket, delete, .. } = req.input;

        let objects: Vec<ObjectToDelete> = delete
            .objects
            .iter()
            .map(|v| {
                let version_id = v
                    .version_id
                    .as_ref()
                    .map(|v| match Uuid::parse_str(v) {
                        Ok(id) => Some(id),
                        Err(_) => None,
                    })
                    .unwrap_or_default();
                ObjectToDelete {
                    object_name: v.key.clone(),
                    version_id,
                }
            })
            .collect();

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let (dobjs, _errs) = store
            .delete_objects(&bucket, objects, ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;
        // info!("delete_objects res {:?} {:?}", &dobjs, errs);

        let deleted = dobjs
            .iter()
            .map(|v| DeletedObject {
                delete_marker: {
                    if v.delete_marker {
                        Some(true)
                    } else {
                        None
                    }
                },
                delete_marker_version_id: v.delete_marker_version_id.clone(),
                key: Some(v.object_name.clone()),
                version_id: v.version_id.clone(),
            })
            .collect();

        // TODO: let errors;

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            // errors,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // mc get  1
        let input = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        // mc get 3

        let GetObjectInput { bucket, key, .. } = req.input;

        let range = HTTPRangeSpec::nil();

        let h = HeaderMap::new();
        let opts = &ObjectOptions::default();

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let reader = store
            .get_object_reader(bucket.as_str(), key.as_str(), range, h, opts)
            .await
            .map_err(to_s3_error)?;

        let info = reader.object_info;

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

        let output = GetObjectOutput {
            body: Some(reader.stream),
            content_length: Some(info.size as i64),
            last_modified,
            content_type,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;
        // mc cp step 2 GetBucketInfo

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        // mc get 2
        let HeadObjectInput { bucket, key, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let info = store
            .get_object_info(&bucket, &key, &ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        let output = HeadObjectOutput {
            content_length: Some(try_!(i64::try_from(info.size))),
            content_type,
            last_modified,
            // metadata: object_metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, _: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // mc ls

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(to_s3_error)?;

        let buckets: Vec<Bucket> = bucket_infos
            .iter()
            .map(|v| Bucket {
                creation_date: v.created.map(Timestamp::from),
                name: Some(v.name.clone()),
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
        // warn!("list_objects_v2 input {:?}", &req.input);

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
        let delimiter = delimiter.unwrap_or_default();

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &prefix,
                &continuation_token.unwrap_or_default(),
                &delimiter,
                max_keys.unwrap_or_default(),
                fetch_owner.unwrap_or_default(),
                &start_after.unwrap_or_default(),
            )
            .await
            .map_err(to_s3_error)?;

        // warn!("object_infos {:?}", object_infos);

        let objects: Vec<Object> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty())
            .map(|v| {
                let mut obj = Object {
                    key: Some(v.name.to_owned()),
                    last_modified: v.mod_time.map(Timestamp::from),
                    size: Some(v.size as i64),
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

        let output = ListObjectsV2Output {
            key_count: Some(key_count),
            max_keys: Some(key_count),
            contents: Some(objects),
            delimiter: Some(delimiter),
            name: Some(bucket),
            prefix: Some(prefix),
            ..Default::default()
        };

        // let output = ListObjectsV2Output { ..Default::default() };
        Ok(S3Response::new(output))
    }

    async fn list_object_versions(
        &self,
        _req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        Err(s3_error!(NotImplemented, "ListObjectVersions is not implemented yet"))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;

        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD", "REDUCED_REDUNDANCY"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            content_length,
            tagging,
            ..
        } = input;

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let content_length = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(xhttp::AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        let mut reader = PutObjReader::new(body, content_length as usize);

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let mut metadata = extract_metadata(&req.headers);
        if let Some(tags) = tagging {
            metadata.insert(xhttp::AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let opts: ObjectOptions = put_opts(&bucket, &key, None, &req.headers, Some(metadata))
            .await
            .map_err(to_s3_error)?;

        let obj_info = store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(to_s3_error)?;

        let e_tag = obj_info.etag;

        // store.put_object(bucket, object, data, opts);

        let output = PutObjectOutput {
            e_tag,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput {
            bucket, key, tagging, ..
        } = req.input;

        // mc cp step 3

        // debug!("create_multipart_upload meta {:?}", &metadata);

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let mut metadata = extract_metadata(&req.headers);

        if let Some(tags) = tagging {
            metadata.insert(xhttp::AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let opts: ObjectOptions = put_opts(&bucket, &key, None, &req.headers, Some(metadata))
            .await
            .map_err(to_s3_error)?;

        let MultipartUploadResult { upload_id, .. } =
            store.new_multipart_upload(&bucket, &key, &opts).await.map_err(to_s3_error)?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        };

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
        let content_length = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(xhttp::AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        // mc cp step 4
        let mut data = PutObjReader::new(body, content_length as usize);
        let opts = ObjectOptions::default();

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        // TODO: hash_reader

        let info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut data, &opts)
            .await
            .map_err(to_s3_error)?;

        let output = UploadPartOutput {
            e_tag: info.etag,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let _input = req.input;

        let output = UploadPartCopyOutput { ..Default::default() };

        Ok(S3Response::new(output))
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        store
            .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, opts)
            .await
            .map_err(to_s3_error)?;

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            ..Default::default()
        };
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        let opts = &ObjectOptions::default();

        store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), opts)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let GetBucketTaggingInput { bucket, .. } = req.input;
        // check bucket exists.
        let _bucket = self
            .head_bucket(S3Request::new(HeadBucketInput {
                bucket: bucket.clone(),
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let data = try_!(xml::serialize(&tagging));

        metadata_sys::update(&bucket, BUCKET_TAGGING_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

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
            .map_err(to_s3_error)?;

        Ok(S3Response::new(DeleteBucketTaggingOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        let tags = encode_tags(tagging.tag_set);

        // TODO: getOpts
        // TODO: Replicate

        store
            .put_object_tags(&bucket, &object, &tags, &ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutObjectTaggingOutput { version_id: None }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let GetObjectTaggingInput { bucket, key: object, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        // TODO: version
        let tags = store
            .get_object_tags(&bucket, &object, &ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;

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
        let DeleteObjectTaggingInput { bucket, key: object, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        // TODO: Replicate
        // TODO: version
        store
            .delete_object_tags(&bucket, &object, &ObjectOptions::default())
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(DeleteObjectTaggingOutput { version_id: None }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let GetBucketVersioningInput { bucket, .. } = req.input;
        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = match lock.as_ref() {
            Some(s) => s,
            None => return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string())),
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let VersioningConfiguration { status, .. } = BucketVersioningSys::get(&bucket).await.map_err(to_s3_error)?;

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

        let data = try_!(xml::serialize(&versioning_configuration));

        metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

        // TODO: globalSiteReplicationSys.BucketMetaHook

        Ok(S3Response::new(PutBucketVersioningOutput {}))
    }

    async fn get_bucket_policy_status(
        &self,
        _req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        Err(s3_error!(NotImplemented, "GetBucketPolicyStatus is not implemented yet"))
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let GetBucketPolicyInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let cfg = match PolicySys::get(&bucket).await {
            Ok(res) => res,
            Err(err) => {
                if BucketMetadataError::BucketPolicyNotFound.is(&err) {
                    return Err(s3_error!(NoSuchBucketPolicy));
                }
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)));
            }
        };

        let policys = try_!(serde_json::to_string(&cfg));

        Ok(S3Response::new(GetBucketPolicyOutput { policy: Some(policys) }))
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        // warn!("input policy {}", &policy);

        let cfg = BucketPolicy::unmarshal(policy.as_bytes()).map_err(to_s3_error)?;

        // warn!("parse policy {:?}", &cfg);

        if let Err(err) = cfg.validate(&bucket) {
            warn!("put_bucket_policy err input {:?}, {:?}", &policy, err);
            return Err(s3_error!(InvalidPolicyDocument));
        }

        let data = cfg.marshal_msg().map_err(to_s3_error)?;

        metadata_sys::update(&bucket, BUCKET_POLICY_CONFIG, data.into())
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let DeleteBucketPolicyInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        metadata_sys::delete(&bucket, BUCKET_POLICY_CONFIG)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(DeleteBucketPolicyOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let GetBucketLifecycleConfigurationInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        // warn!("lifecycle_configuration {:?}", &lifecycle_configuration);

        // TODO: objcetLock

        let Some(input_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };

        let data = try_!(xml::serialize(&input_cfg));
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let DeleteBucketLifecycleInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        metadata_sys::delete(&bucket, BUCKET_LIFECYCLE_CONFIG)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let GetBucketEncryptionInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        // TODO: check kms

        let data = try_!(xml::serialize(&server_side_encryption_configuration));
        metadata_sys::update(&bucket, BUCKET_SSECONFIG, data)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(PutBucketEncryptionOutput::default()))
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let DeleteBucketEncryptionInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;
        metadata_sys::delete(&bucket, BUCKET_SSECONFIG).await.map_err(to_s3_error)?;

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
                warn!("get_object_lock_config err {:?}", err);
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let data = try_!(xml::serialize(&input_cfg));

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let GetBucketReplicationInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let replication_configuration = match metadata_sys::get_replication_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                warn!("get_object_lock_config err {:?}", err);
                None
            }
        };

        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration,
        }))
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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        // TODO: check enable, versioning enable
        let data = try_!(xml::serialize(&replication_configuration));

        metadata_sys::update(&bucket, BUCKET_REPLICATION_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let DeleteBucketReplicationInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;
        metadata_sys::delete(&bucket, BUCKET_REPLICATION_CONFIG)
            .await
            .map_err(to_s3_error)?;

        // TODO: remove targets

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        let GetBucketNotificationConfigurationInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

        let data = try_!(xml::serialize(&notification_configuration));

        metadata_sys::update(&bucket, BUCKET_NOTIFICATION_CONFIG, data)
            .await
            .map_err(to_s3_error)?;

        // TODO: event notice add rule

        Ok(S3Response::new(PutBucketNotificationConfigurationOutput::default()))
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        let GetBucketAclInput { bucket, .. } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(to_s3_error)?;

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

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", e)));
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

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        let PutObjectAclInput {
            bucket,
            key,
            acl,
            access_control_policy,
            ..
        } = req.input;

        let layer = new_object_layer_fn();
        let lock = layer.read().await;
        let store = lock
            .as_ref()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init"))?;

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", e)));
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
}

#[allow(dead_code)]
pub fn bytes_stream<S, E>(stream: S, content_length: usize) -> impl Stream<Item = Result<Bytes, E>> + Send + 'static
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    AsyncTryStream::<Bytes, E, _>::new(|mut y| async move {
        pin_mut!(stream);
        let mut remaining: usize = content_length;
        while let Some(result) = stream.next().await {
            let mut bytes = result?;
            if bytes.len() > remaining {
                bytes.truncate(remaining);
            }
            remaining -= bytes.len();
            y.yield_ok(bytes).await;
        }
        Ok(())
    })
}
