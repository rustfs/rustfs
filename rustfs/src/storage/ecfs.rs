use bytes::Bytes;
use ecstore::disk::error::DiskError;
use ecstore::store_api::BucketOptions;
use ecstore::store_api::CompletePart;
use ecstore::store_api::HTTPRangeSpec;
use ecstore::store_api::MakeBucketOptions;
use ecstore::store_api::MultipartUploadResult;
use ecstore::store_api::ObjectOptions;
use ecstore::store_api::PutObjReader;
use ecstore::store_api::StorageAPI;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use http::HeaderMap;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};
use std::fmt::Debug;
use std::str::FromStr;
use transform_stream::AsyncTryStream;

use ecstore::error::Result;
use ecstore::store::ECStore;
use tracing::debug;

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

#[derive(Debug)]
pub struct FS {
    pub store: ECStore,
}

impl FS {
    pub async fn new(store: ECStore) -> Result<Self> {
        Ok(Self { store })
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
        let input = req.input;

        try_!(
            self.store
                .make_bucket(&input.bucket, &MakeBucketOptions { force_create: true })
                .await
        );

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

        try_!(self.store.delete_bucket(&input.bucket).await);

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let _input = req.input;

        let output = DeleteObjectOutput::default();
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let _input = req.input;

        let output = DeleteObjectsOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // mc get  1
        let input = req.input;

        if let Err(e) = self.store.get_bucket_info(&input.bucket, &BucketOptions {}).await {
            if DiskError::VolumeNotFound.is(&e) {
                return Err(s3_error!(NoSuchBucket));
            } else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", e)));
            }
        }

        let output = GetBucketLocationOutput::default();
        Ok(S3Response::new(output))
    }

    async fn get_object_lock_configuration(
        &self,
        _req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        // mc cp step 1
        let output = GetObjectLockConfigurationOutput::default();
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

        let reader = try_!(
            self.store
                .get_object_reader(bucket.as_str(), key.as_str(), range, h, opts)
                .await
        );

        let info = reader.object_info;

        let content_type = try_!(ContentType::from_str("application/x-msdownload"));
        let last_modified = info.mod_time.map(Timestamp::from);

        let output = GetObjectOutput {
            body: Some(reader.stream),
            content_length: Some(info.size as i64),
            last_modified,
            content_type: Some(content_type),
            ..Default::default()
        };

        debug!("get_object response {:?}", output);
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        if let Err(e) = self.store.get_bucket_info(&input.bucket, &BucketOptions {}).await {
            if DiskError::VolumeNotFound.is(&e) {
                return Err(s3_error!(NoSuchBucket));
            } else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", e)));
            }
        }
        // mc cp step 2 GetBucketInfo

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        // mc get 2
        let HeadObjectInput { bucket, key, .. } = req.input;

        let info = try_!(self.store.get_object_info(&bucket, &key, &ObjectOptions::default()).await);
        debug!("info {:?}", info);

        let content_type = try_!(ContentType::from_str("application/x-msdownload"));
        let last_modified = info.mod_time.map(Timestamp::from);

        let output = HeadObjectOutput {
            content_length: Some(try_!(i64::try_from(info.size))),
            content_type: Some(content_type),
            last_modified,
            // metadata: object_metadata,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, _: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // mc ls

        let bucket_infos = try_!(self.store.list_bucket(&BucketOptions {}).await);

        let buckets: Vec<Bucket> = bucket_infos
            .iter()
            .map(|v| Bucket {
                creation_date: v.created.map(Timestamp::from),
                name: Some(v.name.clone()),
            })
            .collect();

        let output = ListBucketsOutput {
            buckets: Some(buckets),
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

        let object_infos = try_!(
            self.store
                .list_objects_v2(
                    &bucket,
                    &prefix,
                    &continuation_token.unwrap_or_default(),
                    &delimiter,
                    max_keys.unwrap_or_default(),
                    fetch_owner.unwrap_or_default(),
                    &start_after.unwrap_or_default()
                )
                .await
        );

        // warn!("object_infos {:?}", object_infos);

        let objects: Vec<Object> = object_infos
            .objects
            .iter()
            .filter(|v| !v.name.is_empty())
            .map(|v| {
                let mut obj = Object::default();
                obj.key = Some(v.name.to_owned());
                obj.last_modified = v.mod_time.map(Timestamp::from);
                obj.size = Some(v.size as i64);

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
            metadata,
            content_length,
            ..
        } = input;

        debug!("put_object metadata {:?}", metadata);

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let Some(content_length) = content_length else { return Err(s3_error!(IncompleteBody)) };

        let reader = PutObjReader::new(body, content_length as usize);

        try_!(self.store.put_object(&bucket, &key, reader, &ObjectOptions::default()).await);

        // self.store.put_object(bucket, object, data, opts);

        let output = PutObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput {
            bucket, key, metadata, ..
        } = req.input;

        // mc cp step 3

        debug!("create_multipart_upload meta {:?}", &metadata);

        let MultipartUploadResult { upload_id, .. } = try_!(
            self.store
                .new_multipart_upload(&bucket, &key, &ObjectOptions::default())
                .await
        );

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
            ..
        } = req.input;

        let part_id = part_number as usize;

        // let upload_id =

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let content_length = content_length.ok_or_else(|| s3_error!(IncompleteBody))?;

        // mc cp step 4
        let data = PutObjReader::new(body, content_length as usize);
        let opts = ObjectOptions::default();

        try_!(
            self.store
                .put_object_part(&bucket, &key, &upload_id, part_id, data, &opts)
                .await
        );

        let output = UploadPartOutput { ..Default::default() };
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

        // mc cp step 5

        let Some(multipart_upload) = multipart_upload else { return Err(s3_error!(InvalidPart)) };

        let opts = &ObjectOptions::default();

        let mut uploaded_parts = Vec::new();

        for part in multipart_upload.parts.into_iter().flatten() {
            uploaded_parts.push(CompletePart::from(part));
        }

        try_!(
            self.store
                .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, opts)
                .await
        );

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

        let opts = &ObjectOptions::default();
        try_!(
            self.store
                .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), opts)
                .await
        );
        Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
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

// Consumes this body object to return a bytes stream.
// pub fn into_bytes_stream(mut body: StreamingBlob) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
//     futures_util::stream::poll_fn(move |ctx| loop {
//         match Pin::new(&mut body).poll_next(ctx) {
//             Poll::Ready(Some(Ok(data))) => return Poll::Ready(Some(Ok(data))),
//             Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(std::io::Error::new(std::io::ErrorKind::Other, err)))),
//             Poll::Ready(None) => return Poll::Ready(None),
//             Poll::Pending => return Poll::Pending,
//         }
//     })
// }
