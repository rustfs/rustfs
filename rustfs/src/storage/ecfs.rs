use std::fmt::Debug;

use ecstore::disk_api::DiskError;
use ecstore::store_api::BucketOptions;
use ecstore::store_api::MakeBucketOptions;
use ecstore::store_api::MultipartUploadResult;
use ecstore::store_api::ObjectOptions;
use ecstore::store_api::PutObjReader;
use ecstore::store_api::StorageAPI;
use s3s::dto::*;
use s3s::s3_error;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::S3;
use s3s::{S3Request, S3Response};

use anyhow::Result;
use ecstore::store::ECStore;
use tracing::error;

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
    pub async fn new(address: String, endpoints: Vec<String>) -> Result<Self> {
        let store: ECStore = ECStore::new(address, endpoints).await?;
        Ok(Self { store })
    }
}

#[async_trait::async_trait]
impl S3 for FS {
    #[tracing::instrument]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let input = req.input;

        try_!(
            self.store
                .make_bucket(&input.bucket, &MakeBucketOptions { force_create: true })
                .await
        );

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let (bucket, key) = match input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { ref bucket, ref key, .. } => (bucket, key),
        };

        let output = CopyObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let input = req.input;

        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;

        let output = DeleteObjectOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;

        let output = DeleteObjectsOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let input = req.input;

        if let Err(e) = self.store.get_bucket_info(&input.bucket, &BucketOptions {}).await {
            if DiskError::is_err(&e, &DiskError::VolumeNotFound) {
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

    #[tracing::instrument]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;

        println!("get_object: {:?}", &input);

        let output = GetObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        if let Err(e) = self.store.get_bucket_info(&input.bucket, &BucketOptions {}).await {
            if DiskError::is_err(&e, &DiskError::VolumeNotFound) {
                return Err(s3_error!(NoSuchBucket));
            } else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", e)));
            }
        }
        // mc cp step 2 GetBucketInfo

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;

        let output = HeadObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn list_buckets(&self, _: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let output = ListBucketsOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
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

    #[tracing::instrument]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;

        let output = ListObjectsV2Output { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
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

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let Some(content_length) = content_length else { return Err(s3_error!(IncompleteBody)) };

        let reader = PutObjReader::new(body.into(), content_length as usize);

        try_!(self.store.put_object(&bucket, &key, reader, &ObjectOptions::default()).await);

        // self.store.put_object(bucket, object, data, opts);

        let output = PutObjectOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let CreateMultipartUploadInput { bucket, key, .. } = req.input;

        // mc cp step 3

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

    #[tracing::instrument]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let UploadPartInput {
            body,
            upload_id,
            part_number,
            content_length,
            ..
        } = req.input;

        let body = body.ok_or_else(|| s3_error!(IncompleteBody))?;
        let content_length = content_length.ok_or_else(|| s3_error!(IncompleteBody))?;

        // mc cp step 4

        let output = UploadPartOutput { ..Default::default() };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;

        let output = UploadPartCopyOutput { ..Default::default() };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
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

    #[tracing::instrument]
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

        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
    }
}
