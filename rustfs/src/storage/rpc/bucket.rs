use super::*;

impl NodeService {
    pub(super) async fn handle_delete_bucket_metadata(
        &self,
        request: Request<DeleteBucketMetadataRequest>,
    ) -> Result<Response<DeleteBucketMetadataResponse>, Status> {
        let request = request.into_inner();
        let _bucket = request.bucket;

        //todo
        Ok(Response::new(DeleteBucketMetadataResponse {
            success: true,
            error_info: None,
        }))
    }

    pub(super) async fn handle_load_bucket_metadata(
        &self,
        request: Request<LoadBucketMetadataRequest>,
    ) -> Result<Response<LoadBucketMetadataResponse>, Status> {
        let request = request.into_inner();
        let bucket = request.bucket;
        if bucket.is_empty() {
            return Ok(Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("bucket name is missing".to_string()),
            }));
        }

        let Some(store) = new_object_layer_fn() else {
            return Ok(Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some("errServerNotInitialized".to_string()),
            }));
        };

        match load_bucket_metadata(store, &bucket).await {
            Ok(meta) => {
                if let Err(err) = metadata_sys::set_bucket_metadata(bucket, meta).await {
                    return Ok(Response::new(LoadBucketMetadataResponse {
                        success: false,
                        error_info: Some(err.to_string()),
                    }));
                };
                Ok(Response::new(LoadBucketMetadataResponse {
                    success: true,
                    error_info: None,
                }))
            }
            Err(err) => Ok(Response::new(LoadBucketMetadataResponse {
                success: false,
                error_info: Some(err.to_string()),
            })),
        }
    }

    pub(super) async fn handle_delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        match self
            .local_peer
            .delete_bucket(
                &request.bucket,
                &DeleteBucketOptions {
                    force: false,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(Response::new(DeleteBucketResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(Response::new(DeleteBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_get_bucket_info(
        &self,
        request: Request<GetBucketInfoRequest>,
    ) -> Result<Response<GetBucketInfoResponse>, Status> {
        debug!("get bucket info");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(GetBucketInfoResponse {
                    success: false,
                    bucket_info: String::new(),
                    error: Some(DiskError::other(format!("decode BucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.get_bucket_info(&request.bucket, &options).await {
            Ok(bucket_info) => {
                let bucket_info = match serde_json::to_string(&bucket_info) {
                    Ok(bucket_info) => bucket_info,
                    Err(err) => {
                        return Ok(Response::new(GetBucketInfoResponse {
                            success: false,
                            bucket_info: String::new(),
                            error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                        }));
                    }
                };

                Ok(Response::new(GetBucketInfoResponse {
                    success: true,
                    bucket_info,
                    error: None,
                }))
            }

            // println!("vuc")
            Err(err) => Ok(Response::new(GetBucketInfoResponse {
                success: false,
                bucket_info: String::new(),
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_make_bucket(
        &self,
        request: Request<MakeBucketRequest>,
    ) -> Result<Response<MakeBucketResponse>, Status> {
        debug!("make bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<MakeBucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(MakeBucketResponse {
                    success: false,
                    error: Some(DiskError::other(format!("decode MakeBucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.make_bucket(&request.name, &options).await {
            Ok(_) => Ok(Response::new(MakeBucketResponse {
                success: true,
                error: None,
            })),
            Err(err) => Ok(Response::new(MakeBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_list_bucket(
        &self,
        request: Request<ListBucketRequest>,
    ) -> Result<Response<ListBucketResponse>, Status> {
        debug!("list bucket");

        let request = request.into_inner();
        let options = match serde_json::from_str::<BucketOptions>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(ListBucketResponse {
                    success: false,
                    bucket_infos: Vec::new(),
                    error: Some(DiskError::other(format!("decode BucketOptions failed: {err}")).into()),
                }));
            }
        };
        match self.local_peer.list_bucket(&options).await {
            Ok(bucket_infos) => {
                let bucket_infos = bucket_infos
                    .into_iter()
                    .filter_map(|bucket_info| serde_json::to_string(&bucket_info).ok())
                    .collect();
                Ok(Response::new(ListBucketResponse {
                    success: true,
                    bucket_infos,
                    error: None,
                }))
            }

            Err(err) => Ok(Response::new(ListBucketResponse {
                success: false,
                bucket_infos: Vec::new(),
                error: Some(err.into()),
            })),
        }
    }

    pub(super) async fn handle_heal_bucket(
        &self,
        request: Request<HealBucketRequest>,
    ) -> Result<Response<HealBucketResponse>, Status> {
        debug!("heal bucket");
        let request = request.into_inner();
        let options = match serde_json::from_str::<HealOpts>(&request.options) {
            Ok(options) => options,
            Err(err) => {
                return Ok(Response::new(HealBucketResponse {
                    success: false,
                    error: Some(DiskError::other(format!("decode HealOpts failed: {err}")).into()),
                }));
            }
        };

        match self.local_peer.heal_bucket(&request.bucket, &options).await {
            Ok(_) => Ok(Response::new(HealBucketResponse {
                success: true,
                error: None,
            })),

            Err(err) => Ok(Response::new(HealBucketResponse {
                success: false,
                error: Some(err.into()),
            })),
        }
    }
}
