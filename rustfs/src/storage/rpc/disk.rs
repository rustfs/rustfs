use super::*;

impl NodeService {
    pub(super) async fn handle_disk_info(&self, request: Request<DiskInfoRequest>) -> Result<Response<DiskInfoResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<DiskInfoOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode DiskInfoOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.disk_info(&opts).await {
                Ok(disk_info) => match serde_json::to_string(&disk_info) {
                    Ok(disk_info) => Ok(Response::new(DiskInfoResponse {
                        success: true,
                        disk_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(DiskInfoResponse {
                        success: false,
                        disk_info: "".to_string(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(DiskInfoResponse {
                    success: false,
                    disk_info: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DiskInfoResponse {
                success: false,
                disk_info: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_volume(&request.volume).await {
                Ok(_) => Ok(Response::new(DeleteVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteVolumeResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_multiple(
        &self,
        request: Request<ReadMultipleRequest>,
    ) -> Result<Response<ReadMultipleResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let read_multiple_req = match serde_json::from_str::<ReadMultipleReq>(&request.read_multiple_req) {
                Ok(read_multiple_req) => read_multiple_req,
                Err(err) => {
                    return Ok(Response::new(ReadMultipleResponse {
                        success: false,
                        read_multiple_resps: Vec::new(),
                        error: Some(DiskError::other(format!("decode ReadMultipleReq failed: {err}")).into()),
                    }));
                }
            };
            match disk.read_multiple(read_multiple_req).await {
                Ok(read_multiple_resps) => {
                    let read_multiple_resps = read_multiple_resps
                        .into_iter()
                        .filter_map(|read_multiple_resp| serde_json::to_string(&read_multiple_resp).ok())
                        .collect();

                    Ok(Response::new(ReadMultipleResponse {
                        success: true,
                        read_multiple_resps,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(ReadMultipleResponse {
                    success: false,
                    read_multiple_resps: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadMultipleResponse {
                success: false,
                read_multiple_resps: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_versions(
        &self,
        request: Request<DeleteVersionsRequest>,
    ) -> Result<Response<DeleteVersionsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let mut versions = Vec::with_capacity(request.versions.len());
            for version in request.versions.iter() {
                match serde_json::from_str::<FileInfoVersions>(version) {
                    Ok(version) => versions.push(version),
                    Err(err) => {
                        return Ok(Response::new(DeleteVersionsResponse {
                            success: false,
                            errors: Vec::new(),
                            error: Some(DiskError::other(format!("decode FileInfoVersions failed: {err}")).into()),
                        }));
                    }
                };
            }
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionsResponse {
                        success: false,
                        errors: Vec::new(),
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };

            let errors = disk
                .delete_versions(&request.volume, versions, opts)
                .await
                .into_iter()
                .map(|error| match error {
                    Some(e) => e.to_string(),
                    None => "".to_string(),
                })
                .collect();

            Ok(Response::new(DeleteVersionsResponse {
                success: true,
                errors,
                error: None,
            }))
        } else {
            Ok(Response::new(DeleteVersionsResponse {
                success: false,
                errors: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_version(
        &self,
        request: Request<DeleteVersionRequest>,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<DeleteOptions>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk
                .delete_version(&request.volume, &request.path, file_info, request.force_del_marker, opts)
                .await
            {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(Response::new(DeleteVersionResponse {
                        success: true,
                        raw_file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(DeleteVersionResponse {
                        success: false,
                        raw_file_info: "".to_string(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(DeleteVersionResponse {
                    success: false,
                    raw_file_info: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteVersionResponse {
                success: false,
                raw_file_info: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_xl(&self, request: Request<ReadXlRequest>) -> Result<Response<ReadXlResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_xl(&request.volume, &request.path, request.read_data).await {
                Ok(raw_file_info) => match serde_json::to_string(&raw_file_info) {
                    Ok(raw_file_info) => Ok(Response::new(ReadXlResponse {
                        success: true,
                        raw_file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(ReadXlResponse {
                        success: false,
                        raw_file_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(ReadXlResponse {
                    success: false,
                    raw_file_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadXlResponse {
                success: false,
                raw_file_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_version(
        &self,
        request: Request<ReadVersionRequest>,
    ) -> Result<Response<ReadVersionResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let opts = match serde_json::from_str::<ReadOptions>(&request.opts) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(DiskError::other(format!("decode ReadOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk
                .read_version("", &request.volume, &request.path, &request.version_id, &opts)
                .await
            {
                Ok(file_info) => match serde_json::to_string(&file_info) {
                    Ok(file_info) => Ok(Response::new(ReadVersionResponse {
                        success: true,
                        file_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(ReadVersionResponse {
                        success: false,
                        file_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(ReadVersionResponse {
                    success: false,
                    file_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadVersionResponse {
                success: false,
                file_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_write_metadata(
        &self,
        request: Request<WriteMetadataRequest>,
    ) -> Result<Response<WriteMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(WriteMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.write_metadata("", &request.volume, &request.path, file_info).await {
                Ok(_) => Ok(Response::new(WriteMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(WriteMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(WriteMetadataResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_update_metadata(
        &self,
        request: Request<UpdateMetadataRequest>,
    ) -> Result<Response<UpdateMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            let opts = match serde_json::from_str::<UpdateMetadataOpts>(&request.opts) {
                Ok(opts) => opts,
                Err(err) => {
                    return Ok(Response::new(UpdateMetadataResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode UpdateMetadataOpts failed: {err}")).into()),
                    }));
                }
            };

            match disk.update_metadata(&request.volume, &request.path, file_info, &opts).await {
                Ok(_) => Ok(Response::new(UpdateMetadataResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(UpdateMetadataResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(UpdateMetadataResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_metadata(
        &self,
        request: Request<ReadMetadataRequest>,
    ) -> Result<Response<ReadMetadataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_metadata(&request.volume, &request.path).await {
                Ok(data) => Ok(Response::new(ReadMetadataResponse {
                    success: true,
                    data,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ReadMetadataResponse {
                    success: false,
                    data: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadMetadataResponse {
                success: false,
                data: Bytes::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete_paths(
        &self,
        request: Request<DeletePathsRequest>,
    ) -> Result<Response<DeletePathsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.delete_paths(&request.volume, &request.paths).await {
                Ok(_) => Ok(Response::new(DeletePathsResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeletePathsResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeletePathsResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_stat_volume(
        &self,
        request: Request<StatVolumeRequest>,
    ) -> Result<Response<StatVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.stat_volume(&request.volume).await {
                Ok(volume_info) => match serde_json::to_string(&volume_info) {
                    Ok(volume_info) => Ok(Response::new(StatVolumeResponse {
                        success: true,
                        volume_info,
                        error: None,
                    })),
                    Err(err) => Ok(Response::new(StatVolumeResponse {
                        success: false,
                        volume_info: String::new(),
                        error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                    })),
                },
                Err(err) => Ok(Response::new(StatVolumeResponse {
                    success: false,
                    volume_info: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(StatVolumeResponse {
                success: false,
                volume_info: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_volumes().await {
                Ok(volume_infos) => {
                    let volume_infos = volume_infos
                        .into_iter()
                        .filter_map(|volume_info| serde_json::to_string(&volume_info).ok())
                        .collect();
                    Ok(Response::new(ListVolumesResponse {
                        success: true,
                        volume_infos,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(ListVolumesResponse {
                    success: false,
                    volume_infos: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListVolumesResponse {
                success: false,
                volume_infos: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_make_volume(
        &self,
        request: Request<MakeVolumeRequest>,
    ) -> Result<Response<MakeVolumeResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volume(&request.volume).await {
                Ok(_) => Ok(Response::new(MakeVolumeResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumeResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumeResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_make_volumes(
        &self,
        request: Request<MakeVolumesRequest>,
    ) -> Result<Response<MakeVolumesResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.make_volumes(request.volumes.iter().map(|s| &**s).collect()).await {
                Ok(_) => Ok(Response::new(MakeVolumesResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(MakeVolumesResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(MakeVolumesResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_rename_data(
        &self,
        request: Request<RenameDataRequest>,
    ) -> Result<Response<RenameDataResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(RenameDataResponse {
                        success: false,
                        rename_data_resp: String::new(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk
                .rename_data(&request.src_volume, &request.src_path, file_info, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(rename_data_resp) => {
                    let rename_data_resp = match serde_json::to_string(&rename_data_resp) {
                        Ok(file_info) => file_info,
                        Err(err) => {
                            return Ok(Response::new(RenameDataResponse {
                                success: false,
                                rename_data_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(RenameDataResponse {
                        success: true,
                        rename_data_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(RenameDataResponse {
                    success: false,
                    rename_data_resp: String::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameDataResponse {
                success: false,
                rename_data_resp: String::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_list_dir(&self, request: Request<ListDirRequest>) -> Result<Response<ListDirResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.list_dir("", &request.volume, &request.dir_path, request.count).await {
                Ok(volumes) => Ok(Response::new(ListDirResponse {
                    success: true,
                    volumes,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ListDirResponse {
                    success: false,
                    volumes: Vec::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ListDirResponse {
                success: false,
                volumes: Vec::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_write(&self, _request: Request<WriteRequest>) -> Result<Response<WriteResponse>, Status> {
        unimplemented!("write");
        // let request = request.into_inner();
        // if let Some(disk) = self.find_disk(&request.disk).await {
        //     let file_writer = if request.is_append {
        //         disk.append_file(&request.volume, &request.path).await
        //     } else {
        //         disk.create_file("", &request.volume, &request.path, 0).await
        //     };

        //     match file_writer {
        //         Ok(mut file_writer) => match file_writer.write(&request.data).await {
        //             Ok(_) => Ok(Response::new(WriteResponse {
        //                 success: true,
        //                 error: None,
        //             })),
        //             Err(err) => Ok(Response::new(WriteResponse {
        //                 success: false,
        //                 error: Some(err_to_proto_err(&err, &format!("write failed: {}", err))),
        //             })),
        //         },
        //         Err(err) => Ok(Response::new(WriteResponse {
        //             success: false,
        //             error: Some(err_to_proto_err(&err, &format!("get writer failed: {}", err))),
        //         })),
        //     }
        // } else {
        //     Ok(Response::new(WriteResponse {
        //         success: false,
        //         error: Some(err_to_proto_err(
        //             &EcsError::new(StorageError::InvalidArgument(Default::default(), Default::default(), Default::default())),
        //             "can not find disk",
        //         )),
        //     }))
        // }
    }

    pub(super) async fn handle_rename_file(
        &self,
        request: Request<RenameFileRequest>,
    ) -> Result<Response<RenameFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_file(&request.src_volume, &request.src_path, &request.dst_volume, &request.dst_path)
                .await
            {
                Ok(_) => Ok(Response::new(RenameFileResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenameFileResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenameFileResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_rename_part(
        &self,
        request: Request<RenamePartRequest>,
    ) -> Result<Response<RenamePartResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk
                .rename_part(
                    &request.src_volume,
                    &request.src_path,
                    &request.dst_volume,
                    &request.dst_path,
                    request.meta,
                )
                .await
            {
                Ok(_) => Ok(Response::new(RenamePartResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(RenamePartResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(RenamePartResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_check_parts(
        &self,
        request: Request<CheckPartsRequest>,
    ) -> Result<Response<CheckPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(CheckPartsResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.check_parts(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(CheckPartsResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(CheckPartsResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(CheckPartsResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(CheckPartsResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_parts(
        &self,
        request: Request<ReadPartsRequest>,
    ) -> Result<Response<ReadPartsResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_parts(&request.bucket, &request.paths).await {
                Ok(data) => {
                    let data = match rmp_serde::to_vec(&data) {
                        Ok(data) => data,
                        Err(err) => {
                            return Ok(Response::new(ReadPartsResponse {
                                success: false,
                                object_part_infos: Bytes::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(ReadPartsResponse {
                        success: true,
                        object_part_infos: Bytes::copy_from_slice(&data),
                        error: None,
                    }))
                }

                Err(err) => Ok(Response::new(ReadPartsResponse {
                    success: false,
                    object_part_infos: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadPartsResponse {
                success: false,
                object_part_infos: Bytes::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_verify_file(
        &self,
        request: Request<VerifyFileRequest>,
    ) -> Result<Response<VerifyFileResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let file_info = match serde_json::from_str::<FileInfo>(&request.file_info) {
                Ok(file_info) => file_info,
                Err(err) => {
                    return Ok(Response::new(VerifyFileResponse {
                        success: false,
                        check_parts_resp: "".to_string(),
                        error: Some(DiskError::other(format!("decode FileInfo failed: {err}")).into()),
                    }));
                }
            };
            match disk.verify_file(&request.volume, &request.path, &file_info).await {
                Ok(check_parts_resp) => {
                    let check_parts_resp = match serde_json::to_string(&check_parts_resp) {
                        Ok(check_parts_resp) => check_parts_resp,
                        Err(err) => {
                            return Ok(Response::new(VerifyFileResponse {
                                success: false,
                                check_parts_resp: String::new(),
                                error: Some(DiskError::other(format!("encode data failed: {err}")).into()),
                            }));
                        }
                    };
                    Ok(Response::new(VerifyFileResponse {
                        success: true,
                        check_parts_resp,
                        error: None,
                    }))
                }
                Err(err) => Ok(Response::new(VerifyFileResponse {
                    success: false,
                    check_parts_resp: "".to_string(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(VerifyFileResponse {
                success: false,
                check_parts_resp: "".to_string(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            let options = match serde_json::from_str::<DeleteOptions>(&request.options) {
                Ok(options) => options,
                Err(err) => {
                    return Ok(Response::new(DeleteResponse {
                        success: false,
                        error: Some(DiskError::other(format!("decode DeleteOptions failed: {err}")).into()),
                    }));
                }
            };
            match disk.delete(&request.volume, &request.path, options).await {
                Ok(_) => Ok(Response::new(DeleteResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(DeleteResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(DeleteResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_write_all(&self, request: Request<WriteAllRequest>) -> Result<Response<WriteAllResponse>, Status> {
        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.write_all(&request.volume, &request.path, request.data).await {
                Ok(_) => Ok(Response::new(WriteAllResponse {
                    success: true,
                    error: None,
                })),
                Err(err) => Ok(Response::new(WriteAllResponse {
                    success: false,
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(WriteAllResponse {
                success: false,
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }

    pub(super) async fn handle_read_all(&self, request: Request<ReadAllRequest>) -> Result<Response<ReadAllResponse>, Status> {
        debug!("read all");

        let request = request.into_inner();
        if let Some(disk) = self.find_disk(&request.disk).await {
            match disk.read_all(&request.volume, &request.path).await {
                Ok(data) => Ok(Response::new(ReadAllResponse {
                    success: true,
                    data,
                    error: None,
                })),
                Err(err) => Ok(Response::new(ReadAllResponse {
                    success: false,
                    data: Bytes::new(),
                    error: Some(err.into()),
                })),
            }
        } else {
            Ok(Response::new(ReadAllResponse {
                success: false,
                data: Bytes::new(),
                error: Some(DiskError::other("can not find disk".to_string()).into()),
            }))
        }
    }
}
