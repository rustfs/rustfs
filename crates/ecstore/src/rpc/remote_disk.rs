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

use std::path::PathBuf;

use bytes::Bytes;
use futures::lock::Mutex;
use http::{HeaderMap, HeaderValue, Method, header::CONTENT_TYPE};
use rustfs_protos::{
    node_service_time_out_client,
    proto_gen::node_service::{
        CheckPartsRequest, DeletePathsRequest, DeleteRequest, DeleteVersionRequest, DeleteVersionsRequest, DeleteVolumeRequest,
        DiskInfoRequest, ListDirRequest, ListVolumesRequest, MakeVolumeRequest, MakeVolumesRequest, NsScannerRequest,
        ReadAllRequest, ReadMultipleRequest, ReadVersionRequest, ReadXlRequest, RenameDataRequest, RenameFileRequest,
        StatVolumeRequest, UpdateMetadataRequest, VerifyFileRequest, WriteAllRequest, WriteMetadataRequest,
    },
};

use crate::disk::{
    CheckPartsResp, DeleteOptions, DiskAPI, DiskInfo, DiskInfoOptions, DiskLocation, DiskOption, FileInfoVersions,
    ReadMultipleReq, ReadMultipleResp, ReadOptions, RenameDataResp, UpdateMetadataOpts, VolumeInfo, WalkDirOptions,
    endpoint::Endpoint,
};
use crate::{
    disk::error::{Error, Result},
    rpc::build_auth_headers,
};
use crate::{
    disk::{FileReader, FileWriter},
    heal::{
        data_scanner::ShouldSleepFn,
        data_usage_cache::{DataUsageCache, DataUsageEntry},
        heal_commands::{HealScanMode, HealingTracker},
    },
};
use rustfs_filemeta::{FileInfo, RawFileInfo};
use rustfs_protos::proto_gen::node_service::RenamePartRequest;
use rustfs_rio::{HttpReader, HttpWriter};
use tokio::{
    io::AsyncWrite,
    sync::mpsc::{self, Sender},
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::Request;
use tracing::info;
use uuid::Uuid;

#[derive(Debug)]
pub struct RemoteDisk {
    pub id: Mutex<Option<Uuid>>,
    pub addr: String,
    pub url: url::Url,
    pub root: PathBuf,
    endpoint: Endpoint,
}

impl RemoteDisk {
    pub async fn new(ep: &Endpoint, _opt: &DiskOption) -> Result<Self> {
        // let root = fs::canonicalize(ep.url.path()).await?;
        let root = PathBuf::from(ep.get_file_path());
        let addr = if let Some(port) = ep.url.port() {
            format!("{}://{}:{}", ep.url.scheme(), ep.url.host_str().unwrap(), port)
        } else {
            format!("{}://{}", ep.url.scheme(), ep.url.host_str().unwrap())
        };
        Ok(Self {
            id: Mutex::new(None),
            addr,
            url: ep.url.clone(),
            root,
            endpoint: ep.clone(),
        })
    }
}

// TODO: all api need to handle errors
#[async_trait::async_trait]
impl DiskAPI for RemoteDisk {
    #[tracing::instrument(skip(self))]
    fn to_string(&self) -> String {
        self.endpoint.to_string()
    }

    #[tracing::instrument(skip(self))]
    async fn is_online(&self) -> bool {
        // TODO: 连接状态
        if node_service_time_out_client(&self.addr).await.is_ok() {
            return true;
        }
        false
    }

    #[tracing::instrument(skip(self))]
    fn is_local(&self) -> bool {
        false
    }
    #[tracing::instrument(skip(self))]
    fn host_name(&self) -> String {
        self.endpoint.host_port()
    }
    #[tracing::instrument(skip(self))]
    fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }
    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    #[tracing::instrument(skip(self))]
    async fn get_disk_id(&self) -> Result<Option<Uuid>> {
        Ok(*self.id.lock().await)
    }

    #[tracing::instrument(skip(self))]
    async fn set_disk_id(&self, id: Option<Uuid>) -> Result<()> {
        let mut lock = self.id.lock().await;
        *lock = id;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn path(&self) -> PathBuf {
        self.root.clone()
    }

    #[tracing::instrument(skip(self))]
    fn get_disk_location(&self) -> DiskLocation {
        DiskLocation {
            pool_idx: {
                if self.endpoint.pool_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.pool_idx as usize)
                }
            },
            set_idx: {
                if self.endpoint.set_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.set_idx as usize)
                }
            },
            disk_idx: {
                if self.endpoint.disk_idx < 0 {
                    None
                } else {
                    Some(self.endpoint.disk_idx as usize)
                }
            },
        }
    }

    #[tracing::instrument(skip(self))]
    async fn make_volume(&self, volume: &str) -> Result<()> {
        info!("make_volume");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(MakeVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.make_volume(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn make_volumes(&self, volumes: Vec<&str>) -> Result<()> {
        info!("make_volumes");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(MakeVolumesRequest {
            disk: self.endpoint.to_string(),
            volumes: volumes.iter().map(|s| (*s).to_string()).collect(),
        });

        let response = client.make_volumes(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        info!("list_volumes");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ListVolumesRequest {
            disk: self.endpoint.to_string(),
        });

        let response = client.list_volumes(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let infos = response
            .volume_infos
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<VolumeInfo>(&json_str).ok())
            .collect();

        Ok(infos)
    }

    #[tracing::instrument(skip(self))]
    async fn stat_volume(&self, volume: &str) -> Result<VolumeInfo> {
        info!("stat_volume");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(StatVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.stat_volume(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let volume_info = serde_json::from_str::<VolumeInfo>(&response.volume_info)?;

        Ok(volume_info)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_volume(&self, volume: &str) -> Result<()> {
        info!("delete_volume {}/{}", self.endpoint.to_string(), volume);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DeleteVolumeRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.delete_volume(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    // // FIXME: TODO: use writer
    // #[tracing::instrument(skip(self, wr))]
    // async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
    //     let now = std::time::SystemTime::now();
    //     info!("walk_dir {}/{}/{:?}", self.endpoint.to_string(), opts.bucket, opts.filter_prefix);
    //     let mut wr = wr;
    //     let mut out = MetacacheWriter::new(&mut wr);
    //     let mut buf = Vec::new();
    //     opts.serialize(&mut Serializer::new(&mut buf))?;
    //     let mut client = node_service_time_out_client(&self.addr)
    //         .await
    //         .map_err(|err| Error::other(format!("can not get client, err: {}", err)))?;
    //     let request = Request::new(WalkDirRequest {
    //         disk: self.endpoint.to_string(),
    //         walk_dir_options: buf.into(),
    //     });
    //     let mut response = client.walk_dir(request).await?.into_inner();

    //     loop {
    //         match response.next().await {
    //             Some(Ok(resp)) => {
    //                 if !resp.success {
    //                     if let Some(err) = resp.error_info {
    //                         if err == "Unexpected EOF" {
    //                             return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, err)));
    //                         } else {
    //                             return Err(Error::other(err));
    //                         }
    //                     }

    //                     return Err(Error::other("unknown error"));
    //                 }
    //                 let entry = serde_json::from_str::<MetaCacheEntry>(&resp.meta_cache_entry)
    //                     .map_err(|_| Error::other(format!("Unexpected response: {:?}", response)))?;
    //                 out.write_obj(&entry).await?;
    //             }
    //             None => break,
    //             _ => return Err(Error::other(format!("Unexpected response: {:?}", response))),
    //         }
    //     }

    //     info!(
    //         "walk_dir {}/{:?} done {:?}",
    //         opts.bucket,
    //         opts.filter_prefix,
    //         now.elapsed().unwrap_or_default()
    //     );
    //     Ok(())
    // }

    #[tracing::instrument(skip(self))]
    async fn delete_version(
        &self,
        volume: &str,
        path: &str,
        fi: FileInfo,
        force_del_marker: bool,
        opts: DeleteOptions,
    ) -> Result<()> {
        info!("delete_version");
        let file_info = serde_json::to_string(&fi)?;
        let opts = serde_json::to_string(&opts)?;

        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DeleteVersionRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
            force_del_marker,
            opts,
        });

        let response = client.delete_version(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        // let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete_versions(
        &self,
        volume: &str,
        versions: Vec<FileInfoVersions>,
        opts: DeleteOptions,
    ) -> Result<Vec<Option<Error>>> {
        info!("delete_versions");
        let opts = serde_json::to_string(&opts)?;
        let mut versions_str = Vec::with_capacity(versions.len());
        for file_info_versions in versions.iter() {
            versions_str.push(serde_json::to_string(file_info_versions)?);
        }
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DeleteVersionsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            versions: versions_str,
            opts,
        });

        // TODO: use Error not string
        let response = client.delete_versions(request).await?.into_inner();
        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }
        let errors = response
            .errors
            .iter()
            .map(|error| {
                if error.is_empty() {
                    None
                } else {
                    Some(Error::other(error.to_string()))
                }
            })
            .collect();

        Ok(errors)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_paths(&self, volume: &str, paths: &[String]) -> Result<()> {
        info!("delete_paths");
        let paths = paths.to_owned();
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DeletePathsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            paths,
        });

        let response = client.delete_paths(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn write_metadata(&self, _org_volume: &str, volume: &str, path: &str, fi: FileInfo) -> Result<()> {
        info!("write_metadata {}/{}", volume, path);
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(WriteMetadataRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.write_metadata(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn update_metadata(&self, volume: &str, path: &str, fi: FileInfo, opts: &UpdateMetadataOpts) -> Result<()> {
        info!("update_metadata");
        let file_info = serde_json::to_string(&fi)?;
        let opts = serde_json::to_string(&opts)?;

        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(UpdateMetadataRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
            opts,
        });

        let response = client.update_metadata(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn read_version(
        &self,
        _org_volume: &str,
        volume: &str,
        path: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<FileInfo> {
        info!("read_version");
        let opts = serde_json::to_string(opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ReadVersionRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            version_id: version_id.to_string(),
            opts,
        });

        let response = client.read_version(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let file_info = serde_json::from_str::<FileInfo>(&response.file_info)?;

        Ok(file_info)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_xl(&self, volume: &str, path: &str, read_data: bool) -> Result<RawFileInfo> {
        info!("read_xl {}/{}/{}", self.endpoint.to_string(), volume, path);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ReadXlRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            read_data,
        });

        let response = client.read_xl(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let raw_file_info = serde_json::from_str::<RawFileInfo>(&response.raw_file_info)?;

        Ok(raw_file_info)
    }

    #[tracing::instrument(skip(self))]
    async fn rename_data(
        &self,
        src_volume: &str,
        src_path: &str,
        fi: FileInfo,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<RenameDataResp> {
        info!("rename_data {}/{}/{}/{}", self.addr, self.endpoint.to_string(), dst_volume, dst_path);
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(RenameDataRequest {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            file_info,
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_data(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let rename_data_resp = serde_json::from_str::<RenameDataResp>(&response.rename_data_resp)?;

        Ok(rename_data_resp)
    }

    #[tracing::instrument(skip(self))]
    async fn list_dir(&self, _origvolume: &str, volume: &str, _dir_path: &str, _count: i32) -> Result<Vec<String>> {
        info!("list_dir {}/{}", volume, _dir_path);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ListDirRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
        });

        let response = client.list_dir(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(response.volumes)
    }

    #[tracing::instrument(skip(self, wr))]
    async fn walk_dir<W: AsyncWrite + Unpin + Send>(&self, opts: WalkDirOptions, wr: &mut W) -> Result<()> {
        info!("walk_dir {}", self.endpoint.to_string());

        let url = format!(
            "{}/rustfs/rpc/walk_dir?disk={}",
            self.endpoint.grid_host(),
            urlencoding::encode(self.endpoint.to_string().as_str()),
        );

        let opts = serde_json::to_vec(&opts)?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::GET, &mut headers);

        let mut reader = HttpReader::new(url, Method::GET, headers, Some(opts)).await?;

        tokio::io::copy(&mut reader, wr).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file(&self, volume: &str, path: &str) -> Result<FileReader> {
        info!("read_file {}/{}", volume, path);

        let url = format!(
            "{}/rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}",
            self.endpoint.grid_host(),
            urlencoding::encode(self.endpoint.to_string().as_str()),
            urlencoding::encode(volume),
            urlencoding::encode(path),
            0,
            0
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::GET, &mut headers);
        Ok(Box::new(HttpReader::new(url, Method::GET, headers, None).await?))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_file_stream(&self, volume: &str, path: &str, offset: usize, length: usize) -> Result<FileReader> {
        // warn!(
        //     "disk remote read_file_stream {}/{}/{} offset={} length={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     offset,
        //     length
        // );
        let url = format!(
            "{}/rustfs/rpc/read_file_stream?disk={}&volume={}&path={}&offset={}&length={}",
            self.endpoint.grid_host(),
            urlencoding::encode(self.endpoint.to_string().as_str()),
            urlencoding::encode(volume),
            urlencoding::encode(path),
            offset,
            length
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::GET, &mut headers);
        Ok(Box::new(HttpReader::new(url, Method::GET, headers, None).await?))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn append_file(&self, volume: &str, path: &str) -> Result<FileWriter> {
        info!("append_file {}/{}", volume, path);

        let url = format!(
            "{}/rustfs/rpc/put_file_stream?disk={}&volume={}&path={}&append={}&size={}",
            self.endpoint.grid_host(),
            urlencoding::encode(self.endpoint.to_string().as_str()),
            urlencoding::encode(volume),
            urlencoding::encode(path),
            true,
            0
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::PUT, &mut headers);
        Ok(Box::new(HttpWriter::new(url, Method::PUT, headers).await?))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn create_file(&self, _origvolume: &str, volume: &str, path: &str, file_size: i64) -> Result<FileWriter> {
        // warn!(
        //     "disk remote create_file {}/{}/{} file_size={}",
        //     self.endpoint.to_string(),
        //     volume,
        //     path,
        //     file_size
        // );

        let url = format!(
            "{}/rustfs/rpc/put_file_stream?disk={}&volume={}&path={}&append={}&size={}",
            self.endpoint.grid_host(),
            urlencoding::encode(self.endpoint.to_string().as_str()),
            urlencoding::encode(volume),
            urlencoding::encode(path),
            false,
            file_size
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        build_auth_headers(&url, &Method::PUT, &mut headers);
        Ok(Box::new(HttpWriter::new(url, Method::PUT, headers).await?))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn rename_file(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str) -> Result<()> {
        info!("rename_file");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(RenameFileRequest {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
        });

        let response = client.rename_file(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn rename_part(&self, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str, meta: Bytes) -> Result<()> {
        info!("rename_part {}/{}", src_volume, src_path);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(RenamePartRequest {
            disk: self.endpoint.to_string(),
            src_volume: src_volume.to_string(),
            src_path: src_path.to_string(),
            dst_volume: dst_volume.to_string(),
            dst_path: dst_path.to_string(),
            meta,
        });

        let response = client.rename_part(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, volume: &str, path: &str, opt: DeleteOptions) -> Result<()> {
        info!("delete {}/{}/{}", self.endpoint.to_string(), volume, path);
        let options = serde_json::to_string(&opt)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DeleteRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            options,
        });

        let response = client.delete(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn verify_file(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        info!("verify_file");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(VerifyFileRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.verify_file(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

        Ok(check_parts_resp)
    }

    #[tracing::instrument(skip(self))]
    async fn check_parts(&self, volume: &str, path: &str, fi: &FileInfo) -> Result<CheckPartsResp> {
        info!("check_parts");
        let file_info = serde_json::to_string(&fi)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(CheckPartsRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            file_info,
        });

        let response = client.check_parts(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let check_parts_resp = serde_json::from_str::<CheckPartsResp>(&response.check_parts_resp)?;

        Ok(check_parts_resp)
    }

    #[tracing::instrument(skip(self))]
    async fn read_multiple(&self, req: ReadMultipleReq) -> Result<Vec<ReadMultipleResp>> {
        info!("read_multiple {}/{}/{}", self.endpoint.to_string(), req.bucket, req.prefix);
        let read_multiple_req = serde_json::to_string(&req)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ReadMultipleRequest {
            disk: self.endpoint.to_string(),
            read_multiple_req,
        });

        let response = client.read_multiple(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let read_multiple_resps = response
            .read_multiple_resps
            .into_iter()
            .filter_map(|json_str| serde_json::from_str::<ReadMultipleResp>(&json_str).ok())
            .collect();

        Ok(read_multiple_resps)
    }

    #[tracing::instrument(skip(self))]
    async fn write_all(&self, volume: &str, path: &str, data: Bytes) -> Result<()> {
        info!("write_all");
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(WriteAllRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
            data,
        });

        let response = client.write_all(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn read_all(&self, volume: &str, path: &str) -> Result<Bytes> {
        info!("read_all {}/{}", volume, path);
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(ReadAllRequest {
            disk: self.endpoint.to_string(),
            volume: volume.to_string(),
            path: path.to_string(),
        });

        let response = client.read_all(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        Ok(response.data)
    }

    #[tracing::instrument(skip(self))]
    async fn disk_info(&self, opts: &DiskInfoOptions) -> Result<DiskInfo> {
        let opts = serde_json::to_string(&opts)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;
        let request = Request::new(DiskInfoRequest {
            disk: self.endpoint.to_string(),
            opts,
        });

        let response = client.disk_info(request).await?.into_inner();

        if !response.success {
            return Err(response.error.unwrap_or_default().into());
        }

        let disk_info = serde_json::from_str::<DiskInfo>(&response.disk_info)?;

        Ok(disk_info)
    }

    #[tracing::instrument(skip(self, cache, scan_mode, _we_sleep))]
    async fn ns_scanner(
        &self,
        cache: &DataUsageCache,
        updates: Sender<DataUsageEntry>,
        scan_mode: HealScanMode,
        _we_sleep: ShouldSleepFn,
    ) -> Result<DataUsageCache> {
        info!("ns_scanner");
        let cache = serde_json::to_string(cache)?;
        let mut client = node_service_time_out_client(&self.addr)
            .await
            .map_err(|err| Error::other(format!("can not get client, err: {err}")))?;

        let (tx, rx) = mpsc::channel(10);
        let in_stream = ReceiverStream::new(rx);
        let mut response = client.ns_scanner(in_stream).await?.into_inner();
        let request = NsScannerRequest {
            disk: self.endpoint.to_string(),
            cache,
            scan_mode: scan_mode as u64,
        };
        tx.send(request)
            .await
            .map_err(|err| Error::other(format!("can not send request, err: {err}")))?;

        loop {
            match response.next().await {
                Some(Ok(resp)) => {
                    if !resp.update.is_empty() {
                        let data_usage_cache = serde_json::from_str::<DataUsageEntry>(&resp.update)?;
                        let _ = updates.send(data_usage_cache).await;
                    } else if !resp.data_usage_cache.is_empty() {
                        let data_usage_cache = serde_json::from_str::<DataUsageCache>(&resp.data_usage_cache)?;
                        return Ok(data_usage_cache);
                    } else {
                        return Err(Error::other("scan was interrupted"));
                    }
                }
                _ => return Err(Error::other("scan was interrupted")),
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn healing(&self) -> Option<HealingTracker> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_remote_disk_creation() {
        let url = url::Url::parse("http://example.com:9000/path").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();

        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.endpoint.url, url);
        assert_eq!(remote_disk.endpoint.pool_idx, 0);
        assert_eq!(remote_disk.endpoint.set_idx, 1);
        assert_eq!(remote_disk.endpoint.disk_idx, 2);
        assert_eq!(remote_disk.host_name(), "example.com:9000");
    }

    #[tokio::test]
    async fn test_remote_disk_basic_properties() {
        let url = url::Url::parse("http://remote-server:9000").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();

        // Test basic properties
        assert!(!remote_disk.is_local());
        assert_eq!(remote_disk.host_name(), "remote-server:9000");
        assert!(remote_disk.to_string().contains("remote-server"));
        assert!(remote_disk.to_string().contains("9000"));

        // Test disk location
        let location = remote_disk.get_disk_location();
        assert_eq!(location.pool_idx, None);
        assert_eq!(location.set_idx, None);
        assert_eq!(location.disk_idx, None);
        assert!(!location.valid()); // None values make it invalid
    }

    #[tokio::test]
    async fn test_remote_disk_path() {
        let url = url::Url::parse("http://remote-server:9000/storage").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();
        let path = remote_disk.path();

        // Remote disk path should be based on the URL path
        assert!(path.to_string_lossy().contains("storage"));
    }

    #[tokio::test]
    async fn test_remote_disk_disk_id() {
        let url = url::Url::parse("http://remote-server:9000").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();

        // Initially, disk ID should be None
        let initial_id = remote_disk.get_disk_id().await.unwrap();
        assert!(initial_id.is_none());

        // Set a disk ID
        let test_id = Uuid::new_v4();
        remote_disk.set_disk_id(Some(test_id)).await.unwrap();

        // Verify the disk ID was set
        let retrieved_id = remote_disk.get_disk_id().await.unwrap();
        assert_eq!(retrieved_id, Some(test_id));

        // Clear the disk ID
        remote_disk.set_disk_id(None).await.unwrap();
        let cleared_id = remote_disk.get_disk_id().await.unwrap();
        assert!(cleared_id.is_none());
    }

    #[tokio::test]
    async fn test_remote_disk_endpoints_with_different_schemes() {
        let test_cases = vec![
            ("http://server:9000", "server:9000"),
            ("https://secure-server:443", "secure-server"), // Default HTTPS port is omitted
            ("http://192.168.1.100:8080", "192.168.1.100:8080"),
            ("https://secure-server", "secure-server"), // No port specified
        ];

        for (url_str, expected_hostname) in test_cases {
            let url = url::Url::parse(url_str).unwrap();
            let endpoint = Endpoint {
                url: url.clone(),
                is_local: false,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            };

            let disk_option = DiskOption {
                cleanup: false,
                health_check: false,
            };

            let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();

            assert!(!remote_disk.is_local());
            assert_eq!(remote_disk.host_name(), expected_hostname);
            // Note: to_string() might not contain the exact hostname format
            assert!(!remote_disk.to_string().is_empty());
        }
    }

    #[tokio::test]
    async fn test_remote_disk_location_validation() {
        // Test valid location
        let url = url::Url::parse("http://server:9000").unwrap();
        let valid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 1,
            disk_idx: 2,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&valid_endpoint, &disk_option).await.unwrap();
        let location = remote_disk.get_disk_location();
        assert!(location.valid());
        assert_eq!(location.pool_idx, Some(0));
        assert_eq!(location.set_idx, Some(1));
        assert_eq!(location.disk_idx, Some(2));

        // Test invalid location (negative indices)
        let invalid_endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: -1,
            set_idx: -1,
            disk_idx: -1,
        };

        let remote_disk_invalid = RemoteDisk::new(&invalid_endpoint, &disk_option).await.unwrap();
        let invalid_location = remote_disk_invalid.get_disk_location();
        assert!(!invalid_location.valid());
        assert_eq!(invalid_location.pool_idx, None);
        assert_eq!(invalid_location.set_idx, None);
        assert_eq!(invalid_location.disk_idx, None);
    }

    #[tokio::test]
    async fn test_remote_disk_close() {
        let url = url::Url::parse("http://server:9000").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };

        let disk_option = DiskOption {
            cleanup: false,
            health_check: false,
        };

        let remote_disk = RemoteDisk::new(&endpoint, &disk_option).await.unwrap();

        // Test close operation (should succeed)
        let result = remote_disk.close().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_remote_disk_sync_properties() {
        let url = url::Url::parse("https://secure-remote:9000/data").unwrap();
        let endpoint = Endpoint {
            url: url.clone(),
            is_local: false,
            pool_idx: 1,
            set_idx: 2,
            disk_idx: 3,
        };

        // Test endpoint method - we can't test this without creating RemoteDisk instance
        // but we can test that the endpoint contains expected values
        assert_eq!(endpoint.url, url);
        assert!(!endpoint.is_local);
        assert_eq!(endpoint.pool_idx, 1);
        assert_eq!(endpoint.set_idx, 2);
        assert_eq!(endpoint.disk_idx, 3);
    }
}
