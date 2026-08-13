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

use std::sync::Arc;

use http::HeaderMap;
use rustfs_filemeta::FileInfo;

use crate::config::com;
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::storage_api_contracts::{
    object::{DeletedObject, HTTPPreconditions, ObjectIO, ObjectOperations, ObjectToDelete},
    range::HTTPRangeSpec,
};

pub(crate) async fn read_config<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    com::read_config(api, file).await
}

pub(crate) async fn read_config_with_metadata<S>(api: Arc<S>, file: &str, opts: &ObjectOptions) -> Result<(Vec<u8>, ObjectInfo)>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    com::read_config_with_metadata(api, file, opts).await
}

pub(crate) async fn save_config<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    com::save_config(api, file, data).await
}

pub(crate) async fn save_config_with_opts<S>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    com::save_config_with_opts(api, file, data, opts).await
}

pub(crate) async fn save_config_with_opts_quiet<S>(api: Arc<S>, file: &str, data: Vec<u8>, opts: &ObjectOptions) -> Result<()>
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    com::save_config_with_opts_quiet(api, file, data, opts).await
}

pub(crate) async fn delete_config<S>(api: Arc<S>, file: &str) -> Result<()>
where
    S: ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    com::delete_config(api, file).await
}

pub(crate) async fn delete_config_if_match<S>(api: Arc<S>, file: &str, etag: &str) -> Result<()>
where
    S: ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    match api
        .delete_object(
            RUSTFS_META_BUCKET,
            file,
            ObjectOptions {
                http_preconditions: Some(HTTPPreconditions {
                    if_match: Some(etag.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            if err == Error::FileNotFound || matches!(err, Error::ObjectNotFound(_, _)) {
                Err(Error::ConfigNotFound)
            } else {
                Err(err)
            }
        }
    }
}
