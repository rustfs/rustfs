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

use crate::config::com;
use crate::error::Result;
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::storage_api_contracts::{object::ObjectIO, range::HTTPRangeSpec};
use http::HeaderMap;
use std::sync::Arc;

pub(crate) async fn read<S>(api: Arc<S>, file: &str) -> Result<Vec<u8>>
where
    S: ObjectIO<
            Error = crate::error::Error,
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

pub(crate) async fn save<S>(api: Arc<S>, file: &str, data: Vec<u8>) -> Result<()>
where
    S: ObjectIO<
            Error = crate::error::Error,
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
