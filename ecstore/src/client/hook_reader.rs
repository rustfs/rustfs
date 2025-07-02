#![allow(clippy::map_entry)]
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

use std::{collections::HashMap, sync::Arc};

use crate::{
    disk::{
        error::{is_unformatted_disk, DiskError},
        format::{DistributionAlgoVersion, FormatV3},
        new_disk, DiskAPI, DiskInfo, DiskOption, DiskStore,
    },
    store_api::{
        BucketInfo, BucketOptions, CompletePart, DeleteBucketOptions, DeletedObject, GetObjectReader, HTTPRangeSpec,
        ListMultipartsInfo, ListObjectVersionsInfo, ListObjectsV2Info, MakeBucketOptions, MultipartInfo, MultipartUploadResult,
        ObjectIO, ObjectInfo, ObjectOptions, ObjectToDelete, PartInfo, PutObjReader, StorageAPI,
    },
    credentials::{Credentials, SignatureType,},
    api_put_object_multipart::UploadPartParams,
};

use http::HeaderMap;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing::{error, info};
use url::Url;

struct HookReader {
    source: GetObjectReader,
    hook:   GetObjectReader,
}

impl HookReader {
    pub fn new(source: GetObjectReader, hook: GetObjectReader) -> HookReader {
        HookReader {
            source,
            hook,
        }
    }

    fn seek(&self, offset: i64, whence: i64) -> Result<i64> {
        todo!();
    }

    fn read(&self, b: &[u8]) -> Result<i64> {
        todo!();
    }
}