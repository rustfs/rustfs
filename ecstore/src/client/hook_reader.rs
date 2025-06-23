#![allow(clippy::map_entry)]
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