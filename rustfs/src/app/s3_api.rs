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

//! App-local S3 API response helpers.

pub(crate) mod bucket {
    pub(crate) use crate::storage::s3_api::bucket::{
        ListObjectVersionsParams, ListObjectsV2Params, build_list_buckets_output, build_list_object_versions_output,
        build_list_objects_output, build_list_objects_v2_output, parse_list_object_versions_params, parse_list_objects_v2_params,
    };
    pub(crate) use crate::storage::s3_api::common::rustfs_owner;
}

pub(crate) mod multipart {
    pub(crate) use crate::storage::s3_api::multipart::{
        ListMultipartUploadsParams, build_list_multipart_uploads_output, build_list_parts_output,
        parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
    };
}
