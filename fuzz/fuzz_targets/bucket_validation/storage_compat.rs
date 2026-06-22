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

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::error as ecstore_error;

pub(crate) fn check_bucket_and_object_names(bucket: &str, object: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_bucket_and_object_names(bucket, object)
}

pub(crate) fn check_list_objs_args(
    bucket: &str,
    prefix: &str,
    marker: &Option<String>,
) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_list_objs_args(bucket, prefix, marker)
}

pub(crate) fn check_valid_bucket_name_strict(bucket_name: &str) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_valid_bucket_name_strict(bucket_name)
}

pub(crate) fn is_meta_bucketname(name: &str) -> bool {
    ecstore_bucket::utils::is_meta_bucketname(name)
}
