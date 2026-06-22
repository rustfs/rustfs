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

pub(crate) fn check_object_name_for_length_and_slash(
    bucket: &str,
    object: &str,
) -> ecstore_error::Result<()> {
    ecstore_bucket::utils::check_object_name_for_length_and_slash(bucket, object)
}

pub(crate) fn has_bad_path_component(path: &str) -> bool {
    ecstore_bucket::utils::has_bad_path_component(path)
}

pub(crate) fn is_valid_object_prefix(object: &str) -> bool {
    ecstore_bucket::utils::is_valid_object_prefix(object)
}
