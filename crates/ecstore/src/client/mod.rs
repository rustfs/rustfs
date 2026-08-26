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

// The S3-consuming client moved to the `rustfs-s3-client` crate
// (rustfs/backlog#1842). This shim keeps `crate::client::*` paths working for
// in-crate consumers during the migration window; it is deleted once every
// consumer imports `rustfs_s3_client` directly. Only the two server-side
// modules below (misfiled here historically) remain as real ecstore code.

pub use rustfs_s3_client::{
    admin_handler_utils, api_get_options, api_list, api_put_object, api_remove, api_s3_datatypes, credentials, provider_versions,
    signer_error, transition_api,
};

pub mod object_api_utils;
pub mod object_handlers_common;
