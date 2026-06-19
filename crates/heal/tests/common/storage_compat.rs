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

#![allow(unused_imports)]

pub(crate) use rustfs_ecstore::bucket::metadata_sys::init_bucket_metadata_sys;
pub(crate) use rustfs_ecstore::disk::{DiskStore, endpoint::Endpoint};
pub(crate) use rustfs_ecstore::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
pub(crate) use rustfs_ecstore::store::ECStore;
pub(crate) use rustfs_ecstore::store::init_local_disks;
