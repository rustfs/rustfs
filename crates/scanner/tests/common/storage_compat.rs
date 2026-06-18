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

pub(crate) mod ecstore {
    #![allow(unused_imports)]

    pub(crate) mod bucket {
        pub(crate) mod lifecycle {
            pub(crate) use rustfs_ecstore::bucket::lifecycle::lifecycle::TransitionOptions;

            pub(crate) mod bucket_lifecycle_ops {
                pub(crate) use rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::{
                    enqueue_transition_for_existing_objects, init_background_expiry,
                };
            }
        }

        pub(crate) mod metadata {
            pub(crate) use rustfs_ecstore::bucket::metadata::BUCKET_LIFECYCLE_CONFIG;
        }

        pub(crate) mod metadata_sys {
            pub(crate) use rustfs_ecstore::bucket::metadata_sys::{get, init_bucket_metadata_sys, update};
        }

        pub(crate) mod versioning_sys {
            pub(crate) use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
        }
    }

    pub(crate) mod client {
        pub(crate) mod transition_api {
            pub(crate) use rustfs_ecstore::client::transition_api::{ReadCloser, ReaderImpl};
        }
    }

    pub(crate) mod disk {
        pub(crate) use rustfs_ecstore::disk::{DiskAPI, DiskOption, STORAGE_FORMAT_FILE, new_disk};

        pub(crate) mod endpoint {
            pub(crate) use rustfs_ecstore::disk::endpoint::Endpoint;
        }
    }

    pub(crate) mod endpoints {
        pub(crate) use rustfs_ecstore::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints};
    }

    pub(crate) mod global {
        pub(crate) use rustfs_ecstore::global::GLOBAL_TierConfigMgr;
    }

    pub(crate) mod pools {
        pub(crate) use rustfs_ecstore::pools::path2_bucket_object_with_base_path;
    }

    pub(crate) mod store {
        pub(crate) use rustfs_ecstore::store::{ECStore, init_local_disks};
    }

    pub(crate) mod tier {
        pub(crate) mod tier_config {
            pub(crate) use rustfs_ecstore::tier::tier_config::{TierConfig, TierMinIO, TierType};
        }

        pub(crate) mod warm_backend {
            pub(crate) use rustfs_ecstore::tier::warm_backend::{WarmBackend, WarmBackendGetOpts, build_transition_put_options};
        }
    }
}
