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

#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
#[allow(clippy::module_inception, reason = "preserve the public services::tier::tier path")]
pub mod tier;
pub mod tier_admin;
pub mod tier_config;
pub mod tier_gen;
pub mod tier_handlers;
pub(crate) mod tier_mutation_intent;
pub mod tier_mutation_peer;
pub(crate) mod tier_probe_intent;
pub mod warm_backend;
pub mod warm_backend_aliyun;
pub mod warm_backend_azure;
#[cfg(feature = "gcs")]
pub mod warm_backend_gcs;
pub mod warm_backend_huaweicloud;
pub mod warm_backend_minio;
pub mod warm_backend_r2;
pub mod warm_backend_rustfs;
pub mod warm_backend_s3;
pub mod warm_backend_tencent;
pub mod warm_backend_wasabi;
