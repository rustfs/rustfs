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

//! On-Demand Migration (ODM): a bucket can name an external S3-compatible
//! source bucket; GET misses are served from that source and backfilled
//! locally. This module owns the bucket-level configuration model
//! (`on-demand-migration.json` in the bucket metadata file); the runtime is
//! layered on top of it by later tasks (rustfs/backlog#2147).

pub mod config;
pub mod source_client;

pub use config::{
    ConfigPublishHook, FilterConfig, HeadPolicy, ON_DEMAND_MIGRATION_CONFIG_HOOK, ON_DEMAND_MIGRATION_CONFIG_VERSION,
    OnDemandMigrationConfig, OnDemandMigrationConfigError, PathStyle, PolicyConfig, Provider, RangeGetPolicy, SourceConfig,
    SourceCredentials, SourceErrorPolicy, SourceTimeout, TlsConfig, ValidationContext,
};
