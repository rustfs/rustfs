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
//! (`on-demand-migration.json` in the bucket metadata file), the source
//! client, and the per-node runtime (`sys`) that turns configs into live
//! clients guarded by a breaker, a negative cache, singleflight and a pull
//! concurrency limit (rustfs/backlog#2147).

pub mod backfill;
pub mod breaker;
pub mod config;
pub mod negative_cache;
pub mod pull;
pub mod source_client;
pub mod stats;
pub mod sys;

pub use breaker::{
    BREAKER_FAILURE_THRESHOLD, BREAKER_FAILURE_WINDOW, BREAKER_HALF_OPEN_MAX_PROBES, BREAKER_OPEN_DURATION, Breaker,
    BreakerState, BreakerTransition, BreakerVerdict,
};
pub use config::{
    ConfigPublishHook, FilterConfig, HeadPolicy, ON_DEMAND_MIGRATION_CONFIG_HOOK, ON_DEMAND_MIGRATION_CONFIG_VERSION,
    OnDemandMigrationConfig, OnDemandMigrationConfigError, PathStyle, PolicyConfig, Provider, RangeGetPolicy, SourceConfig,
    SourceCredentials, SourceErrorPolicy, SourceTimeout, TlsConfig, ValidationContext,
};
pub use negative_cache::{NEGATIVE_CACHE_MAX_ENTRIES, NegativeCache};
pub use pull::{
    EnqueueOutcome, LocalObject, MAX_MULTIPART_PARTS, OdmWriteBack, PULL_MAX_RETRIES, PULL_RETRY_BASE_DELAYS, PullCompletion,
    PullQueue, PullReason, PullSource, QueuedPullOutcome, SourceBody, WriteBackBody, WriteBackError, WriteBackOutcome,
    WriteBackPart, WriteBackRequest, commit_inline, commit_inline_with, idle_guarded_body,
};
pub use stats::{
    GaugeGuard, LastSourceError, LatencyBucketSnapshot, OdmOp, OdmOutcome, OdmStats, OdmStatsSnapshot, PullFailureReason,
    PullPath, SOURCE_LATENCY_BUCKET_BOUNDS_MS, SourceLatencySnapshot,
};
pub use sys::{
    ApplyOutcome, BucketOdmState, GLOBAL_ON_DEMAND_MIGRATION_SYS, OdmBucketSnapshot, OdmLookup, OdmStateError,
    OnDemandMigrationSys, PullError, PullFollower, PullLeader, PullOutcome, PullResult, PullSlot, source_client_spec,
};
