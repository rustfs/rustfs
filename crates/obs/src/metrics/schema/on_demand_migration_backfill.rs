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

//! On-demand migration backfill descriptors (ODM-12, rustfs/backlog#2159).
//!
//! The `backfill_*` section of the on-demand migration metric family. It
//! lives in its own file so the ODM-10 runtime descriptors and this section
//! merge without touching each other; both share
//! [`on_demand_migration_subsystem`].

use crate::{MetricDescriptor, MetricName, MetricSubsystem, new_counter_md, new_gauge_md};
use std::sync::LazyLock;

pub const SERVER_LABEL: &str = "server";
pub const BUCKET_LABEL: &str = "bucket";
pub const STATE_LABEL: &str = "state";

/// Subsystem path of every on-demand migration metric
/// (`rustfs_on_demand_migration_*`).
pub const ON_DEMAND_MIGRATION_SUBSYSTEM_PATH: &str = "/on-demand-migration";

pub fn on_demand_migration_subsystem() -> MetricSubsystem {
    MetricSubsystem::new(ON_DEMAND_MIGRATION_SUBSYSTEM_PATH)
}

pub static ODM_BACKFILL_JOBS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("backfill_jobs".to_string()),
        "On-demand migration backfill jobs by server, bucket and state (1 for the bucket's current state)",
        &[SERVER_LABEL, BUCKET_LABEL, STATE_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_LISTED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_listed_total".to_string()),
        "Source keys listed by the on-demand migration backfill job, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_ENQUEUED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_enqueued_total".to_string()),
        "Keys queued for pulling by the on-demand migration backfill job, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_PULLED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_pulled_total".to_string()),
        "Objects stored locally by the on-demand migration backfill job, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_SKIPPED_EXISTING_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_skipped_existing_total".to_string()),
        "Keys the on-demand migration backfill job skipped because a local object already existed, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_FAILED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_failed_total".to_string()),
        "Keys the on-demand migration backfill job could not pull, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});

pub static ODM_BACKFILL_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::Custom("backfill_bytes_total".to_string()),
        "Bytes stored locally by the on-demand migration backfill job, by server and bucket",
        &[SERVER_LABEL, BUCKET_LABEL],
        on_demand_migration_subsystem(),
    )
});
