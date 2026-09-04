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

//! Remote tier request metric descriptors.
//!
//! The label set is fixed by the operation and outcome enums the recording
//! site uses, so the series count is bounded by construction and cannot grow
//! with tier names, endpoints or object keys.

use crate::{MetricDescriptor, MetricName, new_counter_md, subsystems};
use std::sync::LazyLock;

pub const OPERATION_LABEL: &str = "operation";
pub const OUTCOME_LABEL: &str = "outcome";

pub static TIER_REQUESTS_SUCCESS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::TierRequestsSuccess,
        "Remote tier requests the backend acknowledged, by operation",
        &[OPERATION_LABEL],
        subsystems::TIER,
    )
});

pub static TIER_REQUESTS_FAILURE_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::TierRequestsFailure,
        "Remote tier requests that did not complete, by operation and outcome",
        &[OPERATION_LABEL, OUTCOME_LABEL],
        subsystems::TIER,
    )
});
