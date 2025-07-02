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

/// Metric descriptors related to cluster configuration
use crate::metrics::{MetricDescriptor, MetricName, new_gauge_md, subsystems};

lazy_static::lazy_static! {
    pub static ref CONFIG_RRS_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigRRSParity,
            "Reduced redundancy storage class parity",
            &[],
            subsystems::CLUSTER_CONFIG
        );

    pub static ref CONFIG_STANDARD_PARITY_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::ConfigStandardParity,
            "Standard storage class parity",
            &[],
            subsystems::CLUSTER_CONFIG
        );
}
