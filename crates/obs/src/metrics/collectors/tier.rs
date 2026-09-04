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

//! Remote tier request metrics collector.

use crate::metrics::report::PrometheusMetric;
use crate::metrics::schema::tier::*;

/// One operation/outcome cell of the tier request counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierRequestStats {
    pub operation: &'static str,
    pub outcome: &'static str,
    pub count: u64,
}

/// Split the fixed operation/outcome cells over the success and failure
/// counters.
///
/// The success counter carries no outcome label: `success` is the only outcome
/// it can report, and repeating it would make the two counters look like they
/// share a label set they do not.
pub fn collect_tier_request_metrics(stats: &[TierRequestStats]) -> Vec<PrometheusMetric> {
    stats
        .iter()
        .map(|stat| {
            if stat.outcome == "success" {
                PrometheusMetric::from_descriptor(&TIER_REQUESTS_SUCCESS_MD, stat.count as f64)
                    .with_label(OPERATION_LABEL, stat.operation)
            } else {
                PrometheusMetric::from_descriptor(&TIER_REQUESTS_FAILURE_MD, stat.count as f64)
                    .with_label(OPERATION_LABEL, stat.operation)
                    .with_label(OUTCOME_LABEL, stat.outcome)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats() -> Vec<TierRequestStats> {
        vec![
            TierRequestStats {
                operation: "put",
                outcome: "success",
                count: 7,
            },
            TierRequestStats {
                operation: "put",
                outcome: "timeout",
                count: 2,
            },
        ]
    }

    #[test]
    fn success_and_failure_land_on_their_own_counters() {
        let metrics = collect_tier_request_metrics(&stats());

        let success = metrics
            .iter()
            .find(|metric| metric.name == TIER_REQUESTS_SUCCESS_MD.get_full_metric_name())
            .expect("a success cell must produce the success counter");
        assert_eq!(success.value, 7.0);
        assert!(
            success.labels.iter().all(|(name, _)| *name != OUTCOME_LABEL),
            "the success counter must not carry an outcome label"
        );

        let failure = metrics
            .iter()
            .find(|metric| metric.name == TIER_REQUESTS_FAILURE_MD.get_full_metric_name())
            .expect("a non-success cell must produce the failure counter");
        assert_eq!(failure.value, 2.0);
        assert!(
            failure
                .labels
                .iter()
                .any(|(name, value)| *name == OUTCOME_LABEL && value.as_ref() == "timeout"),
            "the failure counter must keep the outcome that produced it"
        );
    }
}
