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

//! Backpressure metrics recording functions.

/// Record backpressure state change.
#[inline(always)]
pub fn record_backpressure_state_change(from: &str, to: &str) {
    use metrics::counter;
    counter!("rustfs.backpressure.state.changes", "from" => from.to_string(), "to" => to.to_string()).increment(1);
}

/// Record backpressure rejection.
#[inline(always)]
pub fn record_backpressure_rejection() {
    use metrics::counter;
    counter!("rustfs.backpressure.rejections").increment(1);
}

/// Record concurrent operations count.
#[inline(always)]
pub fn record_concurrent_operations(count: usize) {
    use metrics::gauge;
    gauge!("rustfs.backpressure.concurrent").set(count as f64);
}

/// Record backpressure activation.
#[inline(always)]
pub fn record_backpressure_activation() {
    use metrics::counter;
    counter!("rustfs.backpressure.activations").increment(1);
}

/// Record backpressure deactivation.
#[inline(always)]
pub fn record_backpressure_deactivation() {
    use metrics::counter;
    counter!("rustfs.backpressure.deactivations").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_backpressure_state_change() {
        record_backpressure_state_change("normal", "warning");
        record_backpressure_state_change("warning", "critical");
    }

    #[test]
    fn test_record_backpressure_rejection() {
        record_backpressure_rejection();
    }

    #[test]
    fn test_record_concurrent_operations() {
        record_concurrent_operations(10);
        record_concurrent_operations(32);
    }

    #[test]
    fn test_record_backpressure_activation() {
        record_backpressure_activation();
    }

    #[test]
    fn test_record_backpressure_deactivation() {
        record_backpressure_deactivation();
    }
}
