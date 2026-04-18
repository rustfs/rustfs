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

/// Format the path to the metric name format
/// Replace '/' and '-' with '_'
#[allow(dead_code)]
pub fn format_path_to_metric_name(path: &str) -> String {
    path.trim_start_matches('/').replace(['/', '-'], "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_path_to_metric_name() {
        assert_eq!(format_path_to_metric_name("/api/requests"), "api_requests");
        assert_eq!(format_path_to_metric_name("/system/network/internode"), "system_network_internode");
        assert_eq!(format_path_to_metric_name("/bucket-api"), "bucket_api");
        assert_eq!(format_path_to_metric_name("cluster/health"), "cluster_health");
    }
}
