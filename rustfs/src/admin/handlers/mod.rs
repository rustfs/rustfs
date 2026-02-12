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

pub mod account_info;
pub mod bucket_meta;
pub mod event;
pub mod group;
pub mod heal;
pub mod health;
pub mod is_admin;
pub mod kms;
pub mod kms_dynamic;
pub mod kms_keys;
pub mod metrics;
pub mod policies;
pub mod pools;
pub mod profile;
pub mod profile_admin;
pub mod quota;
pub mod rebalance;
pub mod replication;
pub mod service_account;
pub mod sts;
pub mod system;
pub mod tier;
pub mod trace;
pub mod user;

pub use account_info::AccountInfoHandler;
pub use heal::{BackgroundHealStatusHandler, HealHandler};
pub use health::HealthCheckHandler;
pub use is_admin::IsAdminHandler;
pub use metrics::MetricsHandler;
pub use profile_admin::{ProfileHandler, ProfileStatusHandler};
pub use replication::{GetReplicationMetricsHandler, ListRemoteTargetHandler, RemoveRemoteTargetHandler, SetRemoteTargetHandler};
pub use system::{DataUsageInfoHandler, InspectDataHandler, ServerInfoHandler, ServiceHandle, StorageInfoHandler};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_struct_creation() {
        // Test that handler structs can be created
        let _account_handler = AccountInfoHandler {};
        let _service_handler = ServiceHandle {};
        let _server_info_handler = ServerInfoHandler {};
        let _inspect_data_handler = InspectDataHandler {};
        let _storage_info_handler = StorageInfoHandler {};
        let _data_usage_handler = DataUsageInfoHandler {};
        let _metrics_handler = MetricsHandler {};
        let _heal_handler = HealHandler {};
        let _bg_heal_handler = BackgroundHealStatusHandler {};
        let _replication_metrics_handler = GetReplicationMetricsHandler {};
        let _set_remote_target_handler = SetRemoteTargetHandler {};
        let _list_remote_target_handler = ListRemoteTargetHandler {};
        let _remove_remote_target_handler = RemoveRemoteTargetHandler {};

        // Just verify they can be created without panicking
        // Test passes if we reach this point without panicking
    }

    // Note: Testing the actual async handler implementations requires:
    // 1. S3Request setup with proper headers, URI, and credentials
    // 2. Global object store initialization
    // 3. IAM system initialization
    // 4. Mock or real backend services
    // 5. Authentication and authorization setup
    //
    // These are better suited for integration tests with proper test infrastructure.
    // The current tests focus on data structures and basic functionality that can be
    // tested in isolation without complex dependencies.
}
