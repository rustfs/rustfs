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
pub mod audit;
mod audit_runtime_config;
pub mod batch_job;
pub mod bucket_meta;
pub mod cluster_snapshot;
pub mod config_admin;
pub mod diagnostics;
pub mod durability;
pub mod event;
pub mod extensions;
pub mod group;
pub mod heal;
pub mod health;
pub(crate) mod iam_error;
pub mod idp_compat;
pub mod ilm_transition;
pub mod is_admin;
pub mod kms;
pub mod kms_dynamic;
pub mod kms_keys;
pub mod kms_management;
pub mod metrics;
pub mod module_switch;
mod notify_runtime_access;
pub mod object_data_cache;
pub mod object_zip_download;
pub mod oidc;
pub mod plugins_catalog;
pub mod plugins_instances;
pub mod policies;
pub mod pools;
pub mod profile;
pub mod profile_admin;
pub mod quota;
pub mod rebalance;
pub mod replication;
pub mod scanner;
pub mod service_account;
pub mod site_replication;
pub mod sts;
pub mod system;
pub mod table_catalog;
mod target_descriptor;
pub mod tier;
pub mod tls_debug;
pub mod trace;
pub mod user;
pub mod user_iam;
pub mod user_lifecycle;
pub mod user_policy_binding;

pub(crate) async fn supervise_admin_mutation<T>(
    operation: &'static str,
    mutation: impl std::future::Future<Output = s3s::S3Result<T>> + Send + 'static,
) -> s3s::S3Result<T>
where
    T: Send + 'static,
{
    tokio::spawn(mutation).await.map_err(|err| {
        let outcome = if err.is_cancelled() { "cancelled" } else { "panicked" };
        s3s::s3_error!(InternalError, "{} task {}", operation, outcome)
    })?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_struct_creation() {
        // Test that handler structs can be created
        let _account_handler = account_info::AccountInfoHandler {};
        let _get_config_kv_handler = config_admin::GetConfigKVHandler {};
        let _set_config_kv_handler = config_admin::SetConfigKVHandler {};
        let _del_config_kv_handler = config_admin::DelConfigKVHandler {};
        let _help_config_kv_handler = config_admin::HelpConfigKVHandler {};
        let _get_config_handler = config_admin::GetConfigHandler {};
        let _set_config_handler = config_admin::SetConfigHandler {};
        let _list_audit_targets = audit::ListAuditTargets {};
        let _get_module_switches = module_switch::GetModuleSwitchesHandler {};
        let _get_cluster_snapshot = cluster_snapshot::GetClusterSnapshotHandler {};
        let _get_extension_catalog = extensions::GetExtensionCatalogHandler {};
        let _list_extension_instances = extensions::ListExtensionInstancesHandler {};
        let _get_plugin_catalog = plugins_catalog::GetPluginCatalogHandler {};
        let _create_object_zip_download = object_zip_download::CreateObjectZipDownloadHandler {};
        let _list_plugin_instances = plugins_instances::ListPluginInstancesHandler {};
        let _get_plugin_instance = plugins_instances::GetPluginInstanceHandler {};
        let _put_plugin_instance = plugins_instances::PutPluginInstanceHandler {};
        let _delete_plugin_instance = plugins_instances::DeletePluginInstanceHandler {};
        let _update_module_switches = module_switch::UpdateModuleSwitchesHandler {};
        let _service_handler = system::ServiceHandle {};
        let _server_info_handler = system::ServerInfoHandler {};
        let _inspect_data_handler = system::InspectDataHandler {};
        let _storage_info_handler = system::StorageInfoHandler {};
        let _data_usage_handler = system::DataUsageInfoHandler {};
        let _metrics_handler = metrics::MetricsHandler {};
        let _profile_handler = profile_admin::ProfileHandler {};
        let _profile_status_handler = profile_admin::ProfileStatusHandler {};
        let _tls_status_handler = tls_debug::TlsStatusHandler {};
        let _heal_handler = heal::HealHandler {};
        let _bg_heal_handler = heal::BackgroundHealStatusHandler {};
        let _replication_metrics_handler = replication::GetReplicationMetricsHandler {};
        let _set_remote_target_handler = replication::SetRemoteTargetHandler {};
        let _list_remote_target_handler = replication::ListRemoteTargetHandler {};
        let _remove_remote_target_handler = replication::RemoveRemoteTargetHandler {};
        let _scanner_status_handler = scanner::ScannerStatusHandler {};
        let _manual_transition_handler = ilm_transition::ManualTransitionRunHandler {};
        let _site_replication_add_handler = site_replication::SiteReplicationAddHandler {};
        let _site_replication_info_handler = site_replication::SiteReplicationInfoHandler {};
        let _site_replication_status_handler = site_replication::SiteReplicationStatusHandler {};
        let _table_catalog_config_handler = table_catalog::GetCatalogConfigHandler {};

        // Just verify they can be created without panicking
        // Test passes if we reach this point without panicking
    }

    #[tokio::test]
    async fn supervised_admin_mutation_survives_waiter_cancellation() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            supervise_admin_mutation("test mutation", async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                let _ = completed_tx.send(());
                Ok(())
            })
            .await
        });

        started_rx.await.expect("mutation started");
        waiter.abort();
        release_tx.send(()).expect("release mutation");
        tokio::time::timeout(std::time::Duration::from_secs(1), completed_rx)
            .await
            .expect("detached mutation should complete")
            .expect("completion signal");
    }

    #[tokio::test]
    async fn supervised_admin_mutation_does_not_expose_panic_payload() {
        let error = supervise_admin_mutation::<()>("test mutation", async {
            panic!("do-not-expose-payload");
        })
        .await
        .expect_err("panicking mutation should fail");

        let rendered = error.to_string();
        assert!(rendered.contains("panicked"));
        assert!(!rendered.contains("do-not-expose-payload"));
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
