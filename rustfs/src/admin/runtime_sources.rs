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

use crate::admin::storage_api::runtime_sources::{
    DailyAllTierStats, ECStore, NotificationSys, ScannerMetricsReport, StorageClassConfig, TierConfigMgr,
};
pub(crate) use crate::app::admin_usecase::{
    AdminPoolStatus, DefaultAdminUsecase, QueryPoolStatusRequest, QueryServerInfoRequest,
};
use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::runtime_sources as root_runtime_sources;
pub(crate) use crate::runtime_sources::{
    AppContext, ServerContextSlot, current_action_credentials, current_boot_time, current_bucket_metadata_handle,
    current_bucket_monitor_handle, current_deployment_id, current_endpoints_handle, current_federated_identity_service,
    current_iam_handle, current_kms_runtime_service_manager, current_notification_system_for_context,
    current_object_data_cache_handle_for_context, current_object_store_handle_for_context, current_ready_iam_handle,
    current_region, current_replication_pool_handle, current_replication_stats_handle,
    current_replication_stats_handle_for_context, current_server_config_for_context, current_token_signing_key,
};
#[cfg(test)]
pub(crate) use crate::runtime_sources::{
    IamInterface, KmsInterface, NotificationSystemInterface, ServerConfigInterface, StorageClassInterface,
    publish_test_app_context,
};
use rustfs_config::server_config::Config;
use rustfs_kms::KmsServiceManager;
use rustfs_tls_runtime::GlobalPublishedOutboundTlsState;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) fn default_admin_usecase() -> DefaultAdminUsecase {
    DefaultAdminUsecase::from_global()
}

pub(crate) fn default_object_usecase() -> DefaultObjectUsecase {
    DefaultObjectUsecase::from_global()
}

pub(crate) fn current_app_context() -> Option<Arc<AppContext>> {
    crate::runtime_sources::current_app_context()
}

pub(crate) fn current_object_store_handle() -> Option<Arc<ECStore>> {
    let context = current_app_context();
    current_object_store_handle_for_context(context.as_deref())
}

/// Resolve the object data cache adapter for an admin request through the
/// process AppContext. `None` when no context is initialised (the admin
/// stats/flush handlers then report the cache as unavailable).
pub(crate) fn current_object_data_cache() -> Option<Arc<ObjectDataCacheAdapter>> {
    let context = current_app_context();
    current_object_data_cache_handle_for_context(context.as_deref())
}

/// Resolve the object store for an admin request through the server's context
/// slot injected at router dispatch (backlog#1052 S2). Falls back to the
/// ambient process context when no slot was injected (direct handler tests,
/// paths outside the router) — the single-instance legacy default.
pub(crate) fn object_store_from_req<B>(req: &s3s::S3Request<B>) -> Option<Arc<ECStore>> {
    object_store_from_extensions(&req.extensions)
}

pub(crate) fn app_context_from_req<B>(req: &s3s::S3Request<B>) -> Option<Arc<AppContext>> {
    app_context_from_extensions(&req.extensions)
}

/// Resolve an application context from request extensions.
///
/// An injected slot identifies the server that owns the request. If that
/// server has not finished startup yet, returning its ambient global context
/// could select a different server, so this fails closed. Only requests with
/// no slot retain the legacy ambient fallback.
pub(crate) fn app_context_from_extensions(extensions: &http::Extensions) -> Option<Arc<AppContext>> {
    match extensions.get::<Arc<ServerContextSlot>>() {
        Some(slot) => slot.installed_app_context(),
        None => current_app_context(),
    }
}

/// Field-borrow form of [`object_store_from_req`] for handlers that have
/// already moved other request fields (body, credentials) out of the request.
pub(crate) fn object_store_from_extensions(extensions: &http::Extensions) -> Option<Arc<ECStore>> {
    match extensions.get::<Arc<ServerContextSlot>>() {
        Some(slot) => slot.installed_object_store(),
        None => current_object_store_handle(),
    }
}

pub(crate) fn current_notification_system() -> Option<Arc<NotificationSys>> {
    let context = current_app_context();
    current_notification_system_for_context(context.as_deref())
}

pub(crate) fn current_server_config() -> Option<Config> {
    let context = current_app_context();
    current_server_config_for_context(context.as_deref())
}

pub(crate) fn current_or_init_kms_runtime_service_manager() -> Arc<KmsServiceManager> {
    root_runtime_sources::current_or_init_kms_runtime_service_manager()
        .unwrap_or_else(rustfs_kms::init_global_kms_service_manager)
}

pub(crate) async fn current_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    if let Some(state) = root_runtime_sources::current_outbound_tls_state().await {
        return state;
    }

    root_runtime_sources::fallback_outbound_tls_runtime_interface().state().await
}

pub(crate) fn current_daily_tier_stats() -> DailyAllTierStats {
    root_runtime_sources::current_daily_tier_stats().unwrap_or_default()
}

pub(crate) fn current_runtime_port() -> u16 {
    root_runtime_sources::current_runtime_port().unwrap_or(rustfs_config::DEFAULT_PORT)
}

pub(crate) async fn current_scanner_metrics_report() -> ScannerMetricsReport {
    if let Some(report) = root_runtime_sources::current_scanner_metrics_report().await {
        return report;
    }

    root_runtime_sources::fallback_scanner_metrics_interface().report().await
}

pub(crate) fn current_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    root_runtime_sources::current_tier_config_handle().unwrap_or_else(TierConfigMgr::new)
}

pub(crate) fn publish_server_config(config: Config) {
    if !root_runtime_sources::publish_server_config(config.clone()) {
        root_runtime_sources::fallback_server_config_interface().set(config);
    }
}

pub(crate) fn publish_storage_class_config(config: StorageClassConfig) {
    if !root_runtime_sources::publish_storage_class_config(config.clone()) {
        root_runtime_sources::fallback_storage_class_interface().set(config);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppContext, IamInterface, KmsInterface, ServerContextSlot, app_context_from_extensions, app_context_from_req,
        current_app_context, object_store_from_extensions, publish_test_app_context,
    };
    use crate::admin::router::{Operation, S3Router};
    use hyper::{Method, StatusCode};
    use matchit::Params;
    use rustfs_iam::{store::object::ObjectStore, sys::IamSys};
    use rustfs_kms::KmsServiceManager;
    use s3s::route::S3Route;
    use s3s::{Body, S3ErrorCode, S3Request, S3Response, s3_error};
    use std::sync::Arc;

    struct UnreadyIam;

    impl IamInterface for UnreadyIam {
        fn handle(&self) -> Arc<IamSys<ObjectStore>> {
            panic!("test context does not resolve IAM")
        }

        fn is_ready(&self) -> bool {
            false
        }
    }

    struct TestKms;

    impl KmsInterface for TestKms {
        fn handle(&self) -> Arc<KmsServiceManager> {
            Arc::new(KmsServiceManager::new())
        }
    }

    async fn ambient_context() -> Arc<AppContext> {
        if let Some(context) = current_app_context() {
            return context;
        }

        let env = rustfs_test_utils::TestECStoreEnv::builder()
            .prefix("server_context_slot")
            .disk_count(1)
            .init_bucket_metadata(false)
            .build()
            .await;
        let context = Arc::new(AppContext::new(env.ecstore, Arc::new(UnreadyIam), Arc::new(TestKms)));
        publish_test_app_context(context);
        current_app_context().expect("test context must be globally published")
    }

    fn distinct_context(context: &AppContext) -> Arc<AppContext> {
        Arc::new(AppContext::new(context.object_store(), Arc::new(UnreadyIam), Arc::new(TestKms)))
    }

    fn request(extensions: http::Extensions) -> S3Request<Body> {
        S3Request {
            input: Body::empty(),
            method: Method::GET,
            uri: "/context-probe".parse().expect("test URI"),
            headers: http::HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn request_with_empty_slot_never_uses_ambient_context() {
        let ambient = ambient_context().await;
        let slot = ServerContextSlot::new();
        let mut extensions = http::Extensions::new();
        extensions.insert(slot);

        assert!(app_context_from_extensions(&extensions).is_none());
        assert!(object_store_from_extensions(&extensions).is_none());
        assert!(app_context_from_req(&request(extensions)).is_none());
        assert!(Arc::ptr_eq(
            &current_app_context().expect("ambient context must remain available"),
            &ambient
        ));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn request_with_installed_slot_resolves_only_its_context() {
        let ambient = ambient_context().await;
        let installed = distinct_context(&ambient);
        let slot = ServerContextSlot::new();
        assert!(slot.install(installed.clone()));
        let mut extensions = http::Extensions::new();
        extensions.insert(slot);

        let resolved = app_context_from_extensions(&extensions).expect("installed slot must resolve");
        assert!(Arc::ptr_eq(&resolved, &installed));
        assert!(!Arc::ptr_eq(&resolved, &ambient));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn request_without_slot_keeps_legacy_ambient_resolution() {
        let ambient = ambient_context().await;
        let extensions = http::Extensions::new();

        let resolved = app_context_from_extensions(&extensions).expect("legacy request must use ambient context");
        assert!(Arc::ptr_eq(&resolved, &ambient));
        assert!(object_store_from_extensions(&extensions).is_some());
    }

    struct ContextDependentAdminRoute;

    #[async_trait::async_trait]
    impl Operation for ContextDependentAdminRoute {
        async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> s3s::S3Result<S3Response<(StatusCode, Body)>> {
            app_context_from_req(&req).ok_or_else(|| s3_error!(ServiceUnavailable, "server context is not ready"))?;
            Ok(S3Response::new((StatusCode::NO_CONTENT, Body::empty())))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn context_dependent_admin_route_fails_closed_until_slot_installation() {
        let ambient = ambient_context().await;
        let slot = ServerContextSlot::new();
        let mut router = S3Router::new(false);
        router.set_server_ctx(slot.clone());
        router
            .insert(Method::GET, "/context-probe", ContextDependentAdminRoute)
            .expect("register test route");

        let err = router
            .call(request(http::Extensions::new()))
            .await
            .expect_err("empty slot must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);

        assert!(slot.install(distinct_context(&ambient)));
        let response = router
            .call(request(http::Extensions::new()))
            .await
            .expect("installed slot must serve request");
        assert_eq!(response.status, Some(StatusCode::NO_CONTENT));
    }
}
