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

use super::global::get_global_app_context;
use super::handles::{
    default_bucket_metadata_interface, default_endpoints_interface, default_kms_runtime_interface,
    default_server_config_interface, default_tier_config_interface,
};
use rustfs_config::server_config::Config;
use rustfs_ecstore::bucket::metadata_sys::BucketMetadataSys;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::store::ECStore;
use rustfs_ecstore::tier::tier::TierConfigMgr;
use rustfs_kms::KmsServiceManager;
use std::sync::Arc;
use tokio::sync::RwLock;

fn resolve_context_or_global<T>(context_value: Option<T>, global_value: impl FnOnce() -> Option<T>) -> Option<T> {
    context_value.or_else(global_value)
}

fn resolve_context_presence_or_global<T>(
    context_value: Option<Option<T>>,
    global_value: impl FnOnce() -> Option<T>,
) -> Option<T> {
    match context_value {
        Some(value) => value,
        None => global_value(),
    }
}

fn resolve_context_or_global_value<T>(context_value: Option<T>, global_value: impl FnOnce() -> T) -> T {
    context_value.unwrap_or_else(global_value)
}

/// Resolve KMS runtime service manager using AppContext-first precedence.
pub fn resolve_kms_runtime_service_manager() -> Option<Arc<KmsServiceManager>> {
    resolve_context_or_global(
        get_global_app_context().and_then(|context| context.kms_runtime().service_manager()),
        || default_kms_runtime_interface().service_manager(),
    )
}

/// Resolve bucket metadata handle using AppContext-first precedence.
pub fn resolve_bucket_metadata_handle() -> Option<Arc<RwLock<BucketMetadataSys>>> {
    resolve_context_or_global(get_global_app_context().and_then(|context| context.bucket_metadata().handle()), || {
        default_bucket_metadata_interface().handle()
    })
}

/// Resolve object store handle from AppContext.
pub fn resolve_object_store_handle() -> Option<Arc<ECStore>> {
    resolve_context_or_global(get_global_app_context().map(|context| context.object_store()), || None)
}

/// Resolve endpoints using AppContext-first precedence.
pub fn resolve_endpoints_handle() -> Option<EndpointServerPools> {
    resolve_context_or_global(get_global_app_context().and_then(|context| context.endpoints().handle()), || {
        default_endpoints_interface().handle()
    })
}

/// Resolve tier config handle using AppContext-first precedence.
pub fn resolve_tier_config_handle() -> Arc<RwLock<TierConfigMgr>> {
    resolve_context_or_global_value(get_global_app_context().map(|context| context.tier_config().handle()), || {
        default_tier_config_interface().handle()
    })
}

/// Resolve server config using AppContext-first precedence.
pub fn resolve_server_config() -> Option<Config> {
    resolve_context_presence_or_global(get_global_app_context().map(|context| context.server_config().get()), || {
        default_server_config_interface().get()
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_context_or_global, resolve_context_or_global_value, resolve_context_presence_or_global};

    fn assert_optional_resolver(resource: &'static str, context_value: &'static str, global_value: &'static str) {
        assert_eq!(
            resolve_context_or_global(Some(context_value), || Some(global_value)),
            Some(context_value),
            "{resource} should prefer AppContext value"
        );
        assert_eq!(
            resolve_context_or_global::<&str>(None, || Some(global_value)),
            Some(global_value),
            "{resource} should fall back to global value"
        );
    }

    fn assert_required_resolver(resource: &'static str, context_value: &'static str, global_value: &'static str) {
        assert_eq!(
            resolve_context_or_global_value(Some(context_value), || global_value),
            context_value,
            "{resource} should prefer AppContext value"
        );
        assert_eq!(
            resolve_context_or_global_value::<&str>(None, || global_value),
            global_value,
            "{resource} should fall back to global value"
        );
    }

    fn assert_context_only_resolver(resource: &'static str, context_value: &'static str) {
        assert_eq!(
            resolve_context_or_global(Some(context_value), || None),
            Some(context_value),
            "{resource} should prefer AppContext value"
        );
        assert_eq!(
            resolve_context_or_global::<&str>(None, || None),
            None,
            "{resource} should stay empty when AppContext is absent"
        );
    }

    fn assert_context_presence_resolver(resource: &'static str, context_value: &'static str, global_value: &'static str) {
        assert_eq!(
            resolve_context_presence_or_global(Some(Some(context_value)), || Some(global_value)),
            Some(context_value),
            "{resource} should prefer AppContext value"
        );
        assert_eq!(
            resolve_context_presence_or_global(Some(None), || Some(global_value)),
            None,
            "{resource} should keep empty AppContext value"
        );
        assert_eq!(
            resolve_context_presence_or_global::<&str>(None, || Some(global_value)),
            Some(global_value),
            "{resource} should fall back to global value when AppContext is absent"
        );
    }

    #[test]
    fn optional_resolvers_keep_context_first_global_fallback_contract() {
        assert_optional_resolver("KMS runtime", "kms-runtime-context", "kms-runtime-global");
        assert_optional_resolver("bucket metadata", "bucket-metadata-context", "bucket-metadata-global");
        assert_context_only_resolver("object store", "object-store-context");
        assert_optional_resolver("endpoints", "endpoints-context", "endpoints-global");
        assert_context_presence_resolver("server config", "server-config-context", "server-config-global");
    }

    #[test]
    fn required_resolvers_keep_context_first_global_fallback_contract() {
        assert_required_resolver("tier config", "tier-config-context", "tier-config-global");
        assert_required_resolver("buffer config", "buffer-config-context", "buffer-config-global");
    }
}
