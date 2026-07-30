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

//! OpenTelemetry [`Resource`] construction for RustFS.
//!
//! A `Resource` describes the entity producing telemetry data.  The resource
//! built here includes the service name, service version, deployment
//! environment, the stable RustFS node identity, and the local machine IP
//! address so that data can be correlated across services in a distributed
//! system.

use crate::config::OtelConfig;
use crate::node_identity::{RUSTFS_NODE_ATTRIBUTE, local_node_identity};
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::{
    SCHEMA_URL,
    attribute::{
        DEPLOYMENT_ENVIRONMENT_NAME, NETWORK_LOCAL_ADDRESS, SERVICE_INSTANCE_ID as OTEL_SERVICE_INSTANCE_ID,
        SERVICE_VERSION as OTEL_SERVICE_VERSION,
    },
};
use rustfs_config::{APP_NAME, ENVIRONMENT, SERVICE_VERSION};
use rustfs_utils::get_local_ip_with_default;
use std::borrow::Cow;

/// Build an OpenTelemetry [`Resource`] populated from the provided config.
///
/// The resource carries the following attributes:
/// - `service.name` — from `config.service_name`, defaulting to [`APP_NAME`].
/// - `service.version` — from `config.service_version`, defaulting to
///   [`SERVICE_VERSION`].
/// - `deployment.environment` — from `config.environment`, defaulting to
///   [`ENVIRONMENT`].
/// - `rustfs.node` / `service.instance.id` — the stable RustFS local node name
///   when available, falling back to the local IP during early startup.
/// - `network.local.address` — the primary local IP of the current host,
///   useful as an operational fallback when the stable node name is not yet
///   initialized.
///
/// All attributes are attached to the resource using the semantic conventions
/// schema URL to ensure compatibility with standard OTLP backends.
pub(super) fn build_resource(config: &OtelConfig) -> Resource {
    let local_ip = get_local_ip_with_default();
    let node_identity = local_node_identity(&local_ip);

    Resource::builder()
        .with_service_name(Cow::Borrowed(config.service_name.as_deref().unwrap_or(APP_NAME)).to_string())
        .with_schema_url(
            [
                KeyValue::new(
                    OTEL_SERVICE_VERSION,
                    Cow::Borrowed(config.service_version.as_deref().unwrap_or(SERVICE_VERSION)).to_string(),
                ),
                KeyValue::new(
                    DEPLOYMENT_ENVIRONMENT_NAME,
                    Cow::Borrowed(config.environment.as_deref().unwrap_or(ENVIRONMENT)).to_string(),
                ),
                KeyValue::new(RUSTFS_NODE_ATTRIBUTE, node_identity.clone()),
                KeyValue::new(OTEL_SERVICE_INSTANCE_ID, node_identity),
                KeyValue::new(NETWORK_LOCAL_ADDRESS, local_ip),
            ],
            SCHEMA_URL,
        )
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::Key;

    #[tokio::test]
    async fn build_resource_uses_stable_local_node_identity() {
        let _guard = crate::node_identity::local_node_identity_test_guard().await;
        let previous = rustfs_common::get_global_local_node_name().await;
        rustfs_common::set_global_local_node_name("node1:9000").await;

        let resource = build_resource(&OtelConfig::default());

        assert_eq!(
            resource
                .get(&Key::from_static_str(RUSTFS_NODE_ATTRIBUTE))
                .map(|value| value.to_string()),
            Some("node1:9000".to_string())
        );
        assert_eq!(
            resource
                .get(&Key::from_static_str(OTEL_SERVICE_INSTANCE_ID))
                .map(|value| value.to_string()),
            Some("node1:9000".to_string())
        );

        rustfs_common::set_global_local_node_name(&previous).await;
    }
}
