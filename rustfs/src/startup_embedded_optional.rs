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

use crate::{
    config::Config,
    init::{init_buffer_profile_system, init_kms_system},
    startup_audit::init_event_notifier_and_audit,
};
use tracing::warn;

const LOG_COMPONENT_EMBEDDED: &str = "embedded";
const LOG_SUBSYSTEM_EMBEDDED: &str = "embedded";
const EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED: &str = "embedded_optional_service_skipped";

pub(crate) async fn init_embedded_optional_service_runtime(config: &Config) {
    if let Err(err) = init_kms_system(config).await {
        log_embedded_optional_service_skipped("kms", err);
    }

    init_buffer_profile_system(config);

    if let Err(err) = init_event_notifier_and_audit().await {
        log_embedded_optional_service_skipped("audit", err);
    }
}

fn log_embedded_optional_service_skipped(service: &str, err: impl std::fmt::Display) {
    warn!(
        component = LOG_COMPONENT_EMBEDDED,
        subsystem = LOG_SUBSYSTEM_EMBEDDED,
        event = EVENT_EMBEDDED_OPTIONAL_SERVICE_SKIPPED,
        service,
        error = %err,
        "Embedded optional service initialization skipped"
    );
}
