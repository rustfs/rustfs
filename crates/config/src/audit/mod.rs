//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Audit configuration module
//! This module defines the configuration for audit systems, including
//! webhook and MQTT audit-related settings.

pub(crate) mod mqtt;
pub(crate) mod webhook;

pub use mqtt::*;
pub use webhook::*;

use crate::DEFAULT_DELIMITER;
// --- Audit subsystem identifiers ---
pub const AUDIT_PREFIX: &str = "audit";

pub const AUDIT_ROUTE_PREFIX: &str = const_str::concat!(AUDIT_PREFIX, DEFAULT_DELIMITER);

pub const AUDIT_WEBHOOK_SUB_SYS: &str = "audit_webhook";
pub const AUDIT_MQTT_SUB_SYS: &str = "mqtt_webhook";

pub const AUDIT_STORE_EXTENSION: &str = ".audit";
#[allow(dead_code)]
pub const AUDIT_SUB_SYSTEMS: &[&str] = &[AUDIT_MQTT_SUB_SYS, AUDIT_WEBHOOK_SUB_SYS];
