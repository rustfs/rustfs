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

// src/global.rs
use once_cell::sync::OnceLock;
use std::sync::{Arc, OnceLock};
use crate::system::AuditLoggerSystem;

static AUDIT_LOGGER_SYSTEM: OnceLock<Arc<AuditLoggerSystem>> = OnceLock::new();

pub async fn initialize(config: Config) -> Result<(), AuditLoggerError> {
    let system = AuditLoggerSystem::new(config);
    system.init().await?;
    AUDIT_LOGGER_SYSTEM
        .set(Arc::new(system))
        .map_err(|_| AuditLoggerError::AlreadyInitialized)
}

pub fn audit_logger() -> Option<Arc<AuditLoggerSystem>> {
    AUDIT_LOGGER_SYSTEM.get().cloned()
}
