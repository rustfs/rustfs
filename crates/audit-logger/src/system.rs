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

use crate::config::Config;
use crate::entity::AuditEntry;
use crate::factory::{AuditLoggerError, AuditTarget};
use crate::registry::TargetRegistry;

pub struct AuditLoggerSystem {
    registry: TargetRegistry,
    targets: Vec<Box<dyn AuditTarget + Send + Sync>>,
}

impl AuditLoggerSystem {
    pub fn new(config: Config, registry: TargetRegistry) -> Self {
        Self {
            registry,
            targets: Vec::new(),
        }
    }

    pub async fn init(&mut self, config: &Config) -> Result<(), AuditLoggerError> {
        self.targets = self.registry.create_targets_from_config(config).await?;
        Ok(())
    }

    pub async fn reload_config(&mut self, config: &Config) -> Result<(), AuditLoggerError> {
        self.targets = self.registry.create_targets_from_config(config).await?;
        Ok(())
    }

    pub async fn log(&self, entry: AuditEntry) {
        for target in &self.targets {
            let _ = target.save(entry.clone()).await;
        }
    }
}
