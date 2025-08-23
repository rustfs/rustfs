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

use std::collections::HashMap;
use crate::factory::{AuditTarget, AuditTargetFactory};

pub struct TargetRegistry {
    factories: HashMap<String, Box<dyn AuditTargetFactory>>,
}

impl TargetRegistry {
    pub fn register(&mut self, target_type: &str, factory: Box<dyn AuditTargetFactory>) { ... }
    pub async fn create_targets_from_config(&self, config: &Config) -> Result<Vec<Box<dyn AuditTarget + Send + Sync>>, AuditLoggerError> { ... }
}