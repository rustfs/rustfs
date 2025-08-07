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

use super::Target;
use super::config::Config;
use super::http_target::HttpTarget;
use std::sync::Arc;

pub fn create_targets_from_config(config: &Config) -> Vec<Arc<dyn Target>> {
    let mut targets: Vec<Arc<dyn Target>> = Vec::new();

    // Logger Webhook 目标
    for (name, cfg) in &config.logger_webhook {
        if cfg.enabled {
            println!("Initializing logger webhook target: {}", name);
            let target = HttpTarget::new(format!("logger-webhook-{}", name), cfg.clone());
            targets.push(Arc::new(target));
        }
    }

    // Audit Webhook 目标
    for (name, cfg) in &config.audit_webhook {
        if cfg.enabled {
            println!("Initializing audit webhook target: {}", name);
            let target = HttpTarget::new(format!("audit-webhook-{}", name), cfg.clone());
            targets.push(Arc::new(target));
        }
    }

    // Audit Kafka 目标 (存根)
    for (name, cfg) in &config.audit_kafka {
        if cfg.enabled {
            println!("Initializing audit kafka target: {} (STUBBED)", name);
            // let target = KafkaTarget::new(name.clone(), cfg.clone());
            // targets.push(Arc::new(target));
        }
    }

    if config.console.enabled {
        println!("Console logging is enabled.");
        // 可以在这里添加一个 ConsoleTarget 实现
    }

    targets
}
