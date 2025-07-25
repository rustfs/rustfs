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

use crate::target::ChannelTargetType;
use crate::{
    error::TargetError,
    factory::{MQTTTargetFactory, TargetFactory, WebhookTargetFactory},
    target::Target,
};
use futures::stream::{FuturesUnordered, StreamExt};
use rustfs_config::notify::NOTIFY_ROUTE_PREFIX;
use rustfs_config::{DEFAULT_DELIMITER, ENV_PREFIX};
use rustfs_ecstore::config::{Config, ENABLE_KEY, ENABLE_ON, KVS};
use std::collections::HashMap;
use tracing::{error, info};

/// Registry for managing target factories
pub struct TargetRegistry {
    factories: HashMap<String, Box<dyn TargetFactory>>,
}

impl Default for TargetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetRegistry {
    /// Creates a new TargetRegistry with built-in factories
    pub fn new() -> Self {
        let mut registry = TargetRegistry {
            factories: HashMap::new(),
        };

        // Register built-in factories
        registry.register(ChannelTargetType::Webhook.as_str(), Box::new(WebhookTargetFactory));
        registry.register(ChannelTargetType::Mqtt.as_str(), Box::new(MQTTTargetFactory));

        registry
    }

    /// Registers a new factory for a target type
    pub fn register(&mut self, target_type: &str, factory: Box<dyn TargetFactory>) {
        self.factories.insert(target_type.to_string(), factory);
    }

    /// Creates a target from configuration
    pub async fn create_target(
        &self,
        target_type: &str,
        id: String,
        config: &KVS,
    ) -> Result<Box<dyn Target + Send + Sync>, TargetError> {
        let factory = self
            .factories
            .get(target_type)
            .ok_or_else(|| TargetError::Configuration(format!("Unknown target type: {target_type}")))?;

        // Validate configuration before creating target
        factory.validate_config(&id, config)?;

        // Create target
        factory.create_target(id, config).await
    }

    /// Creates all targets from a configuration
    pub async fn create_targets_from_config(&self, config: &Config) -> Result<Vec<Box<dyn Target + Send + Sync>>, TargetError> {
        let mut new_map: HashMap<String, HashMap<String, KVS>> = HashMap::new();
        // 1. 预先收集所有环境变量
        let all_env: Vec<(String, String)> = std::env::vars().collect();
        // 2. 创建异步任务集合
        let mut tasks = FuturesUnordered::new();

        for target_type in self.factories.keys() {
            let section = format!("{}{}", NOTIFY_ROUTE_PREFIX, target_type).to_lowercase();
            let mut sec_cfg = config.0.get(&section).cloned().unwrap_or_default();
            // 确保默认段存在并克隆
            let default_cfg = sec_cfg.entry(DEFAULT_DELIMITER.to_string()).or_insert_with(KVS::new).clone();
            info!(
                "Processing target type: {} Config: {:?} ,default config:{:?}",
                target_type, sec_cfg, default_cfg
            );
            // 3. 筛选当前类型相关的环境变量覆盖
            let env_pref = format!("{}{}{}{}", ENV_PREFIX, NOTIFY_ROUTE_PREFIX, target_type, DEFAULT_DELIMITER).to_uppercase();
            info!("Collected environment variables for {}: env_pref: {} ", target_type, env_pref.clone(),);
            let mut overrides: HashMap<Option<String>, HashMap<String, String>> = HashMap::new();
            for (k, v) in &all_env {
                // 检查环境变量是否以目标类型前缀开头
                if let Some(rest) = k.strip_prefix(&env_pref) {
                    info!("Processing environment variable: {}", rest);
                    let parts: Vec<&str> = rest.trim_start_matches(DEFAULT_DELIMITER).split(DEFAULT_DELIMITER).collect();
                    let (field, id) = if parts.len() > 1 {
                        info!(
                            "Processing environment variable field: {},id: {}",
                            parts[0].to_lowercase(),
                            parts[1].to_lowercase(),
                        );
                        (parts[0].to_lowercase(), Some(parts[1].to_string()))
                    } else {
                        (parts[0].to_lowercase(), None)
                    };
                    overrides.entry(id.clone()).or_default().insert(field, v.clone());
                }
            }
            info!("Collected environment overrides for {}: {:?}", target_type, overrides);
            // 4. 合并所有实例 ID
            let mut ids: Vec<String> = sec_cfg.keys().filter(|k| *k != DEFAULT_DELIMITER).cloned().collect();
            for id in overrides.keys().filter_map(|x| x.clone()) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }

            // 5. 为每个实例合并配置并创建异步任务
            for id in ids {
                let mut merged = default_cfg.clone();
                if let Some(cfg) = sec_cfg.get(&id) {
                    merged.extend(cfg.clone());
                }
                if let Some(gens) = overrides.get(&None) {
                    for (f, val) in gens {
                        merged.insert(f.clone(), val.clone());
                    }
                }
                if let Some(spec) = overrides.get(&Some(id.clone())) {
                    for (f, val) in spec {
                        merged.insert(f.clone(), val.clone());
                    }
                }
                info!("Merged config for {}/{}: {:?}", target_type, id, merged);
                let enabled = merged
                    .lookup(ENABLE_KEY)
                    .map(|v| v.eq_ignore_ascii_case(ENABLE_ON))
                    .unwrap_or(false);
                info!("Enabled: {}/{},{}", target_type, id, enabled);
                // 将 merged 移动进闭包，并在 new_map 中 clone
                let ttype = target_type.clone();
                let tid = id.clone();
                let mv = merged;
                new_map.entry(section.clone()).or_default().insert(tid.clone(), mv.clone());

                if enabled {
                    info!("Creating target {}/{}", target_type, id);
                    tasks.push(async move {
                        let res = self.create_target(&ttype, tid.clone(), &mv).await;
                        (ttype, tid, res)
                    });
                } else {
                    info!("Skipping disabled target {}/{}", target_type, id);
                }
            }
        }

        // 6. 并发收集创建结果
        let mut results = Vec::new();
        while let Some((tpe, id, res)) = tasks.next().await {
            match res {
                Ok(t) => {
                    info!("Successfully created target {}/{}", tpe, id);
                    results.push(t)
                }
                Err(e) => error!("Failed to create a target {}/{}: {}", tpe, id, e),
            }
        }
        info!("Finished creating targets. Total: {}, Successful: {}", results.len(), results.len());
        // 7. （可选）写回 new_map 到系统配置
        Ok(results)
    }
}
