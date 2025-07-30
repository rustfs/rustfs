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
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, warn};

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
    /// Create all notification targets from system configuration and environment variables.
    /// This method processes the creation of each target concurrently as follows:
    /// 1. Iterate through all registered target types (e.g. webhooks, mqtt).
    /// 2. For each type, resolve its configuration in the configuration file and environment variables.
    /// 3. Identify all target instance IDs that need to be created.
    /// 4. Combine the default configuration, file configuration, and environment variable configuration for each instance.
    /// 5. If the instance is enabled, create an asynchronous task for it to instantiate.
    /// 6. Concurrency executes all creation tasks and collects results.
    pub async fn create_targets_from_config(&self, config: &Config) -> Result<Vec<Box<dyn Target + Send + Sync>>, TargetError> {
        // 预先收集所有环境变量，避免在循环中重复获取
        let all_env: Vec<(String, String)> = std::env::vars().collect();
        // 用于并发执行目标创建的异步任务集合
        let mut tasks = FuturesUnordered::new();
        let mut final_config = config.clone(); // 克隆一份配置，用于聚合最终结果
        // 1. 遍历所有注册的工厂，按目标类型处理
        for (target_type, factory) in &self.factories {
            tracing::Span::current().record("target_type", &target_type.as_str());
            info!("开始处理目标类型...");

            // 2. 准备配置来源
            // 2.1. 获取文件中的配置段，例如 `notify.webhook`
            let section_name = format!("{}{}", NOTIFY_ROUTE_PREFIX, target_type);
            let file_configs = config.0.get(&section_name).cloned().unwrap_or_default();
            // 2.2. 获取该类型的默认配置
            let default_cfg = file_configs.get(DEFAULT_DELIMITER).cloned().unwrap_or_default();
            debug!(?default_cfg, "获取到默认配置");

            // *** 优化点 1: 获取当前目标类型的所有合法字段 ***
            let valid_fields = factory.get_valid_fields();
            debug!(?valid_fields, "获取到合法的配置字段");

            // 3. 从环境变量中解析实例 ID 和配置覆盖项
            let mut instance_ids_from_env = HashSet::new();
            // 3.1. 实例发现：基于 `..._ENABLE_INSTANCEID` 格式
            let enable_prefix = format!("{}{}{}_{}_", ENV_PREFIX, NOTIFY_ROUTE_PREFIX, target_type, ENABLE_KEY).to_uppercase();
            for (key, value) in &all_env {
                if value.eq_ignore_ascii_case(ENABLE_ON)
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("1")
                    || value.eq_ignore_ascii_case("yes")
                {
                    if let Some(id) = key.strip_prefix(&enable_prefix) {
                        if !id.is_empty() {
                            instance_ids_from_env.insert(id.to_lowercase());
                        }
                    }
                }
            }

            // 3.2. 解析所有相关的环境变量配置 例如 `RUSTFS_NOTIFY_WEBHOOK_`
            // 3.2.1. 构建环境变量前缀，例如 `RUSTFS_NOTIFY_WEBHOOK_`
            let env_prefix = format!("{}{}{}_", ENV_PREFIX, NOTIFY_ROUTE_PREFIX, target_type).to_uppercase();
            // 3.2.2. `env_overrides` 用于存储从环境变量解析出的配置，格式为：{实例 ID -> {字段 -> 值}}
            let mut env_overrides: HashMap<String, HashMap<String, String>> = HashMap::new();
            for (key, value) in &all_env {
                if let Some(rest) = key.strip_prefix(&env_prefix) {
                    // 使用 rsplitn 从右侧分割，以正确提取末尾的 INSTANCE_ID
                    // 格式：<FIELD_NAME>_<INSTANCE_ID> 或 <FIELD_NAME>
                    let mut parts = rest.rsplitn(2, '_');

                    // 从右边开始的第一部分是 INSTANCE_ID
                    let instance_id_part = parts.next().unwrap_or(DEFAULT_DELIMITER);
                    // 剩余部分是 FIELD_NAME
                    let field_name_part = parts.next();

                    let (field_name, instance_id) = match field_name_part {
                        // Case 1: 格式为 <FIELD_NAME>_<INSTANCE_ID>
                        // e.g., rest = "ENDPOINT_PRIMARY" -> field_name="ENDPOINT", instance_id="PRIMARY"
                        Some(field) => (field.to_lowercase(), instance_id_part.to_lowercase()),
                        // Case 2: 格式为 <FIELD_NAME> (无 INSTANCE_ID)
                        // e.g., rest = "ENABLE" -> field_name="ENABLE", instance_id="" (通用配置)
                        None => (instance_id_part.to_lowercase(), DEFAULT_DELIMITER.to_string()),
                    };

                    // *** 优化点 2: 验证解析出的 field_name 是否合法 ***
                    if !field_name.is_empty() && valid_fields.contains(&field_name) {
                        debug!(
                            instance_id = %if instance_id.is_empty() { DEFAULT_DELIMITER } else { &instance_id },
                            %field_name,
                            %value,
                            "解析到环境变量"
                        );
                        env_overrides
                            .entry(instance_id)
                            .or_default()
                            .insert(field_name, value.clone());
                    } else {
                        // 忽略不合法的字段名
                        warn!(
                            field_name = %field_name,
                            "忽略非法的环境变量字段，未在目标类型 {} 的合法字段列表中找到",
                            target_type
                        );
                    }
                }
            }
            debug!(?env_overrides, "完成环境变量解析");

            // 4. 确定所有需要处理的实例 ID
            let mut all_instance_ids: HashSet<String> =
                file_configs.keys().filter(|k| *k != DEFAULT_DELIMITER).cloned().collect();
            all_instance_ids.extend(instance_ids_from_env);
            debug!(?all_instance_ids, "确定所有实例 ID");

            // 5. 为每个实例合并配置并创建任务
            for id in all_instance_ids {
                // 5.1. 合并配置，优先级：环境变量 > 文件实例配置 > 文件默认配置
                let mut merged_config = default_cfg.clone();
                // 应用文件中的实例特定配置
                if let Some(file_instance_cfg) = file_configs.get(&id) {
                    merged_config.extend(file_instance_cfg.clone());
                }
                // 应用实例特定的环境变量配置
                if let Some(env_instance_cfg) = env_overrides.get(&id) {
                    // 修复类型不匹配：将 HashMap<String, String> 转为 KVS
                    let mut kvs_from_env = KVS::new();
                    for (k, v) in env_instance_cfg {
                        kvs_from_env.insert(k.clone(), v.clone());
                    }
                    merged_config.extend(kvs_from_env);
                }
                debug!(instance_id = %id, ?merged_config, "完成配置合并");

                // 5.2. 检查实例是否启用
                let enabled = merged_config
                    .lookup(ENABLE_KEY)
                    .map(|v| {
                        v.eq_ignore_ascii_case(ENABLE_ON)
                            || v.eq_ignore_ascii_case("true")
                            || v.eq_ignore_ascii_case("1")
                            || v.eq_ignore_ascii_case("yes")
                    })
                    .unwrap_or(false);

                if enabled {
                    info!(instance_id = %id, "目标已启用，准备创建任务");
                    // 5.3. 为启用的实例创建异步任务
                    let ttype = target_type.clone();
                    let tid = id.clone();
                    tasks.push(async move {
                        let result = factory.create_target(tid.clone(), &merged_config).await;
                        (ttype, tid, result, merged_config)
                    });
                } else {
                    info!(instance_id = %id, "跳过已禁用的目标，将从最终配置中移除");
                    // 从最终配置中移除禁用的目标
                    final_config.0.entry(section_name.clone()).or_default().remove(&id);
                }
            }
        }

        // 6. 并发执行所有创建任务并收集结果
        let mut successful_targets = Vec::new();
        let mut successful_configs = Vec::new();
        while let Some((tpe, id, result, final_config)) = tasks.next().await {
            match result {
                Ok(target) => {
                    info!(target_type = %tpe, instance_id = %id, "成功创建目标");
                    successful_targets.push(target);
                    successful_configs.push((tpe, id, final_config));
                }
                Err(e) => {
                    error!(target_type = %tpe, instance_id = %id, error = %e, "创建目标失败");
                }
            }
        }

        // 7. 聚合新配置并写回系统配置
        if !successful_configs.is_empty() {
            info!("准备将 {} 个成功创建的目标配置更新到系统配置中...", successful_configs.len());
            let mut new_config = config.clone();
            for (target_type, id, kvs) in successful_configs {
                let section_name = format!("{}{}", NOTIFY_ROUTE_PREFIX, target_type).to_lowercase();
                new_config.0.entry(section_name).or_default().insert(id, kvs);
            }

            let Some(store) = rustfs_ecstore::global::new_object_layer_fn() else {
                return Err(TargetError::ServerNotInitialized);
            };

            match rustfs_ecstore::config::com::save_server_config(store, &new_config).await {
                Ok(_) => {
                    info!("成功将新配置保存到系统。")
                }
                Err(e) => {
                    error!("保存新配置失败：{}", e);
                    return Err(TargetError::SaveConfig(e.to_string()));
                }
            }
        }

        info!(count = successful_targets.len(), "所有目标处理完成");
        Ok(successful_targets)
    }
}
