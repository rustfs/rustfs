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

//! Cache implementation for proxy validation

use metrics::{counter, gauge, histogram};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 缓存条目
#[derive(Debug, Clone)]
struct CacheEntry {
    /// 是否可信
    is_trusted: bool,
    /// 缓存时间
    cached_at: Instant,
    /// 过期时间
    expires_at: Instant,
}

/// IP 验证缓存
#[derive(Debug, Clone)]
pub struct IpValidationCache {
    /// 缓存存储
    cache: Arc<RwLock<HashMap<IpAddr, CacheEntry>>>,
    /// 最大容量
    capacity: usize,
    /// 默认 TTL
    default_ttl: Duration,
    /// 是否启用
    enabled: bool,
}

impl IpValidationCache {
    /// 创建新的缓存
    pub fn new(capacity: usize, default_ttl: Duration, enabled: bool) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(capacity))),
            capacity,
            default_ttl,
            enabled,
        }
    }

    /// 检查 IP 是否可信（带缓存）
    pub fn is_trusted(&self, ip: &IpAddr, validator: impl FnOnce(&IpAddr) -> bool) -> bool {
        // 如果缓存未启用，直接验证
        if !self.enabled {
            return validator(ip);
        }

        let now = Instant::now();

        // 检查缓存
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(ip) {
                if now < entry.expires_at {
                    // 缓存命中
                    counter!("proxy.cache.hits").increment(1);
                    return entry.is_trusted;
                }
            }
        }

        // 缓存未命中
        counter!("proxy.cache.misses").increment(1);

        // 验证 IP
        let is_trusted = validator(ip);

        // 更新缓存
        self.update_cache(*ip, is_trusted, now);

        is_trusted
    }

    /// 更新缓存
    fn update_cache(&self, ip: IpAddr, is_trusted: bool, now: Instant) {
        let mut cache = self.cache.write();

        // 检查是否需要清理（如果达到容量限制）
        if cache.len() >= self.capacity {
            self.cleanup_expired(&mut cache, now);

            // 如果仍然满，删除最旧的条目
            if cache.len() >= self.capacity {
                self.evict_oldest(&mut cache);
            }
        }

        // 添加新条目
        let entry = CacheEntry {
            is_trusted,
            cached_at: now,
            expires_at: now + self.default_ttl,
        };

        cache.insert(ip, entry);

        // 更新指标
        gauge!("proxy.cache.size").set(cache.len() as f64);
    }

    /// 清理过期条目
    fn cleanup_expired(&self, cache: &mut HashMap<IpAddr, CacheEntry>, now: Instant) {
        let expired_keys: Vec<_> = cache
            .iter()
            .filter(|(_, entry)| now >= entry.expires_at)
            .map(|(ip, _)| *ip)
            .collect();

        for key in expired_keys.clone() {
            cache.remove(&key);
        }

        if !expired_keys.is_empty() {
            counter!("proxy.cache.evictions").increment(expired_keys.len() as u64);
        }
    }

    /// 淘汰最旧的条目
    fn evict_oldest(&self, cache: &mut HashMap<IpAddr, CacheEntry>) {
        if let Some(oldest_key) = cache.iter().min_by_key(|(_, entry)| entry.cached_at).map(|(ip, _)| *ip) {
            cache.remove(&oldest_key);
            counter!("proxy.cache.evictions").increment(1);
        }
    }

    /// 清空缓存
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
        gauge!("proxy.cache.size").set(0.00);
    }

    /// 获取缓存统计信息
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.read();

        let mut oldest = Instant::now();
        let mut newest = Instant::now();
        let mut expired_count = 0;

        let now = Instant::now();
        for entry in cache.values() {
            if entry.cached_at < oldest {
                oldest = entry.cached_at;
            }
            if entry.cached_at > newest {
                newest = entry.cached_at;
            }
            if now >= entry.expires_at {
                expired_count += 1;
            }
        }

        CacheStats {
            size: cache.len(),
            capacity: self.capacity,
            expired_count,
            oldest_age: now.duration_since(oldest),
            newest_age: now.duration_since(newest),
        }
    }

    /// 定期清理任务
    pub async fn cleanup_task(&self, interval: Duration) {
        let mut interval_timer = tokio::time::interval(interval);

        loop {
            interval_timer.tick().await;
            self.cleanup();
        }
    }

    /// 执行清理
    fn cleanup(&self) {
        let now = Instant::now();
        let mut cache = self.cache.write();
        self.cleanup_expired(&mut cache, now);

        // 记录清理后的指标
        gauge!("proxy.cache.size").set(cache.len() as f64);
    }
}

/// 缓存统计信息
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// 当前缓存大小
    pub size: usize,
    /// 缓存容量
    pub capacity: usize,
    /// 过期条目数量
    pub expired_count: usize,
    /// 最旧条目的年龄
    pub oldest_age: Duration,
    /// 最新条目的年龄
    pub newest_age: Duration,
}

impl CacheStats {
    /// 获取缓存使用率
    pub fn usage_percentage(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            (self.size as f64 / self.capacity as f64) * 100.0
        }
    }

    /// 获取命中率（需要外部跟踪命中/未命中）
    pub fn hit_rate(&self, hits: u64, misses: u64) -> f64 {
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }
}
